package wecom

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// These tests drive the inbound path with callbacks built to WeCom's real wire
// format — signed, AES-encrypted, XML — rather than with a simplified stand-in.
// A real corp account is not needed to prove the parts that actually go wrong:
// signature verification, decryption, receive-id checking and idempotency key
// extraction.

const (
	testCorpID = "wwtestcorp001"
	testToken  = "callback-token"
)

func testBinding() *types.ChannelBinding {
	return &types.ChannelBinding{
		ChannelBindingID: "cb-wecom",
		TenantID:         "tenant-demo",
		AgentAppID:       "assistant",
		Channel:          Name,
		SecretRef:        "secret://prod/tenant-demo/channel/wecom",
		Status:           types.StatusActive,
	}
}

// testSecrets returns the credential blob the channel expects behind a
// secret:// reference.
func testSecrets(ref string) (string, error) {
	return fmt.Sprintf(`{
		"corp_id": %q,
		"secret": "app-secret",
		"agent_id": 1000002,
		"token": %q,
		"encoding_aes_key": %q
	}`, testCorpID, testToken, testAESKey), nil
}

func testChannel() *Channel { return New(testSecrets, nil, nil) }

// buildCallback produces a signed, encrypted callback exactly as WeCom would.
func buildCallback(t *testing.T, inner string) *http.Request {
	t.Helper()

	crypt, err := NewCrypto(testToken, testAESKey, testCorpID)
	if err != nil {
		t.Fatalf("NewCrypto: %v", err)
	}
	encrypted, err := crypt.Encrypt([]byte(inner))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	body, err := xml.Marshal(envelope{
		ToUserName: testCorpID,
		AgentID:    "1000002",
		Encrypt:    encrypted,
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	params := FormatSignatureParams(crypt, encrypted)
	req := httptest.NewRequest(http.MethodPost,
		"/webhook/wecom/demo?"+params.Encode(), strings.NewReader(string(body)))
	return req
}

const textMessageXML = `<xml>
  <ToUserName><![CDATA[wwtestcorp001]]></ToUserName>
  <FromUserName><![CDATA[zhangsan]]></FromUserName>
  <CreateTime>1700000000</CreateTime>
  <MsgType><![CDATA[text]]></MsgType>
  <Content><![CDATA[帮我查一下今天的订单]]></Content>
  <MsgId>1234567890123456</MsgId>
  <AgentID>1000002</AgentID>
</xml>`

func TestVerifyAndDecodeTextMessage(t *testing.T) {
	c := testChannel()
	binding := testBinding()
	req := buildCallback(t, textMessageXML)

	if err := c.Verify(req, binding); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	msgs, err := c.Decode(context.Background(), req, binding)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}

	m := msgs[0]
	if m.Text != "帮我查一下今天的订单" {
		t.Errorf("Text = %q", m.Text)
	}
	if m.ExternalUserID != "zhangsan" {
		t.Errorf("ExternalUserID = %q", m.ExternalUserID)
	}
	// The tenant comes from the binding, never from the payload.
	if m.TenantID != "tenant-demo" || m.AgentAppID != "assistant" {
		t.Errorf("tenant identity not taken from binding: %+v", m)
	}
	// MsgId is the idempotency key, and it lives inside the ciphertext —
	// which is why deduplication cannot happen before decryption.
	if m.ExternalEventID != "1234567890123456" {
		t.Errorf("ExternalEventID = %q, want the platform MsgId", m.ExternalEventID)
	}
	if m.Scope != types.ScopeSingle {
		t.Errorf("Scope = %q", m.Scope)
	}
}

func TestVerifyRejectsTamperedBody(t *testing.T) {
	// Verification has to run before decoding, and it has to fail when the
	// signed payload no longer matches the body.
	c := testChannel()
	binding := testBinding()

	req := buildCallback(t, textMessageXML)
	other := buildCallback(t, strings.Replace(textMessageXML, "zhangsan", "attacker", 1))

	// Keep the first request's signature parameters, swap in the second's body.
	tampered := httptest.NewRequest(http.MethodPost, req.URL.String(), other.Body)
	if err := c.Verify(tampered, binding); err == nil {
		t.Fatal("accepted a body that does not match the signature")
	}
}

func TestVerifyRejectsMissingParameters(t *testing.T) {
	c := testChannel()
	req := httptest.NewRequest(http.MethodPost, "/webhook/wecom/demo", strings.NewReader("<xml/>"))
	if err := c.Verify(req, testBinding()); err == nil {
		t.Fatal("accepted a callback with no signature parameters")
	}
}

func TestURLVerificationHandshake(t *testing.T) {
	// WeCom proves the endpoint holds the key by sending an encrypted
	// echostr that must come back decrypted.
	crypt, err := NewCrypto(testToken, testAESKey, testCorpID)
	if err != nil {
		t.Fatalf("NewCrypto: %v", err)
	}
	const plaintext = "1234567890987654"
	echo, err := crypt.Encrypt([]byte(plaintext))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	params := FormatSignatureParams(crypt, echo)
	params.Set("echostr", echo)
	params.Set("msg_signature", crypt.Signature(params.Get("timestamp"), params.Get("nonce"), echo))

	req := httptest.NewRequest(http.MethodGet, "/webhook/wecom/demo?"+params.Encode(), nil)
	c := testChannel()
	binding := testBinding()

	if err := c.Verify(req, binding); err != nil {
		t.Fatalf("Verify handshake: %v", err)
	}

	// The handshake carries no user message, so Gateway must have nothing to
	// queue and must still ACK.
	msgs, err := c.Decode(context.Background(), req, binding)
	if err != nil {
		t.Fatalf("Decode handshake: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("handshake should decode to no messages, got %d", len(msgs))
	}

	w := httptest.NewRecorder()
	if err := c.Ack(w, req, binding, types.AckInfo{}); err != nil {
		t.Fatalf("Ack handshake: %v", err)
	}
	if got := w.Body.String(); got != plaintext {
		t.Errorf("handshake echoed %q, want the decrypted %q", got, plaintext)
	}
}

func TestAckForMessageIsEmpty(t *testing.T) {
	// An agent run does not fit in the passive-response window, so the
	// callback is acknowledged empty and the answer is pushed later.
	c := testChannel()
	req := buildCallback(t, textMessageXML)
	w := httptest.NewRecorder()

	if err := c.Ack(w, req, testBinding(), types.AckInfo{RequestID: "req-1"}); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if body := w.Body.String(); body != "" {
		t.Errorf("message ACK should be empty, got %q", body)
	}
}

func TestDecodeIgnoresNonTextWithoutError(t *testing.T) {
	// Unsupported types are acknowledged and dropped rather than erroring, so
	// the platform stops retrying something that will never be processed.
	const imageXML = `<xml>
	  <ToUserName><![CDATA[wwtestcorp001]]></ToUserName>
	  <FromUserName><![CDATA[zhangsan]]></FromUserName>
	  <CreateTime>1700000000</CreateTime>
	  <MsgType><![CDATA[image]]></MsgType>
	  <PicUrl><![CDATA[https://example.com/a.jpg]]></PicUrl>
	  <MsgId>999</MsgId>
	</xml>`

	c := testChannel()
	req := buildCallback(t, imageXML)
	msgs, err := c.Decode(context.Background(), req, testBinding())
	if err != nil {
		t.Fatalf("Decode should not error on an unsupported type: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("got %d messages, want 0", len(msgs))
	}
}

func TestDecodeFallsBackWhenMsgIDAbsent(t *testing.T) {
	// Event callbacks carry no MsgId. Without a fallback the idempotency key
	// would be empty and every event would collide with every other.
	const noIDXML = `<xml>
	  <ToUserName><![CDATA[wwtestcorp001]]></ToUserName>
	  <FromUserName><![CDATA[lisi]]></FromUserName>
	  <CreateTime>1700000123</CreateTime>
	  <MsgType><![CDATA[text]]></MsgType>
	  <Content><![CDATA[没有 MsgId 的消息]]></Content>
	</xml>`

	c := testChannel()
	msgs, err := c.Decode(context.Background(), buildCallback(t, noIDXML), testBinding())
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if msgs[0].ExternalEventID != "lisi-1700000123" {
		t.Errorf("fallback event id = %q", msgs[0].ExternalEventID)
	}
}

func TestCapabilitiesDifferFromMock(t *testing.T) {
	// The capability descriptor is what lets the main flow stay free of
	// per-channel branching, so it must actually describe this channel.
	caps := testChannel().Capabilities()
	if !caps.SupportsPush {
		t.Error("WeCom supports push; the ACK-then-reply design depends on it")
	}
	if caps.SupportsEdit {
		t.Error("WeCom cannot edit a sent message")
	}
	if caps.MaxTextLength == 0 {
		t.Error("a text length limit is required so long replies get split")
	}
	if caps.InboundMode != types.InboundModePayload {
		t.Errorf("InboundMode = %q, want payload", caps.InboundMode)
	}
}

func TestCredentialsValidation(t *testing.T) {
	cases := map[string]Credentials{
		"no corp id":  {Secret: "s", AgentID: 1},
		"no secret":   {CorpID: "c", AgentID: 1},
		"no agent id": {CorpID: "c", Secret: "s"},
	}
	for name, creds := range cases {
		t.Run(name, func(t *testing.T) {
			if err := creds.Valid(); err == nil {
				t.Error("want validation error")
			}
		})
	}
	ok := Credentials{CorpID: "c", Secret: "s", AgentID: 1}
	if err := ok.Valid(); err != nil {
		t.Errorf("complete credentials rejected: %v", err)
	}
}
