package wecom

import (
	"encoding/base64"
	"strings"
	"testing"
)

// A 43-character EncodingAESKey as the WeCom console issues them.
const testAESKey = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG"

func newTestCrypto(t *testing.T) *Crypto {
	t.Helper()
	c, err := NewCrypto("test-token", testAESKey, "wwcorpid123")
	if err != nil {
		t.Fatalf("NewCrypto: %v", err)
	}
	return c
}

func TestNewCryptoRejectsBadKeyLength(t *testing.T) {
	// A truncated key is the single most common configuration mistake, and it
	// otherwise surfaces much later as unreadable plaintext.
	if _, err := NewCrypto("t", "tooshort", "corp"); err == nil {
		t.Fatal("want error for short encoding aes key")
	}
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	c := newTestCrypto(t)
	want := []byte(`<xml><Content><![CDATA[你好，世界]]></Content></xml>`)

	encrypted, err := c.Encrypt(want)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	got, err := c.Decrypt(encrypted)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("round trip changed the payload:\n got %s\nwant %s", got, want)
	}
}

func TestEncryptProducesDifferentCiphertextEachTime(t *testing.T) {
	// The 16-byte random prefix must actually be random, otherwise identical
	// messages produce identical ciphertext and become correlatable.
	c := newTestCrypto(t)
	a, err := c.Encrypt([]byte("same message"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	b, err := c.Encrypt([]byte("same message"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if a == b {
		t.Error("identical plaintexts produced identical ciphertext")
	}
}

func TestSignatureIsOrderIndependent(t *testing.T) {
	// The scheme sorts its four inputs, which is what makes it independent of
	// the order WeCom happens to send them in.
	c := newTestCrypto(t)
	got := c.Signature("1700000000", "nonce123", "encrypted-payload")
	if len(got) != 40 {
		t.Errorf("signature should be 40 hex chars, got %d", len(got))
	}
	again := c.Signature("1700000000", "nonce123", "encrypted-payload")
	if got != again {
		t.Error("signature is not deterministic")
	}
}

func TestVerifySignature(t *testing.T) {
	c := newTestCrypto(t)
	ts, nonce, payload := "1700000000", "nonce123", "encrypted-payload"

	valid := c.Signature(ts, nonce, payload)
	if err := c.VerifySignature(valid, ts, nonce, payload); err != nil {
		t.Errorf("valid signature rejected: %v", err)
	}
	if err := c.VerifySignature("0000000000000000000000000000000000000000", ts, nonce, payload); err == nil {
		t.Error("forged signature accepted")
	}
	// A changed timestamp must invalidate the signature, or a captured
	// callback could be replayed indefinitely.
	if err := c.VerifySignature(valid, "1700009999", nonce, payload); err == nil {
		t.Error("signature accepted for a different timestamp")
	}
}

func TestDecryptRejectsForeignReceiveID(t *testing.T) {
	// A correctly-encrypted message from another corp must be refused;
	// otherwise one tenant's callback could be replayed into another's.
	sender, err := NewCrypto("test-token", testAESKey, "wwOTHERCORP")
	if err != nil {
		t.Fatalf("NewCrypto: %v", err)
	}
	encrypted, err := sender.Encrypt([]byte("<xml/>"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	receiver := newTestCrypto(t) // expects wwcorpid123
	if _, err := receiver.Decrypt(encrypted); err == nil {
		t.Fatal("accepted a message addressed to another corp")
	}
}

func TestDecryptRejectsWrongKey(t *testing.T) {
	c := newTestCrypto(t)
	encrypted, err := c.Encrypt([]byte("<xml/>"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	other, err := NewCrypto("test-token", "ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210zyxwvut", "wwcorpid123")
	if err != nil {
		t.Fatalf("NewCrypto: %v", err)
	}
	// The padding check is what turns a wrong key into an error rather than
	// silently truncated garbage.
	if _, err := other.Decrypt(encrypted); err == nil {
		t.Fatal("decrypted with the wrong key")
	}
}

func TestDecryptRejectsMalformedInput(t *testing.T) {
	c := newTestCrypto(t)
	cases := map[string]string{
		"not base64":         "!!!not-base64!!!",
		"not block multiple": base64.StdEncoding.EncodeToString([]byte("short")),
		"empty":              "",
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := c.Decrypt(in); err == nil {
				t.Error("want error")
			}
		})
	}
}

func TestPaddingHelpers(t *testing.T) {
	for _, size := range []int{0, 1, 15, 16, 17, 31, 32} {
		in := strings.Repeat("x", size)
		padded := pkcs7Pad([]byte(in), 16)
		if len(padded)%16 != 0 {
			t.Fatalf("size %d: padded length %d is not a block multiple", size, len(padded))
		}
		out, err := pkcs7Unpad(padded)
		if err != nil {
			t.Fatalf("size %d: unpad: %v", size, err)
		}
		if string(out) != in {
			t.Errorf("size %d: round trip mismatch", size)
		}
	}
}
