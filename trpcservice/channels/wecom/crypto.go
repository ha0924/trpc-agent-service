// 设计依据：docs/IM通道接入设计.md §7「通道接入差异」

package wecom

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// Crypto implements WeCom's callback message encryption.
//
// This is the part of the integration most likely to be got wrong, and the
// failure mode is silent: a mistake in padding or in the length prefix
// produces plausible-looking garbage rather than an error. The layout is
// therefore spelled out here rather than left implicit.
//
// A decrypted payload is:
//
//	16 bytes  random prefix
//	 4 bytes  message length, big endian
//	 N bytes  message
//	 M bytes  receive id (corp id), used to prove the message is ours
type Crypto struct {
	token    string
	aesKey   []byte // 32 bytes, decoded from EncodingAESKey
	receadID string
}

// NewCrypto builds a Crypto from the credentials configured for a binding.
//
// encodingAESKey is the 43-character value from the WeCom console; the real
// key is its base64 decoding, which requires an appended "=" because the
// console strips the padding.
func NewCrypto(token, encodingAESKey, receiveID string) (*Crypto, error) {
	if len(encodingAESKey) != 43 {
		return nil, fmt.Errorf("wecom: encoding aes key must be 43 chars, got %d", len(encodingAESKey))
	}
	key, err := base64.StdEncoding.DecodeString(encodingAESKey + "=")
	if err != nil {
		return nil, fmt.Errorf("wecom: decode aes key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("wecom: aes key must be 32 bytes, got %d", len(key))
	}
	return &Crypto{token: token, aesKey: key, receadID: receiveID}, nil
}

// Signature computes the msg_signature WeCom sends alongside a callback.
//
// The four values are sorted as strings and concatenated, then hashed. Sorting
// is what makes the scheme order-independent, and getting it wrong yields a
// signature that never matches.
func (c *Crypto) Signature(timestamp, nonce, encrypted string) string {
	parts := []string{c.token, timestamp, nonce, encrypted}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, "")))
	return hex.EncodeToString(sum[:])
}

// VerifySignature checks a callback's signature.
//
// The comparison is constant-time so an attacker cannot narrow the expected
// value by timing repeated attempts.
func (c *Crypto) VerifySignature(msgSignature, timestamp, nonce, encrypted string) error {
	want := c.Signature(timestamp, nonce, encrypted)
	if subtleCompare(want, msgSignature) {
		return nil
	}
	return fmt.Errorf("wecom: signature mismatch")
}

// Decrypt verifies and unwraps an encrypted callback payload.
func (c *Crypto) Decrypt(encrypted string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return nil, fmt.Errorf("wecom: base64 decode: %w", err)
	}
	if len(raw) < aes.BlockSize || len(raw)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("wecom: ciphertext length %d is not a block multiple", len(raw))
	}

	block, err := aes.NewCipher(c.aesKey)
	if err != nil {
		return nil, fmt.Errorf("wecom: new cipher: %w", err)
	}
	// The IV is the first 16 bytes of the key, not a random prefix — a WeCom
	// specific choice that differs from most AES-CBC usage.
	plain := make([]byte, len(raw))
	cipher.NewCBCDecrypter(block, c.aesKey[:aes.BlockSize]).CryptBlocks(plain, raw)

	plain, err = pkcs7Unpad(plain)
	if err != nil {
		return nil, err
	}
	if len(plain) < 20 {
		return nil, fmt.Errorf("wecom: plaintext too short: %d bytes", len(plain))
	}

	msgLen := binary.BigEndian.Uint32(plain[16:20])
	if int(msgLen)+20 > len(plain) {
		return nil, fmt.Errorf("wecom: declared length %d exceeds payload", msgLen)
	}
	msg := plain[20 : 20+msgLen]

	// The trailing receive id proves the message was addressed to this corp.
	// Skipping this check would accept a correctly-encrypted message replayed
	// from another tenant's callback.
	gotID := string(plain[20+msgLen:])
	if c.receadID != "" && gotID != c.receadID {
		return nil, fmt.Errorf("wecom: receive id mismatch")
	}
	return msg, nil
}

// Encrypt wraps a plaintext for the passive-reply path.
func (c *Crypto) Encrypt(msg []byte) (string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("wecom: random prefix: %w", err)
	}

	var buf bytes.Buffer
	buf.Write(nonce)
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(msg))); err != nil {
		return "", fmt.Errorf("wecom: write length: %w", err)
	}
	buf.Write(msg)
	buf.WriteString(c.receadID)

	padded := pkcs7Pad(buf.Bytes(), aes.BlockSize)
	block, err := aes.NewCipher(c.aesKey)
	if err != nil {
		return "", fmt.Errorf("wecom: new cipher: %w", err)
	}
	out := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, c.aesKey[:aes.BlockSize]).CryptBlocks(out, padded)
	return base64.StdEncoding.EncodeToString(out), nil
}

func pkcs7Pad(b []byte, blockSize int) []byte {
	n := blockSize - len(b)%blockSize
	return append(b, bytes.Repeat([]byte{byte(n)}, n)...)
}

func pkcs7Unpad(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("wecom: empty plaintext")
	}
	n := int(b[len(b)-1])
	// WeCom pads with 1..32 bytes. A value outside that range means the key
	// is wrong, and reporting it as such beats returning truncated garbage.
	if n < 1 || n > 32 || n > len(b) {
		return nil, fmt.Errorf("wecom: bad padding byte %d (wrong aes key?)", n)
	}
	return b[:len(b)-n], nil
}

// subtleCompare reports whether two strings are equal, in constant time with
// respect to their contents.
func subtleCompare(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := 0; i < len(a); i++ {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}
