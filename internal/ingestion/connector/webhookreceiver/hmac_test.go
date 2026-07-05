package webhookreceiver

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
)

func TestVerifySignature_GenericHMACSHA256(t *testing.T) {
	secret := "s3cr3t"
	body := []byte(`{"event":"settlement.created"}`)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	validSig := hex.EncodeToString(mac.Sum(nil))

	tests := []struct {
		name string
		sig  string
		want bool
	}{
		{"valid", validSig, true},
		{"wrong secret", func() string {
			m := hmac.New(sha256.New, []byte("wrong"))
			m.Write(body)
			return hex.EncodeToString(m.Sum(nil))
		}(), false},
		{"tampered body signature", "00" + validSig[2:], false},
		{"empty", "", false},
		{"not hex", "not-hex-at-all!!", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := VerifySignature(SchemeGenericHMACSHA256, secret, body, tt.sig)
			if got != tt.want {
				t.Errorf("VerifySignature() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVerifySignature_Stripe(t *testing.T) {
	secret := "whsec_test"
	body := []byte(`{"id":"evt_123"}`)
	timestamp := "1700000000"
	signedPayload := []byte(timestamp + ".")
	signedPayload = append(signedPayload, body...)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(signedPayload)
	v1 := hex.EncodeToString(mac.Sum(nil))

	valid := fmt.Sprintf("t=%s,v1=%s", timestamp, v1)
	if !VerifySignature(SchemeStripe, secret, body, valid) {
		t.Error("expected valid Stripe signature to verify")
	}

	// Secret-rotation case: multiple v1 values, only one matches.
	multi := fmt.Sprintf("t=%s,v1=deadbeef,v1=%s", timestamp, v1)
	if !VerifySignature(SchemeStripe, secret, body, multi) {
		t.Error("expected match against second v1 value to verify")
	}

	if VerifySignature(SchemeStripe, secret, []byte(`{"id":"evt_TAMPERED"}`), valid) {
		t.Error("expected signature over different body to fail")
	}

	if VerifySignature(SchemeStripe, secret, body, "malformed-header") {
		t.Error("expected malformed header to fail")
	}

	if VerifySignature(SchemeStripe, secret, body, "") {
		t.Error("expected empty header to fail")
	}
}

func TestVerifySignature_UnknownScheme(t *testing.T) {
	if VerifySignature("made-up-scheme", "secret", []byte("body"), "sig") {
		t.Error("expected unknown scheme to always fail closed")
	}
}
