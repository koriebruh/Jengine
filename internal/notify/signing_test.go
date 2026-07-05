package notify_test

import (
	"testing"
	"time"

	"github.com/koriebruh/Jengine/internal/notify"
)

func TestSignVerify_RoundTrip(t *testing.T) {
	secret := "whsec_test"
	body := []byte(`{"event":"break.created","break_id":"123"}`)
	now := time.Now()

	header := notify.Sign(secret, now, body)
	if !notify.Verify(secret, header, body, now, 5*time.Minute) {
		t.Error("expected valid signature to verify")
	}
}

func TestVerify_WrongSecretFails(t *testing.T) {
	body := []byte(`{"event":"break.created"}`)
	now := time.Now()
	header := notify.Sign("secret-a", now, body)
	if notify.Verify("secret-b", header, body, now, 5*time.Minute) {
		t.Error("expected verification with the wrong secret to fail")
	}
}

func TestVerify_TamperedBodyFails(t *testing.T) {
	secret := "whsec_test"
	now := time.Now()
	header := notify.Sign(secret, now, []byte(`{"amount":100}`))
	if notify.Verify(secret, header, []byte(`{"amount":999999}`), now, 5*time.Minute) {
		t.Error("expected verification against a different body to fail")
	}
}

func TestVerify_ExpiredTimestampFails(t *testing.T) {
	secret := "whsec_test"
	body := []byte(`{"event":"break.created"}`)
	old := time.Now().Add(-1 * time.Hour)
	header := notify.Sign(secret, old, body)
	if notify.Verify(secret, header, body, time.Now(), 5*time.Minute) {
		t.Error("expected a signature outside the clock-skew tolerance to fail (replay protection)")
	}
}

func TestVerify_MalformedHeaderFails(t *testing.T) {
	if notify.Verify("secret", "not-a-valid-header", []byte("body"), time.Now(), 5*time.Minute) {
		t.Error("expected malformed header to fail")
	}
}
