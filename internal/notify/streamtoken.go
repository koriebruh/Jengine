package notify

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// StreamTokenTTL is how long a minted SSE stream token is valid for -
// plans/task/core/21's SSE gateway subsection: "a short-lived, stream-
// scoped token issued via a small POST .../stream-token RPC... that
// mints a token valid only for opening the SSE connection."
const StreamTokenTTL = 60 * time.Second

// MintStreamToken produces an opaque, URL-safe token binding tenantID to
// an expiry, HMAC-signed with secret - verified by
// VerifyStreamToken without a DB round-trip (the SSE gateway may be a
// different process/binary than the one that minted it, per this
// task's own "pick cmd/webhook-dispatcher or a sibling cmd/realtime-gateway"
// note - either way, minting and verifying share only this secret, not
// a database).
func MintStreamToken(secret string, tenantID uuid.UUID, now time.Time) (token string, expiresAt time.Time) {
	expiresAt = now.Add(StreamTokenTTL)
	payload := tenantID.String() + "." + strconv.FormatInt(expiresAt.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload)) //nolint:errcheck // hash.Hash.Write never returns an error
	sig := hex.EncodeToString(mac.Sum(nil))
	return base64.RawURLEncoding.EncodeToString([]byte(payload + "." + sig)), expiresAt
}

// VerifyStreamToken checks token against secret and now, returning the
// bound tenantID if valid. Rejects an expired token or one whose
// signature doesn't match - the same fail-closed posture as
// Verify (the outbound-webhook signature checker) for the same replay-
// prevention reason (an expired token is exactly a captured-and-replayed
// credential).
func VerifyStreamToken(secret string, token string, now time.Time) (uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return uuid.Nil, fmt.Errorf("notify: invalid stream token encoding: %w", err)
	}
	parts := strings.SplitN(string(raw), ".", 3)
	if len(parts) != 3 {
		return uuid.Nil, fmt.Errorf("notify: malformed stream token")
	}
	tenantIDStr, expiresAtStr, sig := parts[0], parts[1], parts[2]

	expiresAtUnix, err := strconv.ParseInt(expiresAtStr, 10, 64)
	if err != nil {
		return uuid.Nil, fmt.Errorf("notify: malformed stream token expiry: %w", err)
	}
	if now.After(time.Unix(expiresAtUnix, 0)) {
		return uuid.Nil, fmt.Errorf("notify: stream token expired")
	}

	payload := tenantIDStr + "." + expiresAtStr
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload)) //nolint:errcheck // hash.Hash.Write never returns an error
	want := mac.Sum(nil)
	got, err := hex.DecodeString(sig)
	if err != nil || !hmac.Equal(got, want) {
		return uuid.Nil, fmt.Errorf("notify: stream token signature mismatch")
	}

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return uuid.Nil, fmt.Errorf("notify: malformed stream token tenant id: %w", err)
	}
	return tenantID, nil
}
