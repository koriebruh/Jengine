package notify

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

// Sign computes the X-Jengine-Signature header value
// (plans/task/core/21 Implementation Notes): "t=<unix_ts>,v1=<hex(hmac_sha256(secret,
// ts + "." + body))>" (Stripe-style). Including the timestamp in the
// signed content - and requiring the receiver to check clock-skew
// tolerance - prevents replay of a captured valid request; never sign
// without it.
func Sign(secret string, ts time.Time, body []byte) string {
	tsStr := strconv.FormatInt(ts.Unix(), 10)
	signedPayload := append([]byte(tsStr+"."), body...)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(signedPayload) //nolint:errcheck // hash.Hash.Write never returns an error
	return fmt.Sprintf("t=%s,v1=%s", tsStr, hex.EncodeToString(mac.Sum(nil)))
}

// Verify checks a signature header (as Sign produced it) against body
// using secret, requiring the embedded timestamp to be within
// maxClockSkew of now - used by this package's own round-trip tests and
// available for a receiver-side reference implementation/docs example.
func Verify(secret string, header string, body []byte, now time.Time, maxClockSkew time.Duration) bool {
	ts, sig, ok := parseSignatureHeader(header)
	if !ok {
		return false
	}

	eventTime := time.Unix(ts, 0)
	skew := now.Sub(eventTime)
	if skew < 0 {
		skew = -skew
	}
	if skew > maxClockSkew {
		return false
	}

	expected := Sign(secret, eventTime, body)
	_, expectedSig, _ := parseSignatureHeader(expected)
	got, err := hex.DecodeString(sig)
	if err != nil {
		return false
	}
	want, err := hex.DecodeString(expectedSig)
	if err != nil {
		return false
	}
	return hmac.Equal(got, want)
}

func parseSignatureHeader(header string) (ts int64, sig string, ok bool) {
	n, err := fmt.Sscanf(header, "t=%d,v1=%s", &ts, &sig)
	if err != nil || n != 2 {
		return 0, "", false
	}
	return ts, sig, true
}
