package webhookreceiver

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// SignatureScheme names a supported provider signature convention -
// plans/task/core/18 names Stripe/Adyen-style plus a plain fallback.
const (
	SchemeStripe            = "stripe"
	SchemeAdyen             = "adyen"
	SchemeGenericHMACSHA256 = "generic-hmac-sha256"
)

// VerifySignature checks rawSignatureHeader against body using secret,
// per scheme's convention. Returns false (never panics/errors past this
// boolean) on any malformed input - an unparsable signature header is
// exactly as untrusted as a wrong one.
func VerifySignature(scheme, secret string, body []byte, rawSignatureHeader string) bool {
	switch scheme {
	case SchemeStripe:
		return verifyStripe(secret, body, rawSignatureHeader)
	case SchemeAdyen:
		return verifyGenericHex(secret, body, rawSignatureHeader)
	case SchemeGenericHMACSHA256:
		return verifyGenericHex(secret, body, rawSignatureHeader)
	default:
		return false
	}
}

// verifyGenericHex checks a bare hex-encoded HMAC-SHA256 of body - the
// convention Adyen's HMAC signature and this connector's own
// "generic-hmac-sha256" scheme both use.
func verifyGenericHex(secret string, body []byte, rawSignatureHeader string) bool {
	if rawSignatureHeader == "" {
		return false
	}
	expected := hmacHex(secret, body)
	got, err := hex.DecodeString(strings.TrimSpace(rawSignatureHeader))
	if err != nil {
		return false
	}
	return hmac.Equal(got, mustHex(expected))
}

// verifyStripe checks Stripe's "t=<ts>,v1=<hex>[,v1=<hex>...]" header
// format: the signed payload is "<ts>.<body>", and Stripe sends multiple
// v1 values during secret rotation - any matching is a valid signature.
func verifyStripe(secret string, body []byte, rawSignatureHeader string) bool {
	if rawSignatureHeader == "" {
		return false
	}
	var timestamp string
	var v1Sigs []string
	for _, part := range strings.Split(rawSignatureHeader, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "t":
			timestamp = kv[1]
		case "v1":
			v1Sigs = append(v1Sigs, kv[1])
		}
	}
	if timestamp == "" || len(v1Sigs) == 0 {
		return false
	}
	signedPayload := []byte(timestamp + ".")
	signedPayload = append(signedPayload, body...)
	expected := hmacHex(secret, signedPayload)
	expectedBytes := mustHex(expected)
	for _, sig := range v1Sigs {
		got, err := hex.DecodeString(strings.TrimSpace(sig))
		if err != nil {
			continue
		}
		if hmac.Equal(got, expectedBytes) {
			return true
		}
	}
	return false
}

func hmacHex(secret string, data []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(data) //nolint:errcheck // hash.Hash.Write never returns an error
	return hex.EncodeToString(mac.Sum(nil))
}

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(fmt.Sprintf("webhookreceiver: internal hex encoding produced invalid hex: %v", err))
	}
	return b
}
