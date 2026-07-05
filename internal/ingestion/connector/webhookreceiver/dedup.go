package webhookreceiver

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
)

// recentTTL bounds how long a delivery key is remembered for transport-
// layer retry suppression - long enough to absorb a gateway's typical
// immediate-retry burst, short enough that the map below can't grow
// unbounded.
const recentTTL = 10 * time.Minute

func computeDeliveryKey(deliveryID string, body []byte) string {
	h := sha256.New()
	if deliveryID != "" {
		h.Write([]byte(deliveryID)) //nolint:errcheck
		h.Write([]byte{0})          //nolint:errcheck
	}
	h.Write(body) //nolint:errcheck
	return hex.EncodeToString(h.Sum(nil))
}

// seenRecently reports whether key was already recorded within recentTTL,
// recording it if not. Sweeps expired entries opportunistically on each
// call rather than running a separate goroutine/ticker - simplest thing
// that bounds memory for a map that's only ever appended to otherwise.
func (c *Connector) seenRecently(key string) bool {
	now := time.Now()
	c.recentMu.Lock()
	defer c.recentMu.Unlock()

	for k, seenAt := range c.recent {
		if now.Sub(seenAt) > recentTTL {
			delete(c.recent, k)
		}
	}

	if seenAt, ok := c.recent[key]; ok && now.Sub(seenAt) <= recentTTL {
		return true
	}
	c.recent[key] = now
	return false
}

// NaturalKeyFunc is a dedup.NaturalKeyFunc (plans/task/core/09) for
// webhook-sourced records: hashes the raw webhook body captured
// verbatim in rec.Raw.Payload. This is the AUTHORITATIVE, persistent
// dedup guard (task 09's ingestion_dedup table + bloom filter) that
// backs up this package's own in-memory transport-layer suppression
// (seenRecently) - the same mechanism every other connector's records
// flow through, not a second dedup path (plans/task/core/18 Common
// Pitfalls).
//
// Wire this in wherever this connector's pipeline is constructed, e.g.:
//
//	dedup.DedupStage{ ..., NaturalKey: webhookreceiver.NaturalKeyFunc }
func NaturalKeyFunc(rec *pipeline.PipelineRecord) string {
	h := sha256.Sum256(rec.Raw.Payload)
	return hex.EncodeToString(h[:])
}
