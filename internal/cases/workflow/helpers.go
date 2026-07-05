package workflow

import (
	"encoding/json"
	"fmt"
	"hash/fnv"

	"github.com/google/uuid"
)

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("workflow: marshal %T: %v", v, err))
	}
	return b
}

func jsonUnmarshal(data json.RawMessage, v any) error {
	return json.Unmarshal(data, v)
}

// hashToIndex deterministically maps id to an index in [0, n) - used by
// AutoAssignActivity so repeated Activity retries for the SAME BreakID
// resolve to the SAME assignee (see AutoAssignActivity's own doc
// comment for why idempotency matters here).
func hashToIndex(id uuid.UUID, n int) int {
	if n <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(id[:])
	return int(h.Sum32() % uint32(n)) //nolint:gosec // modulo of a hash, not a security-sensitive computation
}
