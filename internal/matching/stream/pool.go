// Package stream implements the streaming matching worker's own pieces
// (plans/task/core/19): a bounded Redis-backed candidate pool and the
// per-pool-key-serialized consumer loop. Scoring/blocking logic itself
// is NOT reimplemented here - both this package and the batch worker
// (plans/task/core/12) call internal/matching/core.Match unchanged;
// this package's only job is sourcing a bounded candidate slice from
// Redis instead of a full partition scan, and interpreting the
// MatchOutcome.
package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	matchingcore "github.com/koriebruh/Jengine/internal/matching/core"
)

// CandidatePool is the streaming worker's bounded rolling-window
// candidate source, keyed by (tenantID, poolKey) - deliberately NOT
// parameterized by a blocking key (plans/task/core/19's own sketch
// suggested one, but internal/matching/core.Match already builds its
// own blocking index internally over whatever slice it's given; a
// second blocking layer here would just be redundant filtering, not a
// correctness requirement - see this package's own doc comment).
//
// poolKey is the caller's choice of how to partition the candidate
// space - see Consumer's own doc comment for why this is tenant-scoped
// (poolKey = tenantID) at MVP rather than per-account-pair: the same
// account_group taxonomy gap QA_REPORT.md documents for the batch
// worker (plans/task/core/12) applies here too - there's no schema
// representation of which accounts a rule's scope pairs together, so
// there's no correct narrower key to partition by yet.
type CandidatePool interface {
	// Add inserts rec into the pool for (tenantID, poolKey), trimming
	// anything older than the pool's configured window in the same call
	// (trim-on-write, plans/task/core/19 Implementation Notes) - never
	// unbounded growth.
	Add(ctx context.Context, tenantID uuid.UUID, poolKey string, rec matchingcore.MatchableRecord) error
	// Query returns every currently-pooled candidate for (tenantID,
	// poolKey) - bounded by the window, an evicted-before-matched
	// candidate is expected and caught by the nightly batch pass
	// instead, not a bug (plans/task/core/19 Implementation Notes).
	Query(ctx context.Context, tenantID uuid.UUID, poolKey string) ([]matchingcore.MatchableRecord, error)
	// Remove drops txnID from the pool (e.g. once it's been consumed
	// into a match) - not required for the window to stay bounded
	// (trim-on-write already does that), but avoids matching an
	// already-matched transaction again as someone else's candidate.
	Remove(ctx context.Context, tenantID uuid.UUID, poolKey string, txnID uuid.UUID) error
}

// RedisCandidatePool implements CandidatePool via a Redis sorted set
// (score = ValueDate unix seconds, for window trimming) plus a hash
// holding each candidate's full serialized record - ZADD/ZRANGE alone
// can't hold an arbitrary struct as a member usefully (members must be
// unique strings; using the record's own ID as the member and a
// side-table for the payload avoids embedding JSON as the sort-set
// member, which would break Remove-by-ID).
type RedisCandidatePool struct {
	client    *redis.Client
	keyPrefix string
	window    time.Duration
}

func NewRedisCandidatePool(client *redis.Client, keyPrefix string, window time.Duration) *RedisCandidatePool {
	return &RedisCandidatePool{client: client, keyPrefix: keyPrefix, window: window}
}

func (p *RedisCandidatePool) zsetKey(tenantID uuid.UUID, poolKey string) string {
	return fmt.Sprintf("%s:cand:%s:%s:z", p.keyPrefix, tenantID, poolKey)
}

func (p *RedisCandidatePool) dataKey(tenantID uuid.UUID, poolKey string) string {
	return fmt.Sprintf("%s:cand:%s:%s:data", p.keyPrefix, tenantID, poolKey)
}

func (p *RedisCandidatePool) Add(ctx context.Context, tenantID uuid.UUID, poolKey string, rec matchingcore.MatchableRecord) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("stream: marshal candidate: %w", err)
	}

	zkey := p.zsetKey(tenantID, poolKey)
	dkey := p.dataKey(tenantID, poolKey)
	id := rec.ID.String()

	pipe := p.client.TxPipeline()
	pipe.ZAdd(ctx, zkey, redis.Z{Score: float64(rec.ValueDate.Unix()), Member: id})
	pipe.HSet(ctx, dkey, id, payload)

	// Trim-on-write: drop anything older than the window (plans/task/
	// core/19 Implementation Notes - "TTL/trim-on-write, not unbounded
	// growth"). Evicted members are removed from the zset here; the
	// corresponding data-hash fields are swept by evictExpiredData
	// below in the same pipeline, keeping the two structures in sync
	// without a separate cleanup goroutine.
	cutoff := float64(time.Now().Add(-p.window).Unix())
	pipe.ZRangeByScore(ctx, zkey, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("(%f", cutoff)})
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("stream: add candidate: %w", err)
	}

	// The ZRangeByScore result (last command) lists members that are
	// now stale - remove them from both structures. Done as a second,
	// small pipeline rather than folding into the first: the member
	// list isn't known until the first pipeline's ZRangeByScore result
	// comes back.
	expiredCmd, ok := cmds[len(cmds)-1].(*redis.StringSliceCmd)
	if !ok {
		return nil
	}
	expired, err := expiredCmd.Result()
	if err != nil || len(expired) == 0 {
		return nil
	}
	cleanup := p.client.TxPipeline()
	cleanup.ZRemRangeByScore(ctx, zkey, "-inf", fmt.Sprintf("(%f", cutoff))
	cleanup.HDel(ctx, dkey, expired...)
	_, err = cleanup.Exec(ctx)
	if err != nil {
		return fmt.Errorf("stream: trim expired candidates: %w", err)
	}
	return nil
}

func (p *RedisCandidatePool) Query(ctx context.Context, tenantID uuid.UUID, poolKey string) ([]matchingcore.MatchableRecord, error) {
	dkey := p.dataKey(tenantID, poolKey)
	raw, err := p.client.HGetAll(ctx, dkey).Result()
	if err != nil {
		return nil, fmt.Errorf("stream: query candidates: %w", err)
	}
	records := make([]matchingcore.MatchableRecord, 0, len(raw))
	for _, v := range raw {
		var rec matchingcore.MatchableRecord
		if err := json.Unmarshal([]byte(v), &rec); err != nil {
			return nil, fmt.Errorf("stream: unmarshal candidate: %w", err)
		}
		records = append(records, rec)
	}
	return records, nil
}

func (p *RedisCandidatePool) Remove(ctx context.Context, tenantID uuid.UUID, poolKey string, txnID uuid.UUID) error {
	zkey := p.zsetKey(tenantID, poolKey)
	dkey := p.dataKey(tenantID, poolKey)
	id := txnID.String()

	pipe := p.client.TxPipeline()
	pipe.ZRem(ctx, zkey, id)
	pipe.HDel(ctx, dkey, id)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("stream: remove candidate: %w", err)
	}
	return nil
}

var _ CandidatePool = (*RedisCandidatePool)(nil)
