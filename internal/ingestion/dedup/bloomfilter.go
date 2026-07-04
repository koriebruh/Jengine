package dedup

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// BloomFilter is a fast-path negative check only: false = definitely not
// seen (skip the Postgres round-trip); true = maybe seen (must confirm
// against the authoritative ingestion_dedup table) - never a false
// negative, by construction (plans/task/core/09 Implementation Notes).
type BloomFilter interface {
	MayExist(ctx context.Context, tenantID uuid.UUID, key string) (bool, error)
	Add(ctx context.Context, tenantID uuid.UUID, key string) error
}

// RedisBloomFilter implements BloomFilter as a Go-side bitset backed by
// Redis SETBIT/GETBIT - task 02's compose file provisions plain redis:7,
// not the RedisBloom module, so this avoids an unplanned Redis module
// dependency this late in the build sequence (plans/task/core/09
// Implementation Notes' explicit decision point). Redis serializes
// individual SETBIT/GETBIT calls, so no additional client-side locking
// is needed for concurrent access.
type RedisBloomFilter struct {
	client    *redis.Client
	keyPrefix string
	m         uint64 // bitset size in bits
	k         uint64 // number of hash functions
}

// NewRedisBloomFilter sizes the bitset for expectedItems at
// falsePositiveRate using the standard bloom-filter formulas:
// m = ceil(-(n*ln(p)) / ln(2)^2), k = round((m/n) * ln(2)).
func NewRedisBloomFilter(client *redis.Client, keyPrefix string, expectedItems int, falsePositiveRate float64) *RedisBloomFilter {
	n := float64(expectedItems)
	p := falsePositiveRate

	m := math.Ceil(-(n * math.Log(p)) / (math.Ln2 * math.Ln2))
	k := math.Round((m / n) * math.Ln2)
	if k < 1 {
		k = 1
	}

	return &RedisBloomFilter{client: client, keyPrefix: keyPrefix, m: uint64(m), k: uint64(k)}
}

func (f *RedisBloomFilter) redisKey(tenantID uuid.UUID) string {
	return fmt.Sprintf("%s:%s", f.keyPrefix, tenantID.String())
}

// bitPositions returns f.k bit offsets for key, via double hashing
// (h1 + i*h2) mod m - the standard technique for deriving k independent-
// enough hash functions from two base hashes.
func (f *RedisBloomFilter) bitPositions(key string) []uint64 {
	h1 := fnv.New64a()
	_, _ = h1.Write([]byte(key))
	sum1 := h1.Sum64()

	h2 := fnv.New64()
	_, _ = h2.Write([]byte(key))
	sum2 := h2.Sum64()

	positions := make([]uint64, f.k)
	for i := uint64(0); i < f.k; i++ {
		positions[i] = (sum1 + i*sum2) % f.m
	}
	return positions
}

func (f *RedisBloomFilter) MayExist(ctx context.Context, tenantID uuid.UUID, key string) (bool, error) {
	redisKey := f.redisKey(tenantID)
	for _, pos := range f.bitPositions(key) {
		bit, err := f.client.GetBit(ctx, redisKey, int64(pos)).Result()
		if err != nil {
			return false, fmt.Errorf("dedup: bloom filter GetBit: %w", err)
		}
		if bit == 0 {
			return false, nil // any unset bit proves the key was never added
		}
	}
	return true, nil
}

func (f *RedisBloomFilter) Add(ctx context.Context, tenantID uuid.UUID, key string) error {
	redisKey := f.redisKey(tenantID)
	for _, pos := range f.bitPositions(key) {
		if err := f.client.SetBit(ctx, redisKey, int64(pos), 1).Err(); err != nil {
			return fmt.Errorf("dedup: bloom filter SetBit: %w", err)
		}
	}
	return nil
}

var _ BloomFilter = (*RedisBloomFilter)(nil)
