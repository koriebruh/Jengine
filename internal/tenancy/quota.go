package tenancy

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// QuotaLimiter is a Redis-backed token-bucket rate limiter, keyed per
// tenant+resource. Basic implementation per plans/docs/01-multi-tenancy.md
// §2.4 - full weighted-fair-queuing across matching/streaming workers is
// V1; plans/task/core/12 wires this same primitive into the batch worker's
// job dispatch rather than reimplementing rate limiting there.
//
// Capacity and refill rate both come from the tenant's Quota
// (IngestionRateLimit) via RegistryRepo.GetQuota - the bucket allows up to
// that many requests/sec, refilling continuously at the same rate (no
// separate burst allowance beyond the configured limit itself; the design
// docs don't call for one, so this doesn't invent one).
type QuotaLimiter struct {
	redis    *redis.Client
	registry RegistryRepo
}

func NewQuotaLimiter(rdb *redis.Client, registry RegistryRepo) *QuotaLimiter {
	return &QuotaLimiter{redis: rdb, registry: registry}
}

// tokenBucketScript atomically refills and consumes one token. KEYS[1] is
// the bucket's Redis key (a hash with "tokens_x1000" and "ts_ms" fields).
// ARGV: capacity, refill_rate_per_sec, now_unix_ms.
//
// Tokens are tracked scaled by 1000 (as an integer) because Redis
// truncates Lua numbers to integers when converting a script's return
// value to the RESP protocol - returning a fractional token count
// directly would silently lose precision on every call.
var tokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local bucket = redis.call("HMGET", key, "tokens_x1000", "ts_ms")
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])

if tokens == nil then
  tokens = capacity * 1000
  ts = now
end

local elapsed_sec = math.max(0, (now - ts) / 1000)
tokens = math.min(capacity * 1000, tokens + elapsed_sec * refill_rate * 1000)

local allowed = 0
if tokens >= 1000 then
  tokens = tokens - 1000
  allowed = 1
end

redis.call("HMSET", key, "tokens_x1000", tokens, "ts_ms", now)
redis.call("EXPIRE", key, 3600)

return {allowed, math.floor(tokens)}
`)

// Allow reports whether a request for tenantID/resource is permitted right
// now (per the tenant's configured Quota.IngestionRateLimit), consuming
// one token if so.
func (q *QuotaLimiter) Allow(ctx context.Context, tenantID uuid.UUID, resource string) (allowed bool, retryAfter time.Duration, err error) {
	quota, err := q.registry.GetQuota(ctx, tenantID)
	if err != nil {
		return false, 0, fmt.Errorf("tenancy: quota Allow: loading quota: %w", err)
	}
	rate := quota.IngestionRateLimit
	if rate <= 0 {
		return false, time.Second, nil
	}

	key := fmt.Sprintf("ratelimit:%s:%s", tenantID, resource)
	now := time.Now().UnixMilli()

	res, err := tokenBucketScript.Run(ctx, q.redis, []string{key}, rate, rate, now).Result()
	if err != nil {
		return false, 0, fmt.Errorf("tenancy: quota Allow: %w", err)
	}

	vals, ok := res.([]interface{})
	if !ok || len(vals) != 2 {
		return false, 0, fmt.Errorf("tenancy: quota Allow: unexpected script result %v", res)
	}
	allowedInt, _ := vals[0].(int64)
	tokensRemainingScaled, _ := vals[1].(int64)

	if allowedInt == 1 {
		return true, 0, nil
	}

	tokensRemaining := float64(tokensRemainingScaled) / 1000.0
	deficit := 1 - tokensRemaining
	waitSec := deficit / float64(rate)
	if waitSec < 0 {
		waitSec = 0
	}
	return false, time.Duration(waitSec * float64(time.Second)), nil
}

// HTTPMiddleware wraps handler with quota enforcement for the given
// resource, using the TenantContext already injected by
// tenancy.Middleware. Returns 429 + Retry-After (per
// plans/docs/01-multi-tenancy.md §2.4's "soft-throttle via 429+Retry-After
// before hard failure") when the tenant's bucket is empty.
func (q *QuotaLimiter) HTTPMiddleware(resource string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc, ok := TenantFromContext(r.Context())
		if !ok {
			http.Error(w, "missing tenant context", http.StatusUnauthorized)
			return
		}

		allowed, retryAfter, err := q.Allow(r.Context(), tc.TenantID, resource)
		if err != nil {
			http.Error(w, "quota check failed", http.StatusInternalServerError)
			return
		}
		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds()+1)))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}
