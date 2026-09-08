package ratelimiter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"time"
)

const tokenBucketLua = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local state = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if not tokens or not ts then
  tokens = capacity
  ts = now
end
if now > ts then
  tokens = math.min(capacity, tokens + (now - ts) * rate / 1000000.0)
  ts = now
end

local allowed = 0
local retry_us = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_us = math.ceil((cost - tokens) / rate * 1000000.0)
end
redis.call("HSET", key, "tokens", tokens, "ts", ts)
redis.call("EXPIRE", key, ttl)
return {allowed, math.floor(tokens), retry_us, math.max(0, math.ceil((capacity - tokens) / rate * 1000000.0))}
`

const slidingWindowLua = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local member = ARGV[5]
local ttl = tonumber(ARGV[6])

redis.call("ZREMRANGEBYSCORE", key, "-inf", now - window)
local count = redis.call("ZCARD", key)
local allowed = 0
local retry_ms = 0
if count + cost <= limit then
  for i = 1, cost do
    redis.call("ZADD", key, now, member .. ":" .. i)
  end
  count = count + cost
  allowed = 1
else
  local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
  if oldest[2] then
    retry_ms = math.max(0, tonumber(oldest[2]) + window - now)
  end
end
redis.call("EXPIRE", key, ttl)
return {allowed, math.max(0, limit - count), retry_ms, window}
`

func (l *Limiter) key(key string, suffix string) string {
	// The digest both bounds Redis key size and makes Redis Cluster hash tags
	// stable without exposing arbitrary tenant identifiers in key names.
	sum := sha256Sum(key)
	return fmt.Sprintf("%s:{%s}:%s", l.prefix, sum, suffix)
}

func (l *Limiter) allowTokenBucket(ctx context.Context, key string, cost int64, p Policy) (Result, error) {
	now := l.clock.Now().UnixMicro()
	ttl := int64(math.Ceil(float64(p.Capacity) / p.Rate * 2))
	if ttl < 1 {
		ttl = 1
	}
	cmd := l.client.Eval(ctx, tokenBucketLua, []string{l.key(key, "tb")},
		now, p.Rate, p.Capacity, cost, ttl)
	values, err := cmd.Result()
	if err != nil {
		return Result{}, redisError(err)
	}
	allowed, remaining, retryUS, resetUS, err := parseInts(values, 4)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Allowed: allowed == 1, Remaining: remaining,
		RetryAfter: time.Duration(retryUS) * time.Microsecond,
		ResetAfter: time.Duration(resetUS) * time.Microsecond,
		Algorithm:  TokenBucket,
	}, nil
}

func (l *Limiter) allowSlidingWindow(ctx context.Context, key string, cost int64, p Policy) (Result, error) {
	now := l.clock.Now().UnixMilli()
	ttl := int64(math.Ceil(p.Window.Seconds())) + 1
	if ttl < 1 {
		ttl = 1
	}
	member, err := randomID()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimiter: generate request id: %w", err)
	}
	cmd := l.client.Eval(ctx, slidingWindowLua, []string{l.key(key, "sw")},
		now, p.Window.Milliseconds(), p.Limit, cost, member, ttl)
	values, err := cmd.Result()
	if err != nil {
		return Result{}, redisError(err)
	}
	allowed, remaining, retryMS, resetMS, err := parseInts(values, 4)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Allowed: allowed == 1, Remaining: remaining,
		RetryAfter: time.Duration(retryMS) * time.Millisecond,
		ResetAfter: time.Duration(resetMS) * time.Millisecond,
		Algorithm:  SlidingWindow,
	}, nil
}

func randomID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func parseInts(raw interface{}, want int) (int64, int64, int64, int64, error) {
	values, ok := raw.([]interface{})
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("ratelimiter: malformed redis response type %T", raw)
	}
	if len(values) != want {
		return 0, 0, 0, 0, fmt.Errorf("ratelimiter: malformed redis response: got %d values", len(values))
	}
	result := [4]int64{}
	for i, value := range values {
		switch v := value.(type) {
		case int64:
			result[i] = v
		case int:
			result[i] = int64(v)
		case string:
			parsed, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return 0, 0, 0, 0, fmt.Errorf("ratelimiter: malformed redis response: %w", err)
			}
			result[i] = parsed
		default:
			return 0, 0, 0, 0, fmt.Errorf("ratelimiter: malformed redis response type %T", value)
		}
	}
	return result[0], result[1], result[2], result[3], nil
}
