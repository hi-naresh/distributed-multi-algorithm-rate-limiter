# Distributed Redis Rate Limiter

`ratelimiter` is a standalone Go library for horizontally scaled services. It
keeps all admission state in Redis and uses one Lua script per decision, so
replicas do not rely on local memory or coordination.

## Usage

```go
client := redis.NewClient(&redis.Options{Addr: "redis:6379"})
limiter, err := ratelimiter.New(client, ratelimiter.Config{
    Prefix: "checkout",
    DefaultPolicy: ratelimiter.Policy{
        Algorithm: ratelimiter.TokenBucket,
        Rate: 100, Capacity: 200,
    },
    Policies: map[string]ratelimiter.Policy{
        "enterprise": {
            Algorithm: ratelimiter.SlidingWindow,
            Limit: 1000, Window: time.Minute,
        },
    },
})
if err != nil { /* configuration error */ }

decision, err := limiter.Allow(ctx, customerID, 1)
if err != nil { /* context, policy, or Redis error */ }
if !decision.Allowed {
    // decision.RetryAfter is a server-computed duration.
}
```

`Allow` is context-aware and safe for concurrent callers. A cost greater than
the configured burst/window limit is rejected because it can never succeed.
Policies are immutable after construction; use `PolicyResolver` when policy
data is managed elsewhere. Resolver implementations must be concurrency-safe.
Limits and capacities are capped at `2^53-1`, matching the exact integer range
of the Redis Lua runtime used by the atomic scripts.

## Algorithms

* **Token bucket**: fractional tokens refill at `Rate` per second up to
  `Capacity`. It permits bursts and returns an estimate of the time until the
  requested cost is available.
* **Sliding window**: each accepted unit is a sorted-set member with a
  millisecond timestamp. Expired members are removed atomically before the
  limit is checked. This is precise, but uses one Redis sorted-set member per
  unit and therefore has higher memory cost than a coarse counter.

## Redis consistency and key strategy

Each operation performs its read, expiry cleanup, decision, and write in one
Lua `EVAL`; concurrent requests cannot oversubscribe a limit. Keys are
`<prefix>:{sha256(logical-key)}:<algorithm>`. The hash tag keeps a key's state
on one Redis Cluster slot while avoiding unbounded or sensitive identifiers in
key names. Keys expire automatically after an inactivity-based TTL.

The supplied clock is used to timestamp operations. Production deployments
should use synchronized host clocks (or provide a clock backed by a trusted
time source); Redis does not correct client timestamp skew. Sliding-window
costs are represented as one sorted-set member per unit, so large costs should
be avoided or bounded by the caller. Redis Cluster,
Sentinel, or a managed Redis service can be used through `redis.UniversalClient`.
Use a low-latency Redis topology and size connection pools for peak request
concurrency. A Redis outage is returned as `ErrRedisUnavailable`; callers must
choose their fail-open/fail-closed behavior.

## HTTP service

`cmd/ratelimitd` is a minimal adapter for deployments that prefer a service.
Run it with `REDIS_ADDR=redis:6379 go run ./cmd/ratelimitd`, then call:

```text
GET /v1/allow?key=customer-42&cost=1
GET /healthz
```

Configuration is provided through `ADDR`, `REDIS_ADDR`, `REDIS_PASSWORD`,
`REDIS_PREFIX`, `RATE_LIMIT_ALGORITHM`, `RATE_LIMIT_RATE`,
`RATE_LIMIT_CAPACITY`, `RATE_LIMIT_LIMIT`, and
`RATE_LIMIT_WINDOW_SECONDS`. Invalid numeric environment values fail startup
instead of silently selecting a fallback. Put TLS, authentication, and request-level
authorization at the service boundary or ingress.

## Testing

```sh
go test ./...
go test -race ./...
```

Tests use `miniredis`, an injectable deterministic clock, multiple limiter
instances, cancellation, validation, expiry/refill behavior, and concurrent
atomicity checks.
