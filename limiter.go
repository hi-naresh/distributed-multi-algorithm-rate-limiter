// Package ratelimiter provides a Redis-backed, horizontally scalable rate limiter.
//
// Each decision is made by a single Redis Lua script, so all replicas sharing
// Redis observe the same atomic state. The package intentionally does not keep
// admission state in process memory.
package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrInvalidKey       = errors.New("ratelimiter: key must not be empty")
	ErrInvalidCost      = errors.New("ratelimiter: cost must be greater than zero")
	ErrInvalidPolicy    = errors.New("ratelimiter: invalid policy")
	ErrUnsupportedAlgo  = errors.New("ratelimiter: unsupported algorithm")
	ErrRedisUnavailable = errors.New("ratelimiter: redis unavailable")
)

type Algorithm string

const (
	TokenBucket   Algorithm = "token_bucket"
	SlidingWindow Algorithm = "sliding_window"
)

// Policy controls one key. Limit is the maximum number of requests in a
// sliding window, or the refill rate (tokens/second) for a token bucket.
type Policy struct {
	Algorithm Algorithm
	Limit     int64
	Window    time.Duration // sliding-window interval
	Rate      float64       // token-bucket tokens per second
	Capacity  int64         // token-bucket burst capacity
}

func (p Policy) validate() error {
	switch p.Algorithm {
	case TokenBucket:
		if p.Rate <= 0 || math.IsNaN(p.Rate) || math.IsInf(p.Rate, 0) || p.Capacity <= 0 {
			return fmt.Errorf("%w: token bucket requires positive finite rate and capacity", ErrInvalidPolicy)
		}
	case SlidingWindow:
		if p.Limit <= 0 || p.Window < time.Millisecond {
			return fmt.Errorf("%w: sliding window requires positive limit and window of at least 1ms", ErrInvalidPolicy)
		}
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedAlgo, p.Algorithm)
	}
	return nil
}

// Clock is injectable to make decisions deterministic in tests.
type Clock interface{ Now() time.Time }

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// PolicyResolver can implement dynamic policy lookup. It must be safe for
// concurrent calls. An error fails closed and is returned to the caller.
type PolicyResolver func(key string) (Policy, error)

type Config struct {
	Prefix         string
	DefaultPolicy  Policy
	Policies       map[string]Policy
	PolicyResolver PolicyResolver
	Clock          Clock
}

type Result struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
	ResetAfter time.Duration
	Algorithm  Algorithm
}

// RedisClient is the subset of go-redis needed by Limiter. redis.UniversalClient
// is safe for concurrent use and works with a single Redis server, Sentinel,
// or Cluster.
type RedisClient interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
}

type Limiter struct {
	client        RedisClient
	prefix        string
	defaultPolicy Policy
	policies      map[string]Policy
	resolve       PolicyResolver
	clock         Clock
	mu            sync.RWMutex
}

func New(client RedisClient, cfg Config) (*Limiter, error) {
	if client == nil {
		return nil, errors.New("ratelimiter: nil redis client")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "ratelimiter"
	}
	if strings.ContainsAny(cfg.Prefix, " \t\r\n") {
		return nil, fmt.Errorf("%w: prefix contains whitespace", ErrInvalidPolicy)
	}
	if err := cfg.DefaultPolicy.validate(); err != nil {
		return nil, err
	}
	for key, policy := range cfg.Policies {
		if key == "" {
			return nil, fmt.Errorf("%w: empty policy key", ErrInvalidKey)
		}
		if err := policy.validate(); err != nil {
			return nil, fmt.Errorf("policy %q: %w", key, err)
		}
	}
	if cfg.Clock == nil {
		cfg.Clock = wallClock{}
	}
	policies := make(map[string]Policy, len(cfg.Policies))
	for key, policy := range cfg.Policies {
		policies[key] = policy
	}
	return &Limiter{
		client: client, prefix: cfg.Prefix, defaultPolicy: cfg.DefaultPolicy,
		policies: policies, resolve: cfg.PolicyResolver, clock: cfg.Clock,
	}, nil
}

func (l *Limiter) policy(key string) (Policy, error) {
	l.mu.RLock()
	policy, ok := l.policies[key]
	resolver, defaultPolicy := l.resolve, l.defaultPolicy
	l.mu.RUnlock()
	if ok {
		return policy, nil
	}
	if resolver != nil {
		policy, err := resolver(key)
		if err != nil {
			return Policy{}, err
		}
		if err := policy.validate(); err != nil {
			return Policy{}, err
		}
		return policy, nil
	}
	return defaultPolicy, nil
}

// Allow consumes cost units for key and returns the atomic decision.
func (l *Limiter) Allow(ctx context.Context, key string, cost int64) (Result, error) {
	if strings.TrimSpace(key) == "" {
		return Result{}, ErrInvalidKey
	}
	if cost <= 0 {
		return Result{}, ErrInvalidCost
	}
	policy, err := l.policy(key)
	if err != nil {
		return Result{}, err
	}
	if err := policy.validate(); err != nil {
		return Result{}, err
	}
	if (policy.Algorithm == TokenBucket && cost > policy.Capacity) ||
		(policy.Algorithm == SlidingWindow && cost > policy.Limit) {
		return Result{}, fmt.Errorf("%w: cost exceeds policy maximum", ErrInvalidCost)
	}
	switch policy.Algorithm {
	case TokenBucket:
		return l.allowTokenBucket(ctx, key, cost, policy)
	case SlidingWindow:
		return l.allowSlidingWindow(ctx, key, cost, policy)
	default:
		return Result{}, fmt.Errorf("%w: %q", ErrUnsupportedAlgo, policy.Algorithm)
	}
}

func redisError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %v", ErrRedisUnavailable, err)
}
