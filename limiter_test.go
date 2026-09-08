package ratelimiter

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type testClock struct {
	mu sync.RWMutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.t
}
func (c *testClock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestLimiter(t *testing.T, policy Policy, clock Clock) (*Limiter, *redis.Client, *miniredis.Miniredis) {
	t.Helper()
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
		server.Close()
	})
	limiter, err := New(client, Config{DefaultPolicy: policy, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	return limiter, client, server
}

func TestTokenBucket(t *testing.T) {
	clock := &testClock{t: time.Unix(100, 0)}
	limiter, _, _ := newTestLimiter(t, Policy{Algorithm: TokenBucket, Rate: 2, Capacity: 3}, clock)

	for i := 0; i < 3; i++ {
		result, err := limiter.Allow(context.Background(), "tenant-a", 1)
		if err != nil || !result.Allowed || result.Remaining != int64(2-i) {
			t.Fatalf("request %d: result=%+v err=%v", i, result, err)
		}
	}
	result, err := limiter.Allow(context.Background(), "tenant-a", 1)
	if err != nil || result.Allowed || result.RetryAfter <= 0 {
		t.Fatalf("expected denial with retry: result=%+v err=%v", result, err)
	}
	clock.Add(500 * time.Millisecond)
	result, err = limiter.Allow(context.Background(), "tenant-a", 1)
	if err != nil || !result.Allowed {
		t.Fatalf("expected refill: result=%+v err=%v", result, err)
	}
}

func TestSlidingWindowAndIndependentKeys(t *testing.T) {
	clock := &testClock{t: time.Unix(200, 0)}
	limiter, _, _ := newTestLimiter(t, Policy{Algorithm: SlidingWindow, Limit: 2, Window: time.Second}, clock)
	for _, key := range []string{"a", "a"} {
		result, err := limiter.Allow(context.Background(), key, 1)
		if err != nil || !result.Allowed {
			t.Fatalf("expected allowed: %+v %v", result, err)
		}
	}
	result, err := limiter.Allow(context.Background(), "a", 1)
	if err != nil || result.Allowed || result.RetryAfter <= 0 {
		t.Fatalf("expected window denial: %+v %v", result, err)
	}
	result, err = limiter.Allow(context.Background(), "b", 1)
	if err != nil || !result.Allowed {
		t.Fatalf("keys should be independent: %+v %v", result, err)
	}
	clock.Add(time.Second)
	result, err = limiter.Allow(context.Background(), "a", 1)
	if err != nil || !result.Allowed {
		t.Fatalf("expected expired window: %+v %v", result, err)
	}
}

func TestPoliciesAndValidation(t *testing.T) {
	clock := &testClock{t: time.Unix(300, 0)}
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	limiter, err := New(client, Config{
		DefaultPolicy: Policy{Algorithm: TokenBucket, Rate: 1, Capacity: 1},
		Policies: map[string]Policy{
			"window": {Algorithm: SlidingWindow, Limit: 1, Window: time.Minute},
		},
		Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := limiter.Allow(context.Background(), "", 1); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("empty key error = %v", err)
	}
	if _, err := limiter.Allow(context.Background(), "x", 0); !errors.Is(err, ErrInvalidCost) {
		t.Fatalf("zero cost error = %v", err)
	}
	if _, err := limiter.Allow(context.Background(), "x", 2); !errors.Is(err, ErrInvalidCost) {
		t.Fatalf("oversized cost error = %v", err)
	}
	if _, err := limiter.Allow(nil, "x", 1); !errors.Is(err, ErrInvalidContext) {
		t.Fatalf("nil context error = %v", err)
	}
	result, err := limiter.Allow(context.Background(), "window", 1)
	if err != nil || result.Algorithm != SlidingWindow {
		t.Fatalf("per-key policy not selected: %+v %v", result, err)
	}
}

func TestAtomicAcrossLimiterInstances(t *testing.T) {
	clock := &testClock{t: time.Unix(400, 0)}
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client1 := redis.NewClient(&redis.Options{Addr: server.Addr()})
	client2 := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client1.Close()
	defer client2.Close()
	policy := Policy{Algorithm: SlidingWindow, Limit: 25, Window: time.Minute}
	one, err := New(client1, Config{DefaultPolicy: policy, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	two, err := New(client2, Config{DefaultPolicy: policy, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l := one
			if i%2 == 0 {
				l = two
			}
			result, err := l.Allow(context.Background(), "shared", 1)
			if err != nil {
				t.Errorf("allow: %v", err)
				return
			}
			if result.Allowed {
				allowed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if got := allowed.Load(); got != 25 {
		t.Fatalf("atomic limit violated: allowed %d", got)
	}
}

func TestContextAndRedisErrors(t *testing.T) {
	clock := &testClock{t: time.Unix(500, 0)}
	limiter, client, server := newTestLimiter(t, Policy{Algorithm: TokenBucket, Rate: 1, Capacity: 1}, clock)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := limiter.Allow(ctx, "x", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v", err)
	}
	server.Close()
	if _, err := limiter.Allow(context.Background(), "x", 1); !errors.Is(err, ErrRedisUnavailable) {
		t.Fatalf("redis error = %v", err)
	}
	_ = client
}

func TestResolverValidation(t *testing.T) {
	clock := &testClock{t: time.Unix(600, 0)}
	limiter, _, _ := newTestLimiter(t, Policy{Algorithm: TokenBucket, Rate: 1, Capacity: 1}, clock)
	limiter.resolve = func(string) (Policy, error) {
		return Policy{Algorithm: "unknown"}, nil
	}
	if _, err := limiter.Allow(context.Background(), "x", 1); !errors.Is(err, ErrUnsupportedAlgo) {
		t.Fatalf("resolver policy error = %v", err)
	}
}

func TestResolverErrorsAreReturned(t *testing.T) {
	clock := &testClock{t: time.Unix(650, 0)}
	limiter, _, _ := newTestLimiter(t, Policy{Algorithm: TokenBucket, Rate: 1, Capacity: 1}, clock)
	want := errors.New("policy backend unavailable")
	limiter.resolve = func(string) (Policy, error) {
		return Policy{}, want
	}
	if _, err := limiter.Allow(context.Background(), "x", 1); !errors.Is(err, want) {
		t.Fatalf("resolver error = %v, want %v", err, want)
	}
}

func TestConfigurationEdgeCases(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	valid := Policy{Algorithm: TokenBucket, Rate: 1, Capacity: 1}
	tests := []struct {
		name string
		cfg  Config
		err  error
	}{
		{"missing default", Config{}, ErrUnsupportedAlgo},
		{"whitespace prefix", Config{Prefix: "bad prefix", DefaultPolicy: valid}, ErrInvalidPolicy},
		{"sub-millisecond window", Config{DefaultPolicy: Policy{Algorithm: SlidingWindow, Limit: 1, Window: time.Microsecond}}, ErrInvalidPolicy},
		{"empty policy key", Config{DefaultPolicy: valid, Policies: map[string]Policy{"": valid}}, ErrInvalidKey},
		{"zero token rate", Config{DefaultPolicy: Policy{Algorithm: TokenBucket, Rate: 0, Capacity: 1}}, ErrInvalidPolicy},
		{"infinite token rate", Config{DefaultPolicy: Policy{Algorithm: TokenBucket, Rate: math.Inf(1), Capacity: 1}}, ErrInvalidPolicy},
		{"zero token capacity", Config{DefaultPolicy: Policy{Algorithm: TokenBucket, Rate: 1, Capacity: 0}}, ErrInvalidPolicy},
		{"unsafe token capacity", Config{DefaultPolicy: Policy{Algorithm: TokenBucket, Rate: 1, Capacity: maxScriptInteger + 1}}, ErrInvalidPolicy},
		{"zero window limit", Config{DefaultPolicy: Policy{Algorithm: SlidingWindow, Limit: 0, Window: time.Second}}, ErrInvalidPolicy},
		{"unsafe window limit", Config{DefaultPolicy: Policy{Algorithm: SlidingWindow, Limit: maxScriptInteger + 1, Window: time.Second}}, ErrInvalidPolicy},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(client, test.cfg)
			if !errors.Is(err, test.err) {
				t.Fatalf("error=%v, want errors.Is(..., %v)", err, test.err)
			}
		})
	}
	if _, err := New(nil, Config{DefaultPolicy: valid}); err == nil {
		t.Fatal("nil Redis client should fail")
	}
}
