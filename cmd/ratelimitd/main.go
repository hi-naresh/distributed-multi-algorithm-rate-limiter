// ratelimitd is a small HTTP adapter around the ratelimiter package.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/example/distributedratelimiter"
)

type allowResponse struct {
	Allowed    bool   `json:"allowed"`
	Remaining  int64  `json:"remaining"`
	RetryAfter int64  `json:"retry_after_ms"`
	ResetAfter int64  `json:"reset_after_ms"`
	Algorithm  string `json:"algorithm"`
}

func main() {
	addr := env("ADDR", ":8080")
	redisAddr := env("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: os.Getenv("REDIS_PASSWORD")})
	defer rdb.Close()

	policy, err := loadPolicy()
	if err != nil {
		log.Fatal(err)
	}
	limiter, err := ratelimiter.New(rdb, ratelimiter.Config{Prefix: env("REDIS_PREFIX", "ratelimiter"), DefaultPolicy: policy})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/v1/allow", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		cost := int64(1)
		if value := r.URL.Query().Get("cost"); value != "" {
			var parseErr error
			cost, parseErr = strconv.ParseInt(value, 10, 64)
			if parseErr != nil {
				http.Error(w, "invalid cost", http.StatusBadRequest)
				return
			}
		}
		result, err := limiter.Allow(r.Context(), key, cost)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, ratelimiter.ErrInvalidKey) || errors.Is(err, ratelimiter.ErrInvalidCost) {
				status = http.StatusBadRequest
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(allowResponse{
			Allowed: result.Allowed, Remaining: result.Remaining,
			RetryAfter: result.RetryAfter.Milliseconds(), ResetAfter: result.ResetAfter.Milliseconds(),
			Algorithm: string(result.Algorithm),
		})
	})

	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("ratelimitd listening on %s", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func loadPolicy() (ratelimiter.Policy, error) {
	rate, err := envFloat("RATE_LIMIT_RATE", 10)
	if err != nil {
		return ratelimiter.Policy{}, err
	}
	capacity, err := envInt64("RATE_LIMIT_CAPACITY", 20)
	if err != nil {
		return ratelimiter.Policy{}, err
	}
	limit, err := envInt64("RATE_LIMIT_LIMIT", 20)
	if err != nil {
		return ratelimiter.Policy{}, err
	}
	windowSeconds, err := envInt64("RATE_LIMIT_WINDOW_SECONDS", 60)
	if err != nil {
		return ratelimiter.Policy{}, err
	}
	return ratelimiter.Policy{
		Algorithm: algorithm(env("RATE_LIMIT_ALGORITHM", string(ratelimiter.TokenBucket))),
		Rate:      rate,
		Capacity:  capacity,
		Limit:     limit,
		Window:    time.Duration(windowSeconds) * time.Second,
	}, nil
}

func envInt64(name string, fallback int64) (int64, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return parsed, nil
}

func envFloat(name string, fallback float64) (float64, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return parsed, nil
}

func algorithm(value string) ratelimiter.Algorithm {
	return ratelimiter.Algorithm(value)
}
