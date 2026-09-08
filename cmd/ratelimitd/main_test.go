package main

import (
	"strings"
	"testing"
)

func TestLoadPolicyRejectsMalformedEnvironment(t *testing.T) {
	t.Setenv("RATE_LIMIT_RATE", "not-a-number")
	if _, err := loadPolicy(); err == nil {
		t.Fatal("expected malformed rate to fail")
	}
}

func TestLoadPolicyUsesDefaults(t *testing.T) {
	t.Setenv("RATE_LIMIT_RATE", "")
	t.Setenv("RATE_LIMIT_CAPACITY", "")
	t.Setenv("RATE_LIMIT_LIMIT", "")
	t.Setenv("RATE_LIMIT_WINDOW_SECONDS", "")
	policy, err := loadPolicy()
	if err != nil {
		t.Fatalf("loadPolicy: %v", err)
	}
	if policy.Rate != 10 || policy.Capacity != 20 || policy.Limit != 20 {
		t.Fatalf("unexpected defaults: %+v", policy)
	}
}

func TestLoadPolicyReportsVariableName(t *testing.T) {
	t.Setenv("RATE_LIMIT_WINDOW_SECONDS", "bad")
	_, err := loadPolicy()
	if err == nil {
		t.Fatalf("expected named parse error, got %v", err)
	}
	if got := err.Error(); got == "" || !strings.Contains(got, "RATE_LIMIT_WINDOW_SECONDS") {
		t.Fatalf("error=%q, want variable name", got)
	}
}
