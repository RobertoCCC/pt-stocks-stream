package main

import (
	"strings"
	"testing"
)

func TestParseFlagsRequiresRedisURL(t *testing.T) {
	t.Setenv("REDIS_URL", "")
	_, err := parseFlags([]string{})
	if err == nil || !strings.Contains(err.Error(), "redis-url") {
		t.Errorf("expected redis-url required, got %v", err)
	}
}

func TestParseFlagsRejectsTimeoutGeInterval(t *testing.T) {
	_, err := parseFlags([]string{
		"-redis-url=redis://localhost:6379",
		"-interval=2s",
		"-timeout=2s",
	})
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Errorf("expected timeout-vs-interval error, got %v", err)
	}
}

func TestParseFlagsRejectsUnknownSource(t *testing.T) {
	_, err := parseFlags([]string{
		"-redis-url=redis://localhost:6379",
		"-source=alpaca",
	})
	if err == nil || !strings.Contains(err.Error(), "source must") {
		t.Errorf("expected source validation error, got %v", err)
	}
}

func TestParseFlagsAcceptsAllowedOriginsCSV(t *testing.T) {
	cfg, err := parseFlags([]string{
		"-redis-url=redis://localhost:6379",
		"-allowed-origins=app.example.com, foo.bar",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	want := []string{"app.example.com", "foo.bar"}
	if len(cfg.allowedOrigins) != len(want) {
		t.Fatalf("origins=%v, want %v", cfg.allowedOrigins, want)
	}
	for i := range want {
		if cfg.allowedOrigins[i] != want[i] {
			t.Errorf("origins[%d]=%q, want %q", i, cfg.allowedOrigins[i], want[i])
		}
	}
}
