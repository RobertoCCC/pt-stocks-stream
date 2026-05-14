package main

import (
	"context"
	"strings"
	"testing"
)

func TestParseFlagsRejectsTimeoutGeInterval(t *testing.T) {
	_, err := parseFlags([]string{"-interval=2s", "-timeout=2s"})
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Errorf("expected timeout-vs-interval error, got %v", err)
	}
}

func TestParseFlagsRejectsUnknownSource(t *testing.T) {
	_, err := parseFlags([]string{"-source=alpaca"})
	if err == nil {
		t.Fatal("expected error for unknown source")
	}
}

func TestParseFlagsRejectsUnknownSink(t *testing.T) {
	_, err := parseFlags([]string{"-sink=kafka"})
	if err == nil || !strings.Contains(err.Error(), "sink must") {
		t.Errorf("expected sink validation error, got %v", err)
	}
}

func TestParseFlagsRedisSinkRequiresURL(t *testing.T) {
	t.Setenv("REDIS_URL", "") // ensure env doesn't leak into the assertion
	_, err := parseFlags([]string{"-sink=redis"})
	if err == nil || !strings.Contains(err.Error(), "redis-url") {
		t.Errorf("expected redis-url required error, got %v", err)
	}
}

func TestOpenSinkRejectsUnknown(t *testing.T) {
	_, _, err := openSink(context.Background(), config{sink: "kafka"})
	if err == nil {
		t.Fatal("expected error for unknown sink")
	}
}

func TestOpenSinkStdoutReturnsNoop(t *testing.T) {
	w, closeFn, err := openSink(context.Background(), config{sink: "stdout"})
	if err != nil {
		t.Fatalf("openSink: %v", err)
	}
	if w == nil {
		t.Fatal("expected non-nil writer for stdout sink")
	}
	if err := closeFn(); err != nil {
		t.Errorf("close stdout sink: %v", err)
	}
}
