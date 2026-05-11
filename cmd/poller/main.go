// Command poller fetches PSI-20 quotes on a fixed interval and publishes
// them to a downstream sink (stdout today, Redis pub/sub next).
//
// The binary is deliberately small: configuration via flags, one ticker loop,
// retries with bounded exponential backoff, and graceful shutdown on SIGTERM.
// The fetcher behind the loop is chosen at startup (real Yahoo Finance or a
// synthetic random walk) so the same binary runs in production and in CI.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/RobertoCCC/pt-stocks-stream/internal/quote"
	"github.com/RobertoCCC/pt-stocks-stream/internal/redisbus"
	"github.com/RobertoCCC/pt-stocks-stream/internal/synthetic"
	"github.com/RobertoCCC/pt-stocks-stream/internal/tickers"
	"github.com/RobertoCCC/pt-stocks-stream/internal/yahoo"
)

// fetcher is the contract the poller depends on. Defined locally so we don't
// drag the yahoo/synthetic packages into a shared interface — idiomatic Go.
type fetcher interface {
	Fetch(ctx context.Context, symbols []string) ([]quote.Quote, error)
}

type config struct {
	source       string
	interval     time.Duration
	maxRetry     int
	timeout      time.Duration
	logFormat    string
	sink         string
	redisURL     string
	redisChannel string
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("poller", flag.ContinueOnError)
	cfg := config{}
	fs.StringVar(&cfg.source, "source", envOr("POLLER_SOURCE", "synthetic"),
		"quote source: yahoo | synthetic")
	fs.DurationVar(&cfg.interval, "interval", envDuration("POLLER_INTERVAL", 15*time.Second),
		"how often to fetch a new batch of quotes")
	fs.IntVar(&cfg.maxRetry, "max-retry", 5,
		"max retries per tick before logging and moving on")
	fs.DurationVar(&cfg.timeout, "timeout", 8*time.Second,
		"per-fetch timeout (must be < interval)")
	fs.StringVar(&cfg.logFormat, "log-format", envOr("LOG_FORMAT", "json"),
		"log format: json | text")
	fs.StringVar(&cfg.sink, "sink", envOr("POLLER_SINK", "stdout"),
		"where to write tick envelopes: stdout | redis")
	fs.StringVar(&cfg.redisURL, "redis-url", envOr("REDIS_URL", ""),
		"redis URL (redis://… or rediss://…); required when -sink=redis")
	fs.StringVar(&cfg.redisChannel, "redis-channel", envOr("REDIS_CHANNEL", redisbus.DefaultChannel),
		"pub/sub channel name used by the wsserver subscribers")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if cfg.timeout >= cfg.interval {
		return cfg, fmt.Errorf("timeout (%v) must be shorter than interval (%v)", cfg.timeout, cfg.interval)
	}
	if cfg.source != "yahoo" && cfg.source != "synthetic" {
		return cfg, fmt.Errorf("source must be yahoo|synthetic, got %q", cfg.source)
	}
	if cfg.sink != "stdout" && cfg.sink != "redis" {
		return cfg, fmt.Errorf("sink must be stdout|redis, got %q", cfg.sink)
	}
	if cfg.sink == "redis" && cfg.redisURL == "" {
		return cfg, fmt.Errorf("sink=redis requires -redis-url (or REDIS_URL)")
	}
	return cfg, nil
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	logger := buildLogger(cfg.logFormat, os.Stderr)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	f := pickFetcher(cfg.source)

	sink, closeSink, err := openSink(ctx, cfg)
	if err != nil {
		logger.Error("sink init", "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := closeSink(); err != nil {
			logger.Warn("sink close", "err", err)
		}
	}()

	logger.Info("poller starting",
		"source", cfg.source,
		"sink", cfg.sink,
		"interval", cfg.interval,
		"tickers", len(tickers.PSI20),
	)

	exitCode := run(ctx, logger, f, sink, cfg)
	os.Exit(exitCode)
}

// run owns the main ticker loop. Extracted so tests can drive it with a fake
// fetcher and a cancellable context.
func run(ctx context.Context, logger *slog.Logger, f fetcher, sink io.Writer, cfg config) int {
	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()

	enc := json.NewEncoder(sink)
	var seq atomic.Uint64

	// Tick immediately on start so we don't wait `interval` before the first
	// poll — keeps boot latency invisible to operators tailing logs.
	tickOnce(ctx, logger, f, enc, &seq, cfg)

	for {
		select {
		case <-ctx.Done():
			logger.Info("poller shutting down", "reason", ctx.Err())
			return 0
		case <-ticker.C:
			tickOnce(ctx, logger, f, enc, &seq, cfg)
		}
	}
}

func tickOnce(parent context.Context, logger *slog.Logger, f fetcher, enc *json.Encoder, seq *atomic.Uint64, cfg config) {
	fetchCtx, cancel := context.WithTimeout(parent, cfg.timeout)
	defer cancel()

	quotes, err := fetchWithRetry(fetchCtx, logger, f, cfg.maxRetry)
	if err != nil {
		logger.Error("fetch failed, skipping tick", "err", err)
		return
	}

	msg := quote.Message{
		Type:     quote.MsgTick,
		Quotes:   quotes,
		Seq:      seq.Add(1),
		ServerTS: time.Now().UnixMilli(),
	}
	if err := enc.Encode(msg); err != nil {
		// Sink errors are not recoverable (stdout closed, broken pipe). Log
		// and let the next tick try again — kept simple intentionally.
		logger.Error("encode message", "err", err)
		return
	}
	logger.Debug("tick published", "seq", msg.Seq, "quotes", len(quotes))
}

// fetchWithRetry wraps a single fetch call with bounded exponential backoff.
// Jitter is full (random in [0, base)) — recommended in AWS's "Exponential
// Backoff And Jitter" article for avoiding thundering herds when many pollers
// retry against the same upstream.
func fetchWithRetry(ctx context.Context, logger *slog.Logger, f fetcher, maxRetry int) ([]quote.Quote, error) {
	const baseDelay = 200 * time.Millisecond
	const capDelay = 5 * time.Second

	var lastErr error
	for attempt := 0; attempt <= maxRetry; attempt++ {
		quotes, err := f.Fetch(ctx, tickers.Symbols())
		if err == nil {
			return quotes, nil
		}
		lastErr = err

		// Permanent failures (4xx that isn't 429): don't retry.
		var httpErr *yahoo.HTTPError
		if errors.As(err, &httpErr) && httpErr.Status >= 400 && httpErr.Status < 500 && httpErr.Status != 429 {
			return nil, err
		}

		if attempt == maxRetry {
			break
		}
		delay := backoff(attempt, baseDelay, capDelay)
		logger.Warn("fetch error, retrying", "attempt", attempt+1, "delay", delay, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, lastErr
}

func backoff(attempt int, base, cap time.Duration) time.Duration {
	// 2^attempt * base, capped, then jittered to [0, computed).
	exp := base << attempt
	if exp <= 0 || exp > cap {
		exp = cap
	}
	//nolint:gosec // not used for cryptography — full-jitter backoff
	return time.Duration(float64(exp) * randFloat())
}

// randFloat is var-scoped so tests can stub determinism if needed. Default
// uses the math/rand/v2 global which is automatically seeded per process.
var randFloat = defaultRandFloat

func pickFetcher(source string) fetcher {
	switch source {
	case "yahoo":
		return yahoo.New()
	default:
		return synthetic.New()
	}
}

func buildLogger(format string, w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	switch format {
	case "text":
		return slog.New(slog.NewTextHandler(w, opts))
	default:
		return slog.New(slog.NewJSONHandler(w, opts))
	}
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
