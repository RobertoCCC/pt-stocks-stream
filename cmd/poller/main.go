// Command poller fetches PSI-20 quotes on a fixed interval and publishes
// them to a downstream sink (stdout for local dev, Redis pub/sub for the
// split-services deployment shape).
//
// The actual ticker loop, retries, and JSON envelope live in
// internal/poller — this binary is just flag parsing and wiring. The
// cmd/allinone binary reuses internal/poller on a different shape (poller
// and wsserver collocated in a single process, required by the Render free
// tier which no longer supports background workers).
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RobertoCCC/pt-stocks-stream/internal/poller"
	"github.com/RobertoCCC/pt-stocks-stream/internal/redisbus"
	"github.com/RobertoCCC/pt-stocks-stream/internal/synthetic"
	"github.com/RobertoCCC/pt-stocks-stream/internal/tickers"
	"github.com/RobertoCCC/pt-stocks-stream/internal/yahoo"
)

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

	if err := poller.Run(ctx, logger, f, sink, poller.Config{
		Interval: cfg.interval,
		Timeout:  cfg.timeout,
		MaxRetry: cfg.maxRetry,
	}); err != nil {
		logger.Error("poller run", "err", err)
		os.Exit(1)
	}
}

func pickFetcher(source string) poller.Fetcher {
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
