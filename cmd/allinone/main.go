// Command allinone runs the poller and the wsserver in a single process,
// connected via the same Redis pub/sub channel.
//
// This is the shape we ship to Render's free tier — they removed background
// workers from the free plan, so the two-service split (cmd/poller +
// cmd/wsserver, see render.yaml comments) only works on paid plans. The
// architecture is unchanged: the poller still publishes to Redis and the
// hub still subscribes from Redis. The only thing that's different is that
// both goroutines live in the same OS process, sharing the Upstash hop.
//
// To scale beyond one machine, swap this binary for the split deployment —
// no code changes required, just docker-compose.yml (or render.yaml on a
// paid plan) wiring two services instead of one.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/RobertoCCC/pt-stocks-stream/internal/poller"
	"github.com/RobertoCCC/pt-stocks-stream/internal/redisbus"
	"github.com/RobertoCCC/pt-stocks-stream/internal/synthetic"
	"github.com/RobertoCCC/pt-stocks-stream/internal/tickers"
	"github.com/RobertoCCC/pt-stocks-stream/internal/wsserver"
	"github.com/RobertoCCC/pt-stocks-stream/internal/yahoo"
	"github.com/redis/go-redis/v9"
)

type config struct {
	addr           string
	redisURL       string
	redisChannel   string
	allowedOrigins []string
	source         string
	interval       time.Duration
	timeout        time.Duration
	maxRetry       int
	logFormat      string
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("allinone", flag.ContinueOnError)
	cfg := config{}
	fs.StringVar(&cfg.addr, "addr", envOr("PORT_ADDR", envOr("WS_ADDR", ":8080")),
		"HTTP listen address")
	fs.StringVar(&cfg.redisURL, "redis-url", envOr("REDIS_URL", ""),
		"redis URL (redis://… or rediss://…); required")
	fs.StringVar(&cfg.redisChannel, "redis-channel", envOr("REDIS_CHANNEL", redisbus.DefaultChannel),
		"pub/sub channel name shared by poller and wsserver")
	origins := fs.String("allowed-origins", envOr("WS_ALLOWED_ORIGINS", ""),
		"comma-separated host patterns allowed to upgrade (empty = any)")
	fs.StringVar(&cfg.source, "source", envOr("POLLER_SOURCE", "synthetic"),
		"quote source: yahoo | synthetic")
	fs.DurationVar(&cfg.interval, "interval", envDuration("POLLER_INTERVAL", 15*time.Second),
		"how often the embedded poller fetches a new batch")
	fs.DurationVar(&cfg.timeout, "timeout", envDuration("POLLER_TIMEOUT", 8*time.Second),
		"per-fetch timeout (must be < interval)")
	fs.IntVar(&cfg.maxRetry, "max-retry", 5,
		"max retries per tick before logging and moving on")
	fs.StringVar(&cfg.logFormat, "log-format", envOr("LOG_FORMAT", "json"),
		"log format: json | text")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if cfg.redisURL == "" {
		return cfg, errors.New("redis-url is required (or REDIS_URL env)")
	}
	if cfg.source != "yahoo" && cfg.source != "synthetic" {
		return cfg, fmt.Errorf("source must be yahoo|synthetic, got %q", cfg.source)
	}
	if cfg.timeout >= cfg.interval {
		return cfg, fmt.Errorf("timeout (%v) must be shorter than interval (%v)", cfg.timeout, cfg.interval)
	}
	if *origins != "" {
		for _, o := range strings.Split(*origins, ",") {
			if trimmed := strings.TrimSpace(o); trimmed != "" {
				cfg.allowedOrigins = append(cfg.allowedOrigins, trimmed)
			}
		}
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

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Single Redis client used by both halves. One TCP connection pool, one
	// place to fail fast on startup if Upstash is unreachable.
	dialCtx, cancelDial := context.WithTimeout(rootCtx, 10*time.Second)
	bus, err := redisbus.New(dialCtx, cfg.redisURL)
	cancelDial()
	if err != nil {
		logger.Error("redis connect", "err", err)
		os.Exit(1)
	}
	defer bus.Close()

	hub := wsserver.NewHub(logger)
	go hub.Run(rootCtx)

	ps, err := bus.Subscribe(rootCtx, cfg.redisChannel)
	if err != nil {
		logger.Error("redis subscribe", "err", err)
		os.Exit(1)
	}
	defer ps.Close()

	go pump(rootCtx, logger, ps.Channel(), hub)

	// Embedded poller. Writes ticks to the same channel the hub subscribes to —
	// same wire format as the split deployment, just shorter round-trip.
	go func() {
		f := pickFetcher(cfg.source)
		w := &poller.RedisWriter{Bus: bus, Channel: cfg.redisChannel}
		logger.Info("embedded poller starting",
			"source", cfg.source,
			"interval", cfg.interval,
			"tickers", len(tickers.PSI20),
		)
		if err := poller.Run(rootCtx, logger, f, w, poller.Config{
			Interval: cfg.interval,
			Timeout:  cfg.timeout,
			MaxRetry: cfg.maxRetry,
		}); err != nil {
			logger.Error("poller run", "err", err)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/ws", wsserver.Handler(hub, logger, wsserver.HandlerOptions{AllowedOrigins: cfg.allowedOrigins}))
	mux.Handle("/healthz", wsserver.HealthHandler(hub))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "pt-stocks-stream allinone — try /ws or /healthz")
	})

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	logger.Info("allinone listening",
		"addr", cfg.addr,
		"channel", cfg.redisChannel,
		"origins", cfg.allowedOrigins,
	)

	serverErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-rootCtx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErr:
		logger.Error("server error", "err", err)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown", "err", err)
	}
}

// pump pulls messages off the Redis subscription and offers them to the hub.
// Identical to cmd/wsserver/pump on purpose: the deploy shape changes, the
// data path doesn't.
func pump(ctx context.Context, logger *slog.Logger, in <-chan *redis.Message, hub *wsserver.Hub) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-in:
			if !ok {
				logger.Warn("redis subscription channel closed")
				return
			}
			if !hub.Broadcast([]byte(msg.Payload)) {
				logger.Warn("hub broadcast buffer full, dropping frame", "channel", msg.Channel)
			}
		}
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
