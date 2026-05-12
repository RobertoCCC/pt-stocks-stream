// Command wsserver fan-outs the PSI-20 stream from Redis pub/sub to all
// connected WebSocket clients.
//
// One process subscribes to Redis once and broadcasts every received frame
// to its in-memory hub; the hub then writes to each client's bounded send
// queue. Multiple replicas can run side-by-side without coordinating —
// each one keeps its own client set and consumes the same Redis channel
// independently, which is exactly the property pub/sub gives us for free.
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

	"github.com/RobertoCCC/pt-stocks-stream/internal/redisbus"
	"github.com/RobertoCCC/pt-stocks-stream/internal/wsserver"
	"github.com/redis/go-redis/v9"
)

type config struct {
	addr           string
	redisURL       string
	redisChannel   string
	allowedOrigins []string
	logFormat      string
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("wsserver", flag.ContinueOnError)
	cfg := config{}
	fs.StringVar(&cfg.addr, "addr", envOr("WS_ADDR", ":8080"),
		"HTTP listen address")
	fs.StringVar(&cfg.redisURL, "redis-url", envOr("REDIS_URL", ""),
		"redis URL (redis://… or rediss://…); required")
	fs.StringVar(&cfg.redisChannel, "redis-channel", envOr("REDIS_CHANNEL", redisbus.DefaultChannel),
		"pub/sub channel name to subscribe to")
	origins := fs.String("allowed-origins", envOr("WS_ALLOWED_ORIGINS", ""),
		"comma-separated host patterns allowed to upgrade (empty = any)")
	fs.StringVar(&cfg.logFormat, "log-format", envOr("LOG_FORMAT", "json"),
		"log format: json | text")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if cfg.redisURL == "" {
		return cfg, errors.New("redis-url is required (or REDIS_URL env)")
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

	mux := http.NewServeMux()
	mux.Handle("/ws", wsserver.Handler(hub, logger, wsserver.HandlerOptions{AllowedOrigins: cfg.allowedOrigins}))
	mux.Handle("/healthz", wsserver.HealthHandler(hub))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "pt-stocks-stream wsserver — try /ws or /healthz")
	})

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	logger.Info("wsserver listening",
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

// pump pulls messages off the Redis subscription and offers them to the
// hub. If the hub broadcast buffer is full we drop and log — better to
// shed one tick than to back-pressure the entire pub/sub chain.
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
				logger.Warn("hub broadcast buffer full, dropping frame",
					"channel", msg.Channel,
				)
			}
		}
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
