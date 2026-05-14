package main

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/RobertoCCC/pt-stocks-stream/internal/poller"
	"github.com/RobertoCCC/pt-stocks-stream/internal/redisbus"
)

// sinkKind selects where each tick envelope is written.
type sinkKind string

const (
	sinkStdout sinkKind = "stdout"
	sinkRedis  sinkKind = "redis"
)

// openSink returns the writer used by the ticker loop and a cleanup function.
// The cleanup is called once the parent context exits, so the redis client
// can drain in-flight publishes before the process dies.
func openSink(ctx context.Context, cfg config) (io.Writer, func() error, error) {
	switch sinkKind(cfg.sink) {
	case sinkStdout, "":
		return os.Stdout, func() error { return nil }, nil
	case sinkRedis:
		if cfg.redisURL == "" {
			return nil, nil, errors.New("sink=redis requires -redis-url (or REDIS_URL)")
		}
		bus, err := redisbus.New(ctx, cfg.redisURL)
		if err != nil {
			return nil, nil, err
		}
		w := &poller.RedisWriter{Bus: bus, Channel: cfg.redisChannel}
		return w, bus.Close, nil
	default:
		return nil, nil, errors.New("sink must be stdout|redis, got " + cfg.sink)
	}
}
