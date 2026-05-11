package main

import (
	"context"
	"errors"
	"io"
	"os"

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
		w := &redisWriter{bus: bus, channel: cfg.redisChannel}
		return w, bus.Close, nil
	default:
		return nil, nil, errors.New("sink must be stdout|redis, got " + cfg.sink)
	}
}

// redisWriter adapts a redis pub/sub channel to io.Writer so json.Encoder
// can drive it unchanged. Each Write becomes one PUBLISH; this matches how
// json.Encoder.Encode emits exactly one frame per call (object + newline).
type redisWriter struct {
	bus     *redisbus.Bus
	channel string
}

// Write publishes the payload as-is. The newline appended by json.Encoder
// is harmless on the wire and helps when humans inspect the channel with
// `redis-cli SUBSCRIBE`.
func (w *redisWriter) Write(p []byte) (int, error) {
	// Use Background here: the json.Encoder doesn't carry our run context,
	// and a per-tick publish should respect the network's own timeout via
	// go-redis defaults rather than be cut short by a stale deadline.
	if err := w.bus.Publish(context.Background(), w.channel, p); err != nil {
		return 0, err
	}
	return len(p), nil
}
