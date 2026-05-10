// Package redisbus is a thin wrapper around go-redis pub/sub. It exists so
// the poller and the WebSocket server can share one connection-and-channel
// contract without spreading go-redis types across the codebase.
//
// The pub/sub model is intentional: each fan-out replica subscribes to the
// same channel and pushes frames to its own WebSocket clients. A single
// publisher (the poller) drives the whole mesh, which keeps Redis usage
// constant regardless of how many readers we scale out.
package redisbus

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// DefaultChannel is the pub/sub channel used by the PSI-20 stream. Exported
// so both publisher and subscriber binaries can reference the same value.
const DefaultChannel = "psi20.ticks"

// Bus owns a Redis client. The zero value is not usable; construct with New.
type Bus struct {
	client *redis.Client
}

// New connects to Redis using a redis://… or rediss://… URL and verifies the
// link with a PING before returning. The context bounds the dial+ping; long
// reads/writes during normal use are bounded per-call by the caller.
func New(ctx context.Context, url string) (*Bus, error) {
	if url == "" {
		return nil, errors.New("redisbus: empty url")
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redisbus: parse url: %w", err)
	}
	client := redis.NewClient(opt)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisbus: ping: %w", err)
	}
	return &Bus{client: client}, nil
}

// NewFromClient wraps an already-configured *redis.Client. Useful for tests
// that point go-redis at a miniredis instance.
func NewFromClient(client *redis.Client) *Bus {
	return &Bus{client: client}
}

// Close releases the underlying connections. Safe to call multiple times;
// the second call returns whatever go-redis decides (usually nil).
func (b *Bus) Close() error {
	if b == nil || b.client == nil {
		return nil
	}
	return b.client.Close()
}

// Publish pushes a single payload to the channel. Callers pass already-
// encoded JSON; the bus is byte-oriented so we don't double-encode.
func (b *Bus) Publish(ctx context.Context, channel string, payload []byte) error {
	if err := b.client.Publish(ctx, channel, payload).Err(); err != nil {
		return fmt.Errorf("redisbus: publish: %w", err)
	}
	return nil
}

// Subscribe returns the raw *redis.PubSub so callers can stream messages
// over its channel. The handshake (Receive) is performed here so the caller
// can fail fast if the subscription cannot be established.
func (b *Bus) Subscribe(ctx context.Context, channel string) (*redis.PubSub, error) {
	ps := b.client.Subscribe(ctx, channel)
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return nil, fmt.Errorf("redisbus: subscribe: %w", err)
	}
	return ps, nil
}
