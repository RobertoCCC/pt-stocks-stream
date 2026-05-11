package main

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RobertoCCC/pt-stocks-stream/internal/quote"
	"github.com/RobertoCCC/pt-stocks-stream/internal/redisbus"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestRedisSinkEndToEnd starts an in-memory Redis, wires tickOnce to a
// redisWriter, and asserts that a subscriber sees the JSON envelope.
func TestRedisSinkEndToEnd(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	bus := redisbus.NewFromClient(client)
	channel := "test.psi20"

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ps, err := bus.Subscribe(ctx, channel)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = ps.Close() })

	f := &stubFetcher{resp: []quote.Quote{{Symbol: "GALP.LS", Price: 14.32, Currency: "EUR"}}}
	w := &redisWriter{bus: bus, channel: channel}
	enc := json.NewEncoder(w)
	var seq atomic.Uint64

	tickOnce(ctx, discardLogger(), f, enc, &seq, config{timeout: time.Second, maxRetry: 1})

	select {
	case msg := <-ps.Channel():
		var m quote.Message
		if err := json.Unmarshal([]byte(msg.Payload), &m); err != nil {
			t.Fatalf("decode envelope: %v\npayload=%s", err, msg.Payload)
		}
		if m.Type != quote.MsgTick || m.Seq != 1 || len(m.Quotes) != 1 {
			t.Errorf("unexpected envelope: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for envelope on redis channel")
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
