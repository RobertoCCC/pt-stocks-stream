package redisbus

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestBus(t *testing.T) (*Bus, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewFromClient(client), mr
}

func TestPublishDeliversToSubscriber(t *testing.T) {
	bus, _ := newTestBus(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ps, err := bus.Subscribe(ctx, "psi20.ticks")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = ps.Close() })

	if err := bus.Publish(ctx, "psi20.ticks", []byte(`{"hello":"world"}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case msg := <-ps.Channel():
		if msg.Payload != `{"hello":"world"}` {
			t.Errorf("payload=%q, want JSON literal", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pub/sub message")
	}
}

func TestNewRejectsEmptyURL(t *testing.T) {
	if _, err := New(context.Background(), ""); err == nil {
		t.Fatal("expected error on empty url")
	}
}

func TestNewRejectsBadURL(t *testing.T) {
	if _, err := New(context.Background(), "not-a-url"); err == nil {
		t.Fatal("expected parse error on garbage url")
	}
}
