package wsserver

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// newTestClient builds a Client wired only as a send-queue. We bypass the
// websocket.Conn since the hub never touches it directly.
func newTestClient(buf int) *Client {
	return &Client{send: make(chan []byte, buf), remote: "test"}
}

func TestHubBroadcastFansOut(t *testing.T) {
	hub := NewHub(quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	a := newTestClient(2)
	b := newTestClient(2)
	hub.register <- a
	hub.register <- b

	if !hub.Broadcast([]byte("hello")) {
		t.Fatal("hub broadcast buffer full unexpectedly")
	}

	for _, c := range []*Client{a, b} {
		select {
		case got := <-c.send:
			if string(got) != "hello" {
				t.Errorf("client got %q, want hello", got)
			}
		case <-time.After(time.Second):
			t.Fatal("client did not receive frame")
		}
	}
}

func TestHubSnapshotReplaysOnRegister(t *testing.T) {
	hub := NewHub(quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	// Pre-existing client receives the broadcast.
	first := newTestClient(2)
	hub.register <- first
	hub.Broadcast([]byte("snap-1"))
	<-first.send

	// New client connects after the broadcast and should immediately get
	// the cached snapshot.
	late := newTestClient(2)
	hub.register <- late
	select {
	case got := <-late.send:
		if string(got) != "snap-1" {
			t.Errorf("late client got %q, want snap-1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("late client did not receive snapshot")
	}
}

func TestHubBackpressureDropsForSlowClient(t *testing.T) {
	hub := NewHub(quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	slow := newTestClient(1) // tiny buffer so 2 frames overflow
	hub.register <- slow

	hub.Broadcast([]byte("a"))
	hub.Broadcast([]byte("b"))
	// Give the hub goroutine a moment to drain the broadcast buffer.
	time.Sleep(50 * time.Millisecond)

	if got := hub.Metrics().Dropped; got == 0 {
		t.Errorf("expected at least 1 drop for slow client, got %d", got)
	}
}

func TestHubUnregisterCleansUp(t *testing.T) {
	hub := NewHub(quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	c := newTestClient(1)
	hub.register <- c
	hub.unregister <- c
	// Give it a moment to process.
	time.Sleep(20 * time.Millisecond)

	if got := hub.Metrics().Connected; got != 0 {
		t.Errorf("connected=%d, want 0", got)
	}
}

func TestEnqueueDropCountsIncrement(t *testing.T) {
	c := newTestClient(0)
	var dropped atomic.Int64
	if c.enqueue([]byte("x"), quietLogger(), &dropped) {
		t.Error("enqueue on full buffer should return false")
	}
	if dropped.Load() != 1 {
		t.Errorf("dropped=%d, want 1", dropped.Load())
	}
}
