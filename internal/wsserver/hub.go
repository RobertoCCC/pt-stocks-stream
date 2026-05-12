// Package wsserver implements the WebSocket fan-out layer.
//
// One Hub instance owns the registry of connected clients and forwards each
// inbound frame from Redis to every client's bounded send queue. The Hub is
// the only goroutine that mutates the registry, which means client lookups
// never need a lock — registration and unregistration are channel ops.
//
// Backpressure is per-client: if a client's send queue is full we drop the
// frame for that one client and increment a counter. We never block the Hub
// goroutine on a slow consumer, which would amplify a single slow browser
// into a stall for everyone.
package wsserver

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Hub multiplexes one inbound stream (Redis pub/sub) onto N WebSocket
// clients. Construct with NewHub and drive with Run.
type Hub struct {
	logger     *slog.Logger
	clients    map[*Client]struct{}
	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte

	// snapshot is the most recent frame the hub published. New clients get
	// this immediately so they don't have to wait for the next tick.
	snapshotMu sync.RWMutex
	snapshot   []byte

	// Metrics — atomic for cheap concurrent reads from /healthz.
	connected atomic.Int64
	dropped   atomic.Int64
	delivered atomic.Int64
}

// HubMetrics is a serialisable snapshot of hub counters for /healthz.
type HubMetrics struct {
	Connected int64 `json:"connected"`
	Dropped   int64 `json:"dropped"`
	Delivered int64 `json:"delivered"`
}

// NewHub builds a hub with sane channel sizes. The broadcast buffer is
// small on purpose: at one PSI-20 tick per 15 seconds we don't need depth,
// and a small buffer surfaces backpressure problems early.
func NewHub(logger *slog.Logger) *Hub {
	return &Hub{
		logger:     logger,
		clients:    make(map[*Client]struct{}),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan []byte, 8),
	}
}

// Run owns the hub goroutine. It exits when ctx is cancelled, after which
// no further registrations or broadcasts are accepted.
func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			h.logger.Info("hub shutting down", "reason", ctx.Err())
			return
		case c := <-h.register:
			h.clients[c] = struct{}{}
			h.connected.Store(int64(len(h.clients)))
			h.logger.Debug("client registered", "addr", c.remote, "connected", len(h.clients))
			// Snapshot replay: lets a fresh tab show prices instantly
			// rather than waiting for the next 15s tick.
			if snap := h.Snapshot(); snap != nil {
				c.enqueue(snap, h.logger, &h.dropped)
			}
		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				h.connected.Store(int64(len(h.clients)))
				close(c.send)
				h.logger.Debug("client unregistered", "addr", c.remote, "connected", len(h.clients))
			}
		case frame := <-h.broadcast:
			h.setSnapshot(frame)
			for c := range h.clients {
				if c.enqueue(frame, h.logger, &h.dropped) {
					h.delivered.Add(1)
				}
			}
		}
	}
}

// Broadcast hands a frame to the hub. Returns false if the hub buffer is
// full, which means the publisher should drop or coalesce instead of
// blocking the redisbus subscriber goroutine.
func (h *Hub) Broadcast(frame []byte) bool {
	select {
	case h.broadcast <- frame:
		return true
	default:
		return false
	}
}

// Snapshot returns the most recent broadcast frame or nil if none yet.
func (h *Hub) Snapshot() []byte {
	h.snapshotMu.RLock()
	defer h.snapshotMu.RUnlock()
	if h.snapshot == nil {
		return nil
	}
	cp := make([]byte, len(h.snapshot))
	copy(cp, h.snapshot)
	return cp
}

func (h *Hub) setSnapshot(frame []byte) {
	h.snapshotMu.Lock()
	defer h.snapshotMu.Unlock()
	h.snapshot = append(h.snapshot[:0], frame...)
}

// Metrics returns a point-in-time copy. Cheap; safe from any goroutine.
func (h *Hub) Metrics() HubMetrics {
	return HubMetrics{
		Connected: h.connected.Load(),
		Dropped:   h.dropped.Load(),
		Delivered: h.delivered.Load(),
	}
}
