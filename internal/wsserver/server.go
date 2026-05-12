package wsserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/coder/websocket"
)

// HandlerOptions configures the HTTP handler returned by Handler.
type HandlerOptions struct {
	// AllowedOrigins is the list of origins permitted to upgrade to a
	// WebSocket. Empty means "any origin" (development default — set this
	// for production). Wildcards are not supported.
	AllowedOrigins []string
}

// Handler returns an http.Handler that upgrades requests to WebSocket and
// hands them to the Hub. The handler is the public surface of this
// package; everything else (Hub, Client) is plumbing.
func Handler(hub *Hub, logger *slog.Logger, opts HandlerOptions) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: opts.AllowedOrigins,
		})
		if err != nil {
			logger.Warn("ws accept failed", "remote", r.RemoteAddr, "err", err)
			return
		}

		client := &Client{
			conn:   conn,
			remote: r.RemoteAddr,
			send:   make(chan []byte, sendBufferSize),
		}

		// Tie this connection's lifetime to the request context so a
		// server shutdown cancels its reads and writes.
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		select {
		case hub.register <- client:
		case <-ctx.Done():
			_ = conn.Close(websocket.StatusGoingAway, "shutting down")
			return
		}
		defer func() {
			// If the hub goroutine already exited (shutdown) we don't
			// want this to block. ctx will be cancelled in that path.
			select {
			case hub.unregister <- client:
			case <-ctx.Done():
			}
		}()

		// readPump on the main goroutine: when it returns (peer closed or
		// errored) we cancel ctx and the writePump exits too.
		done := make(chan struct{})
		go func() {
			client.writePump(ctx, logger)
			close(done)
		}()
		client.readPump(ctx, logger)
		cancel()
		<-done
	})
}

// HealthHandler returns a JSON handler exposing hub counters. Render's
// health check pings /healthz so we keep it cheap and read-only.
func HealthHandler(hub *Hub) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hub.Metrics())
	})
}
