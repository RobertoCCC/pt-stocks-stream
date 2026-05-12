package wsserver

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// sendBufferSize is the depth of each client's outbound queue. At one
// PSI-20 frame every 15s a slow consumer would need to fall ~3 minutes
// behind before we start dropping — long enough to absorb a transient
// network hiccup but short enough that a wedged consumer doesn't pile up
// indefinitely.
const sendBufferSize = 16

// pingInterval is how often the server pokes the peer with a ping frame.
// Browsers reply with pongs automatically; coder/websocket surfaces the
// pong as a successful read on Reader, which keeps our read loop honest.
const pingInterval = 20 * time.Second

// writeTimeout bounds a single Write so a peer that stops draining its
// socket can't park a server goroutine forever.
const writeTimeout = 5 * time.Second

// Client is one WebSocket connection. Construct via Hub.serveConn (see
// server.go) — never directly.
type Client struct {
	conn   *websocket.Conn
	remote string
	send   chan []byte
}

// enqueue is the non-blocking write path used by the Hub. If the send
// buffer is full we drop the frame and bump the shared counter. We never
// block the hub goroutine on a slow consumer.
func (c *Client) enqueue(frame []byte, logger *slog.Logger, dropped *atomic.Int64) bool {
	select {
	case c.send <- frame:
		return true
	default:
		dropped.Add(1)
		logger.Warn("dropping frame, client send buffer full",
			"addr", c.remote,
			"buffer", cap(c.send),
		)
		return false
	}
}

// writePump owns the connection writes. It exits when the send channel is
// closed (hub unregister) or when ctx is cancelled (server shutdown).
func (c *Client) writePump(ctx context.Context, logger *slog.Logger) {
	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.send:
			if !ok {
				// Hub unregistered us. Send a clean close so the browser
				// sees a normal closure rather than an abrupt drop.
				_ = c.conn.Close(websocket.StatusNormalClosure, "bye")
				return
			}
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := c.conn.Write(wctx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				logger.Debug("write error, closing client", "addr", c.remote, "err", err)
				return
			}
		case <-pingTicker.C:
			pctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := c.conn.Ping(pctx)
			cancel()
			if err != nil {
				logger.Debug("ping failed, closing client", "addr", c.remote, "err", err)
				return
			}
		}
	}
}

// readPump drains inbound frames. We don't expect client→server traffic in
// this app, but the loop is required to keep the underlying connection
// healthy (pong handling lives inside coder/websocket and surfaces here as
// silent reads).
func (c *Client) readPump(ctx context.Context, logger *slog.Logger) {
	for {
		_, _, err := c.conn.Read(ctx)
		if err != nil {
			logger.Debug("read closed", "addr", c.remote, "err", err)
			return
		}
	}
}
