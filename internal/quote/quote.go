// Package quote defines the wire types shared by poller, wsserver and clients.
//
// The JSON tags are part of the public contract — renaming a field breaks
// clients. Keep them stable.
package quote

import "time"

// Quote is a single point-in-time price observation for one ticker.
//
// All monetary values are in the instrument's listing currency (EUR for the
// PSI-20). Change and ChangePercent are versus the previous trading day's
// close, mirroring how Yahoo Finance reports them — this matches what users
// expect to see on Bloomberg/Reuters tickers.
type Quote struct {
	Symbol        string    `json:"symbol"`
	Name          string    `json:"name"`
	Price         float64   `json:"price"`
	Currency      string    `json:"currency"`
	Change        float64   `json:"change"`
	ChangePercent float64   `json:"change_percent"`
	Volume        int64     `json:"volume"`
	Timestamp     time.Time `json:"timestamp"`
}

// MessageType discriminates the WS frame variants.
type MessageType string

const (
	// MsgSnapshot is the full last-known state sent to a client right after
	// it connects, so a late joiner sees prices immediately without waiting
	// for the next poll cycle.
	MsgSnapshot MessageType = "snapshot"
	// MsgTick is a delta — one or more quotes that changed since the prior
	// publish. Clients merge ticks into their local state by symbol.
	MsgTick MessageType = "tick"
	// MsgPing is a server-originated keepalive. Clients should respond with
	// a pong frame within the heartbeat window.
	MsgPing MessageType = "ping"
)

// Message is the envelope pushed over the WebSocket. Seq is monotonic per
// server process and lets clients detect dropped frames in their session.
type Message struct {
	Type     MessageType `json:"type"`
	Quotes   []Quote     `json:"quotes,omitempty"`
	Seq      uint64      `json:"seq"`
	ServerTS int64       `json:"server_ts"` // milliseconds since epoch
}
