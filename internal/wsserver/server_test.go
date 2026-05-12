package wsserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestHandlerDeliversBroadcastsToConnectedClient(t *testing.T) {
	hub := NewHub(quietLogger())
	hubCtx, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubCtx)

	srv := httptest.NewServer(Handler(hub, quietLogger(), HandlerOptions{}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	conn, _, err := websocket.Dial(dialCtx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Wait until the hub has registered the client; otherwise the
	// broadcast races the register channel.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && hub.Metrics().Connected == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if hub.Metrics().Connected == 0 {
		t.Fatal("client never registered with hub")
	}

	hub.Broadcast([]byte(`{"type":"tick"}`))

	readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer readCancel()
	_, payload, err := conn.Read(readCtx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(payload) != `{"type":"tick"}` {
		t.Errorf("payload=%q, want tick envelope", payload)
	}
}

func TestHealthHandlerReturnsMetrics(t *testing.T) {
	hub := NewHub(quietLogger())
	srv := httptest.NewServer(HealthHandler(hub))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status=%d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type=%q, want application/json", ct)
	}
}
