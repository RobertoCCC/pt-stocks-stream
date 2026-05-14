package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RobertoCCC/pt-stocks-stream/internal/quote"
	"github.com/RobertoCCC/pt-stocks-stream/internal/redisbus"
	"github.com/RobertoCCC/pt-stocks-stream/internal/yahoo"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type stubFetcher struct {
	calls atomic.Int32
	resp  []quote.Quote
	err   error
}

func (s *stubFetcher) Fetch(_ context.Context, _ []string) ([]quote.Quote, error) {
	s.calls.Add(1)
	return s.resp, s.err
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestTickOnceEmitsEnvelope(t *testing.T) {
	f := &stubFetcher{
		resp: []quote.Quote{
			{Symbol: "GALP.LS", Price: 14.32, Currency: "EUR"},
		},
	}
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	var seq atomic.Uint64

	tickOnce(context.Background(), discardLogger(), f, enc, &seq, Config{Timeout: time.Second, MaxRetry: 1})

	var msg quote.Message
	if err := json.Unmarshal(buf.Bytes(), &msg); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if msg.Type != quote.MsgTick {
		t.Errorf("Type=%q, want tick", msg.Type)
	}
	if msg.Seq != 1 {
		t.Errorf("Seq=%d, want 1", msg.Seq)
	}
	if len(msg.Quotes) != 1 || msg.Quotes[0].Symbol != "GALP.LS" {
		t.Errorf("Quotes=%v, want one GALP.LS", msg.Quotes)
	}
}

func TestFetchWithRetrySucceedsAfterTransient(t *testing.T) {
	prev := RandFloat
	RandFloat = func() float64 { return 0 } // zero-jitter for fast test
	t.Cleanup(func() { RandFloat = prev })

	flaky := &flakeyFetcher{failFor: 2, ok: []quote.Quote{{Symbol: "OK", Price: 1}}}
	got, err := fetchWithRetry(context.Background(), discardLogger(), flaky, 5)
	if err != nil {
		t.Fatalf("fetchWithRetry err: %v", err)
	}
	if len(got) != 1 || got[0].Symbol != "OK" {
		t.Errorf("got %v, want one OK quote", got)
	}
	if flaky.calls != 3 {
		t.Errorf("calls=%d, want 3 (2 fails + 1 ok)", flaky.calls)
	}
}

func TestFetchWithRetryFailsFastOn4xxExcept429(t *testing.T) {
	prev := RandFloat
	RandFloat = func() float64 { return 0 }
	t.Cleanup(func() { RandFloat = prev })

	f := &stubFetcher{err: &yahoo.HTTPError{Status: 401, Body: "unauth"}}
	_, err := fetchWithRetry(context.Background(), discardLogger(), f, 5)
	if err == nil {
		t.Fatal("expected error")
	}
	if got := f.calls.Load(); got != 1 {
		t.Errorf("calls=%d, want 1 (no retry on 401)", got)
	}
}

func TestFetchWithRetryRespectsContextCancel(t *testing.T) {
	prev := RandFloat
	RandFloat = func() float64 { return 1.0 } // max jitter so delay is meaningful
	t.Cleanup(func() { RandFloat = prev })

	f := &stubFetcher{err: errors.New("boom")}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := fetchWithRetry(ctx, discardLogger(), f, 10)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

// TestRedisWriterEndToEnd boots an in-memory Redis, wires a RedisWriter as
// the poller sink, and asserts that a subscriber receives the JSON envelope.
// This is the contract cmd/allinone relies on to bridge poller → hub via
// pub/sub without going through stdout.
func TestRedisWriterEndToEnd(t *testing.T) {
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
	w := &RedisWriter{Bus: bus, Channel: channel}
	enc := json.NewEncoder(w)
	var seq atomic.Uint64

	tickOnce(ctx, discardLogger(), f, enc, &seq, Config{Timeout: time.Second, MaxRetry: 1})

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

// flakeyFetcher fails the first `failFor` calls and then returns `ok`.
type flakeyFetcher struct {
	failFor int
	calls   int
	ok      []quote.Quote
}

func (f *flakeyFetcher) Fetch(_ context.Context, _ []string) ([]quote.Quote, error) {
	f.calls++
	if f.calls <= f.failFor {
		return nil, errors.New("transient")
	}
	return f.ok, nil
}
