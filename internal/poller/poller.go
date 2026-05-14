// Package poller is the PSI-20 tick-and-publish core.
//
// The cmd/poller binary is a thin CLI wrapper that picks a fetcher and a
// sink and then calls Run. The cmd/allinone binary reuses the very same
// Run on a different deployment shape (poller + wsserver collocated in one
// process) — moving the loop out of package main is what lets the two
// binaries share code without import cycles.
//
// Retries use bounded exponential backoff with full jitter (uniform in
// [0, 2^attempt * base)) following AWS's "Exponential Backoff And Jitter"
// recommendation. Permanent 4xx responses (anything other than 429) fail
// fast — we never want to retry a 401 sixteen times in a row.
package poller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/RobertoCCC/pt-stocks-stream/internal/quote"
	"github.com/RobertoCCC/pt-stocks-stream/internal/tickers"
	"github.com/RobertoCCC/pt-stocks-stream/internal/yahoo"
)

// Fetcher is the contract Run depends on. Defined here, where it's consumed,
// rather than alongside yahoo/synthetic — idiomatic Go: accept interfaces,
// return concrete types.
type Fetcher interface {
	Fetch(ctx context.Context, symbols []string) ([]quote.Quote, error)
}

// Config is the subset of options Run actually needs. The CLI binaries hold
// a bigger config (flags, log format, sink choice, redis URL, …) and pass
// the relevant fields here.
type Config struct {
	// Interval between successive batches. The first tick fires immediately
	// on Run so operators tailing logs see activity without waiting.
	Interval time.Duration
	// Timeout is the per-fetch deadline. Must be < Interval, otherwise a
	// slow upstream would pile up overlapping calls.
	Timeout time.Duration
	// MaxRetry caps the retry attempts per tick. Once exhausted, the tick is
	// skipped and the loop waits for the next interval.
	MaxRetry int
}

// Run drives the ticker loop until ctx is cancelled. Each tick fetches a
// batch from f, retries transient failures with full-jitter backoff, and
// writes one JSON envelope per tick to sink. Returns nil on clean shutdown.
//
// Callers own the sink: it might be os.Stdout (CLI dev mode), a Redis
// publisher adapter, or any other io.Writer. Run never closes it.
func Run(ctx context.Context, logger *slog.Logger, f Fetcher, sink io.Writer, cfg Config) error {
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	enc := json.NewEncoder(sink)
	var seq atomic.Uint64

	// Tick immediately on start — see godoc above.
	tickOnce(ctx, logger, f, enc, &seq, cfg)

	for {
		select {
		case <-ctx.Done():
			logger.Info("poller shutting down", "reason", ctx.Err())
			return nil
		case <-ticker.C:
			tickOnce(ctx, logger, f, enc, &seq, cfg)
		}
	}
}

func tickOnce(parent context.Context, logger *slog.Logger, f Fetcher, enc *json.Encoder, seq *atomic.Uint64, cfg Config) {
	fetchCtx, cancel := context.WithTimeout(parent, cfg.Timeout)
	defer cancel()

	quotes, err := fetchWithRetry(fetchCtx, logger, f, cfg.MaxRetry)
	if err != nil {
		logger.Error("fetch failed, skipping tick", "err", err)
		return
	}

	msg := quote.Message{
		Type:     quote.MsgTick,
		Quotes:   quotes,
		Seq:      seq.Add(1),
		ServerTS: time.Now().UnixMilli(),
	}
	if err := enc.Encode(msg); err != nil {
		// Sink errors are not recoverable (stdout closed, broken pipe). Log
		// and let the next tick try again — kept simple intentionally.
		logger.Error("encode message", "err", err)
		return
	}
	logger.Debug("tick published", "seq", msg.Seq, "quotes", len(quotes))
}

func fetchWithRetry(ctx context.Context, logger *slog.Logger, f Fetcher, maxRetry int) ([]quote.Quote, error) {
	const baseDelay = 200 * time.Millisecond
	const capDelay = 5 * time.Second

	var lastErr error
	for attempt := 0; attempt <= maxRetry; attempt++ {
		quotes, err := f.Fetch(ctx, tickers.Symbols())
		if err == nil {
			return quotes, nil
		}
		lastErr = err

		// Permanent failures (4xx that isn't 429): don't retry.
		var httpErr *yahoo.HTTPError
		if errors.As(err, &httpErr) && httpErr.Status >= 400 && httpErr.Status < 500 && httpErr.Status != 429 {
			return nil, err
		}

		if attempt == maxRetry {
			break
		}
		delay := backoff(attempt, baseDelay, capDelay)
		logger.Warn("fetch error, retrying", "attempt", attempt+1, "delay", delay, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, lastErr
}

func backoff(attempt int, base, cap time.Duration) time.Duration {
	// 2^attempt * base, capped, then jittered to [0, computed).
	exp := base << attempt
	if exp <= 0 || exp > cap {
		exp = cap
	}
	//nolint:gosec // not used for cryptography — full-jitter backoff
	return time.Duration(float64(exp) * RandFloat())
}

// RandFloat is package-level so tests can stub determinism (zero for
// no-jitter, 1.0 to maximise the computed delay). Defaults to the math/rand/v2
// global generator, which is automatically seeded per process.
var RandFloat = func() float64 { return rand.Float64() }
