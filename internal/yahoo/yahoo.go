// Package yahoo wraps the unofficial Yahoo Finance /v7/quote endpoint.
//
// The endpoint is not part of any stable contract — Yahoo has historically
// changed shapes and added bot-protection (cookies, "crumb" tokens) without
// notice. This client is intentionally narrow: it asks for the fields the
// app cares about, validates them, and surfaces problems via typed errors so
// callers can fall back to synthetic data or retry on a different schedule.
package yahoo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/RobertoCCC/pt-stocks-stream/internal/quote"
	"github.com/RobertoCCC/pt-stocks-stream/internal/tickers"
)

const (
	defaultBaseURL = "https://query1.finance.yahoo.com"
	defaultUA      = "Mozilla/5.0 (compatible; pt-stocks-stream/0.1; +https://github.com/RobertoCCC/pt-stocks-stream)"
)

// ErrEmptyResponse signals that the upstream returned a 200 with no quotes —
// usually a sign that the endpoint started requiring a crumb cookie.
var ErrEmptyResponse = errors.New("yahoo: no quotes in response")

// HTTPError wraps a non-2xx response so callers can decide whether to retry
// based on status (429 → backoff, 5xx → retry, 4xx → fail fast).
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("yahoo: http %d: %s", e.Status, truncate(e.Body, 200))
}

// Client is a thin wrapper around http.Client with sensible defaults for the
// Yahoo Finance v7 endpoint. Zero value is unusable; construct via New.
type Client struct {
	baseURL    string
	userAgent  string
	httpClient *http.Client
}

// Option configures the Client. Using functional options keeps the public API
// stable while letting tests override internals (URL, transport).
type Option func(*Client)

// WithBaseURL overrides the upstream host; mostly useful in tests that point
// at a httptest.Server.
func WithBaseURL(u string) Option { return func(c *Client) { c.baseURL = u } }

// WithHTTPClient replaces the underlying http.Client. Use this to inject
// custom timeouts, transports, or test doubles.
func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.httpClient = hc } }

// WithUserAgent overrides the User-Agent header. Yahoo blocks empty UAs.
func WithUserAgent(ua string) Option { return func(c *Client) { c.userAgent = ua } }

// New returns a Client configured for production use: 10s timeout, the
// default Yahoo host, and a polite User-Agent identifying the project.
func New(opts ...Option) *Client {
	c := &Client{
		baseURL:   defaultBaseURL,
		userAgent: defaultUA,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Fetch retrieves current quotes for the given symbols in a single round-trip.
// The returned slice preserves the order of `symbols` so the UI can render
// deterministically. Symbols missing from the upstream response are silently
// omitted — callers that need strict matching should check len(out) vs input.
//
// The method respects ctx for cancellation and applies no internal retry; the
// poller layer composes this call with backoff so retry policy lives in one
// place.
func (c *Client) Fetch(ctx context.Context, symbols []string) ([]quote.Quote, error) {
	if len(symbols) == 0 {
		return nil, nil
	}

	endpoint := c.baseURL + "/v7/finance/quote?" + url.Values{
		"symbols": {strings.Join(symbols, ",")},
		"fields": {strings.Join([]string{
			"symbol", "regularMarketPrice", "regularMarketChange",
			"regularMarketChangePercent", "regularMarketVolume",
			"regularMarketTime", "currency",
		}, ",")},
	}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("yahoo: build request: %w", err)
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("yahoo: do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, &HTTPError{Status: resp.StatusCode, Body: string(body)}
	}

	var envelope struct {
		QuoteResponse struct {
			Result []struct {
				Symbol                     string  `json:"symbol"`
				RegularMarketPrice         float64 `json:"regularMarketPrice"`
				RegularMarketChange        float64 `json:"regularMarketChange"`
				RegularMarketChangePercent float64 `json:"regularMarketChangePercent"`
				RegularMarketVolume        int64   `json:"regularMarketVolume"`
				RegularMarketTime          int64   `json:"regularMarketTime"`
				Currency                   string  `json:"currency"`
			} `json:"result"`
			Error any `json:"error"`
		} `json:"quoteResponse"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("yahoo: decode body: %w", err)
	}

	results := envelope.QuoteResponse.Result
	if len(results) == 0 {
		return nil, ErrEmptyResponse
	}

	// Build a symbol→row map so we can emit in input order. Yahoo doesn't
	// promise order even when we asked in a specific order.
	byKey := make(map[string]int, len(results))
	for i, r := range results {
		byKey[r.Symbol] = i
	}

	out := make([]quote.Quote, 0, len(symbols))
	for _, sym := range symbols {
		idx, ok := byKey[sym]
		if !ok {
			continue
		}
		r := results[idx]
		out = append(out, quote.Quote{
			Symbol:        r.Symbol,
			Name:          tickers.NameOf(r.Symbol),
			Price:         r.RegularMarketPrice,
			Currency:      r.Currency,
			Change:        r.RegularMarketChange,
			ChangePercent: r.RegularMarketChangePercent,
			Volume:        r.RegularMarketVolume,
			Timestamp:     time.Unix(r.RegularMarketTime, 0).UTC(),
		})
	}
	return out, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
