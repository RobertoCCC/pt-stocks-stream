package yahoo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestFetchDecodesAndPreservesInputOrder(t *testing.T) {
	body := `{"quoteResponse":{"result":[
		{"symbol":"EDP.LS","regularMarketPrice":3.78,"regularMarketChange":-0.04,"regularMarketChangePercent":-1.05,"regularMarketVolume":1234567,"regularMarketTime":1747000000,"currency":"EUR"},
		{"symbol":"GALP.LS","regularMarketPrice":14.32,"regularMarketChange":0.12,"regularMarketChangePercent":0.85,"regularMarketVolume":987654,"regularMarketTime":1747000000,"currency":"EUR"}
	],"error":null}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") == "" {
			t.Errorf("expected User-Agent to be set")
		}
		q, _ := url.ParseQuery(r.URL.RawQuery)
		if got := q.Get("symbols"); got != "GALP.LS,EDP.LS" {
			t.Errorf("symbols query = %q, want %q", got, "GALP.LS,EDP.LS")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	c := New(WithBaseURL(srv.URL))
	got, err := c.Fetch(context.Background(), []string{"GALP.LS", "EDP.LS"})
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(got)=%d, want 2", len(got))
	}
	// Output order should match input order, not response order.
	if got[0].Symbol != "GALP.LS" {
		t.Errorf("got[0].Symbol=%q, want GALP.LS", got[0].Symbol)
	}
	if got[0].Price != 14.32 {
		t.Errorf("got[0].Price=%v, want 14.32", got[0].Price)
	}
	if got[0].Name == "" {
		t.Errorf("expected NameOf to fill issuer name; got empty")
	}
	wantTS := time.Unix(1747000000, 0).UTC()
	if !got[0].Timestamp.Equal(wantTS) {
		t.Errorf("got[0].Timestamp=%v, want %v", got[0].Timestamp, wantTS)
	}
}

func TestFetchEmptyResultReturnsTypedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"quoteResponse":{"result":[],"error":null}}`))
	}))
	defer srv.Close()

	c := New(WithBaseURL(srv.URL))
	_, err := c.Fetch(context.Background(), []string{"X.LS"})
	if !errors.Is(err, ErrEmptyResponse) {
		t.Fatalf("Fetch err = %v, want ErrEmptyResponse", err)
	}
}

func TestFetchNon2xxIsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`rate limited`))
	}))
	defer srv.Close()

	c := New(WithBaseURL(srv.URL))
	_, err := c.Fetch(context.Background(), []string{"X.LS"})
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("Fetch err = %v, want *HTTPError", err)
	}
	if httpErr.Status != http.StatusTooManyRequests {
		t.Errorf("HTTPError.Status=%d, want 429", httpErr.Status)
	}
	if !strings.Contains(httpErr.Body, "rate limited") {
		t.Errorf("HTTPError.Body=%q, want to contain 'rate limited'", httpErr.Body)
	}
}

func TestFetchEmptySymbolsShortCircuits(t *testing.T) {
	// Use an obviously-broken base URL — if the method does an HTTP call it'll fail.
	c := New(WithBaseURL("http://127.0.0.1:1"))
	got, err := c.Fetch(context.Background(), nil)
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	if got != nil {
		t.Errorf("got=%v, want nil for empty input", got)
	}
}

func TestFetchUnknownSymbolInResponseIsIgnored(t *testing.T) {
	body := `{"quoteResponse":{"result":[
		{"symbol":"NOPE.LS","regularMarketPrice":1.0,"currency":"EUR","regularMarketTime":1}
	],"error":null}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	c := New(WithBaseURL(srv.URL))
	got, err := c.Fetch(context.Background(), []string{"GALP.LS"})
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d quotes, want 0 (NOPE.LS not requested)", len(got))
	}
}
