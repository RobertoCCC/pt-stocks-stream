// Package synthetic produces fake-but-plausible quote streams for offline
// development and CI. It exposes the same Fetch signature as yahoo.Client so
// it can be swapped in via a flag without touching the poller logic.
//
// Each ticker walks a geometric Brownian motion with a small drift and ~0.3%
// per-tick volatility. The Fetcher is goroutine-safe so the poller can call
// Fetch from a ticker loop without external locking.
package synthetic

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/RobertoCCC/pt-stocks-stream/internal/quote"
	"github.com/RobertoCCC/pt-stocks-stream/internal/tickers"
)

// Fetcher is a goroutine-safe synthetic quote source.
type Fetcher struct {
	mu          sync.Mutex
	rng         *rand.Rand
	prices      map[string]float64
	prevClose   map[string]float64
	volatility  float64
	seedInitial bool
}

// Option customises the synthetic stream.
type Option func(*Fetcher)

// WithSeed sets a deterministic RNG seed. Useful in tests so runs are
// reproducible.
func WithSeed(seed uint64) Option {
	return func(f *Fetcher) {
		f.rng = rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	}
}

// WithVolatility overrides the per-tick volatility (sigma). The default is
// 0.003 (~0.3%), comparable to real intraday tick volatility on the PSI-20.
func WithVolatility(sigma float64) Option {
	return func(f *Fetcher) { f.volatility = sigma }
}

// New constructs a Fetcher with reasonable starting prices for the PSI-20
// universe. Unknown symbols default to 10.00 EUR on first sight.
func New(opts ...Option) *Fetcher {
	f := &Fetcher{
		rng:         rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0xA5A5A5A5)),
		prices:      defaultStartingPrices(),
		prevClose:   defaultStartingPrices(),
		volatility:  0.003,
		seedInitial: true,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// Fetch returns the next quote for each requested symbol, advancing the
// internal random walk by one step per symbol. Quotes are returned in the
// same order as the input.
func (f *Fetcher) Fetch(_ context.Context, symbols []string) ([]quote.Quote, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := time.Now().UTC().Truncate(time.Second)
	out := make([]quote.Quote, 0, len(symbols))
	for _, sym := range symbols {
		cur, ok := f.prices[sym]
		if !ok {
			cur = 10.00
		}
		// Geometric Brownian step: price *= exp((drift - 0.5*σ²)·dt + σ·√dt·N(0,1)).
		// With dt=1 (one tick) and drift=0, this simplifies to the term below.
		shock := f.rng.NormFloat64() * f.volatility
		next := cur * (1 + shock)
		if next < 0.01 {
			next = 0.01 // floor to avoid runaway negatives
		}
		f.prices[sym] = next

		base := f.prevClose[sym]
		if base == 0 {
			base = cur
		}
		change := next - base
		changePct := 0.0
		if base != 0 {
			changePct = (change / base) * 100
		}

		out = append(out, quote.Quote{
			Symbol:        sym,
			Name:          tickers.NameOf(sym),
			Price:         round2(next),
			Currency:      "EUR",
			Change:        round2(change),
			ChangePercent: round2(changePct),
			Volume:        int64(f.rng.IntN(1_000_000) + 100_000),
			Timestamp:     now,
		})
	}
	return out, nil
}

// defaultStartingPrices returns hand-picked numbers in the same order of
// magnitude as real PSI-20 prices in 2026. They make the demo feel grounded
// without claiming to be accurate.
func defaultStartingPrices() map[string]float64 {
	return map[string]float64{
		"ALTR.LS":  5.10,
		"BCP.LS":   0.42,
		"COR.LS":   8.95,
		"CTT.LS":   3.60,
		"EDP.LS":   3.78,
		"EDPR.LS":  15.40,
		"EGL.LS":   3.12,
		"GALP.LS":  14.32,
		"GLINT.LS": 6.20,
		"IBS.LS":   6.80,
		"JMT.LS":   18.91,
		"NBA.LS":   3.55,
		"NOS.LS":   3.40,
		"PHR.LS":   0.08,
		"RAM.LS":   7.10,
		"REN.LS":   2.65,
		"SCT.LS":   0.78,
		"SEM.LS":   14.20,
		"SON.LS":   1.05,
		"TLF.LS":   0.21,
	}
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}
