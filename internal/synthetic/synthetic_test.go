package synthetic

import (
	"context"
	"testing"
)

func TestFetchReturnsRequestedSymbolsInOrder(t *testing.T) {
	f := New(WithSeed(42))
	got, err := f.Fetch(context.Background(), []string{"GALP.LS", "EDP.LS", "JMT.LS"})
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	wantSyms := []string{"GALP.LS", "EDP.LS", "JMT.LS"}
	if len(got) != len(wantSyms) {
		t.Fatalf("len(got)=%d, want %d", len(got), len(wantSyms))
	}
	for i, w := range wantSyms {
		if got[i].Symbol != w {
			t.Errorf("got[%d].Symbol=%q, want %q", i, got[i].Symbol, w)
		}
		if got[i].Price <= 0 {
			t.Errorf("got[%d].Price=%v, want > 0", i, got[i].Price)
		}
		if got[i].Currency != "EUR" {
			t.Errorf("got[%d].Currency=%q, want EUR", i, got[i].Currency)
		}
	}
}

func TestSeedIsDeterministic(t *testing.T) {
	syms := []string{"GALP.LS", "EDP.LS"}
	a := New(WithSeed(99))
	b := New(WithSeed(99))
	ga, _ := a.Fetch(context.Background(), syms)
	gb, _ := b.Fetch(context.Background(), syms)
	for i := range ga {
		if ga[i].Price != gb[i].Price {
			t.Errorf("seed %d not deterministic: ga[%d].Price=%v, gb[%d].Price=%v",
				99, i, ga[i].Price, i, gb[i].Price)
		}
	}
}

func TestPriceFloorPreventsNegatives(t *testing.T) {
	// Crank volatility absurdly high; floor should still hold.
	f := New(WithSeed(1), WithVolatility(10.0))
	for range 200 {
		got, _ := f.Fetch(context.Background(), []string{"PHR.LS"})
		if got[0].Price < 0.01 {
			t.Fatalf("price dropped below floor: %v", got[0].Price)
		}
	}
}

func TestUnknownSymbolGetsFallbackPrice(t *testing.T) {
	f := New(WithSeed(7))
	got, err := f.Fetch(context.Background(), []string{"UNKNOWN.LS"})
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	if got[0].Price <= 0 {
		t.Errorf("expected non-zero fallback price; got %v", got[0].Price)
	}
	if got[0].Name != "UNKNOWN.LS" {
		t.Errorf("expected NameOf to return symbol fallback; got %q", got[0].Name)
	}
}
