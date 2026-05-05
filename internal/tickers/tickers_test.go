package tickers

import "testing"

func TestSymbolsMatchesPSI20Order(t *testing.T) {
	syms := Symbols()
	if len(syms) != len(PSI20) {
		t.Fatalf("len(Symbols())=%d, want %d", len(syms), len(PSI20))
	}
	for i, s := range syms {
		if s != PSI20[i].Symbol {
			t.Errorf("Symbols()[%d]=%q, want %q", i, s, PSI20[i].Symbol)
		}
	}
}

func TestNameOfKnown(t *testing.T) {
	got := NameOf("GALP.LS")
	if got != "Galp Energia" {
		t.Errorf("NameOf(GALP.LS)=%q, want %q", got, "Galp Energia")
	}
}

func TestNameOfUnknownReturnsSymbol(t *testing.T) {
	got := NameOf("XXXX.LS")
	if got != "XXXX.LS" {
		t.Errorf("NameOf(XXXX.LS)=%q, want fallback %q", got, "XXXX.LS")
	}
}

func TestPSI20AllSuffixedLS(t *testing.T) {
	for _, tk := range PSI20 {
		if len(tk.Symbol) < 4 || tk.Symbol[len(tk.Symbol)-3:] != ".LS" {
			t.Errorf("ticker %q does not end with .LS", tk.Symbol)
		}
		if tk.Name == "" {
			t.Errorf("ticker %q has empty Name", tk.Symbol)
		}
	}
}
