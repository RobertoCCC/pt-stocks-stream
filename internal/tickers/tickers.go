// Package tickers holds the PSI-20 constituent list and its Yahoo Finance
// symbols. Membership of the PSI-20 changes infrequently (a few times per year
// at most); the list here is a representative snapshot as of 2026 and is
// trivial to update.
package tickers

// Ticker pairs a Yahoo Finance symbol with the issuer's display name.
type Ticker struct {
	// Symbol is the Yahoo Finance identifier (e.g. "GALP.LS"). Suffix ".LS"
	// denotes the Lisbon Euronext market.
	Symbol string
	// Name is the human-readable issuer name shown in the UI.
	Name string
}

// PSI20 is the working set of tickers the poller fetches and the wsserver
// fans out. Order is preserved for deterministic UI rendering.
var PSI20 = []Ticker{
	{Symbol: "ALTR.LS", Name: "Altri"},
	{Symbol: "BCP.LS", Name: "Banco Comercial Português"},
	{Symbol: "COR.LS", Name: "Corticeira Amorim"},
	{Symbol: "CTT.LS", Name: "CTT"},
	{Symbol: "EDP.LS", Name: "EDP"},
	{Symbol: "EDPR.LS", Name: "EDP Renováveis"},
	{Symbol: "EGL.LS", Name: "Mota-Engil"},
	{Symbol: "GALP.LS", Name: "Galp Energia"},
	{Symbol: "GLINT.LS", Name: "Glintt"},
	{Symbol: "IBS.LS", Name: "Ibersol"},
	{Symbol: "JMT.LS", Name: "Jerónimo Martins"},
	{Symbol: "NBA.LS", Name: "Navigator"},
	{Symbol: "NOS.LS", Name: "NOS"},
	{Symbol: "PHR.LS", Name: "Pharol"},
	{Symbol: "RAM.LS", Name: "Ramada"},
	{Symbol: "REN.LS", Name: "REN"},
	{Symbol: "SCT.LS", Name: "Sonae Capital"},
	{Symbol: "SEM.LS", Name: "Semapa"},
	{Symbol: "SON.LS", Name: "Sonae"},
	{Symbol: "TLF.LS", Name: "Teixeira Duarte"},
}

// Symbols returns just the Yahoo identifiers, in the same order as PSI20.
// Useful when building a comma-separated query for the Yahoo /v7/quote endpoint.
func Symbols() []string {
	out := make([]string, len(PSI20))
	for i, t := range PSI20 {
		out[i] = t.Symbol
	}
	return out
}

// NameOf returns the issuer name for a Yahoo symbol, or the symbol itself if
// the ticker is unknown. Safe to call from hot paths — it's a small linear
// scan over 20 entries.
func NameOf(symbol string) string {
	for _, t := range PSI20 {
		if t.Symbol == symbol {
			return t.Name
		}
	}
	return symbol
}
