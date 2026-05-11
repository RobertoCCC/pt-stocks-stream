package main

import "math/rand/v2"

// defaultRandFloat returns a uniform random number in [0, 1) using the
// math/rand/v2 global generator, which is automatically seeded per process.
// Isolated in its own file so unit tests can shadow the package-level
// `randFloat` variable without depending on rand/v2 internals.
func defaultRandFloat() float64 { return rand.Float64() }
