//go:build race

package domain

// raceEnabled is what the allocation pin checks: a race build allocates where an ordinary
// one does not, so the count it reports is the detector's and not this package's.
const raceEnabled = true
