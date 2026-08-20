package api

import "testing"

func TestParseSeqRejectsGarbageInsteadOfWideningTheRange(t *testing.T) {
	// The regression this guards. These previously went through `strconv.ParseUint(s, 10, 64)` with
	// the error discarded into `_`, so every one of them became 0 — which the SQL reads as "no lower
	// bound" / "no upper bound". Bad input silently returned MORE data than was asked for, and a
	// validation bug that widens a range is one nobody notices working.
	for _, s := range []string{"banana", "-1", "1.5", "0x10", " 12", "12 ", "1e3", "99999999999999999999999"} {
		t.Run(s, func(t *testing.T) {
			if _, err := parseSeq(s, 0); err == nil {
				t.Fatalf("parseSeq(%q) was accepted, want an error", s)
			}
		})
	}
}

func TestParseSeqDefaultsWhenAbsent(t *testing.T) {
	got, err := parseSeq("", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Fatalf("parseSeq(\"\") = %d, want 0", got)
	}
}

func TestParseSeqAcceptsValidValues(t *testing.T) {
	got, err := parseSeq("1787122665320", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 1787122665320 {
		t.Fatalf("parseSeq = %d, want 1787122665320", got)
	}
}

func TestParseLimitDefaultsAndBounds(t *testing.T) {
	if got, err := parseLimit(""); err != nil || got != defaultReplayLimit {
		t.Fatalf("parseLimit(\"\") = %d, %v; want %d, nil", got, err, defaultReplayLimit)
	}
	if got, err := parseLimit("10"); err != nil || got != 10 {
		t.Fatalf("parseLimit(\"10\") = %d, %v; want 10, nil", got, err)
	}
	if got, err := parseLimit("5000"); err != nil || got != maxReplayLimit {
		t.Fatalf("parseLimit at the ceiling = %d, %v; want %d, nil", got, err, maxReplayLimit)
	}
}

func TestParseLimitRejectsRatherThanClamps(t *testing.T) {
	// Rejecting matters more than the bound itself. Clamping would answer a request for 100000 rows
	// with 5000 and no indication, which reads to the caller as "that is all the data there is".
	for _, s := range []string{"0", "-1", "5001", "100000", "banana", ""} {
		if s == "" {
			continue
		}
		t.Run(s, func(t *testing.T) {
			if _, err := parseLimit(s); err == nil {
				t.Fatalf("parseLimit(%q) was accepted, want an error", s)
			}
		})
	}
}
