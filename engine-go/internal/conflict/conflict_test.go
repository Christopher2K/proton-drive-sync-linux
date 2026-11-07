package conflict

import (
	"strings"
	"testing"
	"time"
)

func at() time.Time {
	return time.Date(2026, 10, 23, 14, 5, 0, 0, time.UTC)
}

// Port of conflict.rs plain_file.
func TestPlainFile(t *testing.T) {
	want := "report (conflict 2026-10-23 1405).pdf"
	if got := Name("report.pdf", at()); got != want {
		t.Errorf("Name() = %q, want %q", got, want)
	}
}

// Port of conflict.rs no_extension.
func TestNoExtension(t *testing.T) {
	want := "README (conflict 2026-10-23 1405)"
	if got := Name("README", at()); got != want {
		t.Errorf("Name() = %q, want %q", got, want)
	}
}

// Port of conflict.rs dotfile_has_no_extension.
func TestDotfileHasNoExtension(t *testing.T) {
	want := ".env (conflict 2026-10-23 1405)"
	if got := Name(".env", at()); got != want {
		t.Errorf("Name() = %q, want %q", got, want)
	}
}

// Port of conflict.rs multi_dot_keeps_last_extension.
func TestMultiDotKeepsLastExtension(t *testing.T) {
	want := "archive.tar (conflict 2026-10-23 1405).gz"
	if got := Name("archive.tar.gz", at()); got != want {
		t.Errorf("Name() = %q, want %q", got, want)
	}
}

// Port of conflict.rs zero_padded_minutes.
func TestZeroPaddedMinutes(t *testing.T) {
	tm := time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)
	want := "a (conflict 2026-01-02 0304).txt"
	if got := Name("a.txt", tm); got != want {
		t.Errorf("Name() = %q, want %q", got, want)
	}
}

// Port of conflict.rs dst_fold_instants_get_distinct_utc_names: a DST fold
// repeats the local wall clock, but the UTC instants — and therefore the
// generated names — are distinct.
func TestDstFoldInstantsGetDistinctUTCMames(t *testing.T) {
	// US Eastern fall-back 2026-11-01: 01:30 EDT then 01:30 EST.
	first := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC)
	second := time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC)
	edt := time.FixedZone("EDT", -4*3600)
	est := time.FixedZone("EST", -5*3600)
	if first.In(edt).Format("2006-01-02 15:04") != second.In(est).Format("2006-01-02 15:04") {
		t.Fatal("the fold must repeat the local wall clock for this test to be meaningful")
	}
	a, b := Name("a.txt", first), Name("a.txt", second)
	if a != "a (conflict 2026-11-01 0530).txt" {
		t.Errorf("first instant: %q", a)
	}
	if b != "a (conflict 2026-11-01 0630).txt" {
		t.Errorf("second instant: %q", b)
	}
	if a == b {
		t.Error("fold instants must produce distinct names")
	}
}

// Port of conflict.rs odd_but_legal_names_keep_a_valid_extension (TG-13
// matrix): the suffix must never eat a trailing dot/space, normalize
// Unicode, or introduce a path separator.
func TestOddButLegalNamesKeepAValidExtension(t *testing.T) {
	cases := []struct{ name, ext string }{
		{"café.txt", ".txt"},              // NFC
		{"cafe\u0301.txt", ".txt"},        // NFD
		{"rocket \U0001f680.txt", ".txt"}, // emoji
		{"with space.txt", ".txt"},
		{"it's \"quoted\".txt", ".txt"},
		{"line\nbreak.txt", ".txt"},
		{"trailing dot.", "."},
		{"~leading.txt", ".txt"},
	}
	for _, tc := range cases {
		conflict := Name(tc.name, at())
		if conflict == tc.name {
			t.Errorf("%q: conflict name equals original", tc.name)
		}
		if strings.Contains(conflict, "/") {
			t.Errorf("conflict name must stay a sibling: %q", conflict)
		}
		if !strings.HasSuffix(conflict, tc.ext) {
			t.Errorf("extension %q must survive for %q: %q", tc.ext, tc.name, conflict)
		}
	}
	// Extension-less names (including trailing whitespace) keep the suffix
	// whole rather than inventing an extension.
	if got := Name("trailing space ", at()); got != "trailing space  (conflict 2026-10-23 1405)" {
		t.Errorf("trailing space: %q", got)
	}
}
