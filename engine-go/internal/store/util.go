package store

import "sort"

func sortStrings(v []string) { sort.Strings(v) }

// sortHydratedByAge sorts by age ascending (Rust sort_by_key on the age;
// ties — which only occur for never-accessed entries at 0 — fall back to
// path for a deterministic order).
func sortHydratedByAge(v []HydratedAge) {
	sort.Slice(v, func(i, j int) bool {
		if v[i].Age != v[j].Age {
			return v[i].Age < v[j].Age
		}
		return v[i].Rel < v[j].Rel
	})
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
