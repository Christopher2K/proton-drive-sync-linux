package main

import "github.com/egoist/mygo"

func appVersion() string {
	v := mygo.App.Version()
	if v == "" {
		return "0.3.0"
	}
	return v
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
