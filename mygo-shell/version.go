package main

import "github.com/egoist/mygo"

func appVersion() string {
	v := mygo.App.Version()
	if v == "" {
		return "0.6.4"
	}
	return v
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
