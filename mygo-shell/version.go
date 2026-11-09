package main

import "github.com/egoist/mygo"

func appVersion() string {
	v := mygo.App.Version()
	if v == "" {
		return "0.6.2"
	}
	return v
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
