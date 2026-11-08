package protocol

import (
	"encoding/json"
	"testing"
)

func TestDecodeShellCommands(t *testing.T) {
	cases := []struct {
		line string
		want Command
	}{
		{`{"cmd":"StartLogin"}`, Command{Cmd: CmdStartLogin}},
		{`{"cmd":"Pause"}`, Command{Cmd: CmdPause}},
		{`{"cmd":"Resume"}`, Command{Cmd: CmdResume}},
		{`{"cmd":"SignOut"}`, Command{Cmd: CmdSignOut}},
		{`{"cmd":"Shutdown"}`, Command{Cmd: CmdShutdown}},
		{`{"cmd":"SetMode","mode":"mirror"}`, Command{Cmd: CmdSetMode, Mode: "mirror"}},
		{`{"cmd":"SetCacheCap","cap":5}`, Command{Cmd: CmdSetCacheCap, Cap: u64ptr(5)}},
	}
	for _, tc := range cases {
		var got Command
		if err := json.Unmarshal([]byte(tc.line), &got); err != nil {
			t.Fatalf("decode %s: %v", tc.line, err)
		}
		if got.Cmd != tc.want.Cmd {
			t.Errorf("%s: cmd = %q, want %q", tc.line, got.Cmd, tc.want.Cmd)
		}
		if got.Mode != tc.want.Mode {
			t.Errorf("%s: mode = %q, want %q", tc.line, got.Mode, tc.want.Mode)
		}
		if (got.Cap == nil) != (tc.want.Cap == nil) || (got.Cap != nil && *got.Cap != *tc.want.Cap) {
			t.Errorf("%s: cap = %v, want %v", tc.line, got.Cap, tc.want.Cap)
		}
	}
}

// The serde tuple-variant indexed spelling must also be tolerated.
func TestDecodeIndexedCommands(t *testing.T) {
	var got Command
	if err := json.Unmarshal([]byte(`{"cmd":"SetMode","0":"placeholders"}`), &got); err != nil {
		t.Fatal(err)
	}
	if m := got.NormalizedMode(); m != "placeholders" {
		t.Errorf("NormalizedMode() = %q, want placeholders", m)
	}
	if err := json.Unmarshal([]byte(`{"cmd":"SetCacheCap","0":5}`), &got); err != nil {
		t.Fatal(err)
	}
	cap, ok := got.NormalizedCacheCap()
	if !ok || cap == nil || *cap != 5 {
		t.Errorf("NormalizedCacheCap() = %v, %v; want &5, true", cap, ok)
	}
}

// Update lines must match what mygo-shell's engineUpdate decodes, byte for
// byte against the Rust engine's serde output for the important variants.
func TestEncodeUpdatesGolden(t *testing.T) {
	cases := []struct {
		name string
		in   Update
		want string
	}{
		{"AuthRequired", Update{Update: UpdAuthRequired}, `{"update":"AuthRequired"}`},
		{"AuthOk", Update{Update: UpdAuthOk, Email: "a@b.c"}, `{"update":"AuthOk","email":"a@b.c"}`},
		{"LoginUrl", Update{Update: UpdLoginUrl, URL: "https://x", UserCode: "ABCD-EFGH"},
			`{"update":"LoginUrl","url":"https://x","user_code":"ABCD-EFGH"}`},
		{
			"Status",
			Update{Update: UpdStatus, State: "Running", SyncedNodes: 3, Mode: "placeholders", CacheCap: u64ptr(0), NeedsSignIn: true},
			// Some(0) serializes as 0 (serde omits only None), so a zero cap
			// is present on the wire — the shell distinguishes nil vs 0.
			`{"update":"Status","state":"Running","synced_nodes":3,"mode":"placeholders","cache_cap":0,"needs_sign_in":true}`,
		},
		{"Activity", Update{Update: UpdActivity, AtUnix: 1731000000, Kind: "Uploaded", Path: "a/b.txt"},
			`{"update":"Activity","at_unix":1731000000,"kind":"Uploaded","path":"a/b.txt"}`},
		{"Inbox", Update{Update: UpdInbox, Conflicts: []string{"x"}, Errored: []ErroredItem{{Path: "y", Error: "boom"}}},
			`{"update":"Inbox","conflicts":["x"],"errored":[{"path":"y","error":"boom"}]}`},
		{"Failed", Update{Update: UpdFailed, Error: "kaput"}, `{"update":"Failed","error":"kaput"}`},
		{"Exited", Update{Update: UpdExited, Reason: "shutdown"}, `{"update":"Exited","reason":"shutdown"}`},
	}
	for _, tc := range cases {
		got, err := MarshalUpdate(tc.in)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if string(got) != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", tc.name, got, tc.want)
		}
	}
}

// cache_cap is Option<u64>: null must be expressible when present-but-null
// (unlimited), and omitted when never set.
func TestCacheCapNullVsAbsent(t *testing.T) {
	null := Update{Update: UpdStatus, CacheCap: nil}
	// Go cannot distinguish nil-pointer-set vs absent without a shadow flag,
	// so the protocol treats "field present with null" as simply omitted —
	// the shell reads both the same way (CacheCap *uint64 stays nil).
	b, _ := MarshalUpdate(null)
	if want := `{"update":"Status"}`; string(b) != want {
		t.Errorf("null cache_cap: got %s, want %s", b, want)
	}
	set := Update{Update: UpdStatus, CacheCap: u64ptr(1073741824)}
	b, _ = MarshalUpdate(set)
	if want := `{"update":"Status","cache_cap":1073741824}`; string(b) != want {
		t.Errorf("set cache_cap: got %s, want %s", b, want)
	}
}

func u64ptr(v uint64) *uint64 { return &v }
