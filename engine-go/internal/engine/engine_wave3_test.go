// Engine battery port (wave 3): skip placeholders for tests whose Rust
// counterparts depend on features not yet ported to the Go engine.
package engine_test

import "testing"

// ---------------------------------------------------------------------------
// Skip-placeholder tests: mode conversion needs BackingDir/set_sync_mode
// (Phase 2 steps 5-6); the large-sparse test needs streaming uploads in the
// Go mock. Both stay out until those land.
// ---------------------------------------------------------------------------

func TestModeConversionPreservesPendingUploadAcrossRestart(t *testing.T) {
	t.Skip("Phase 2 steps 5-6: placeholder mode / BackingDir conversion not yet ported")
}

func TestModeSwitchWhileGuardPausedKeepsTombstonesAndTrashesNothing(t *testing.T) {
	t.Skip("Phase 2 steps 5-6: set_sync_mode / placeholder mode not yet ported")
}

func TestLargeSparseLocalFileUploadsWithoutTruncation(t *testing.T) {
	t.Skip("Phase 2: enable_streaming_uploads not available in the Go mock")
}
