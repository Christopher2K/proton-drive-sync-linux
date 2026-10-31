//! Keyboard and menu actions understood by the application shell.

use gpui::actions;

actions!(
    pds_app,
    [
        Quit,
        TogglePause,
        OpenDashboard,
        OpenSyncFolder,
        OpenSettings,
        ShowOverview,
        ShowActivity,
        ShowConflicts,
        ShowSettings,
    ]
);
