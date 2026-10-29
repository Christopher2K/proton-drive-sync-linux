//! Keyboard and menu actions understood by the application shell.

use gpui::actions;

actions!(
    pds_app,
    [
        Quit,
        TogglePause,
        ShowOverview,
        ShowActivity,
        ShowConflicts,
        ShowSettings,
    ]
);
