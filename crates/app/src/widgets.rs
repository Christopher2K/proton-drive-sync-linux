//! Dark theme constants and the DIY button helper (gpui has no built-in
//! button widget; a styled stateful div is the idiom — gpui.md §4).

use gpui::{ClickEvent, Context, Div, Stateful, Window, div, prelude::*, rgb};

// Palette (neutral dark theme; no Proton branding, SPEC.md §5).
pub const BG: u32 = 0x17181c;
pub const PANEL: u32 = 0x1f2126;
pub const BORDER: u32 = 0x2e3138;
pub const TEXT: u32 = 0xd7dae0;
pub const DIM: u32 = 0x8a8f98;
pub const ACCENT: u32 = 0x2f6fde;
pub const ACCENT_HOVER: u32 = 0x3d7ef0;
pub const OK: u32 = 0x3f9e5f;
pub const WARN: u32 = 0xc98a2f;
pub const DANGER: u32 = 0xc94f4f;

/// A clickable button; the handler mutates the view (or its model) via the
/// listener wrapper.
pub fn button<V: 'static>(
    id: &'static str,
    label: impl Into<gpui::SharedString>,
    cx: &mut Context<V>,
    on_click: impl Fn(&mut V, &ClickEvent, &mut Window, &mut Context<V>) + 'static,
) -> Stateful<Div> {
    div()
        .id(id)
        .px_4()
        .py_2()
        .rounded_md()
        .bg(rgb(ACCENT))
        .text_color(rgb(0xffffff))
        .cursor_pointer()
        .hover(|s| s.bg(rgb(ACCENT_HOVER)))
        .on_click(cx.listener(on_click))
        .child(label.into())
}

/// A full-width button row (the big pause/resume toggle).
pub fn wide_button<V: 'static>(
    id: &'static str,
    label: impl Into<gpui::SharedString>,
    color: u32,
    cx: &mut Context<V>,
    on_click: impl Fn(&mut V, &ClickEvent, &mut Window, &mut Context<V>) + 'static,
) -> Stateful<Div> {
    div()
        .id(id)
        .w_full()
        .px_4()
        .py_3()
        .rounded_md()
        .flex()
        .justify_center()
        .bg(rgb(color))
        .text_color(rgb(0xffffff))
        .text_base()
        .cursor_pointer()
        .hover(|s| s.bg(rgb(ACCENT_HOVER)))
        .on_click(cx.listener(on_click))
        .child(label.into())
}

/// A dimmed secondary-text line.
pub fn dim(text: impl Into<gpui::SharedString>) -> Div {
    div().text_color(rgb(DIM)).child(text.into())
}
