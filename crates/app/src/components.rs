//! Reusable presentation primitives. GPUI has no widget library, so these are
//! intentionally small, styled `div`s rather than a general design system.

use gpui::{
    ClickEvent, Context, Div, ElementId, FontWeight, SharedString, Stateful, Window, div,
    prelude::*, px, rgb,
};

use crate::theme;

/// A neutral surface used for grouped content.
pub fn card() -> Div {
    div()
        .flex()
        .flex_col()
        .gap_3()
        .p_4()
        .rounded(px(12.0))
        .border_1()
        .border_color(rgb(theme::BORDER))
        .bg(rgb(theme::CARD))
}

/// A button filled with the accent colour (primary action).
pub fn primary_button<V: 'static>(
    id: impl Into<ElementId>,
    label: impl Into<SharedString>,
    cx: &mut Context<V>,
    on_click: impl Fn(&mut V, &ClickEvent, &mut Window, &mut Context<V>) + 'static,
) -> Stateful<Div> {
    button_base(id, label, theme::ACCENT, 0xffffff, cx, on_click)
        .hover(|s| s.bg(rgb(theme::ACCENT_HOVER)))
}

/// A bordered button on the card surface (secondary action).
pub fn secondary_button<V: 'static>(
    id: impl Into<ElementId>,
    label: impl Into<SharedString>,
    cx: &mut Context<V>,
    on_click: impl Fn(&mut V, &ClickEvent, &mut Window, &mut Context<V>) + 'static,
) -> Stateful<Div> {
    button_base(id, label, theme::CARD_HOVER, theme::TEXT, cx, on_click)
        .border_1()
        .border_color(rgb(theme::BORDER_STRONG))
        .hover(|s| s.bg(rgb(theme::BORDER)))
}

/// A destructive button (sign out, discard).
pub fn danger_button<V: 'static>(
    id: impl Into<ElementId>,
    label: impl Into<SharedString>,
    cx: &mut Context<V>,
    on_click: impl Fn(&mut V, &ClickEvent, &mut Window, &mut Context<V>) + 'static,
) -> Stateful<Div> {
    button_base(id, label, theme::DANGER, 0xffffff, cx, on_click)
        .hover(|s| s.bg(rgb(0xd95f5f)))
}

/// A full-width, prominent button (pause / resume).
pub fn wide_button<V: 'static>(
    id: impl Into<ElementId>,
    label: impl Into<SharedString>,
    color: u32,
    cx: &mut Context<V>,
    on_click: impl Fn(&mut V, &ClickEvent, &mut Window, &mut Context<V>) + 'static,
) -> Stateful<Div> {
    button_base(id, label, color, 0xffffff, cx, on_click)
        .w_full()
        .py_2()
        .flex()
        .justify_center()
        .hover(|s| s.opacity(0.9))
}

/// A quiet text button (footer links, inline actions).
pub fn text_button<V: 'static>(
    id: impl Into<ElementId>,
    label: impl Into<SharedString>,
    cx: &mut Context<V>,
    on_click: impl Fn(&mut V, &ClickEvent, &mut Window, &mut Context<V>) + 'static,
) -> Stateful<Div> {
    div()
        .id(id)
        .px_2()
        .py_1()
        .rounded(px(6.0))
        .text_color(rgb(theme::MUTED))
        .cursor_pointer()
        .hover(|s| s.text_color(rgb(theme::TEXT)).bg(rgb(theme::CARD_HOVER)))
        .on_click(cx.listener(on_click))
        .child(label.into())
}

/// A small on/off switch. GPUI ships no toggle widget, so this is a styled
/// track with a knob positioned by flex alignment.
pub fn toggle_switch<V: 'static>(
    id: impl Into<ElementId>,
    on: bool,
    cx: &mut Context<V>,
    on_click: impl Fn(&mut V, &ClickEvent, &mut Window, &mut Context<V>) + 'static,
) -> Stateful<Div> {
    let mut track = div()
        .id(id)
        .flex()
        .items_center()
        .w(px(38.0))
        .h(px(22.0))
        .p(px(2.0))
        .rounded_full()
        .bg(rgb(if on {
            theme::ACCENT
        } else {
            theme::BORDER_STRONG
        }))
        .cursor_pointer()
        .on_click(cx.listener(on_click));
    track = if on {
        track.justify_end()
    } else {
        track.justify_start()
    };
    track.child(
        div()
            .size(px(18.0))
            .rounded_full()
            .bg(rgb(0xffffff)),
    )
}

fn button_base<V: 'static>(
    id: impl Into<ElementId>,
    label: impl Into<SharedString>,
    bg: u32,
    fg: u32,
    cx: &mut Context<V>,
    on_click: impl Fn(&mut V, &ClickEvent, &mut Window, &mut Context<V>) + 'static,
) -> Stateful<Div> {
    div()
        .id(id)
        .px_3()
        .py_1p5()
        .rounded(px(8.0))
        .bg(rgb(bg))
        .text_color(rgb(fg))
        .text_sm()
        .font_weight(FontWeight::MEDIUM)
        .cursor_pointer()
        .on_click(cx.listener(on_click))
        .child(label.into())
}

/// A sidebar navigation row.
pub fn nav_item<V: 'static>(
    id: impl Into<ElementId>,
    label: &'static str,
    selected: bool,
    badge: Option<usize>,
    cx: &mut Context<V>,
    on_click: impl Fn(&mut V, &ClickEvent, &mut Window, &mut Context<V>) + 'static,
) -> Stateful<Div> {
    let mut row = div()
        .id(id)
        .flex()
        .items_center()
        .justify_between()
        .gap_2()
        .px_3()
        .py_2()
        .rounded(px(8.0))
        .text_sm()
        .cursor_pointer()
        .text_color(rgb(if selected { theme::TEXT } else { theme::MUTED }))
        .on_click(cx.listener(on_click))
        .child(label);

    if selected {
        row = row.bg(rgb(theme::CARD_HOVER));
    } else {
        row = row.hover(|s| s.bg(rgb(theme::CARD)).text_color(rgb(theme::TEXT)));
    }

    if let Some(count) = badge.filter(|n| *n > 0) {
        row = row.child(pill(
            count.to_string(),
            theme::WARN,
            theme::WARN_SOFT,
        ));
    }
    row
}

/// A small rounded label.
pub fn pill(text: impl Into<SharedString>, fg: u32, bg: u32) -> Div {
    div()
        .px_2()
        .py_0p5()
        .rounded_full()
        .bg(rgb(bg))
        .text_xs()
        .text_color(rgb(fg))
        .child(text.into())
}

/// A dot + label status indicator.
pub fn status_dot(text: impl Into<SharedString>, color: u32) -> Div {
    div()
        .flex()
        .items_center()
        .gap_2()
        .child(div().size(px(7.0)).rounded_full().bg(rgb(color)))
        .child(div().text_sm().text_color(rgb(color)).child(text.into()))
}

/// Section heading inside a page.
pub fn section_title(title: impl Into<SharedString>, subtitle: Option<&'static str>) -> Div {
    let mut block = div()
        .flex()
        .flex_col()
        .gap_0p5()
        .child(
            div()
                .text_base()
                .font_weight(FontWeight::SEMIBOLD)
                .child(title.into()),
        );
    if let Some(subtitle) = subtitle {
        block = block.child(div().text_xs().text_color(rgb(theme::MUTED)).child(subtitle));
    }
    block
}

/// A metric tile used on the overview.
pub fn stat(label: &'static str, value: String, color: u32) -> Div {
    div()
        .flex()
        .flex_col()
        .gap_1()
        .p_3()
        .rounded(px(10.0))
        .bg(rgb(theme::INSET))
        .child(
            div()
                .text_2xl()
                .font_weight(FontWeight::SEMIBOLD)
                .text_color(rgb(color))
                .child(value),
        )
        .child(div().text_xs().text_color(rgb(theme::MUTED)).child(label))
}

/// Empty-state block for lists.
pub fn empty_state(title: &'static str, body: &'static str) -> Div {
    div()
        .flex()
        .flex_col()
        .items_center()
        .justify_center()
        .gap_2()
        .py(px(48.0))
        .child(
            div()
                .text_sm()
                .font_weight(FontWeight::MEDIUM)
                .text_color(rgb(theme::TEXT))
                .child(title),
        )
        .child(
            div()
                .text_sm()
                .text_color(rgb(theme::MUTED))
                .child(body),
        )
}

/// A coloured information banner (guard pauses, failures, notices).
pub fn banner(
    title: impl Into<SharedString>,
    body: impl Into<SharedString>,
    fg: u32,
    bg: u32,
) -> Div {
    div()
        .flex()
        .flex_col()
        .gap_1()
        .p_3()
        .rounded(px(10.0))
        .border_1()
        .border_color(rgb(fg))
        .bg(rgb(bg))
        .child(
            div()
                .text_sm()
                .font_weight(FontWeight::MEDIUM)
                .text_color(rgb(fg))
                .child(title.into()),
        )
        .child(
            div()
                .text_sm()
                .text_color(rgb(theme::MUTED))
                .child(body.into()),
        )
}

/// A key/value row used in settings.
pub fn field_row(label: &'static str, value: impl Into<SharedString>) -> Div {
    div()
        .flex()
        .items_center()
        .justify_between()
        .gap_4()
        .py_2()
        .border_b_1()
        .border_color(rgb(theme::BORDER))
        .child(div().text_sm().text_color(rgb(theme::MUTED)).child(label))
        .child(
            div()
                .text_sm()
                .text_color(rgb(theme::TEXT))
                .child(value.into()),
        )
}
