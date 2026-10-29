//! Activity page: a filterable, virtualised log of sync events.

use gpui::{AnyElement, Context, FontWeight, div, prelude::*, px, rgb, uniform_list};

use crate::components;
use crate::state::{ActivityFilter, Page};
use crate::theme;
use crate::ui::{AppView, activity_row};

pub fn render(view: &mut AppView, cx: &mut Context<AppView>) -> AnyElement {
    let filter = view.filter;
    let mut filters = div().flex().items_center().flex_wrap().gap_2();
    for candidate in ActivityFilter::ALL {
        let active = candidate == filter;
        let count = view.state.activity_count(candidate);
        filters = filters.child(
            div()
                .id(("filter", candidate as usize))
                .flex()
                .items_center()
                .gap_2()
                .px_3()
                .py_1()
                .rounded_full()
                .text_xs()
                .cursor_pointer()
                .bg(rgb(if active {
                    theme::ACCENT
                } else {
                    theme::CARD
                }))
                .text_color(rgb(if active {
                    theme::TEXT
                } else {
                    theme::MUTED
                }))
                .hover(|s| s.bg(rgb(theme::CARD_HOVER)))
                .on_click(cx.listener(move |this, _, _, cx| this.set_filter(candidate, cx)))
                .child(candidate.label())
                .child(
                    div()
                        .text_color(rgb(if active {
                            theme::TEXT
                        } else {
                            theme::FAINT
                        }))
                        .child(count.to_string()),
                ),
        );
    }

    let rows = view.state.filtered_activity(filter);
    let row_count = rows.len();

    let list = uniform_list(
        "activity-list",
        row_count,
        cx.processor(move |_this, range: std::ops::Range<usize>, _window, _cx| {
            range
                .map(|ix| div().h(px(42.0)).child(activity_row(&rows[ix])))
                .collect::<Vec<_>>()
        }),
    )
    .flex_1();

    let mut card = div()
        .flex()
        .flex_col()
        .flex_1()
        .min_h(px(0.0))
        .rounded(px(12.0))
        .border_1()
        .border_color(rgb(theme::BORDER))
        .bg(rgb(theme::CARD))
        .overflow_hidden()
        .child(
            div()
                .flex()
                .items_center()
                .justify_between()
                .px_4()
                .py_3()
                .border_b_1()
                .border_color(rgb(theme::BORDER))
                .child(
                    div()
                        .text_sm()
                        .font_weight(FontWeight::MEDIUM)
                        .child(format!(
                            "{} {}",
                            row_count,
                            if row_count == 1 { "event" } else { "events" }
                        )),
                )
                .child(components::text_button(
                    "activity-settings",
                    "Settings",
                    cx,
                    |this, _, _, cx| this.set_page(Page::Settings, cx),
                )),
        );

    if row_count == 0 {
        card = card.child(components::empty_state(
            "Nothing here yet",
            "Sync activity will show up as files move in either direction.",
        ));
    } else {
        card = card.child(
            div()
                .flex()
                .gap_3()
                .px_4()
                .py_2()
                .border_b_1()
                .border_color(rgb(theme::BORDER))
                .text_xs()
                .text_color(rgb(theme::FAINT))
                .child(div().w(px(64.0)).child("Time"))
                .child(div().w(px(92.0)).child("Event"))
                .child(div().flex_1().child("Path")),
        );
        card = card.child(list);
    }

    div()
        .flex()
        .flex_col()
        .gap_4()
        .flex_1()
        .min_h(px(0.0))
        .p(px(theme::PAGE_PADDING))
        .child(filters)
        .child(card)
        .into_any_element()
}
