//! Conflicts and failed uploads inbox.

use gpui::{AnyElement, Context, FontWeight, div, prelude::*, px, rgb};

use crate::components;
use crate::theme;
use crate::ui::AppView;

pub fn render(view: &mut AppView, _cx: &mut Context<AppView>) -> AnyElement {
    let conflicts = view.state.conflicts.clone();
    let errored = view.state.errored.clone();

    let mut content = div()
        .id("conflicts-scroll")
        .flex()
        .flex_col()
        .gap_5()
        .flex_1()
        .min_h(px(0.0))
        .overflow_y_scroll()
        .p(px(theme::PAGE_PADDING));

    if conflicts.is_empty() && errored.is_empty() {
        content = content.child(
            components::card().child(components::empty_state(
                "Everything is in sync",
                "No conflicts or failed uploads. Keep-both copies and upload errors will show up here.",
            )),
        );
    }

    if !conflicts.is_empty() {
        let mut card = components::card().child(components::section_title(
            "Conflicts",
            Some("The remote copy changed while a local edit was pending"),
        ));
        for path in &conflicts {
            card = card.child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .p_3()
                    .rounded(px(8.0))
                    .bg(rgb(theme::INSET))
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .truncate()
                            .child(path.clone()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(theme::MUTED))
                            .child(
                                "Kept both versions — the local copy will be renamed with a \
                                 conflict suffix and uploaded separately.",
                            ),
                    ),
            );
        }
        content = content.child(card);
    }

    if !errored.is_empty() {
        let mut card = components::card().child(components::section_title(
            "Failed uploads",
            Some("These files stay local until the next successful retry"),
        ));
        for (path, error) in &errored {
            card = card.child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .p_3()
                    .rounded(px(8.0))
                    .bg(rgb(theme::INSET))
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .truncate()
                            .child(path.clone()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(theme::DANGER))
                            .child(if error.is_empty() {
                                "Upload failed without a reported reason.".to_string()
                            } else {
                                error.clone()
                            }),
                    ),
            );
        }
        content = content.child(card);
    }

    content.into_any_element()
}
