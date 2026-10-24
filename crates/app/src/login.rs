//! First-run login view (SPEC.md §5 disclosure + fork login, protocol.md
//! `auth.loginFork` / `auth.signInUrl`).

use gpui::{Context, Entity, Subscription, Window, div, prelude::*, px, rgb};

use crate::app_model::{AppModel, open_in_browser};
use crate::model::{self, DISCLOSURE, LoginState, ModelState, UiCommand};
use crate::widgets::{self, BORDER, DANGER, DIM, PANEL, TEXT};

pub struct LoginView {
    model: Entity<AppModel>,
    _sub: Subscription,
}

impl LoginView {
    pub fn new(model: Entity<AppModel>, cx: &mut Context<Self>) -> Self {
        let sub = cx.observe(&model, |_this, _model, cx| cx.notify());
        Self { model, _sub: sub }
    }

    fn start_login(&mut self, cx: &mut Context<Self>) {
        self.model.update(cx, |m, _| m.send(UiCommand::StartLogin));
    }
}

impl Render for LoginView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let state: ModelState = self.model.read(cx).state.clone();

        let mut body = div().flex().flex_col().gap_3().w_full();

        // Mandated disclosure (SPEC.md §5) — always visible on this screen.
        body = body.child(
            div()
                .p_3()
                .rounded_md()
                .border_1()
                .border_color(rgb(BORDER))
                .bg(rgb(PANEL))
                .text_color(rgb(DIM))
                .text_sm()
                .child(DISCLOSURE),
        );

        match &state.login {
            LoginState::Idle => {
                body = body
                    .child(div().child("Sign in with your Proton account to start syncing."))
                    .child(widgets::button(
                        "sign-in",
                        "Sign in with Proton",
                        cx,
                        |this, _, _, cx| {
                            this.start_login(cx);
                        },
                    ));
            }
            LoginState::Waiting => {
                body = body.child(widgets::dim("Contacting Proton…"));
            }
            LoginState::Url { url, user_code } => {
                body = body.child(
                    div().child("Finish signing in in your browser (the page was opened for you):"),
                );
                if !user_code.is_empty() {
                    body = body.child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .p_3()
                            .rounded_md()
                            .bg(rgb(PANEL))
                            .child(widgets::dim("Verification code"))
                            .child(
                                div()
                                    .text_xl()
                                    .text_color(rgb(TEXT))
                                    .child(user_code.clone()),
                            ),
                    );
                }
                body = body
                    .child(div().text_sm().text_color(rgb(DIM)).child(url.clone()))
                    .child({
                        let url = url.clone();
                        widgets::button(
                            "open-url",
                            "Open sign-in page again",
                            cx,
                            move |_, _, _, _| {
                                open_in_browser(&url);
                            },
                        )
                    })
                    .child(widgets::dim("Waiting for sign-in to complete…"));
            }
            LoginState::Failed(error) => {
                body = body
                    .child(
                        div()
                            .text_color(rgb(DANGER))
                            .child(format!("Sign-in failed: {error}")),
                    )
                    .child(widgets::button(
                        "retry",
                        "Try again",
                        cx,
                        |this, _, _, cx| {
                            this.start_login(cx);
                        },
                    ));
            }
        }

        if state.phase == model::Phase::Starting && state.login != LoginState::Idle {
            body = body.child(widgets::dim("Starting the sync engine…"));
        }

        div()
            .flex()
            .flex_col()
            .size_full()
            .bg(rgb(widgets::BG))
            .text_color(rgb(TEXT))
            .child(
                div()
                    .flex()
                    .items_center()
                    .px_4()
                    .py_3()
                    .border_b_1()
                    .border_color(rgb(BORDER))
                    .child(div().text_lg().child(model::APP_TITLE)),
            )
            .child(div().flex_1().p_4().px(px(20.0)).child(body))
    }
}
