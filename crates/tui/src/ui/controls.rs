//! Control bar and status line.

use ratatui::{
    layout::Rect,
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use crate::{app::App, theme::Theme};

/// Draw the control bar/status line
pub fn draw(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    // Build control hints
    let mut spans = vec![
        Span::styled(" q", theme.text_primary()),
        Span::styled(" Quit", theme.text_muted()),
        Span::raw(" │ "),
        Span::styled("↑↓", theme.text_primary()),
        Span::styled(" Navigate", theme.text_muted()),
        Span::raw(" │ "),
        Span::styled("Tab", theme.text_primary()),
        Span::styled(" Switch Screen", theme.text_muted()),
        Span::raw(" │ "),
        Span::styled("r", theme.text_primary()),
        Span::styled(" Refresh", theme.text_muted()),
        Span::raw(" │ "),
        Span::styled("/", theme.text_primary()),
        Span::styled(" Filter", theme.text_muted()),
        Span::raw(" │ "),
        Span::styled("t", theme.text_primary()),
        Span::styled(" Theme", theme.text_muted()),
        Span::raw(" │ "),
        Span::styled("?", theme.text_primary()),
        Span::styled(" Help", theme.text_muted()),
    ];

    // Add status message if present
    if let Some((message, _)) = &app.status_message {
        spans.push(Span::raw(" │ "));
        spans.push(Span::styled(message, theme.text_warning()));
    }
    if !app.filter.is_empty() || app.filter_editing {
        spans.push(Span::raw(" │ "));
        spans.push(Span::styled(
            format!(
                "filter: {}{}",
                app.filter,
                if app.filter_editing { "_" } else { "" }
            ),
            theme.text_warning(),
        ));
    }

    let controls = Paragraph::new(Line::from(spans));
    frame.render_widget(controls, area);
}
