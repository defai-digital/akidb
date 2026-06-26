//! Main layout for the TUI dashboard.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

use crate::{
    app::{App, Panel},
    theme::Theme,
    ui::{controls, health, metrics, topology},
};

/// Draw the main application layout
pub fn draw(frame: &mut Frame, app: &App) {
    let theme = Theme::by_name(&app.config.theme.name);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Header
            Constraint::Min(10),    // Main content
            Constraint::Length(3),  // Metrics bar
            Constraint::Length(1),  // Controls/status
        ])
        .split(frame.area());

    draw_header(frame, chunks[0], app, &theme);
    draw_main_content(frame, chunks[1], app, &theme);
    metrics::draw(frame, chunks[2], app, &theme);
    controls::draw(frame, chunks[3], app, &theme);

    // Draw help overlay if active
    if app.show_help {
        draw_help_overlay(frame, &theme);
    }
}

/// Draw the header section
fn draw_header(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    let title = format!(
        " AkiDB Thor Edition {} ",
        if app.config.mock_mode {
            "[MOCK]"
        } else {
            ""
        }
    );

    // Build status info
    let coordinator_count = app.cluster_state.coordinators.len();
    let shard_count = app.cluster_state.shards.len();
    let leader = app
        .cluster_state
        .leader_id
        .as_ref()
        .map(|id| format!("Leader: {}...", &id[..12.min(id.len())]))
        .unwrap_or_else(|| "No leader".to_string());

    let status_text = format!(
        "Coordinators: {} | Shards: {} | {}",
        coordinator_count, shard_count, leader
    );

    let header_text = vec![
        Line::from(vec![
            Span::styled(&title, theme.header()),
            Span::raw(" "),
            Span::styled(status_text, theme.text_muted()),
        ]),
    ];

    let header = Paragraph::new(header_text).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme.border())
            .title_style(theme.header()),
    );

    frame.render_widget(header, area);
}

/// Draw the main content area with topology and health panels
fn draw_main_content(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    let show_both = app.config.layout.show_topology && app.config.layout.show_health;

    if show_both {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(40), // Topology
                Constraint::Percentage(60), // Health
            ])
            .split(area);

        topology::draw(
            frame,
            chunks[0],
            app,
            theme,
            app.selected_panel == Panel::Topology,
        );
        health::draw(
            frame,
            chunks[1],
            app,
            theme,
            app.selected_panel == Panel::Health,
        );
    } else if app.config.layout.show_topology {
        topology::draw(frame, area, app, theme, true);
    } else if app.config.layout.show_health {
        health::draw(frame, area, app, theme, true);
    }
}

/// Draw help overlay
fn draw_help_overlay(frame: &mut Frame, theme: &Theme) {
    let area = frame.area();

    // Calculate centered popup area
    let popup_width = 50.min(area.width.saturating_sub(4));
    let popup_height = 15.min(area.height.saturating_sub(4));
    let popup_x = (area.width.saturating_sub(popup_width)) / 2;
    let popup_y = (area.height.saturating_sub(popup_height)) / 2;

    let popup_area = Rect::new(popup_x, popup_y, popup_width, popup_height);

    // Clear background
    let clear = Block::default().style(Style::default().bg(theme.bg));
    frame.render_widget(clear, popup_area);

    let help_text = vec![
        Line::from(Span::styled(
            "Keyboard Shortcuts",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from("Navigation:"),
        Line::from("  j/↓    Move down"),
        Line::from("  k/↑    Move up"),
        Line::from("  h/←    Previous panel"),
        Line::from("  l/→    Next panel"),
        Line::from("  Tab    Switch panel"),
        Line::from(""),
        Line::from("Actions:"),
        Line::from("  r      Refresh data"),
        Line::from("  t      Cycle theme"),
        Line::from("  e      Evict node"),
        Line::from("  ?/F1   Toggle help"),
        Line::from("  q      Quit"),
    ];

    let help = Paragraph::new(help_text).block(
        Block::default()
            .title(" Help ")
            .borders(Borders::ALL)
            .border_style(theme.border_active()),
    );

    frame.render_widget(help, popup_area);
}

/// Format a vector count with K/M suffixes
pub fn format_vector_count(count: u64) -> String {
    if count >= 1_000_000 {
        format!("{:.1}M", count as f64 / 1_000_000.0)
    } else if count >= 1_000 {
        format!("{:.1}K", count as f64 / 1_000.0)
    } else {
        count.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_vector_count() {
        assert_eq!(format_vector_count(0), "0");
        assert_eq!(format_vector_count(500), "500");
        assert_eq!(format_vector_count(1000), "1.0K");
        assert_eq!(format_vector_count(1500), "1.5K");
        assert_eq!(format_vector_count(1_000_000), "1.0M");
        assert_eq!(format_vector_count(2_500_000), "2.5M");
    }
}
