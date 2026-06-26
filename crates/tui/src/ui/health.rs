//! Health sparklines panel showing per-shard health trends.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Sparkline},
    Frame,
};

use crate::{
    app::{App, NodeStatus, ShardInfo},
    theme::Theme,
};

/// Draw the health panel with sparklines for each shard
pub fn draw(frame: &mut Frame, area: Rect, app: &App, theme: &Theme, is_active: bool) {
    let border_style = if is_active {
        theme.border_active()
    } else {
        theme.border()
    };

    let block = Block::default()
        .title(" Health Trends ")
        .title_style(if is_active {
            theme.header()
        } else {
            theme.text()
        })
        .borders(Borders::ALL)
        .border_style(border_style);

    let inner = block.inner(area);
    frame.render_widget(block, area);

    if app.cluster_state.shards.is_empty() {
        let no_shards = Paragraph::new("No shards connected").style(theme.text_muted());
        frame.render_widget(no_shards, inner);
        return;
    }

    // Calculate row height for each shard
    let shard_count = app.cluster_state.shards.len();
    let available_height = inner.height as usize;
    let row_height = (available_height / shard_count).max(3);

    // Draw each shard's health row
    for (i, shard) in app.cluster_state.shards.iter().enumerate() {
        let y = inner.y + (i * row_height) as u16;
        if y >= inner.y + inner.height {
            break;
        }

        let actual_height = row_height.min((inner.y + inner.height - y) as usize);
        let row_area = Rect::new(inner.x, y, inner.width, actual_height as u16);

        let is_selected = is_active
            && app.selected_panel == crate::app::Panel::Health
            && app.selected_index == i;

        draw_shard_health_row(frame, row_area, shard, &app.metrics.history, theme, is_selected);
    }
}

/// Draw a single shard's health row
fn draw_shard_health_row(
    frame: &mut Frame,
    area: Rect,
    shard: &ShardInfo,
    history: &crate::app::MetricsHistory,
    theme: &Theme,
    is_selected: bool,
) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(30), // Shard info
            Constraint::Min(20),    // Sparkline
        ])
        .split(area);

    // Shard info section
    draw_shard_info(frame, chunks[0], shard, theme, is_selected);

    // Health sparkline section
    draw_health_sparkline(frame, chunks[1], shard, history, theme);
}

/// Draw shard information
fn draw_shard_info(
    frame: &mut Frame,
    area: Rect,
    shard: &ShardInfo,
    theme: &Theme,
    is_selected: bool,
) {
    let status_icon = match shard.status {
        NodeStatus::Healthy => "●",
        NodeStatus::Unhealthy => "○",
        NodeStatus::Unknown => "◌",
    };

    let status_style = match shard.status {
        NodeStatus::Healthy => theme.text_success(),
        NodeStatus::Unhealthy => theme.text_error(),
        NodeStatus::Unknown => theme.text_muted(),
    };

    let health_style = if shard.health_score >= 0.9 {
        theme.text_success()
    } else if shard.health_score >= 0.7 {
        theme.text_warning()
    } else {
        theme.text_error()
    };

    let mut lines = vec![
        Line::from(vec![
            Span::styled(status_icon, status_style),
            Span::raw(" "),
            Span::styled(
                &shard.id,
                if is_selected {
                    theme.highlight()
                } else {
                    theme.text()
                },
            ),
        ]),
        Line::from(vec![
            Span::styled("  Health: ", theme.text_muted()),
            Span::styled(format!("{:.0}%", shard.health_score * 100.0), health_style),
        ]),
    ];

    // Add GPU info if available
    if let Some(gpu_mem) = shard.gpu_memory_percent {
        let gpu_style = if gpu_mem > 90.0 {
            theme.text_error()
        } else if gpu_mem > 75.0 {
            theme.text_warning()
        } else {
            theme.text_muted()
        };
        lines.push(Line::from(vec![
            Span::styled("  GPU: ", theme.text_muted()),
            Span::styled(format!("{:.0}%", gpu_mem), gpu_style),
        ]));
    }

    let info = Paragraph::new(lines);
    frame.render_widget(info, area);
}

/// Draw health sparkline
fn draw_health_sparkline(
    frame: &mut Frame,
    area: Rect,
    shard: &ShardInfo,
    history: &crate::app::MetricsHistory,
    theme: &Theme,
) {
    // Get health history data
    let health_data = history.get_shard_health(&shard.id);

    // Convert to u64 for sparkline (scaled to 0-100)
    let data: Vec<u64> = health_data.iter().map(|h| (h * 100.0) as u64).collect();

    // Determine sparkline style based on current health
    let sparkline_style = if shard.health_score >= 0.9 {
        theme.sparkline()
    } else if shard.health_score >= 0.7 {
        theme.sparkline_warning()
    } else {
        theme.sparkline_error()
    };

    // If we have no data, show a placeholder
    if data.is_empty() {
        let placeholder = Paragraph::new("No history data").style(theme.text_muted());
        frame.render_widget(placeholder, area);
        return;
    }

    let sparkline = Sparkline::default()
        .data(&data)
        .max(100)
        .style(sparkline_style);

    frame.render_widget(sparkline, area);
}
