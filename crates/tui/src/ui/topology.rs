//! Topology panel showing coordinator and shard structure.

use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem},
    Frame,
};

use crate::{
    app::{App, NodeStatus},
    theme::Theme,
    ui::layout::format_vector_count,
};

/// Draw the topology panel
pub fn draw(frame: &mut Frame, area: Rect, app: &App, theme: &Theme, is_active: bool) {
    let border_style = if is_active {
        theme.border_active()
    } else {
        theme.border()
    };

    let block = Block::default()
        .title(" Cluster Topology ")
        .title_style(if is_active {
            theme.header()
        } else {
            theme.text()
        })
        .borders(Borders::ALL)
        .border_style(border_style);

    let items = build_topology_items(app, theme);

    let list = List::new(items)
        .block(block)
        .highlight_style(theme.highlight());

    frame.render_widget(list, area);
}

/// Build list items for the topology tree
fn build_topology_items<'a>(app: &App, theme: &Theme) -> Vec<ListItem<'a>> {
    let mut items = vec![];

    // Coordinators section header
    items.push(ListItem::new(Line::from(vec![Span::styled(
        "─ Coordinators",
        theme.text_primary(),
    )])));

    // Coordinator entries
    for (i, coord) in app.cluster_state.coordinators.iter().enumerate() {
        let status_icon = match coord.status {
            NodeStatus::Healthy => "●",
            NodeStatus::Unhealthy => "○",
            NodeStatus::Unknown => "◌",
        };

        let status_style = match coord.status {
            NodeStatus::Healthy => theme.text_success(),
            NodeStatus::Unhealthy => theme.text_error(),
            NodeStatus::Unknown => theme.text_muted(),
        };

        let mut spans = vec![
            Span::raw("  "),
            Span::styled(status_icon, status_style),
            Span::raw(" "),
        ];

        // Coordinator ID and address
        let id_style = if coord.is_self {
            Style::default()
                .fg(theme.primary)
                .add_modifier(Modifier::BOLD)
        } else {
            theme.text()
        };

        spans.push(Span::styled(coord.id.clone(), id_style));
        spans.push(Span::styled(
            format!(" ({})", coord.address),
            theme.text_muted(),
        ));

        // Leader marker
        if coord.is_leader {
            spans.push(Span::styled(" ★ LEADER", theme.leader()));
        }

        // Self marker
        if coord.is_self {
            spans.push(Span::styled(" (self)", theme.text_muted()));
        }

        // Add selection indicator if this item is selected
        let line = if app.selected_index == i && app.selected_panel == crate::app::Panel::Topology {
            Line::from(spans).style(theme.highlight())
        } else {
            Line::from(spans)
        };

        items.push(ListItem::new(line));
    }

    // Empty line separator
    items.push(ListItem::new(Line::from("")));

    // Shards section header
    items.push(ListItem::new(Line::from(vec![Span::styled(
        "─ Shards",
        theme.text_primary(),
    )])));

    // Shard entries
    let coord_count = app.cluster_state.coordinators.len();
    for (i, shard) in app.cluster_state.shards.iter().enumerate() {
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

        let mut spans = vec![
            Span::raw("  "),
            Span::styled(status_icon, status_style),
            Span::raw(" "),
        ];

        // Shard ID and address
        spans.push(Span::styled(shard.id.clone(), theme.text()));
        spans.push(Span::styled(
            format!(" ({})", shard.address),
            theme.text_muted(),
        ));

        // Vector count
        spans.push(Span::styled(
            format!(" [{}]", format_vector_count(shard.vector_count)),
            theme.text_primary(),
        ));

        // GPU info if available
        if let Some(gpu_mem) = shard.gpu_memory_percent {
            let gpu_style = if gpu_mem > 90.0 {
                theme.text_error()
            } else if gpu_mem > 75.0 {
                theme.text_warning()
            } else {
                theme.text_muted()
            };
            spans.push(Span::styled(format!(" GPU:{:.0}%", gpu_mem), gpu_style));
        }

        // Temperature if available
        if let Some(temp) = shard.temperature {
            let temp_style = if temp > 80.0 {
                theme.text_error()
            } else if temp > 70.0 {
                theme.text_warning()
            } else {
                theme.text_muted()
            };
            spans.push(Span::styled(format!(" {:.0}°C", temp), temp_style));
        }

        // Selection indicator
        // Offset by coord_count + 2 (for header and separator)
        let adjusted_index = coord_count + 2 + i;
        let line = if app.selected_index == adjusted_index
            && app.selected_panel == crate::app::Panel::Topology
        {
            Line::from(spans).style(theme.highlight())
        } else {
            Line::from(spans)
        };

        items.push(ListItem::new(line));
    }

    items
}
