//! Metrics bar showing cluster-wide performance metrics.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph},
    Frame,
};

use crate::{app::App, theme::Theme};

/// Draw the metrics bar
pub fn draw(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    let metrics = &app.metrics;

    // Split into sections
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(50), // Text metrics
            Constraint::Percentage(25), // Coverage gauge
            Constraint::Percentage(25), // Backpressure gauge
        ])
        .split(area);

    // Text metrics
    draw_text_metrics(frame, chunks[0], app, theme);

    // Coverage gauge
    draw_coverage_gauge(frame, chunks[1], metrics.coverage, theme);

    // Backpressure gauge
    draw_backpressure_gauge(frame, chunks[2], metrics.backpressure, theme);
}

/// Draw text-based metrics
fn draw_text_metrics(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    let metrics = &app.metrics;

    // Build metrics line
    let slo_indicator = if metrics.within_slo {
        Span::styled("✓ SLO", theme.text_success())
    } else {
        Span::styled("✗ SLO", theme.text_error())
    };

    let qps_style = theme.text();
    let latency_style = if metrics.p99_latency_ms > 50.0 {
        theme.text_warning()
    } else {
        theme.text()
    };

    let metrics_line = Line::from(vec![
        Span::styled("QPS: ", theme.text_muted()),
        Span::styled(format!("{:.0}", metrics.qps), qps_style),
        Span::raw(" │ "),
        Span::styled("P50: ", theme.text_muted()),
        Span::styled(format!("{:.1}ms", metrics.p50_latency_ms), latency_style),
        Span::raw(" │ "),
        Span::styled("P95: ", theme.text_muted()),
        Span::styled(format!("{:.1}ms", metrics.p95_latency_ms), latency_style),
        Span::raw(" │ "),
        Span::styled("P99: ", theme.text_muted()),
        Span::styled(format!("{:.1}ms", metrics.p99_latency_ms), latency_style),
        Span::raw(" │ "),
        slo_indicator,
    ]);

    let paragraph = Paragraph::new(metrics_line).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme.border())
            .title(" Metrics ")
            .title_style(theme.text()),
    );

    frame.render_widget(paragraph, area);
}

/// Draw coverage gauge
fn draw_coverage_gauge(frame: &mut Frame, area: Rect, coverage: f32, theme: &Theme) {
    let coverage_percent = (coverage * 100.0) as u16;

    let gauge_style = if coverage >= 1.0 {
        theme.text_success()
    } else if coverage >= 0.8 {
        theme.text_warning()
    } else {
        theme.text_error()
    };

    let gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme.border())
                .title(" Coverage ")
                .title_style(theme.text()),
        )
        .gauge_style(gauge_style)
        .percent(coverage_percent)
        .label(format!("{:.0}%", coverage * 100.0));

    frame.render_widget(gauge, area);
}

/// Draw backpressure gauge
fn draw_backpressure_gauge(frame: &mut Frame, area: Rect, backpressure: f32, theme: &Theme) {
    let bp_percent = (backpressure * 100.0) as u16;

    let gauge_style = if backpressure <= 0.1 {
        theme.text_success()
    } else if backpressure <= 0.5 {
        theme.text_warning()
    } else {
        theme.text_error()
    };

    let gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme.border())
                .title(" Backpressure ")
                .title_style(theme.text()),
        )
        .gauge_style(gauge_style)
        .percent(bp_percent)
        .label(format!("{:.0}%", backpressure * 100.0));

    frame.render_widget(gauge, area);
}
