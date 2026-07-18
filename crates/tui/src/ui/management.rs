//! Pure renderers for read/plan-only Operations Console screens.

use chrono::{DateTime, Utc};
use ratatui::{
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
    Frame,
};

use crate::app::{App, ImportField, Screen};
use crate::model::LoadState;
use crate::theme::Theme;

pub fn draw(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    if area.width < 60 || area.height < 8 {
        let compact = Paragraph::new(vec![
            Line::from("Terminal is too small for this view."),
            Line::from("Recommended minimum: 80×24. q quit · ? help"),
        ])
        .block(block(app.screen.title(), theme))
        .wrap(Wrap { trim: true });
        frame.render_widget(compact, area);
        return;
    }

    match app.screen {
        Screen::Overview => {}
        Screen::Collections => draw_collections(frame, area, app, theme),
        Screen::Operations => draw_operations(frame, area, app, theme),
        Screen::Snapshots => draw_snapshots(frame, area, app, theme),
        Screen::ImportPlan => draw_import_plan(frame, area, app, theme),
        Screen::Access => draw_access(frame, area, app, theme),
        Screen::Audit => draw_audit(frame, area, app, theme),
    }
}

fn draw_collections(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    let mut lines = state_banner(&app.console.collections, theme);
    if let Some(collections) = state_value(&app.console.collections) {
        if collections.is_empty() {
            lines.push(Line::from(Span::styled(
                "— No collections",
                theme.text_muted(),
            )));
        } else {
            lines.push(Line::from(Span::styled(
                "NAME                 VECTORS   DIMS   METRIC   PRECISION   MODEL / CHUNK",
                theme.text_primary(),
            )));
            for (index, collection) in collections
                .iter()
                .filter(|collection| {
                    filter_matches(
                        &app.filter,
                        &[
                            &collection.name,
                            &collection.metric,
                            &collection.embedding_model_id,
                        ],
                    )
                })
                .enumerate()
            {
                let line = Line::from(format!(
                    "{:<20} {:>8} {:>6}   {:<7}  {:<9}   {} / {}",
                    truncate(&collection.name, 20),
                    collection.vector_count,
                    collection.dimensions,
                    collection.metric,
                    collection.vector_precision,
                    truncate(&collection.embedding_model_id, 24),
                    collection.chunk_strategy,
                ));
                lines.push(if index == app.selected_index {
                    line.style(theme.highlight())
                } else {
                    line
                });
            }
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "[READ ONLY] Registered schemas may not have a live physical index.",
                theme.text_muted(),
            )));
        }
    }
    render(frame, area, " Collections · inventory only ", lines, theme);
}

fn draw_operations(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    let mut lines = state_banner(&app.console.operations, theme);
    if let Some(operations) = state_value(&app.console.operations) {
        if operations.is_empty() {
            lines.push(Line::from(Span::styled(
                "— No recorded operations",
                theme.text_muted(),
            )));
        } else {
            lines.push(Line::from(Span::styled(
                "STATE                  TYPE                    TARGET                 PROGRESS   UPDATED",
                theme.text_primary(),
            )));
            for (index, operation) in operations
                .iter()
                .filter(|operation| {
                    filter_matches(
                        &app.filter,
                        &[
                            &operation.id,
                            &operation.operation_type,
                            &operation.state,
                            &operation.target,
                        ],
                    )
                })
                .enumerate()
            {
                let progress = operation
                    .progress_percent
                    .map(|value| format!("{value:>5.0}%"))
                    .unwrap_or_else(|| "    — ".to_string());
                let line = Line::from(format!(
                    "{:<22} {:<23} {:<22} {}   {}",
                    truncate(&operation.state, 22),
                    truncate(&operation.operation_type, 23),
                    truncate(&operation.target, 22),
                    progress,
                    format_time(operation.updated_at_ms),
                ));
                lines.push(if index == app.selected_index {
                    line.style(theme.highlight())
                } else {
                    line
                });
                if index == app.selected_index {
                    lines.push(Line::from(Span::styled(
                        format!(
                            "  id={} · items={} · bytes={}{}",
                            operation.id,
                            operation.items_processed,
                            format_bytes(operation.bytes_processed),
                            operation
                                .problem
                                .as_ref()
                                .map(|problem| format!(" · problem={problem}"))
                                .unwrap_or_default()
                        ),
                        theme.text_muted(),
                    )));
                }
            }
        }
    }
    render(
        frame,
        area,
        " Operations · status/history only ",
        lines,
        theme,
    );
}

fn draw_snapshots(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    let mut lines = state_banner(&app.console.snapshots, theme);
    lines.push(Line::from(Span::styled(
        "Snapshot integrity and restore-test evidence are independent.",
        theme.text_warning(),
    )));
    if let Some(snapshots) = state_value(&app.console.snapshots) {
        if snapshots.is_empty() {
            lines.push(Line::from(Span::styled(
                "— No snapshots",
                theme.text_muted(),
            )));
        } else {
            lines.push(Line::from(Span::styled(
                "ID                    COLLECTION       SIZE       MANIFEST  CHECKSUM/VERIFY       RESTORE TEST",
                theme.text_primary(),
            )));
            for (index, snapshot) in snapshots
                .iter()
                .filter(|snapshot| {
                    filter_matches(
                        &app.filter,
                        &[
                            &snapshot.id,
                            &snapshot.collection,
                            &snapshot.verification_state,
                            &snapshot.restore_test_state,
                        ],
                    )
                })
                .enumerate()
            {
                let line = Line::from(format!(
                    "{:<21} {:<16} {:>9}  {:<8}  {:<20}  {}",
                    truncate(&snapshot.id, 21),
                    truncate(&snapshot.collection, 16),
                    format_bytes(snapshot.size_bytes),
                    if snapshot.manifest_present {
                        "present"
                    } else {
                        "missing"
                    },
                    truncate(&snapshot.verification_state, 20),
                    snapshot.restore_test_state,
                ));
                lines.push(if index == app.selected_index {
                    line.style(theme.highlight())
                } else {
                    line
                });
                if index == app.selected_index {
                    lines.push(Line::from(Span::styled(
                        format!("  created {}", format_time(snapshot.created_at_ms)),
                        theme.text_muted(),
                    )));
                }
            }
        }
    }
    render(frame, area, " Snapshots · evidence only ", lines, theme);
}

fn draw_import_plan(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    let form = &app.import_form;
    let mut lines = vec![
        Line::from(Span::styled(
            "[VALIDATION ONLY] No import execution RPC exists in console v1.",
            theme.text_warning(),
        )),
        form_line(
            "Staging ID",
            &form.staging_id,
            ImportField::StagingId,
            app,
            theme,
        ),
        form_line(
            "Object ID",
            &form.object_id,
            ImportField::ObjectId,
            app,
            theme,
        ),
        form_line("ETag", &form.etag, ImportField::Etag, app, theme),
        form_line(
            "Size bytes",
            &form.size_bytes,
            ImportField::SizeBytes,
            app,
            theme,
        ),
        form_line(
            "Collection",
            &form.collection,
            ImportField::Collection,
            app,
            theme,
        ),
        form_line(
            "Duplicate policy",
            &form.duplicate_policy,
            ImportField::DuplicatePolicy,
            app,
            theme,
        ),
        Line::from(Span::styled(
            "i edit · Enter next field · Esc finish editing · p request plan",
            theme.text_muted(),
        )),
        Line::from(""),
    ];
    lines.extend(state_banner(&app.console.import_plan, theme));
    if let Some(plan) = state_value(&app.console.import_plan) {
        lines.push(Line::from(format!("Plan       {}", plan.plan_id)));
        lines.push(Line::from(format!(
            "Target     {}/{}",
            plan.workspace_id, plan.target_id
        )));
        lines.push(Line::from(format!(
            "Source     {} · {}",
            truncate(&plan.source_fingerprint, 16),
            format_bytes(plan.source_bytes)
        )));
        lines.push(Line::from(format!(
            "Estimate   expanded={} documents={} chunks={} vectors={}",
            optional_bytes(plan.estimated_expanded_bytes),
            optional_count(plan.estimated_documents),
            optional_count(plan.estimated_chunks),
            optional_count(plan.estimated_vectors),
        )));
        lines.push(Line::from(format!(
            "Expires    {} · executable={} · hash={}…",
            format_time(plan.expires_at_ms),
            plan.executable,
            truncate(&plan.plan_hash, 12)
        )));
        for finding in &plan.findings {
            lines.push(Line::from(format!(
                "{} {}: {}",
                finding.severity, finding.code, finding.message
            )));
        }
    }
    render(frame, area, " Import Plan ", lines, theme);
}

fn draw_access(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    let mut lines = state_banner(&app.console.capabilities, theme);
    if let Some(capabilities) = state_value(&app.console.capabilities) {
        lines.extend([
            Line::from(format!(
                "Server      {} · management API v{}",
                capabilities.server_version, capabilities.api_version
            )),
            Line::from(format!(
                "Connection  TLS={} · auth mode={} · authenticated={}",
                capabilities.tls_active, capabilities.auth_mode, capabilities.authenticated
            )),
            Line::from(format!(
                "Identity    workspace={} · agent={}",
                capabilities.workspace_id,
                capabilities.agent_id.as_deref().unwrap_or("—")
            )),
            Line::from(format!(
                "Credential  source={}",
                capabilities.credential_source
            )),
            Line::from(Span::styled(
                "Credential values and paths are never included in rendered state.",
                theme.text_muted(),
            )),
            Line::from(""),
        ]);
        for capability in &capabilities.capabilities {
            let status = if capability.supported && capability.authorized {
                "✓ available"
            } else if capability.supported {
                "[DENIED]"
            } else {
                "— unsupported"
            };
            lines.push(Line::from(format!(
                "{:<24} {:<14} {}",
                capability.name, status, capability.unavailable_reason
            )));
        }
    }
    render(frame, area, " Access · diagnostics only ", lines, theme);
}

fn draw_audit(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    let mut lines = state_banner(&app.console.audit, theme);
    if let Some(page) = state_value(&app.console.audit) {
        lines.push(Line::from(Span::styled(
            format!(
                "Retention: {} · integrity: {}",
                page.retention_notice, page.integrity_status
            ),
            theme.text_warning(),
        )));
        lines.push(Line::from(Span::styled(
            "TIME                 ACTOR              ACTION                       OUTCOME     TARGET",
            theme.text_primary(),
        )));
        if page.events.is_empty() {
            lines.push(Line::from(Span::styled(
                "— No audit events",
                theme.text_muted(),
            )));
        }
        for (index, event) in page
            .events
            .iter()
            .filter(|event| {
                filter_matches(
                    &app.filter,
                    &[
                        &event.actor_id,
                        &event.action,
                        &event.target,
                        &event.outcome,
                        &event.reason_code,
                    ],
                )
            })
            .enumerate()
        {
            let line = Line::from(format!(
                "{:<20} {:<18} {:<28} {:<11} {}",
                format_time(event.occurred_at_ms),
                truncate(&event.actor_id, 18),
                truncate(&event.action, 28),
                event.outcome,
                truncate(&event.target, 28),
            ));
            lines.push(if index == app.selected_index {
                line.style(theme.highlight())
            } else {
                line
            });
            if index == app.selected_index {
                lines.push(Line::from(Span::styled(
                    format!(
                        "  reason={} · request={}",
                        event.reason_code, event.request_id
                    ),
                    theme.text_muted(),
                )));
            }
        }
    }
    render(frame, area, " Audit · server-redacted ", lines, theme);
}

fn form_line<'a>(
    label: &str,
    value: &'a str,
    field: ImportField,
    app: &App,
    theme: &Theme,
) -> Line<'a> {
    let line = Line::from(vec![
        Span::styled(format!("{label:<18}"), theme.text_muted()),
        Span::raw(if value.is_empty() { "—" } else { value }),
    ]);
    if app.import_form.active_field == field {
        line.style(theme.highlight())
    } else {
        line
    }
}

fn state_banner<T>(state: &LoadState<T>, theme: &Theme) -> Vec<Line<'static>> {
    match state {
        LoadState::NotLoaded => vec![Line::from(Span::styled("— Not loaded", theme.text_muted()))],
        LoadState::Loading { previous: None } => {
            vec![Line::from(Span::styled("… Loading", theme.text_muted()))]
        }
        LoadState::Loading { previous: Some(_) } => vec![Line::from(Span::styled(
            "… Refreshing; previous data shown",
            theme.text_muted(),
        ))],
        LoadState::Ready { partial: true, .. } => vec![Line::from(Span::styled(
            "! Partial response",
            theme.text_warning(),
        ))],
        LoadState::Ready { .. } => Vec::new(),
        LoadState::Stale { error, .. } => vec![Line::from(Span::styled(
            format!("! Stale data: {error}"),
            theme.text_warning(),
        ))],
        LoadState::Denied { capability } => vec![Line::from(Span::styled(
            format!("[DENIED] {capability}"),
            theme.text_error(),
        ))],
        LoadState::Unsupported { reason } => vec![Line::from(Span::styled(
            format!("— Unsupported: {reason}"),
            theme.text_muted(),
        ))],
        LoadState::Failed(error) => vec![Line::from(Span::styled(
            format!("× {error}"),
            theme.text_error(),
        ))],
    }
}

fn state_value<T>(state: &LoadState<T>) -> Option<&T> {
    match state {
        LoadState::Ready { value, .. } | LoadState::Stale { value, .. } => Some(value),
        LoadState::Loading {
            previous: Some(value),
        } => Some(value),
        _ => None,
    }
}

fn render(frame: &mut Frame, area: Rect, title: &str, lines: Vec<Line<'_>>, theme: &Theme) {
    let paragraph = Paragraph::new(lines)
        .block(block(title, theme))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}

fn block<'a>(title: &'a str, theme: &Theme) -> Block<'a> {
    Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(theme.border_active())
}

fn format_time(timestamp_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .map(|time| time.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "—".to_string())
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

fn optional_bytes(value: Option<u64>) -> String {
    value
        .map(format_bytes)
        .unwrap_or_else(|| "unknown".to_string())
}

fn optional_count(value: Option<u64>) -> String {
    value
        .map(|count| count.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        value.to_string()
    } else {
        value
            .chars()
            .take(max.saturating_sub(1))
            .collect::<String>()
            + "…"
    }
}

fn filter_matches(filter: &str, values: &[&str]) -> bool {
    if filter.is_empty() {
        return true;
    }
    let filter = filter.to_ascii_lowercase();
    values
        .iter()
        .any(|value| value.to_ascii_lowercase().contains(&filter))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_are_human_readable() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
    }

    #[test]
    fn truncation_is_bounded() {
        assert_eq!(truncate("abcdef", 4), "abc…");
    }

    #[test]
    fn filters_are_case_insensitive() {
        assert!(filter_matches("FAIL", &["operation_failed", "target"]));
        assert!(!filter_matches("snapshot", &["collection", "ready"]));
    }
}
