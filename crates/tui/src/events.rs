//! Event handling for the TUI dashboard.

use std::time::Duration;

use crossterm::event::{self, Event as CrosstermEvent, KeyCode, KeyEvent, KeyModifiers};
use tokio::sync::mpsc;

use crate::action::Action;
use crate::app::{App, ClusterState, MetricsState};

/// TUI events
#[derive(Debug)]
pub enum Event {
    /// Tick event for periodic updates
    Tick,
    /// Keyboard input event
    Key(KeyEvent),
    /// Cluster state update from coordinator
    ClusterUpdate(ClusterState),
    /// Metrics update from coordinator
    MetricsUpdate(MetricsState),
    /// Result from the bounded management effect worker
    ConsoleAction(Action),
    /// Resize event
    Resize(u16, u16),
}

/// Event handler that manages async event streams
pub struct EventHandler {
    /// Receiver for events
    rx: mpsc::UnboundedReceiver<Event>,
    /// Sender for events (kept alive to allow external event injection)
    tx: mpsc::UnboundedSender<Event>,
}

impl EventHandler {
    /// Create a new event handler with the given tick rate
    pub fn new(tick_rate: Duration) -> Self {
        let tick_rate = sanitize_tick_rate(tick_rate);
        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn tick timer task
        let tx_tick = tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_rate);
            loop {
                interval.tick().await;
                if tx_tick.send(Event::Tick).is_err() {
                    break;
                }
            }
        });

        // Spawn keyboard event listener in a separate thread
        // (crossterm events are blocking)
        let tx_key = tx.clone();
        std::thread::spawn(move || {
            loop {
                // Poll for events with a small timeout
                if event::poll(Duration::from_millis(100)).unwrap_or(false) {
                    match event::read() {
                        Ok(CrosstermEvent::Key(key)) => {
                            if tx_key.send(Event::Key(key)).is_err() {
                                break;
                            }
                        }
                        Ok(CrosstermEvent::Resize(width, height))
                            if tx_key.send(Event::Resize(width, height)).is_err() =>
                        {
                            break;
                        }
                        _ => {}
                    }
                }
            }
        });

        Self { rx, tx }
    }

    /// Get the next event
    pub async fn next(&mut self) -> Option<Event> {
        self.rx.recv().await
    }

    /// Get a sender clone for injecting events
    pub fn sender(&self) -> mpsc::UnboundedSender<Event> {
        self.tx.clone()
    }
}

fn sanitize_tick_rate(tick_rate: Duration) -> Duration {
    tick_rate.max(Duration::from_millis(1))
}

/// Handle a key event and update app state
/// Returns true if the app should quit
pub fn handle_key_event(app: &mut App, key: KeyEvent) -> bool {
    // Ctrl-C remains an unconditional clean exit.
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        app.should_quit = true;
        return true;
    }

    // Import-plan editing captures printable input but never credential data.
    if app.screen == crate::app::Screen::ImportPlan && app.import_form.editing {
        match key.code {
            KeyCode::Esc => app.import_form.editing = false,
            KeyCode::Enter | KeyCode::Tab => {
                app.import_form.active_field = app.import_form.active_field.next();
            }
            KeyCode::Backspace => {
                app.import_form.active_value_mut().pop();
            }
            KeyCode::Char(character) if !character.is_control() => {
                app.import_form.active_value_mut().push(character);
            }
            _ => {}
        }
        return false;
    }

    if app.filter_editing {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => app.filter_editing = false,
            KeyCode::Backspace => {
                app.filter.pop();
            }
            KeyCode::Char(character) if !character.is_control() => app.filter.push(character),
            _ => {}
        }
        return false;
    }

    if key.code == KeyCode::Char('q') {
        app.should_quit = true;
        return true;
    }

    // Handle help toggle
    if key.code == KeyCode::Char('?') || key.code == KeyCode::F(1) {
        app.show_help = !app.show_help;
        return false;
    }

    // If help is showing, any key closes it
    if app.show_help {
        app.show_help = false;
        return false;
    }

    match key.code {
        // Navigation
        KeyCode::Up | KeyCode::Char('k') => {
            app.select_previous();
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.select_next();
        }
        KeyCode::Left | KeyCode::Char('h') => {
            app.previous_screen();
        }
        KeyCode::Right | KeyCode::Char('l') => {
            app.next_screen();
        }
        KeyCode::Tab => {
            app.next_screen();
        }
        KeyCode::BackTab => {
            app.previous_screen();
        }

        // Actions
        KeyCode::Char('r') => {
            app.set_status("Refreshing...");
            app.queue_refresh(app.screen);
        }
        KeyCode::Char('/') => {
            app.filter_editing = true;
        }
        KeyCode::Char('i') if app.screen == crate::app::Screen::ImportPlan => {
            app.import_form.editing = true;
        }
        KeyCode::Char('p') if app.screen == crate::app::Screen::ImportPlan => {
            app.request_import_plan();
        }

        // Theme cycling
        KeyCode::Char('t') => {
            let new_theme = match app.config.theme.name.as_str() {
                "default" => "minimal",
                "minimal" => "high-contrast",
                _ => "default",
            };
            app.config.theme.name = new_theme.to_string();
            app.set_status(format!("Theme: {}", new_theme));
        }

        _ => {}
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TuiConfig;

    #[test]
    fn test_quit_keys() {
        let mut app = App::new(TuiConfig::default());

        // Test 'q' key
        let key = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
        assert!(handle_key_event(&mut app, key));
        assert!(app.should_quit);
    }

    #[test]
    fn test_navigation_keys() {
        let mut app = App::with_mock_data(TuiConfig::default());

        // Test down navigation
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::NONE);
        handle_key_event(&mut app, key);
        assert_eq!(app.selected_index, 1);

        // Test up navigation
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::NONE);
        handle_key_event(&mut app, key);
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn test_screen_switch() {
        let mut app = App::new(TuiConfig::default());
        use crate::app::Screen;

        assert_eq!(app.screen, Screen::Overview);

        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE);
        handle_key_event(&mut app, key);
        assert_eq!(app.screen, Screen::Collections);
    }

    #[test]
    fn test_zero_tick_rate_is_sanitized() {
        assert_eq!(sanitize_tick_rate(Duration::ZERO), Duration::from_millis(1));
    }
}
