//! Color themes for the TUI dashboard.

use ratatui::style::{Color, Modifier, Style};

/// Theme definition for the TUI
#[derive(Debug, Clone)]
pub struct Theme {
    /// Theme name
    pub name: String,
    /// Background color
    pub bg: Color,
    /// Foreground color
    pub fg: Color,
    /// Primary accent color
    pub primary: Color,
    /// Secondary accent color
    pub secondary: Color,
    /// Success/healthy color
    pub success: Color,
    /// Warning color
    pub warning: Color,
    /// Error/unhealthy color
    pub error: Color,
    /// Muted/inactive color
    pub muted: Color,
    /// Border color
    pub border: Color,
    /// Highlight color for selections
    pub highlight: Color,
}

impl Default for Theme {
    fn default() -> Self {
        Self::default_theme()
    }
}

impl Theme {
    /// Get theme by name
    pub fn by_name(name: &str) -> Self {
        match name {
            "minimal" => Self::minimal_theme(),
            "high-contrast" => Self::high_contrast_theme(),
            _ => Self::default_theme(),
        }
    }

    /// Default theme with cyan accents
    pub fn default_theme() -> Self {
        Self {
            name: "default".to_string(),
            bg: Color::Reset,
            fg: Color::White,
            primary: Color::Cyan,
            secondary: Color::Blue,
            success: Color::Green,
            warning: Color::Yellow,
            error: Color::Red,
            muted: Color::DarkGray,
            border: Color::Gray,
            highlight: Color::LightCyan,
        }
    }

    /// Minimal theme with fewer colors
    pub fn minimal_theme() -> Self {
        Self {
            name: "minimal".to_string(),
            bg: Color::Reset,
            fg: Color::White,
            primary: Color::White,
            secondary: Color::Gray,
            success: Color::White,
            warning: Color::White,
            error: Color::White,
            muted: Color::DarkGray,
            border: Color::DarkGray,
            highlight: Color::White,
        }
    }

    /// High contrast theme for accessibility
    pub fn high_contrast_theme() -> Self {
        Self {
            name: "high-contrast".to_string(),
            bg: Color::Black,
            fg: Color::White,
            primary: Color::LightYellow,
            secondary: Color::LightBlue,
            success: Color::LightGreen,
            warning: Color::LightYellow,
            error: Color::LightRed,
            muted: Color::Gray,
            border: Color::White,
            highlight: Color::LightYellow,
        }
    }

    // Style builders

    /// Style for normal text
    pub fn text(&self) -> Style {
        Style::default().fg(self.fg)
    }

    /// Style for muted text
    pub fn text_muted(&self) -> Style {
        Style::default().fg(self.muted)
    }

    /// Style for primary/title text
    pub fn text_primary(&self) -> Style {
        Style::default().fg(self.primary)
    }

    /// Style for success indicators
    pub fn text_success(&self) -> Style {
        Style::default().fg(self.success)
    }

    /// Style for warning indicators
    pub fn text_warning(&self) -> Style {
        Style::default().fg(self.warning)
    }

    /// Style for error indicators
    pub fn text_error(&self) -> Style {
        Style::default().fg(self.error)
    }

    /// Style for borders
    pub fn border(&self) -> Style {
        Style::default().fg(self.border)
    }

    /// Style for active/selected border
    pub fn border_active(&self) -> Style {
        Style::default().fg(self.primary)
    }

    /// Style for highlighted/selected items
    pub fn highlight(&self) -> Style {
        Style::default()
            .fg(self.highlight)
            .add_modifier(Modifier::BOLD)
    }

    /// Style for header text
    pub fn header(&self) -> Style {
        Style::default()
            .fg(self.primary)
            .add_modifier(Modifier::BOLD)
    }

    /// Style for leader indicator
    pub fn leader(&self) -> Style {
        Style::default()
            .fg(self.success)
            .add_modifier(Modifier::BOLD)
    }

    /// Style for sparkline
    pub fn sparkline(&self) -> Style {
        Style::default().fg(self.success)
    }

    /// Style for sparkline in warning state
    pub fn sparkline_warning(&self) -> Style {
        Style::default().fg(self.warning)
    }

    /// Style for sparkline in error state
    pub fn sparkline_error(&self) -> Style {
        Style::default().fg(self.error)
    }

    /// Style for gauge fill
    pub fn gauge_fill(&self) -> Style {
        Style::default().fg(self.primary)
    }

    /// Style for gauge background
    pub fn gauge_bg(&self) -> Style {
        Style::default().fg(self.muted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_theme_by_name() {
        let default = Theme::by_name("default");
        assert_eq!(default.name, "default");

        let minimal = Theme::by_name("minimal");
        assert_eq!(minimal.name, "minimal");

        let high_contrast = Theme::by_name("high-contrast");
        assert_eq!(high_contrast.name, "high-contrast");

        let unknown = Theme::by_name("unknown");
        assert_eq!(unknown.name, "default");
    }
}
