use crate::state::{Focus, KeybindingMode};
use hjkl_config::{AppConfig, Validate, ValidationError, ensure_non_empty_str, ensure_non_zero};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Bundled default config — the single source of truth for default values.
/// User overrides are deep-merged on top via [`hjkl_config::load_layered_from`].
pub const DEFAULTS_TOML: &str = include_str!("config.toml");

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MainConfig {
    pub editor: EditorConfig,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EditorConfig {
    pub keybindings: KeybindingMode,
    pub lsp_binary: String,
    pub mouse_scroll_lines: usize,
    pub leader_key: char,
    /// Whether `Ctrl+Shift+Enter` (run-all) stops on the first query error.
    pub stop_on_error: bool,
    /// Seconds before cached schema data (databases / tables / columns) is
    /// considered stale and re-fetched in the background. `0` disables TTL.
    pub schema_ttl_secs: u64,
}

impl Default for MainConfig {
    /// Parses the bundled [`DEFAULTS_TOML`]. Panics if the bundled file is
    /// malformed — that's a build-time bug caught by [`tests::defaults_parse`].
    fn default() -> Self {
        toml::from_str(DEFAULTS_TOML)
            .expect("bundled sqeel-core/src/config.toml is invalid; build-time bug")
    }
}

impl AppConfig for MainConfig {
    const APPLICATION: &'static str = "sqeel";
}

impl Validate for MainConfig {
    type Error = ValidationError;

    fn validate(&self) -> Result<(), Self::Error> {
        ensure_non_empty_str(&self.editor.lsp_binary, "editor.lsp_binary")?;
        ensure_non_zero(self.editor.mouse_scroll_lines, "editor.mouse_scroll_lines")?;
        // leader_key is a `char` — multi-char and empty leaders are
        // already rejected at parse time by serde's char deserializer
        // (TOML strings of length != 1 fail to convert to `char`). No
        // additional validation needed here.
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConnectionConfig {
    pub url: String,
    // Derived from filename at load time; not present in the .toml file itself.
    #[serde(default, skip_serializing)]
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct TabCursor {
    pub name: String,
    pub row: usize,
    pub col: usize,
}

/// Lightweight pointer persisted in session.toml for a single results tab.
/// Success rows live in a separate JSON under
/// `~/.local/share/sqeel/results/<conn>/<filename>.json`. Error + cancelled
/// outcomes are stored inline.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Default)]
pub struct SavedResultRef {
    /// Present only for success tabs — on-disk JSON payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub scroll: usize,
    #[serde(default)]
    pub col_scroll: usize,
    /// Error text captured when the query failed. `None` for success /
    /// cancelled tabs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// True for tabs whose batch slot was skipped after an earlier error.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cancelled: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct Session {
    connection: String,
    #[serde(default)]
    schema_cursor: usize,
    #[serde(default)]
    schema_cursor_path: Option<String>,
    #[serde(default)]
    schema_expanded_paths: Vec<String>,
    #[serde(default)]
    focus: Focus,
    #[serde(default)]
    schema_search: Option<String>,
    #[serde(default)]
    tab_cursors: Vec<TabCursor>,
    #[serde(default)]
    active_tab: usize,
    #[serde(default)]
    result_tabs: Vec<SavedResultRef>,
    #[serde(default)]
    active_result_tab: usize,
}

/// Data restored from session.toml.
#[derive(Debug, Default)]
pub struct SessionData {
    pub connection: Option<String>,
    /// Numeric fallback cursor — used only when `schema_cursor_path` lookup fails.
    pub schema_cursor: usize,
    /// Preferred cursor: `"db/table/col"` path string for stable restore across schema changes.
    pub schema_cursor_path: Option<String>,
    /// Expanded node paths, e.g. `["mydb", "mydb/users"]`.
    pub schema_expanded_paths: Vec<String>,
    pub focus: Focus,
    pub schema_search: Option<String>,
    /// Per-tab editor cursor positions, keyed by tab name.
    pub tab_cursors: Vec<TabCursor>,
    pub active_tab: usize,
    pub result_tabs: Vec<SavedResultRef>,
    pub active_result_tab: usize,
}

/// Process-wide override for the config dir, set by `--sandbox` so
/// dev-mode runs don't touch the user's real `~/.config/sqeel/`.
/// `None` (the default) falls back to [`hjkl_config::config_dir`] keyed
/// off the [`AppConfig`] impl on [`MainConfig`].
static CONFIG_DIR_OVERRIDE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// Install a sandbox config dir. Idempotent — first call wins.
/// Subsequent calls are silently ignored so a misconfigured caller
/// can't surprise the user mid-run by repointing the dir.
pub fn set_config_dir_override(path: PathBuf) {
    let _ = CONFIG_DIR_OVERRIDE.set(path);
}

/// Resolve the sqeel config root. Sandbox override (set via
/// [`set_config_dir_override`] from `--sandbox`) wins; otherwise routes
/// through [`hjkl_config::config_dir`] using
/// [`MainConfig`]'s [`AppConfig`] impl, so the application name lives in
/// exactly one place (the trait constant). Per-platform paths:
///
/// - Linux: `$XDG_CONFIG_HOME/sqeel` (default `~/.config/sqeel`)
/// - macOS: `~/Library/Application Support/sh.kryptic.sqeel`
/// - Windows: `%APPDATA%\kryptic\sqeel\config`
pub fn config_dir() -> Option<PathBuf> {
    if let Some(p) = CONFIG_DIR_OVERRIDE.get() {
        return Some(p.clone());
    }
    hjkl_config::config_dir::<MainConfig>().ok()
}

/// Load + validate `MainConfig`.
///
/// Defaults are bundled into the binary via [`DEFAULTS_TOML`]; the user
/// file at `<config_dir>/config.toml` is **deep-merged** on top
/// (only overridden fields need to appear there). Unknown keys are
/// rejected. Validation is run on the merged result and surfaces
/// out-of-range values (empty `lsp_binary`, zero `mouse_scroll_lines`).
/// Multi-char or empty `leader_key` is caught at parse time by serde's
/// `char` deserializer (TOML strings of length != 1 fail to convert).
///
/// Missing user file → bundled defaults only. **Never writes to disk** —
/// callers that want to scaffold a starter config can use
/// [`hjkl_config::write_default`] explicitly.
pub fn load_main_config() -> anyhow::Result<MainConfig> {
    let cfg = match config_dir() {
        Some(dir) => {
            let path = dir.join("config.toml");
            if path.exists() {
                hjkl_config::load_layered_from::<MainConfig>(DEFAULTS_TOML, &path)?
            } else {
                MainConfig::default()
            }
        }
        None => MainConfig::default(),
    };
    cfg.validate().map_err(|e| anyhow::anyhow!(e))?;
    Ok(cfg)
}

pub fn load_connections() -> anyhow::Result<Vec<ConnectionConfig>> {
    let conns_dir = config_dir()
        .ok_or_else(|| anyhow::anyhow!("cannot determine config dir"))?
        .join("conns");

    if !conns_dir.exists() {
        return Ok(vec![]);
    }

    let mut conns = Vec::new();
    for entry in std::fs::read_dir(&conns_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("toml") {
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string();
            let content = std::fs::read_to_string(&path)?;
            let mut conn: ConnectionConfig = toml::from_str(&content)?;
            conn.name = name;
            conns.push(conn);
        }
    }
    Ok(conns)
}

/// Save session state to session.toml.
#[allow(clippy::too_many_arguments)]
pub fn save_session(
    name: &str,
    schema_cursor: usize,
    schema_cursor_path: Option<String>,
    schema_expanded_paths: Vec<String>,
    focus: Focus,
    schema_search: Option<String>,
    tab_cursors: Vec<TabCursor>,
    active_tab: usize,
    result_tabs: Vec<SavedResultRef>,
    active_result_tab: usize,
) -> anyhow::Result<()> {
    let dir = config_dir().ok_or_else(|| anyhow::anyhow!("cannot determine config dir"))?;
    std::fs::create_dir_all(&dir)?;
    let content = toml::to_string(&Session {
        connection: name.to_string(),
        schema_cursor,
        schema_cursor_path,
        schema_expanded_paths,
        focus,
        schema_search,
        tab_cursors,
        active_tab,
        result_tabs,
        active_result_tab,
    })?;
    std::fs::write(dir.join("session.toml"), content)?;
    Ok(())
}

fn load_session_inner() -> Option<Session> {
    let path = config_dir()?.join("session.toml");
    let content = std::fs::read_to_string(path).ok()?;
    toml::from_str(&content).ok()
}

/// Load full session data (connection name + schema cursor).
pub fn load_session_data() -> SessionData {
    let Some(s) = load_session_inner() else {
        return SessionData::default();
    };
    SessionData {
        connection: if s.connection.is_empty() {
            None
        } else {
            Some(s.connection)
        },
        schema_cursor: s.schema_cursor,
        schema_cursor_path: s.schema_cursor_path,
        schema_expanded_paths: s.schema_expanded_paths,
        focus: s.focus,
        schema_search: s.schema_search,
        tab_cursors: s.tab_cursors,
        active_tab: s.active_tab,
        result_tabs: s.result_tabs,
        active_result_tab: s.active_result_tab,
    }
}

/// Load only the last-used connection name from session.toml.
pub fn load_session() -> Option<String> {
    load_session_inner().and_then(|s| {
        if s.connection.is_empty() {
            None
        } else {
            Some(s.connection)
        }
    })
}

pub fn delete_connection(name: &str) -> anyhow::Result<()> {
    let path = config_dir()
        .ok_or_else(|| anyhow::anyhow!("cannot determine config dir"))?
        .join("conns")
        .join(format!("{name}.toml"));
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

pub fn save_connection(name: &str, url: &str) -> anyhow::Result<()> {
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        anyhow::bail!("Connection name may only contain letters, digits, - and _");
    }
    let conns_dir = config_dir()
        .ok_or_else(|| anyhow::anyhow!("cannot determine config dir"))?
        .join("conns");
    std::fs::create_dir_all(&conns_dir)?;
    let conn = ConnectionConfig {
        url: url.to_string(),
        name: String::new(),
    };
    let content = toml::to_string(&conn)?;
    std::fs::write(conns_dir.join(format!("{name}.toml")), content)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    /// Build-time check: the bundled defaults must parse into `MainConfig`.
    /// If this fails, `MainConfig::default()` would panic at runtime.
    #[test]
    fn defaults_parse() {
        let cfg: MainConfig =
            toml::from_str(DEFAULTS_TOML).expect("bundled config.toml must parse");
        assert_eq!(cfg.editor.keybindings, KeybindingMode::Vim);
        assert_eq!(cfg.editor.lsp_binary, "sqls");
        assert_eq!(cfg.editor.mouse_scroll_lines, 3);
        assert_eq!(cfg.editor.leader_key, ' ');
        assert!(cfg.editor.stop_on_error);
        assert_eq!(cfg.editor.schema_ttl_secs, 300);
    }

    #[test]
    fn defaults_match_default_impl() {
        let parsed: MainConfig = toml::from_str(DEFAULTS_TOML).unwrap();
        let dflt = MainConfig::default();
        assert_eq!(parsed.editor.keybindings, dflt.editor.keybindings);
        assert_eq!(parsed.editor.lsp_binary, dflt.editor.lsp_binary);
        assert_eq!(parsed.editor.leader_key, dflt.editor.leader_key);
    }

    #[test]
    fn defaults_pass_validation() {
        MainConfig::default()
            .validate()
            .expect("bundled defaults must validate");
    }

    #[test]
    fn default_config_has_vim_bindings() {
        let config = MainConfig::default();
        assert_eq!(config.editor.keybindings, KeybindingMode::Vim);
    }

    #[test]
    fn default_config_has_sqls_lsp() {
        let config = MainConfig::default();
        assert_eq!(config.editor.lsp_binary, "sqls");
    }

    /// Partial user TOML over bundled defaults: only overridden fields appear
    /// in the user file; unspecified fields keep their bundled value.
    #[test]
    fn user_partial_override_keeps_defaults() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            f,
            "[editor]\nlsp_binary = \"/opt/sqls\"\nmouse_scroll_lines = 5"
        )
        .unwrap();
        let cfg: MainConfig = hjkl_config::load_layered_from(DEFAULTS_TOML, f.path()).unwrap();
        // User overrides took effect:
        assert_eq!(cfg.editor.lsp_binary, "/opt/sqls");
        assert_eq!(cfg.editor.mouse_scroll_lines, 5);
        // Non-overridden fields retain bundled values:
        assert_eq!(cfg.editor.keybindings, KeybindingMode::Vim);
        assert_eq!(cfg.editor.leader_key, ' ');
        assert!(cfg.editor.stop_on_error);
        assert_eq!(cfg.editor.schema_ttl_secs, 300);
    }

    #[test]
    fn user_unknown_key_is_rejected() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "[editor]\nbogus = 1").unwrap();
        let err =
            hjkl_config::load_layered_from::<MainConfig>(DEFAULTS_TOML, f.path()).unwrap_err();
        assert!(matches!(err, hjkl_config::ConfigError::Invalid { .. }));
    }

    #[test]
    fn validate_rejects_zero_mouse_scroll_lines() {
        let mut cfg = MainConfig::default();
        cfg.editor.mouse_scroll_lines = 0;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.field, "editor.mouse_scroll_lines");
    }

    #[test]
    fn validate_rejects_empty_lsp_binary() {
        let mut cfg = MainConfig::default();
        cfg.editor.lsp_binary = String::new();
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.field, "editor.lsp_binary");
    }

    /// Multi-char leader strings must be rejected at parse time — serde's
    /// `char` deserializer fails on TOML strings of length != 1. This
    /// pins the contract: users who write `leader_key = "ab"` get a
    /// `ConfigError::Invalid` instead of a silently truncated leader.
    #[test]
    fn parse_rejects_multi_char_leader_key() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "[editor]\nleader_key = \"ab\"").unwrap();
        let err = hjkl_config::load_layered_from::<MainConfig>(DEFAULTS_TOML, f.path()).unwrap_err();
        assert!(matches!(err, hjkl_config::ConfigError::Invalid { .. }));
    }

    #[test]
    fn parse_rejects_empty_leader_key() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "[editor]\nleader_key = \"\"").unwrap();
        let err = hjkl_config::load_layered_from::<MainConfig>(DEFAULTS_TOML, f.path()).unwrap_err();
        assert!(matches!(err, hjkl_config::ConfigError::Invalid { .. }));
    }

    #[test]
    fn parse_accepts_unicode_single_char_leader_key() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "[editor]\nleader_key = \"α\"").unwrap();
        let cfg: MainConfig =
            hjkl_config::load_layered_from(DEFAULTS_TOML, f.path()).unwrap();
        assert_eq!(cfg.editor.leader_key, 'α');
    }

    #[test]
    fn connection_config_parse() {
        let conn: ConnectionConfig = toml::from_str(
            r#"
url = "mysql://user:pass@localhost/mydb"
name = "local"
"#,
        )
        .unwrap();
        assert_eq!(conn.url, "mysql://user:pass@localhost/mydb");
    }
}
