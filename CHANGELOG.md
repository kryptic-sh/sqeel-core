# Changelog

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **`sql_grammar_blocking() -> anyhow::Result<Arc<Grammar>>`** — new public
  function for sync callers (tests, CLI one-shots). Wraps the async loader with
  `recv_blocking`; caches the result in the shared `SQL_GRAMMAR` static.
- **`Highlighter::new_async() -> Highlighter`** — constructs a `Highlighter`
  immediately without blocking. When the grammar is not yet ready, all highlight
  calls return empty spans (plain-text fallback). The grammar attaches on the
  first successful `try_upgrade()` tick.
- **`Highlighter::try_upgrade(&mut self)`** — polls the non-blocking grammar
  cache each tick and attaches the inner `hjkl_bonsai::Highlighter` once the
  background load resolves. No-op once the highlighter is ready.
- **`Highlighter::is_ready() -> bool`** — returns whether the inner highlighter
  is attached.
- Added `tracing` dependency for warn-level diagnostics on async load failures.

### Changed

- **`sql_grammar()` signature changed from `anyhow::Result<Arc<Grammar>>` to
  `Option<Arc<Grammar>>`** (breaking for direct callers). Returns `None` on
  first call while the background grammar load is in-flight; returns `Some(arc)`
  once the grammar is cached. Callers that need a `Result` should use
  `sql_grammar_blocking()` instead.
- **`Highlighter::inner` is now `Option<hjkl_bonsai::Highlighter>`**. All
  highlight/parse methods short-circuit gracefully to empty vecs when `None`.
  `Highlighter::new()` continues to block (calls `sql_grammar_blocking`) and is
  still appropriate for tests. `Highlighter::default()` now uses `new_async()`
  instead of `new()`.
- Process-wide async grammar loader (`AsyncGrammarLoader`) stored in
  `OnceLock<AsyncGrammarLoader>`; in-flight handle in
  `Mutex<Option<LoadHandle>>`. Once the grammar resolves, the handle is cleared
  and subsequent calls fast-return from the `SQL_GRAMMAR` cache.
- `MainConfig`, `EditorConfig`, `load_main_config`, `config_dir`, and
  `set_config_dir_override` extracted into the new `sqeel-config` crate.
  Re-exported from `sqeel_core::config` so all existing call sites are
  unaffected. `sqeel-config` is now a direct dependency.
- `ConnectionConfig`, `load_connections`, `save_connection`, and
  `delete_connection` moved to `sqeel-config`. Re-exported from
  `sqeel_core::config` — all call sites in `sqeel-core::state`, `sqeel-tui`, and
  `apps/sqeel` are unaffected.

## [0.3.1] - 2026-05-05

### Changed

- **`hjkl-bonsai` 0.3 → 0.5.** Migrated the two 0.4 breaking call sites in
  `sql_grammar()`: `GrammarLoader::user_default` and `Grammar::load` now receive
  `registry.meta()` (`&ManifestMeta`). No `LangSpec` is constructed manually and
  sqeel ships no `bonsai.toml`, so those breaking changes had no further impact.
  The 0.5 `highlight_range_with_injections` API is available for future
  viewport-scoped adoption; `sqeel-core`'s `Highlighter` wrapper continues to
  use `highlight_range` internally, which is correct for its caller-driven parse
  model.

## [0.3.0] - 2026-05-03

### Changed

- **`hjkl-bonsai` 0.2 → 0.3.** Grammar storage subdir renamed `hjkl/` →
  `bonsai/`, and macOS/Windows now follow XDG-everywhere instead of
  `~/Library/Application Support` / `%APPDATA%`. Existing grammars under the old
  paths are not migrated — sqeel re-fetches and re-compiles them into the new
  `~/.local/share/bonsai/grammars/` location on first use.
- **Config loading migrated to `hjkl-config` 0.2.** Defaults now live in
  `crates/sqeel-core/src/config.toml`, bundled via `include_str!()` and parsed
  at runtime as the single source of truth. The user file at
  `<config_dir>/config.toml` is **deep-merged** on top via
  `hjkl_config::load_layered_from` — only the fields you want to override need
  to appear there. Unknown keys are an error.
- Dropped `DEFAULT_CONFIG` const, `default_*` helper fns, and per-field
  `#[serde(default = "...")]` attrs (no default values live in Rust code
  anymore). `MainConfig::default()` parses the bundled TOML.
- `MainConfig` and `EditorConfig` are now `#[serde(deny_unknown_fields)]`.
- `load_main_config` no longer auto-writes a default config file when one is
  missing — returns bundled defaults in memory instead. Use
  `hjkl_config::write_default` explicitly if a starter file is needed.
- `load_main_config` now validates the merged config and returns an error if
  `editor.lsp_binary` is empty or `editor.mouse_scroll_lines` is `0`. Multi-char
  or empty `leader_key` values are caught at parse time by serde's `char`
  deserializer (see breaking change below).
- **Breaking:** `EditorConfig::leader_key` is now `char` (was `String`). Made
  the invariant unrepresentable at the type level — TOML strings of length != 1
  fail to deserialize with a span-aware error from hjkl-config. Existing user
  files keep working: `leader_key = " "` still parses (a single space is a valid
  `char`). Multi-char strings like `"ab"` now fail at parse time rather than
  being silently truncated or surfaced via a runtime validation message.
- **XDG-everywhere path migration on macOS/Windows.** Both `config_dir()` and
  `data_dir()` (the latter in `persistence.rs`) now route through hjkl-config
  0.2's XDG-everywhere resolver. Linux paths are unchanged (`~/.config/sqeel/`,
  `~/.local/share/sqeel/`). macOS moves from
  `~/Library/Application Support/sqeel/` to `~/.config/sqeel/` (config) and
  `~/.local/share/sqeel/` (data). Windows moves from `%APPDATA%\sqeel\` to
  `~/.config/sqeel/` and `~/.local/share/sqeel/`. Existing macOS/Windows users
  will need to move their `config.toml`, `conns/`, `session.toml`, and
  `queries/`/`results/` data to the new locations. `$XDG_CONFIG_HOME` and
  `$XDG_DATA_HOME` are now honored on every platform — no more per-platform
  conditionals in dotfile setups. Sandbox override (`set_config_dir_override` /
  `set_data_dir_override`) for `--sandbox` is preserved.

### Added

- `hjkl-config = "0.2"` dependency.
- `MainConfig` impls `hjkl_config::AppConfig` (`APPLICATION = "sqeel"`) and
  `hjkl_config::Validate` (with `ValidationError` as the associated error). The
  `Validate` hook composes the shared `ensure_non_empty_str` / `ensure_non_zero`
  helpers so error messages carry field names.
- `pub const DEFAULTS_TOML: &str` — exposes the bundled defaults string for
  downstream tooling.
- 11 new tests covering bundled-defaults parse, partial user overrides
  preserving defaults, unknown-key rejection, validation boundaries (empty
  `lsp_binary`, zero `mouse_scroll_lines`), and parse-level rejection of
  multi-char / empty / unicode-single-char `leader_key`.

## [0.2.3] - 2026-05-03

### Changed

- Migrated to `hjkl-bonsai` 0.2 (was 0.1). 0.2 swaps the bundled-grammar
  registry for a runtime `dlopen`-based loader; first invocation for an unseen
  language clones `derekstride/tree-sitter-sql` and compiles `parser.so` into
  the user's XDG data dir. Subsequent runs hit the cache.
- Cache the resolved `Arc<Grammar>` in a process-wide `Mutex<Option<...>>` so
  concurrent `Highlighter::new()` callers (parallel tests, multi-buffer editor
  sessions) share one dlopen handle instead of racing.

## [0.2.2] - 2026-05-03

### Changed

- `deny.toml`: allow `CDLA-Permissive-2.0` (transitive via webpki-roots) and
  ignore RUSTSEC-2023-0071 (rsa Marvin attack — transitive via sqlx-mysql, no
  fix available, threat model is remote timing).
- CI: extracted shared lint/test jobs (`fmt`, `clippy`, `test`, `deny`) into a
  reusable `_tests.yml` workflow called by both `ci.yml` and `release.yml`.

## [0.2.1] - 2026-05-03

### Changed

- Migrated `sqeel-core` from the `kryptic-sh/sqeel` monorepo into its own
  repository ([kryptic-sh/sqeel-core](https://github.com/kryptic-sh/sqeel-core))
  with full git history preserved.
- Bumped hjkl deps from 0.2 to 0.3 (`hjkl-engine`).
- Replaced removed `hjkl-tree-sitter` with `hjkl-bonsai` 0.1 for syntax
  highlighting.
- Loosened dep pins from `=0.X.Y` exact to `"0.X"` caret-minor, matching the
  hjkl-\* sibling pattern.

### Added

- Standalone `LICENSE`, `.gitignore`, `deny.toml`, `rust-toolchain.toml`, and CI
  workflows at the repo root.

[Unreleased]: https://github.com/kryptic-sh/sqeel-core/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/kryptic-sh/sqeel-core/releases/tag/v0.3.1
[0.3.0]: https://github.com/kryptic-sh/sqeel-core/releases/tag/v0.3.0
[0.2.3]: https://github.com/kryptic-sh/sqeel-core/releases/tag/v0.2.3
[0.2.2]: https://github.com/kryptic-sh/sqeel-core/releases/tag/v0.2.2
[0.2.1]: https://github.com/kryptic-sh/sqeel-core/releases/tag/v0.2.1
