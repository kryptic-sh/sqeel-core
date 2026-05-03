# Changelog

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- **Config loading migrated to `hjkl-config` 0.1.** Defaults now live in
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
  `editor.lsp_binary` is empty, `editor.mouse_scroll_lines` is `0`, or
  `editor.leader_key` is not exactly one character.

### Added

- `hjkl-config = "0.1"` dependency.
- `MainConfig` impls `hjkl_config::AppConfig` (`APPLICATION = "sqeel"`) and
  `hjkl_config::Validate` (with `ValidationError` as the associated error). The
  `Validate` hook composes the shared `ensure_non_empty_str` / `ensure_non_zero`
  helpers so error messages carry field names.
- `pub const DEFAULTS_TOML: &str` — exposes the bundled defaults string for
  downstream tooling.
- 8 new tests covering bundled-defaults parse, partial user overrides preserving
  defaults, unknown-key rejection, and validation boundaries.

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

[Unreleased]: https://github.com/kryptic-sh/sqeel-core/compare/v0.2.3...HEAD
[0.2.3]: https://github.com/kryptic-sh/sqeel-core/releases/tag/v0.2.3
[0.2.2]: https://github.com/kryptic-sh/sqeel-core/releases/tag/v0.2.2
[0.2.1]: https://github.com/kryptic-sh/sqeel-core/releases/tag/v0.2.1
