# Changelog

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
