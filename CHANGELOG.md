# Changelog

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.1] - 2026-05-03

### Changed

- Migrated `sqeel-core` from the `kryptic-sh/sqeel` monorepo into its own
  repository
  ([kryptic-sh/sqeel-core](https://github.com/kryptic-sh/sqeel-core)) with
  full git history preserved.
- Bumped hjkl deps from 0.2 to 0.3 (`hjkl-engine`).
- Replaced removed `hjkl-tree-sitter` with `hjkl-bonsai` 0.1 for syntax
  highlighting.
- Loosened dep pins from `=0.X.Y` exact to `"0.X"` caret-minor, matching the
  hjkl-* sibling pattern.

### Added

- Standalone `LICENSE`, `.gitignore`, `deny.toml`, `rust-toolchain.toml`, and
  CI workflows at the repo root.
