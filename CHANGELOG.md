# Changelog

## Unreleased

- Add verified TLS/mTLS and strict endpoint parsing; disable application-level
  gRPC retry policies and resolver-supplied service configuration.
- Add persistent local launch receipts, task identity checks, finite guest
  lifetime, bounded shutdown and validated exit status.
- Require `--acp-cwd` for mapped ACP or explicit `--raw-stdio` for trusted bytes.
  Raw mode and cwd mapping cannot be combined.
- Validate bounded complete JSON-RPC frames in both directions, reject duplicate
  keys/invalid UTF-8/excessive nesting, disable client filesystem/terminal
  capabilities and reject corresponding remote requests.
- Add failure-path tests, TLS/mTLS tests, fuzzing and vulnerability checks.

Existing deployments must provision persistent mode-0700 `--state-dir` storage,
keep one owner per task, and reconcile any pre-existing remote processes before
switching binaries. Receipts cannot identify processes started by the old bridge.
See PRODUCTION.md for remaining upstream guarantees and qualification gates.
