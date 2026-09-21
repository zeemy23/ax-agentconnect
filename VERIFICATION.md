# Hardening verification

Evaluation date: 2026-09-21. These results describe a private evaluation,
not certification of another installation.

## Automated checks

- Go race tests against simulated AX and guest gRPC services: passed.
- TLS trust, certificate-name validation and mutual TLS: simulated services passed.
- Launch interlock, ambiguous-start retry rejection, task identity checks,
  malformed ACP frames, capability filtering, chunk ownership, exit validation,
  signal escalation and blocked-output cancellation: passed.
- Go vet, module verification, formatting and Linux amd64/arm64 builds: passed.
- ACP parser fuzzing: 127,281 executions in a bounded 10-second run; no failure.
- govulncheck v1.8.0 source scan: no known vulnerabilities found at evaluation time.

## Real AX / Substrate checks

A disposable debug task ran provider-free programs using the hardened adapter:

| Check | Observed result |
| --- | --- |
| Raw `/bin/cat` | Exact bytes returned; exit 0; receipt removed |
| Sleeping process with 1-second guest timeout | Exit 137 in about 1 second; receipt removed |
| Process ignoring SIGTERM, 1-second shutdown grace | SIGKILL escalation; exit 137 in about 1 second; receipt removed |
| Remote `terminal/create` request in mapped ACP mode | Frame withheld; adapter failed; receipt retained |
| Relaunch after rejected remote request | Blocked by retained receipt |

The disposable task and worker pool were deleted after reconciliation. These
checks did not call a model provider. Final buffer-ownership and blocked-output
watchdog refinements are covered by automated regression tests.

Earlier text-only browser chats with Pi, Codex and Claude ACP adapters are
recorded in the README. They predate this hardening pass and do not establish
acceptance of tool use, account connector isolation or recovery behavior.

## Still requiring deployment qualification

Actual ingress authentication/TLS and bypass prevention, durable storage across
node loss, single task ownership across launchers and probes, guest account
connector isolation, harness tool/permission workflows, resource exhaustion,
and upstream outage/replacement recovery remain installation-level gates.
See [PRODUCTION.md](PRODUCTION.md). The evaluation deployment remains a development
stack; it has not been promoted to production.
