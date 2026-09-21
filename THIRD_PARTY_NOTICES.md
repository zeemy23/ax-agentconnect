# Third-party provenance

This bridge was initially developed as a standalone experiment alongside
Turnhaven and extracted without Turnhaven runtime code or deployment configuration.

It imports, rather than vendors, these direct dependencies:

- [google/ax](https://github.com/google/ax), Apache-2.0.
- [agent-substrate/env](https://github.com/agent-substrate/env), Apache-2.0.
- [grpc-go](https://github.com/grpc/grpc-go), Apache-2.0.

Exact direct and transitive versions are recorded in `go.mod` and `go.sum`.
Dependency licenses and notices remain in their upstream modules and apply when
redistributing their code or linked binaries. This file is not a complete binary
license bundle; prepare one before publishing binary/container releases.

AgentConnect, AX, Pi, Codex and Claude are names of their respective upstream
projects or owners. References identify compatibility and do not imply endorsement.
No harness executable or AgentConnect source is bundled here.
