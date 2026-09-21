# ax-agentconnect

Run an ACP harness inside an existing [AX](https://github.com/google/ax) task
from [AgentConnect](https://github.com/agentconnect-md/agentconnect).

A small Go executable forwards stdio between an ACP client and a process in an
[Agent Substrate](https://github.com/agent-substrate/substrate) debug guest.
Harness choice is an operator-configured command: Pi, Codex, Claude Code, or
another ACP executable. Adding a harness does not require a bridge code change.

**Pre-production.** The repository is private during initial development, with
Apache-2.0 licensing and portable source prepared for public release. This is an
independent integration, not an official Google or AgentConnect project.

```text
AgentConnect daemon → ax-agentconnect → AX task / Substrate guest → ACP harness
```

## Build

Install Go 1.27.1 or newer, then:

```sh
git clone https://github.com/zeemy23/ax-agentconnect.git
cd ax-agentconnect
go build -trimpath -o bin/ax-agentconnect .
go test -race ./...
go vet ./...
```

While the repository is private, cloning requires authorized GitHub access.
Dependencies are pinned in `go.mod` and `go.sum`. AX is pinned to commit
`d8ed0fe38bceb7842d3c47817d53d16ccdfcb601`; its matching Substrate environment
client provides the required stdin and process-streaming APIs.

## Run

First provision an AX task with `spec.debug: true`, wait for `Running`, and
install your chosen ACP executable in its image. Create its sandbox workspace
and configure its provider credentials separately. The bridge does not install
harnesses, provision credentials or create tasks.

Forward the AX API and atenet router to local ports, then run:

```sh
./bin/ax-agentconnect \
  --server 127.0.0.1:18080 \
  --router 127.0.0.1:18082 \
  --atespace default \
  --task my-agent \
  --state-dir "$HOME/.local/state/ax-agentconnect" \
  --acp-cwd /workspace/work \
  -- /usr/local/bin/pi-acp
```

Replace the final command with `/usr/local/bin/codex-acp`,
`/usr/local/bin/claude-agent-acp`, or another installed ACP command.
Arguments after `--` are exact argv tokens. Alternatively, repeat `--command`
and `--arg`; no shell is inserted. Provider settings belong to the harness or
its task, not this transport.

## AgentConnect configuration

Merge [examples/runtimes.json](examples/runtimes.json) into the daemon's config.
It defines three custom runtimes with separate fixed tasks and commands.
Install the bridge on the daemon host; install the ACP harnesses inside their
respective AX images. The example uses local port-forwards; adapt those endpoints
to where the daemon actually runs.

Set `externalExecution: true` so AgentConnect launches this transport on the
daemon host. Set `sessionMcpServers: "unsupported"` because this integration
does not route daemon-local MCP servers into the remote guest. Choose the
matching custom runtime when creating an AgentConnect agent.

Merge these AgentConnect settings for an external runtime:

```json
{
  "security": { "requireSandbox": false },
  "limits": { "agentStartAttempts": 1 }
}
```

`security.requireSandbox: true` is incompatible with `externalExecution`.
AgentConnect otherwise retries a start three times by default, while an AX
receipt deliberately blocks an uncertain relaunch. An AX task admits one active
bridge, including AgentConnect runtime capability probes. Use a separate task
for each concurrently active bridge and one shared persistent state directory
across launchers. Tasks must exist before daemon probes run.

AgentConnect host environment variables, runtime overrides and account-app
connector settings do not cross AX's process boundary. Configure provider
credentials and account-connector policy in the task image or harness and
verify isolation there. `sessionMcpServers: "unsupported"` only suppresses
daemon-local MCP injection; this bridge does not provide account-connector
isolation.

Use HTTPS endpoints for verified TLS; optional endpoint-specific CA and client
certificate/key flags support mTLS through a trusted ingress. Plaintext is
limited to loopback unless `--insecure-in-cluster` is explicitly set. This
opt-in does not add authentication. See [PRODUCTION.md](PRODUCTION.md) for the
single-owner deployment contract, TLS configuration and recovery procedure.

## Transport contract

- Validates endpoints and task identity before launch. ACP input cannot change
  the configured task or argv. Optional `--expected-task-id` checks AX status.id.
- Exclusively creates and fsyncs a private local launch receipt before starting.
  A confirmed process ID is recorded before forwarding IO. Concurrent/restarted
  launchers using the same persistent state cannot silently duplicate a start.
- Calls `StartProcess` once with a 10-second RPC deadline and a guest-enforced
  process lifetime (default 1h). Application-level gRPC retry/service-config
  policies are disabled. Uncertain outcomes retain the receipt for reconciliation.
- Separates stdout/stderr, forwards stdin EOF and shutdown signals, and bounds
  shutdown with `--shutdown-timeout` (default 10s). A second shutdown signal or
  expired grace period escalates to SIGKILL. Cleanup remains best effort during
  upstream outages; the native process lifetime supplies an independent bound.
- Mapped ACP mode requires `--acp-cwd`. It validates complete UTF-8 JSON-RPC
  NDJSON up to 4 MiB per frame, rejects duplicate keys and excessive nesting,
  maps session cwd, and rejects nonempty session MCP server lists. It disables
  daemon-host filesystem/terminal capabilities and rejects remote requests for
  them. Other ACP capabilities, permission requests, notifications and responses
  remain harness-owned.
- `--raw-stdio` explicitly enables trusted byte transport without ACP validation,
  cwd mapping or capability filtering. It cannot be combined with `--acp-cwd`.

## Verification

See [VERIFICATION.md](VERIFICATION.md) for the evidence and remaining
deployment gates.

The automated tests use real pinned gRPC clients with **simulated** AX/guest
services. They cover transport, process exit, signals, uncertain starts, input
mapping and JSON-RPC frame preservation. CI runs race tests, vet and build.

A separate private Kubernetes evaluation on 2026-09-21 completed live browser
chat through AgentConnect 1.60.0, this bridge, AX and each of:

| Harness | ACP package used |
| --- | --- |
| Pi | `pi-acp@0.0.33` with `@earendil-works/pi-coding-agent@0.85.1` |
| Codex | `@agentconnect.md/codex-acp@1.12.0-agentconnect.2` |
| Claude Code / Agent SDK | `@agentclientprotocol/claude-agent-acp@0.79.0` |

All used one Sonnet 4.6 provider binding via a separate scoped evaluation broker.
This was bounded text-prompt acceptance, not qualification of tool execution,
all models, permission workflows or production recovery. The cluster manifests,
credentials and broker are not part of this repository.

## Known limits

The local launch guard is not distributed admission. All launchers must share
one persistent directory and canonical AX endpoint, and one owner must retain
exclusive control of each task. AX's pinned API cannot atomically fence actor
replacement or deduplicate an unknown remote start. The bridge cannot provide
exactly-once execution, automatic reconnect/attach, workspace synchronization,
or remote filesystem/terminal client services. A received signal is not proof
that a remote process exited.

Do not blindly remove launch receipts after an error. Follow the explicit
[reconciliation procedure](PRODUCTION.md#launch-receipts-and-uncertain-results).
Production qualification of the complete deployment remains separate from this
repository's tests; the original evaluation stack is still a development setup.

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md). Licensed under [Apache-2.0](LICENSE).
Dependencies retain their respective licenses; see
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
