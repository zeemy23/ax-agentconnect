# ax-agentconnect

Run an ACP harness inside an existing [AX](https://github.com/google/ax) task
from [AgentConnect](https://github.com/agentconnect-md/agentconnect).

A small Go executable forwards stdio between an ACP client and a process in an
[Agent Substrate](https://github.com/agent-substrate/substrate) debug guest.
Harness choice is an operator-configured command: Pi, Codex, Claude Code, or
another ACP executable. Adding a harness does not require a bridge code change.

**Experimental.** The repository is private during initial development, with
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

For trusted internal cluster endpoints, explicitly add `--insecure-in-cluster`.
The default rejects non-loopback endpoints. This switch permits plaintext gRPC;
it does not provide TLS or authentication. Keep AX and the guest router private.

## Transport contract

- Looks up the selected task once and requires `Running`, a nonempty actor and
  debug access. Client protocol input cannot change the configured task or argv.
- Calls `StartProcess` once with stdin enabled and a 10-second deadline. An
  ambiguous start fails without retrying; the process may still exist remotely.
- Forwards stdout to stdout, stderr to stderr, EOF to guest stdin, and
  SIGINT/SIGTERM to the guest process. Missing final process status is an error.
- By default, copies protocol bytes transparently. With `--acp-cwd`, validates
  input JSON-RPC NDJSON up to 4 MiB per frame, rewrites only `params.cwd` for
  `session/new` and `session/load`, and rejects nonempty session `mcpServers`.
  Other frames, including client replies and capability negotiation, are preserved.

## Verification

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

This is a process transport, not a durable execution controller. It does not
implement start deduplication, task generation fencing, reconnect/attach,
workspace synchronization or remote filesystem/terminal client capabilities.
The client and harness must support any negotiated capabilities themselves.
Do not share the fixed workspace among unrelated tenants or concurrent sessions.
Do not blindly relaunch after a connection failure: reconcile the existing
remote process first. See [SECURITY.md](SECURITY.md) for the trust boundary.

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md). Licensed under [Apache-2.0](LICENSE).
Dependencies retain their respective licenses; see
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
