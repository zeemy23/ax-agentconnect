# Deployment and recovery contract

The supported deployment target is a **single-owner Linux service** with one
configured AX task per active ACP process. This adapter is not a multi-tenant
scheduler. Meeting the requirements below is necessary before production use;
passing local tests alone does not establish an end-to-end production system.

## Required controls

- Only trusted operators may set endpoints, atespace, task, executable and argv.
  Do not interpolate client or model text into bridge flags.
- For an AgentConnect external runtime, merge `{"security":{"requireSandbox":false},
  "limits":{"agentStartAttempts":1}}`. `security.requireSandbox: true` is
  incompatible with `externalExecution`; the one-start setting avoids daemon
  retries colliding with an uncertain AX receipt.
- Keep AX and the debug guest router private. Prefer HTTPS with verified server
  certificates and mTLS at a trusted gRPC ingress. The pinned upstream services
  may require a TLS/auth proxy; the bridge does not configure that proxy for you.
  Loopback port-forwards are suitable for development. Plain cluster traffic
  requires an explicit `--insecure-in-cluster` acceptance of that trust boundary.
- Mount a persistent, private local directory at `--state-dir` (mode 0700).
  Every launcher for a given AX task must use the same directory and canonical
  AX authority. Separate directories, DNS aliases, hosts or replicas bypass the
  local interlock. Do not use an ephemeral container layer or `emptyDir` for
  production state. Network filesystems/distributed locking are not qualified.
- Give each unrelated principal/workspace a separate AX task. Do not replace
  that task or reassign its actor while a process may still be running.
  `--expected-task-id` is a preflight assertion, not an atomic generation fence.
- Give every concurrently active bridge its own task; keep launchers on the
  shared persistent state directory.
  AgentConnect may start custom-runtime capability probes during startup or
  refresh; a probe counts as active use of that fixed task.
- Set a finite `--process-timeout` (default 1h, maximum 24h) and
  `--shutdown-timeout` (default 10s, maximum 5m). Arrange task-level CPU, memory,
  disk/spool and egress limits externally. The bridge cannot cap the guest's
  output spool storage or enforce upstream provider budgets.
- Use `--acp-cwd` for the mapped ACP integration. It fixes the sandbox working
  directory and constrains protocol negotiation; it does not copy workspaces.
  Raw mode deliberately bypasses ACP validation and is only for trusted peers
  whose capabilities and path semantics already match.
- Grant provider access through execution-scoped credentials. No credentials
  should be passed as command arguments. Receipt files store an argv digest,
  not argv or provider secrets. Harness stderr may contain sensitive output;
  apply retention/access controls to deployment logs.
- AgentConnect host environment, runtime overrides and account-app connector
  settings are not forwarded through AX. Configure and verify provider
  credentials and account-connector policy in the task image or harness; this
  bridge does not provide account-connector isolation.

## TLS example

Each endpoint has independent trust and client credentials:

```sh
ax-agentconnect \
  --server https://ax.internal.example:443 \
  --server-ca /run/secrets/ca.pem \
  --server-cert /run/secrets/client.pem \
  --server-key /run/secrets/client-key.pem \
  --router https://guest.internal.example:443 \
  --router-ca /run/secrets/ca.pem \
  --router-cert /run/secrets/client.pem \
  --router-key /run/secrets/client-key.pem \
  --state-dir /var/lib/ax-agentconnect \
  --atespace agents --task worker-a --expected-task-id TASK_ID \
  --acp-cwd /workspace/work \
  --process-timeout 1h --shutdown-timeout 10s \
  -- /usr/local/bin/pi-acp
```

System certificate roots are used when a CA file is omitted. The optional
`--server-tls-name` / `--router-tls-name` selects the expected certificate name;
it never disables certificate verification. TLS configuration on a plaintext
endpoint must fail rather than silently ignoring the credentials.

## Launch receipts and uncertain results

Before `StartProcess`, the CLI exclusively creates and fsyncs a launch receipt.
Concurrent launchers sharing that directory cannot both start the task. After a
successful start response, the process ID is saved and fsynced before stdio is
forwarded. A successful, validated final process exit removes the receipt,
including a known non-zero remote exit code. An input failure, bridge crash,
transport loss, missing or invalid exit message, or uncertain start keeps it,
even if best-effort cleanup was attempted. This intentionally fails closed.

If a launch is blocked:

1. Stop supervisors from retrying. Preserve the receipt and bridge diagnostics.
2. If the receipt is unreadable or invalid JSON, treat the outcome as unknown.
   Preserve its exact bytes; do not truncate, delete or retry. Inspect the
   dedicated task with upstream operator APIs and prove no prior process can
   run before moving the receipt aside.
3. Read the receipt's AX endpoint, atespace, task, actor, task ID and process ID.
   Verify the task still refers to the same actor before interacting with it.
4. If a process ID exists, inspect it through the guest process API, terminate
   it if appropriate, and verify it exited. A delivered signal alone is not
   proof of exit. If the process ID is absent, do not guess or rerun: the start
   RPC may have committed remotely. Reconcile using the upstream operator tools
   or stop the dedicated task and prove no previous process can run.
5. Only after reconciliation, remove that single receipt and restart. There is
   no automatic stale-receipt deletion, lease expiry, or force-retry flag.

The guest API has no start idempotency key or atomic actor-generation precondition.
A local receipt cannot recover an unknown process ID, fence actor replacement,
or provide exactly-once execution across independent hosts. Process timeout is
a resource bound, not proof that an old process cannot resume after suspension.
These guarantees require upstream support or a durable admission/controller layer.

## Qualification gates

Repository CI checks race-enabled transport tests, malformed input, TLS trust,
launch guard behavior, formatting, vet, vulnerability scanning and Linux builds.
The fake gRPC services are simulated evidence. Test your deployment separately:

- Trusted and rejected mTLS identities at the actual ingress; no plaintext bypass.
- SIGTERM, EOF, stalled input/output, abrupt daemon loss, and a lost start reply.
- Persistent receipts after pod/node restart; one task owner across all replicas.
- Harness tools and permission decisions inside the sandbox, without daemon-host
  filesystem/terminal execution; provider credential and network isolation.
- Storage exhaustion, upstream outage, task suspension/replacement and rollback.

The original private development stack used plaintext internal endpoints,
devAuth and ephemeral daemon state. It is not a production deployment and has
not been promoted by this hardening work. Do not infer tool/recovery acceptance
from its earlier successful text-only browser chats.
