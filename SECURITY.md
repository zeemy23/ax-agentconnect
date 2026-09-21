# Security

This experimental bridge has no production support or security guarantees.
Treat its configuration, AX API and Substrate debug guest endpoints as privileged
operator interfaces. Debug process access can execute commands inside the selected
actor. The bridge supports verified TLS/mTLS. Authentication enforcement at the AX and
guest ingress, network isolation and authorization remain deployment duties.
The plaintext non-loopback opt-in is not authentication.

Do not use caller-supplied text to select task names, endpoints or commands.
Do not pass upstream provider secrets in command arguments. Keep credentials
scoped to the selected execution and out of logs and repository files. AgentConnect
host environment, runtime overrides and account-app connector settings do not
cross AX's process boundary. Configure and verify provider credentials and
account-connector policy in the task image or harness; this bridge does not
provide account-connector isolation. `sessionMcpServers: "unsupported"` only
suppresses daemon-local MCP injection.

One task lookup does not fence actor identity against concurrent replacement.
An AX task admits one active bridge, including AgentConnect runtime capability
probes; use separate tasks for concurrent bridges and one shared persistent
state directory across launchers.
An uncertain process start is not retried. A persistent local launch receipt
blocks relaunch until the operator reconciles it. The interlock is not distributed
and can be bypassed by separate state directories or endpoint aliases. Failed
connections may leave a process running; a finite guest lifetime bounds it, but
operators must verify exit before relaunching. Cwd mapping does
not provide filesystem isolation, workspace transfer or remote MCP access.

Report suspected vulnerabilities through GitHub's private vulnerability reporting
feature on this repository when available. While private, contact the repository
owner through an existing private channel. Do not post exploit details or
credentials in public issues. There is no guaranteed response SLA yet.

Mapped ACP mode strips client filesystem/terminal capabilities and rejects
corresponding remote requests. Raw mode is an explicit trust opt-in and performs
no protocol filtering. All other extensions remain subject to the ACP client
implementation's authorization. See [deployment requirements](PRODUCTION.md).
