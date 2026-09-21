# Security

This experimental bridge has no production support or security guarantees.
Treat its configuration, AX API and Substrate debug guest endpoints as privileged
operator interfaces. Debug process access can execute commands inside the selected
actor. The bridge currently uses plaintext gRPC; network isolation and access
control are the deployer's responsibility. The non-loopback opt-in is not auth.

Do not use caller-supplied text to select task names, endpoints or commands.
Do not pass upstream provider secrets in command arguments. Keep credentials
scoped to the selected execution and out of logs and repository files.

One task lookup does not fence actor identity against concurrent replacement.
An uncertain process start is not retried. Failed connections may leave a process
running; operators must reconcile it before launching again. Cwd mapping does
not provide filesystem isolation, workspace transfer or remote MCP access.

Report suspected vulnerabilities through GitHub's private vulnerability reporting
feature on this repository when available. While private, contact the repository
owner through an existing private channel. Do not post exploit details or
credentials in public issues. There is no guaranteed response SLA yet.
