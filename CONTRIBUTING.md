# Contributing

Keep the bridge harness agnostic. Prefer an operator-configured command over
harness-specific branches; keep provider credentials and lifecycle management
outside the stdio transport. Discuss protocol or authority changes in an issue
before a large implementation.

Use Go 1.27.1 or newer. Before opening a pull request:

```sh
gofmt -w *.go
go test -race ./...
go vet ./...
go build -o bin/ax-agentconnect .
```

Add a focused regression test for behavior changes. Label simulated tests and
live deployment evidence separately. Document changes to the transport contract
or supported upstream versions. Never include credentials, personal endpoints,
private cluster manifests or generated binaries.

Contributions are made under the repository's Apache-2.0 license. Be respectful,
keep reviews technical, and credit upstream work. No CLA is required.
