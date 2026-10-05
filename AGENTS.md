# Repository guidelines

## Layout

- The root `broker` package is the transport-independent core and uses only the
  standard library. Tests live beside source files.
- `natsjs`, `sqs`, `sns` and `azuresb` are separate adapter modules. Each owns its
  native SDK integration and fixtures.
- `otelbroker` is an optional integration module.
- Runnable commands live under module-owned `cmd` directories.
  Traced commands and cross-module tests live under `otelbroker/examples`.
- Core guides live in `docs`. Module READMEs own adapter-specific contracts.

## Build and verify

Use the [development guide](docs/development.md) for local module composition.
Run `go build ./...`, `go vet ./...`, `go test -race -count=1 ./...` and
`golangci-lint run` from each affected module. Root commands do not include
nested modules. Native transport tests require their documented fixtures.
Use `GOWORK=off` for independent dependency resolution when the declared versions
are available. Keep workspace files local.

Format Go with `gofmt`, keep imports organized and follow `.golangci.yml`,
including its line-length limit. Prefer deterministic tests and clean up owned
external resources. Use meaningful behavior checks for API changes. Verify
Markdown examples separately without adding repository tests just for the prose.

## Documentation

Write shareable documentation in English for readers with no task history.
Describe Broker as a messaging abstraction. Keep overviews focused on purpose,
minimal use and navigation. Put detailed setup and transport contracts in their
own guides, and compatibility changes in the migration guide.

Assume normal Go development knowledge. Include environment details only when
needed to run or understand the example. Leave incidental directory choices to
the reader. Keep versions in manifests and configurations unless an exact value
is necessary for a documented procedure. Add brief comments where an example
first introduces an unfamiliar API. Check code, relative links and expected
results before finalizing documentation.

## Contributions and configuration

Use Conventional Commit messages describing the change, with an optional
meaningful package scope. PR descriptions should explain the resulting behavior
and relevant validation. Include an issue link only when one exists.

Do not commit credentials, local absolute paths, transient diagnostics or private
planning context. Examples receive credentials through native SDK configuration
or environment variables. Preserve application ownership of topology and client
lifetime, and retain diagnostic causes with redaction at the logging boundary.
