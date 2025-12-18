# Repository Guidelines

## Project Structure & Module Organization

- Core library lives in the repo root (`package broker`) with unit tests alongside (`*_test.go`).
- This repo is multi-module: each backend has its own `go.mod`/`go.sum` and should be developed/tested from that directory:
  - `natsjs/` (JetStream, module path `github.com/velmie/broker/natsjs/v2`)
  - `sqs/`, `sns/` (AWS backends)
  - `azuresb/` (Azure Service Bus)
  - `otelbroker/` (OpenTelemetry middleware)
  - `idempotency/` (idempotent consumer middleware)
- Generated mocks live in `mock/` (and some modules have their own `mock/`).
- Runnable examples live in `_examples/` (e.g. `_examples/sqs-subscribe/`, `_examples/sns-publish/`).
- Library documentation lives in `docs/`.

## Build, Test, and Development Commands

- `go test ./...` - run tests for the current module (nested modules are not included from repo root).
- `cd natsjs && go test -v -cover ./...` - CI-equivalent for JetStream; requires a running Docker daemon (`ory/dockertest`).
- `golangci-lint run` - lint the current module using `.golangci.yml`.
- `go generate ./...` - regenerate mocks (run per module).
- Run all module tests:
  - `for d in . natsjs otelbroker sns sqs azuresb idempotency; do (cd "$d" && go test ./...); done`

## Coding Style & Naming Conventions

- Format with `gofmt` (tabs) and keep imports tidy (`goimports`/`gci`, local prefix `github.com/velmie/broker`).
- Keep lines <= 140 chars (see `.golangci.yml`).
- Follow Go naming: exported `PascalCase`, unexported `camelCase`, packages/directories lowercase.

## Testing Guidelines

- Use Go's `testing` package; assertions commonly use `stretchr/testify`.
- Prefer deterministic tests; integration tests should clean up external resources (notably Docker-based tests in `natsjs/`).

## Commit & Pull Request Guidelines

- Commit messages follow Conventional Commits seen in history: `feat: ...`, `fix: ...`, `refactor(scope): ...`, `chore: ...` (scopes like `natsjs`, `otelbroker`).
- PRs should include: clear description, linked issue (if any), tests for behavior changes, and updates to docs/examples when relevant.
- Before opening a PR, run `golangci-lint` and the relevant module's `go test`.

## Configuration & Security Tips

- Do not commit credentials. Examples under `_examples/` expect cloud credentials via standard SDK configuration (env/credentials files).
