# Develop and run examples

The repository contains independent Go modules. The root module uses only the
standard library. Adapter and integration modules declare their own SDK and
library dependencies. Toolchain requirements belong to those manifests; CI tools
and service images are selected in [the workflow](../.github/workflows/github-ci.yml).

## Local workspace

Use a Go workspace to compose checked-out modules without changing their
manifests. From the repository root, with no existing `go.work`:

```sh
go work init . ./natsjs ./sqs ./sns ./azuresb ./idempotency ./otelbroker \
  ./idempotency/examples ./otelbroker/examples
go work edit \
  -replace=github.com/velmie/broker@v1.0.0=. \
  -replace=github.com/velmie/broker/natsjs/v3@v3.0.0=./natsjs \
  -replace=github.com/velmie/broker/sqs@v1.0.0=./sqs \
  -replace=github.com/velmie/broker/sns@v1.0.0=./sns \
  -replace=github.com/velmie/broker/azuresb@v1.0.0=./azuresb \
  -replace=github.com/velmie/broker/idempotency@v1.0.0=./idempotency \
  -replace=github.com/velmie/broker/otelbroker@v1.0.0=./otelbroker
```

The version-qualified replacements match the module requirements and keep source
resolution local even when those tags are unavailable. If a workspace already
exists, add the modules with `go work use` and apply the same replacements.
Keep `go.work` and `go.work.sum` local to the checkout. An explicitly set `GOWORK`
can override automatic discovery; check `go env GOWORK` if local changes are not
being used.

## Checks

Run checks from each affected module directory. A root `./...` does not include
nested modules:

```sh
go build ./...
go vet ./...
go test -race -count=1 ./...
golangci-lint run
```

Use `GOWORK=off` when verifying a module against its declared dependencies rather
than local replacements. Referenced versions must then be available through the
configured module proxy. Workspace checks and independent module resolution test
different contracts.

Native NATS, SQS and SNS tests require Docker and own their temporary fixtures.
Image overrides are `NATS_TEST_IMAGE`, `SQS_TEST_IMAGE`, `SNS_TEST_IMAGE` and
`SNS_BINARY_TEST_IMAGE`. Keep the binary-forwarding fixture selection from CI;
it covers a separate emulator compatibility path. Azure tests require the
explicit fixtures below. No new tests are needed merely to check a Markdown
example: build or run its code separately.

## Executable examples

Start the appropriate service, then use the command in its guide. Examples use
disposable resources or explicitly dedicated entities:

| Example | Service and guide |
| --- | --- |
| Typed command and reply | [NATS with JetStream](../natsjs/README.md#run-the-example) |
| Native JetStream features | [Caller-owned sessions](../natsjs/cmd/native/README.md) |
| Visibility and redelivery | [SQS with ElasticMQ](../sqs/README.md#run-the-example) |
| Raw and notification forwarding | [SNS/SQS with Moto](../sns/README.md#run-the-example) |
| Completion-marker replay | [In-memory idempotency](../idempotency/README.md#run-the-example) |
| Replay after native redelivery | [Idempotency with NATS](../idempotency/examples/README.md) |
| Queue or subscription processing | [Azure Service Bus](../azuresb/readme.md#run-the-example) |
| Tracing and native observations | [OpenTelemetry examples](../otelbroker/examples/README.md) |

The test fixtures and CI configuration are the authority for compatible service
images. The command's address flags select an already running service. Local AWS
examples accept loopback HTTP endpoints and use synthetic credentials. Keep real
credentials in native SDK configuration or the required environment variables.

## Azure emulator tests

Supply a dedicated emulator queue, topic and subscription with no unrelated
messages or competing consumers. Configure a sixty-second PeekLock duration for
the renewal and expiry scenarios. Entities must already exist; the tests do not
provision them.

```sh
export AZURESB_TEST_CONNECTION_STRING='<emulator connection string with UseDevelopmentEmulator=true>'
export AZURESB_TEST_QUEUE='<dedicated queue>'
export AZURESB_TEST_TOPIC='<dedicated topic>'
export AZURESB_TEST_SUBSCRIPTION='<dedicated subscription>'
```

Run from `azuresb`:

```sh
go test -race -count=1 -timeout 10m ./...
```

Run traced Azure scenarios from `otelbroker/examples`:

```sh
go test -race -count=1 -timeout 10m ./azureorders
```

Without the connection variable, runtime emulator tests skip. Unit tests still
run. Emulator checks cover supported publication, receipt, renewal, settlement
and dead-letter paths. The transfer-subqueue test checks address selection and
an empty receive, not a real failed auto-forwarding flow. Emulator results do not
establish cloud IAM, durability or deployment readiness.
