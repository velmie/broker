# Traced messaging examples

These commands connect typed Broker processing and publication to OpenTelemetry
and native delivery observations. They export spans to stdout. Application
wiring owns the provider, SDK clients, consumers and cleanup order.

Prepare the [development workspace](../../docs/development.md#local-workspace),
then run commands from `otelbroker/examples`.

## NATS JetStream

The command requires a running NATS server with JetStream:

```sh
go run ./natsorders -url nats://127.0.0.1:4222
```

The command creates temporary topology and traces command publication, processing,
receipt publication and confirmed acknowledgment. It removes the topology after
joined completion. See [observer wiring](natsorders/observer.go).

## SQS and SNS

The command requires a running Moto service at a loopback HTTP endpoint:

```sh
go run ./awsorders -endpoint http://127.0.0.1:5000
```

One invocation creates and cleans up three routes: direct SQS, raw SNS-to-SQS and
SNS notification envelopes. Each route traces publication, processing and native
SQS deletion separately. The command uses synthetic credentials. See
[route composition](awsorders/route.go) and [observer wiring](awsorders/observer.go).

## Azure Service Bus

Use dedicated existing entities with no unrelated messages or competing consumers.
Supply a send/receive connection string through the environment, then select a
queue or a topic/subscription pair:

```sh
export AZURE_SERVICEBUS_CONNECTION_STRING='<connection string from your secret manager>'
go run ./azureorders -queue '<dedicated queue>' -id order-example-001
```

```sh
go run ./azureorders -topic '<dedicated topic>' -subscription '<dedicated subscription>' -id order-example-002
```

PeekLock is the default and produces a later completion span. With
`-receive-mode receive-and-delete`, the service removes the delivery on receipt,
so the command produces no later acknowledgment span. Topology is not created or
deleted. See [observer wiring](azureorders/observer.go) and the
[adapter's ownership contract](../../azuresb/readme.md#bind-native-resources).

## Read the output

Processing spans describe callback outcomes. Native spans describe service
responses and may report an unknown result. Their contexts and timing do not
turn separate effects into an atomic transaction.

The commands join consumer work and owned cleanup before flushing telemetry.
Cleanup and export failures remain visible. Recovery classifiers permit only
explicitly selected failures before application processing and inspect every
independent cause. They do not retry arbitrary business or settlement failures.

Run module tests as described in [development](../../docs/development.md#checks).
Azure runtime tests require explicit [emulator fixtures](../../docs/development.md#azure-emulator-tests).
