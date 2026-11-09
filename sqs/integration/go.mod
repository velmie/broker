module github.com/velmie/broker/sqs/integration

go 1.26.0

require (
	github.com/aws/aws-sdk-go-v2 v1.47.0
	github.com/aws/aws-sdk-go-v2/credentials v1.20.5
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.0
	github.com/nats-io/nats.go v1.54.0
	github.com/velmie/broker v1.2.0
	github.com/velmie/broker/natsjs/v3 v3.3.0
	github.com/velmie/broker/sqs v1.1.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.3 // indirect
	github.com/aws/smithy-go v1.28.1 // indirect
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)
