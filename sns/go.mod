module github.com/velmie/broker/sns

go 1.24

require (
	github.com/aws/aws-sdk-go-v2 v1.47.0
	github.com/aws/aws-sdk-go-v2/credentials v1.20.5
	github.com/aws/aws-sdk-go-v2/service/sns v1.47.1
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.0
	github.com/aws/smithy-go v1.28.1
	github.com/velmie/broker v1.0.0
	github.com/velmie/broker/sqs v1.0.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.3 // indirect
)
