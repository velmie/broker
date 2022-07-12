module github.com/velmie/broker/sns

go 1.18

replace github.com/velmie/broker => ../

require (
	github.com/aws/aws-sdk-go v1.44.42
	github.com/pkg/errors v0.9.1
	github.com/velmie/broker v0.0.0-00010101000000-000000000000
)

require github.com/jmespath/go-jmespath v0.4.0 // indirect
