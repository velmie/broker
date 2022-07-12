module github.com/velmie/broker/sqs

go 1.18

replace github.com/velmie/broker => ../

require (
	github.com/aws/aws-sdk-go v1.44.42
	github.com/golang/mock v1.6.0
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.5
	github.com/velmie/broker v0.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
