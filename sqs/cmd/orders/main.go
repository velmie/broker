// Command orders provisions a disposable local SQS queue and runs one typed order.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/aws/smithy-go"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sqs"
)

const (
	operationBudget        = 30 * time.Second
	sourceName             = "orders"
	exampleAmount          = 42
	maxDiagnosticLabel     = 128
	maxDiagnosticDepth     = 8
	maxDiagnosticNodes     = 32
	exampleVisibility      = 2 * time.Second
	exampleRenewalInterval = 200 * time.Millisecond
	exampleProcessingTime  = 2500 * time.Millisecond
)

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:9324", "loopback ElasticMQ endpoint")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt)

	if err := run(ctx, *endpoint, logger); err != nil {
		logger.Error("orders example failed", slog.Any("error", err))
		stopSignals()
		os.Exit(1)
	}
	stopSignals()
}

type order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func run(ctx context.Context, endpoint string, logger *slog.Logger) (result error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "http" || !net.ParseIP(parsed.Hostname()).IsLoopback() || parsed.User != nil || parsed.RawQuery != "" {
		return errors.New("endpoint must be a loopback HTTP URL")
	}
	client := awssqs.NewFromConfig(aws.Config{Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("synthetic",
			"synthetic",
			"")},
		func(options *awssqs.Options) { options.BaseEndpoint = aws.String(endpoint) })
	setupCtx, cancelSetup := context.WithTimeout(ctx, operationBudget)
	defer cancelSetup()
	name := fmt.Sprintf("broker-orders-%d", time.Now().UnixNano())
	created, err := client.CreateQueue(setupCtx, &awssqs.CreateQueueInput{QueueName: aws.String(name)})
	if err != nil {
		return &adapter.OperationError{Source: sourceName, Operation: "CreateQueue", Cause: err}
	}
	defer func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), operationBudget)
		defer cancelCleanup()
		_, cleanupErr := client.DeleteQueue(cleanupCtx, &awssqs.DeleteQueueInput{QueueUrl: created.QueueUrl})
		if cleanupErr != nil {
			result = errors.Join(result, &adapter.OperationError{Source: sourceName, Operation: "DeleteQueue", Cause: cleanupErr})
		}
	}()
	// Name lookup is an explicit SDK operation at wiring, with no adapter cache.
	resolved, err := client.GetQueueUrl(setupCtx, &awssqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return &adapter.OperationError{Source: sourceName, Operation: "GetQueueURL", Cause: err}
	}
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{QueueURL: aws.ToString(resolved.QueueUrl), Source: sourceName})
	if err != nil {
		return err
	}
	publish := broker.NewTypedPublisher[order](broker.EncoderFunc(json.Marshal),
		publisher,
		func(_ context.Context,
			value order) (broker.MessageMetadata,
			error) {
			return broker.MessageMetadata{ID: value.ID}, nil
		})
	if publishErr := publish(ctx, order{ID: "order-example-001", Amount: exampleAmount}); publishErr != nil {
		return publishErr
	}
	runCtx, stopRun := context.WithTimeout(ctx, operationBudget)
	defer stopRun()
	accepted := false
	renewed := false
	consumer,
		err := adapter.NewConsumer(client,
		adapter.ConsumerConfig{QueueURL: aws.ToString(resolved.QueueUrl),
			Source:            sourceName,
			WaitTimeSeconds:   1,
			VisibilityTimeout: exampleVisibility,
			ResilientReceive:  true,
			Observer: &adapter.Observer{Record: func(_ context.Context,
				o adapter.Observation) {
				if o.Outcome != adapter.OutcomeAccepted {
					return
				}
				switch o.Operation {
				case adapter.OperationRetry:
					logger.Info("SQS redelivery requested")
				case adapter.OperationRenew:
					if !renewed {
						renewed = true
						logger.Info("SQS visibility renewed")
					}
				case adapter.OperationDelete:
					accepted = true
					logger.Info("SQS deletion accepted", "source", o.Source)
					stopRun()
				}
			}}})
	if err != nil {
		return err
	}
	err = consumer.Run(runCtx, orderHandler(logger))
	if accepted && onlyCancellation(err) {
		return nil
	}
	return err
}

// ReplaceAttr receives original error objects and redacts both bound and call-site
// attributes before JSON serialization. SDK codes preserve native failure detail
// without exposing messages that can contain URLs, credentials or request data.
func redactAttribute(_ []string, attribute slog.Attr) slog.Attr {
	err, ok := attribute.Value.Any().(error)
	if !ok {
		return attribute
	}
	remaining := maxDiagnosticNodes
	return slog.Any(attribute.Key, describeError(err, 0, &remaining))
}

// Each node describes only its own error. Unwrap edges preserve association of
// primary and cleanup failures without pairing metadata from different branches.
type diagnosticError struct {
	Type      string             `json:"type"`
	Operation string             `json:"operation,omitempty"`
	Source    string             `json:"source,omitempty"`
	Field     string             `json:"field,omitempty"`
	Code      string             `json:"code,omitempty"`
	Function  string             `json:"function,omitempty"`
	Kind      string             `json:"kind,omitempty"`
	Reason    string             `json:"reason,omitempty"`
	CauseType string             `json:"cause_type,omitempty"`
	Cause     *diagnosticError   `json:"cause,omitempty"`
	Causes    []*diagnosticError `json:"causes,omitempty"`
	Truncated bool               `json:"truncated,omitempty"`
}

func describeError(err error, depth int, remaining *int) *diagnosticError {
	node := &diagnosticError{Type: fmt.Sprintf("%T", err)}
	if depth >= maxDiagnosticDepth || *remaining <= 0 {
		node.Truncated = true
		return node
	}
	*remaining--
	switch value := err.(type) {
	case *adapter.OperationError:
		node.Operation, node.Source = safeLabel(value.Operation), safeLabel(value.Source)
	case *adapter.FieldError:
		node.Field = safeLabel(value.Field)
		node.Reason = safeValidationReason(value.Cause)
	case *strconv.NumError:
		node.Function = safeLabel(value.Func)
		switch value.Err {
		case strconv.ErrSyntax:
			node.Kind = "syntax"
		case strconv.ErrRange:
			node.Kind = "range"
		}
	case smithy.APIError:
		node.Code = safeLabel(value.ErrorCode())
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		for _, cause := range wrapped.Unwrap() {
			if cause == nil {
				continue
			}
			if *remaining <= 0 {
				node.Truncated = true
				break
			}
			node.Causes = append(node.Causes, describeError(cause, depth+1, remaining))
		}
	case interface{ Unwrap() error }:
		if cause := wrapped.Unwrap(); cause != nil {
			node.CauseType = fmt.Sprintf("%T", cause)
			node.Cause = describeError(cause, depth+1, remaining)
		}
	}
	return node
}

func safeLabel(value string) string {
	if len(value) > maxDiagnosticLabel {
		value = value[:maxDiagnosticLabel]
	}
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-[]/", r) {
			return r
		}
		return '_'
	}, value)
}

func onlyCancellation(err error) bool {
	if err == nil || err == context.Canceled {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if !onlyCancellation(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		cause := wrapped.Unwrap()
		return cause != nil && onlyCancellation(cause)
	}
	return false
}

// This example recognizes fixed adapter validation messages. Arbitrary application
// errors are not interpolated. Expand the selection for application-specific types.
func safeValidationReason(err error) string {
	switch err.Error() {
	case "invalid name", "duplicate name", "reserved identity conflict or case alias", "empty value",
		"invalid SQS text", "missing receipt", "reserved case alias", "logical ID requires String",
		"expected positive uint64", "list values are unsupported", "invalid data type",
		"expected nonempty valid text value", "expected nonempty binary value", "unsupported data type",
		"maximum 10 attributes including logical id", "body and attributes exceed 1 MiB",
		"expected 1..1048576 bytes of valid SQS text", "FIFO requires 1..128 printable ASCII characters without spaces":
		return err.Error()
	default:
		return ""
	}
}

func orderHandler(logger *slog.Logger) broker.Handler {
	return broker.NewTypedDeliveryHandler(broker.DecodeJSON[order],
		func(callbackCtx context.Context, delivery broker.Delivery, value order) (broker.Disposition, error) {
			if value.Amount != exampleAmount {
				return nil, errors.New("order amount mismatch")
			}
			attempt := delivery.(broker.AttemptReader).Attempt()
			if !attempt.Known {
				return nil, errors.New("example requires known native delivery attempt")
			}
			if attempt.Number == 1 {
				logger.Info("requesting demonstration retry before order processing")
				return broker.RetryAfter{Delay: time.Second, Cause: errors.New("demonstration retry before business work")}, nil
			}
			timer := time.NewTimer(exampleProcessingTime)
			defer timer.Stop()
			select {
			case <-callbackCtx.Done():
				return nil, context.Cause(callbackCtx)
			case <-timer.C:
			}
			logger.Info("order processed", "amount", value.Amount)
			return broker.Handled{}, nil
		}, broker.WithRequirements(broker.AttemptsRequirement{}, broker.RedeliveryRequirement{},
			broker.KeepAliveRequirement{Interval: exampleRenewalInterval}))
}
