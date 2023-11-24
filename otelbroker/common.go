package otelbroker

import (
	"github.com/velmie/broker"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the instrumentation scope name.
const (
	ScopeName = "github.com/velmie/broker/otelbroker"
	Version   = "0.1.0"
)

func newTracer(tp trace.TracerProvider) trace.Tracer {
	return tp.Tracer(ScopeName, trace.WithInstrumentationVersion(Version))
}

func commonAttributes(topic string, msg *broker.Message) []attribute.KeyValue {
	attr := []attribute.KeyValue{
		semconv.MessagingDestinationName(topic),
		semconv.MessagingMessagePayloadSizeBytes(len(msg.Body)),
	}
	if msg.ID != "" {
		attr = append(attr, semconv.MessagingMessageID(msg.ID))
	}

	if correlationID := msg.Header.GetCorrelationID(); correlationID != "" {
		attr = append(attr, semconv.MessagingMessageConversationID(correlationID))
	}

	return attr
}
