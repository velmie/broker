package broker

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"strings"
	"time"
	"unicode"
)

const maxDiagnosticSourceBytes = 256

// ProcessingLogConfig configures optional processing records. The callbacks must
// support consumer concurrency and must not panic. They cannot change delivery
// policy. The configured slog handler owns sink-failure handling and redaction.
type ProcessingLogConfig struct {
	// LogSuccess includes successful callback results. It never claims ACK.
	LogSuccess bool
	// Level overrides severity, for example for expected business rejection.
	// Default: error for failures, warn for decode/metadata rejection or requested
	// retry, info for other callback results. It sees original outcome objects.
	Level func(Disposition, error) slog.Level
	// Attributes selects application-approved fields from the original callback
	// outcome while Delivery is valid. A non-nil error suppresses any disposition;
	// RetryAfter.Cause can explain a requested retry when the error is nil.
	// They are grouped under "attributes" and still require boundary redaction.
	// Bodies, headers and message/correlation IDs are not included by default.
	Attributes func(context.Context, Delivery, Disposition, error) []slog.Attr
}

// LogProcessing records callback outcomes with original errors, stage, source
// and duration. Success is silent unless enabled. RetryAfter.Cause is an error
// object even when the callback error is nil. Processing success/requested retry
// does not establish any later transport outcome. Observe those at the adapter.
//
// Logger is required. Its handler must redact application-specific errors and
// custom attributes, retaining safe field/entity details. Standard slog ignores
// handler return errors, so the handler must report its sink failures itself.
// Logger handlers and config callbacks must not panic or block waiting for their
// own consumer. Wrap RecoverPanics inside this middleware for processing panics.
func LogProcessing(logger *slog.Logger, config ProcessingLogConfig) HandlerMiddleware {
	return HandlerMiddleware{Wrap: func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, delivery Delivery) (Disposition, error) {
			started := time.Now()
			result, err := next(ctx, delivery)
			retry, retryRequested := result.(RetryAfter)
			if err == nil && !retryRequested && !config.LogSuccess {
				return result, err
			}
			level := processingLogLevel(result, err)
			if config.Level != nil {
				level = config.Level(result, err)
			}
			if !logger.Enabled(ctx, level) {
				return result, err
			}
			attrs := []slog.Attr{
				slog.String("operation", "process"), slog.String("source", diagnosticSource(delivery.Source())),
				slog.String("outcome", processingOutcome(result, err)), slog.Duration("duration", time.Since(started)),
			}
			if err != nil {
				attrs = append(attrs, slog.String("stage", processingStage(err)), slog.Any("error", err))
			}
			if retryRequested && retry.Cause != nil {
				attrs = append(attrs, slog.Any("cause", retry.Cause))
			}
			if config.Attributes != nil {
				attrs = append(attrs, slog.Attr{Key: "attributes", Value: slog.GroupValue(config.Attributes(ctx, delivery, result, err)...)})
			}
			logger.LogAttrs(ctx, level, "processing result", attrs...)
			return result, err
		}
	}}
}

func processingLogLevel(result Disposition, err error) slog.Level {
	if err != nil {
		stage := processingStage(err)
		if stage == StageDecode || stage == StageMetadata {
			return slog.LevelWarn
		}
		return slog.LevelError
	}
	if _, retry := result.(RetryAfter); retry {
		return slog.LevelWarn
	}
	return slog.LevelInfo
}

func processingStage(err error) string {
	for err != nil {
		switch value := err.(type) {
		case *StageError:
			return value.Stage
		case *DecodeError:
			return StageDecode
		case *PanicError:
			return stagePanic
		}
		err = errors.Unwrap(err)
	}
	return "process"
}

func processingOutcome(result Disposition, err error) string {
	if err != nil {
		return "failed"
	}
	switch result.(type) {
	case Handled:
		return "handled"
	case RetryAfter:
		return "retry_requested"
	default:
		return "returned"
	}
}

func diagnosticSource(source string) string {
	// Keep native source identities while removing URL credentials and query data.
	if strings.Contains(source, "://") || strings.HasPrefix(source, "//") {
		parsed, err := url.Parse(source)
		if err != nil {
			return "invalid-source"
		}
		source = parsed.Host
	} else {
		source, _, _ = strings.Cut(source, "?")
		source, _, _ = strings.Cut(source, "#")
	}
	var safe strings.Builder
	for _, character := range source {
		if unicode.IsControl(character) || !unicode.IsGraphic(character) {
			character = '_'
		}
		if safe.Len()+len(string(character)) > maxDiagnosticSourceBytes {
			break
		}
		safe.WriteRune(character)
	}
	return safe.String()
}
