// Package azuresb binds caller-configured Azure SDK senders and receivers to broker operations.
package azuresb

import (
	"errors"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
)

const (
	operationTimeoutField   = "OperationTimeout"
	defaultReceiveTimeout   = 30 * time.Second
	defaultOperationTimeout = 30 * time.Second
	defaultCloseTimeout     = 30 * time.Second
	maxSourceLength         = 128
)

// PublisherConfig declares a safe diagnostic source and finite send budget. Zero timeout means 30 seconds.
type PublisherConfig struct {
	Source           string
	OperationTimeout time.Duration
	Observer         *Observer
}

// ConsumerConfig declares the factory's receive mode. The factory must use this
// same mode because the SDK cannot expose it for validation. Zero timeouts mean
// 30 seconds. Receive, native operations and cleanup have independent budgets.
// OperationTimeout bounds each renewal and terminal call. Choose the keepalive
// interval and operation budget to fit the entity lock duration configured by its owner.
type ConsumerConfig struct {
	Source           string
	ReceiveMode      azservicebus.ReceiveMode
	ReceiveTimeout   time.Duration
	OperationTimeout time.Duration
	CloseTimeout     time.Duration
	Shutdown         broker.ShutdownPolicy
	Observer         *Observer
}

func validateSource(source string) error {
	if source == "" || len(source) > maxSourceLength || strings.IndexFunc(source, func(r rune) bool {
		allowed := (r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-/", r))
		return !allowed
	}) >= 0 {
		return errors.New("Source: expected 1..128 safe ASCII label characters")
	}
	return nil
}
func normalizeConsumer(c ConsumerConfig) (ConsumerConfig, error) {
	if err := validateSource(c.Source); err != nil {
		return c, err
	}
	if c.ReceiveMode != azservicebus.ReceiveModePeekLock && c.ReceiveMode != azservicebus.ReceiveModeReceiveAndDelete {
		return c, errors.New("ReceiveMode: unsupported mode")
	}
	if c.ReceiveTimeout == 0 {
		c.ReceiveTimeout = defaultReceiveTimeout
	}
	if c.OperationTimeout == 0 {
		c.OperationTimeout = defaultOperationTimeout
	}
	if c.CloseTimeout == 0 {
		c.CloseTimeout = defaultCloseTimeout
	}
	for _, field := range []struct {
		name    string
		timeout time.Duration
	}{
		{"ReceiveTimeout", c.ReceiveTimeout},
		{operationTimeoutField, c.OperationTimeout},
		{"CloseTimeout", c.CloseTimeout},
	} {
		if field.timeout < 0 {
			return c, &FieldError{Field: field.name, Cause: errors.New("must be positive")}
		}
	}
	if c.Shutdown.Mode != broker.ShutdownGraceful && c.Shutdown.Mode != broker.ShutdownCancel ||
		c.Shutdown.GracePeriod < 0 || c.Shutdown.Mode == broker.ShutdownCancel && c.Shutdown.GracePeriod != 0 {
		return c, errors.New("shutdown: invalid policy")
	}
	return c, nil
}
