package azuresb

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// AzureSenderOption configures an Azure sender
type AzureSenderOption func(*azureSenderConfig)

type azureSenderConfig struct {
	client           *azservicebus.Client
	connectionString string
}

// WithSenderClient sets an existing azservicebus.Client for the sender
func WithSenderClient(client *azservicebus.Client) AzureSenderOption {
	return func(cfg *azureSenderConfig) {
		cfg.client = client
	}
}

// WithSenderConnectionString sets the connection string for creating a new client
func WithSenderConnectionString(connStr string) AzureSenderOption {
	return func(cfg *azureSenderConfig) {
		cfg.connectionString = connStr
	}
}

// NewAzureSender creates a new ASBSender for the given topic
func NewAzureSender(topic string, opts ...AzureSenderOption) (ASBSender, error) {
	var cfg azureSenderConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.client == nil {
		if cfg.connectionString == "" {
			return nil, fmt.Errorf("azuresb: no client or connection string provided")
		}
		client, err := azservicebus.NewClientFromConnectionString(cfg.connectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("azuresb: failed to create client: %w", err)
		}
		cfg.client = client
	}
	sender, err := cfg.client.NewSender(topic, nil)
	if err != nil {
		return nil, fmt.Errorf("azuresb: failed to create sender for topic %q: %w", topic, err)
	}
	return sender, nil
}

// DefaultSenderFactory is the default implementation of the SenderFactory interface
type DefaultSenderFactory struct {
	opts []AzureSenderOption
}

// NewDefaultSenderFactory creates a new DefaultSenderFactory with the given options
func NewDefaultSenderFactory(opts ...AzureSenderOption) *DefaultSenderFactory {
	return &DefaultSenderFactory{opts: opts}
}

// CreateSender creates a new ASBSender for the given topic
func (f DefaultSenderFactory) CreateSender(topic string) (ASBSender, error) {
	s, err := NewAzureSender(topic, f.opts...)
	if err != nil {
		return nil, fmt.Errorf("azuresb.DefaultSenderFactory.CreateSender: %w", err)
	}
	return s, nil
}
