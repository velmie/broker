package azuresb

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const (
	ReceiverTypeQueue        = ReceiverType("queue")
	ReceiverTypeSubscription = ReceiverType("subscription")
)

// asbReceiver abstracts the methods of the Azure Service Bus receiver
type asbReceiver interface {
	ReceiveMessages(ctx context.Context, maxMessages int, options *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error)
	CompleteMessage(ctx context.Context, message *azservicebus.ReceivedMessage, options *azservicebus.CompleteMessageOptions) error
	AbandonMessage(ctx context.Context, message *azservicebus.ReceivedMessage, options *azservicebus.AbandonMessageOptions) error
	Close(ctx context.Context) error
}

// Receiver is an interface that abstracts the Azure Service Bus message receiver
type Receiver interface {
	// Receive retrieves a single message
	// It should block until a message is received or the context is cancelled
	Receive(ctx context.Context) (*Message, error)
	// Close closes the receiver and releases its resources
	Close() error
}

type ReceiverType string

// AzureReceiver implements the Receiver interface using the Azure Service Bus SDK
type AzureReceiver struct {
	receiver      asbReceiver
	client        *azservicebus.Client
	managedClient bool
}

// AzureReceiverOption configures the creation of an AzureReceiver
type AzureReceiverOption func(*azureReceiverConfig)

type azureReceiverConfig struct {
	existingReceiver asbReceiver
	client           *azservicebus.Client
	connectionString string
	receiverType     ReceiverType
	subscriptionName string
	receiverOptions  *azservicebus.ReceiverOptions
}

// WithExistingReceiver allows supplying an already created asbReceiver
func WithExistingReceiver(r asbReceiver) AzureReceiverOption {
	return func(cfg *azureReceiverConfig) {
		cfg.existingReceiver = r
	}
}

// WithReceiverClient allows providing an existing azservicebus.Client
func WithReceiverClient(client *azservicebus.Client) AzureReceiverOption {
	return func(cfg *azureReceiverConfig) {
		cfg.client = client
	}
}

// WithReceiverConnectionString instructs NewAzureReceiver to create its own client using the given connection string
func WithReceiverConnectionString(connStr string) AzureReceiverOption {
	return func(cfg *azureReceiverConfig) {
		cfg.connectionString = connStr
	}
}

// WithReceiverType explicitly sets the receiver type.
// Defaults to ReceiverTypeQueue.
func WithReceiverType(rt ReceiverType) AzureReceiverOption {
	return func(cfg *azureReceiverConfig) {
		cfg.receiverType = rt
	}
}

// WithSubscriptionName sets the subscription name and marks the receiver type as ReceiverTypeSubscription.
// When using a subscription receiver, the topic provided to Subscribe is treated as the topic name.
func WithSubscriptionName(subscriptionName string) AzureReceiverOption {
	return func(cfg *azureReceiverConfig) {
		cfg.receiverType = ReceiverTypeSubscription
		cfg.subscriptionName = subscriptionName
	}
}

// WithReceiverOptions allows passing custom Azure receiver options (azservicebus.ReceiverOptions)
// to NewReceiverForSubscription or NewReceiverForQueue.
func WithReceiverOptions(options *azservicebus.ReceiverOptions) AzureReceiverOption {
	return func(cfg *azureReceiverConfig) {
		cfg.receiverOptions = options
	}
}

// NewAzureReceiver creates a new AzureReceiver using the given topic and options
// The topic parameter (provided by the Subscriber) is used as the queue name for queue receivers
// or as the topic name for subscription receivers
func NewAzureReceiver(topic string, opts ...AzureReceiverOption) (Receiver, error) {
	var cfg azureReceiverConfig
	cfg.receiverType = ReceiverTypeQueue

	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.existingReceiver != nil {
		return &AzureReceiver{
			receiver:      cfg.existingReceiver,
			client:        nil,
			managedClient: false,
		}, nil
	}

	if cfg.client != nil {
		return createReceiverFromClient(topic, cfg, cfg.client)
	}

	if cfg.connectionString != "" {
		client, err := azservicebus.NewClientFromConnectionString(cfg.connectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create service bus client: %w", err)
		}
		ar, err := createReceiverFromClient(topic, cfg, client)
		if err != nil {
			return nil, err
		}
		azureReceiver := ar.(*AzureReceiver)
		azureReceiver.client = client
		azureReceiver.managedClient = true
		return azureReceiver, nil
	}

	return nil, errors.New("insufficient configuration: provide an existing receiver, client, or connection string")
}

func createReceiverFromClient(topic string, cfg azureReceiverConfig, client *azservicebus.Client) (Receiver, error) {
	if cfg.receiverType == ReceiverTypeSubscription {
		if cfg.subscriptionName == "" {
			return nil, errors.New("subscription name must be provided for a subscription receiver")
		}
		r, err := client.NewReceiverForSubscription(topic, cfg.subscriptionName, cfg.receiverOptions)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to create receiver for subscription %q on topic %q: %w",
				cfg.subscriptionName,
				topic,
				err,
			)
		}
		return &AzureReceiver{
			receiver:      r,
			client:        client,
			managedClient: false,
		}, nil
	}

	r, err := client.NewReceiverForQueue(topic, cfg.receiverOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create receiver for queue %q: %w", topic, err)
	}
	return &AzureReceiver{
		receiver:      r,
		client:        client,
		managedClient: false,
	}, nil
}

func (r *AzureReceiver) Receive(ctx context.Context) (*Message, error) {
	msgs, err := r.receiver.ReceiveMessages(ctx, 1, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to receive messages: %w", err)
	}
	if len(msgs) == 0 {
		return nil, fmt.Errorf("no messages received")
	}
	receivedMsg := msgs[0]

	// Define functions to complete or abandon the message.
	completeFunc := func() error {
		if err := r.receiver.CompleteMessage(ctx, receivedMsg, nil); err != nil {
			return fmt.Errorf("failed to complete message %q: %w", receivedMsg.MessageID, err)
		}
		return nil
	}
	abandonFunc := func() error {
		if err := r.receiver.AbandonMessage(ctx, receivedMsg, nil); err != nil {
			return fmt.Errorf("failed to abandon message %q: %w", receivedMsg.MessageID, err)
		}
		return nil
	}

	headers := make(map[string]string)
	for k, v := range receivedMsg.ApplicationProperties {
		headers[k] = fmt.Sprintf("%v", v)
	}

	// Wrap the Azure SDK message into our domain-specific Message.
	msg := NewMessage(receivedMsg.Body, headers, receivedMsg.MessageID, completeFunc, abandonFunc)
	return msg, nil
}

func (r *AzureReceiver) Close() error {
	var lastErr error
	if err := r.receiver.Close(context.Background()); err != nil {
		lastErr = fmt.Errorf("failed to close receiver: %w", err)
	}
	if r.managedClient && r.client != nil {
		if err := r.client.Close(context.Background()); err != nil {
			lastErr = fmt.Errorf("failed to close client: %w", err)
		}
	}
	return lastErr
}
