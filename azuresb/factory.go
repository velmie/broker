package azuresb

import "fmt"

type DefaultReceiverFactory struct {
	opts []AzureReceiverOption
}

func NewDefaultReceiverFactory(opts ...AzureReceiverOption) *DefaultReceiverFactory {
	return &DefaultReceiverFactory{opts: opts}
}

func (f DefaultReceiverFactory) CreateReceiver(topic string) (Receiver, error) {
	r, err := NewAzureReceiver(topic, f.opts...)
	if err != nil {
		return nil, fmt.Errorf("azuresb.DefaultReceiverFactory.CreateReceiver: %w", err)
	}
	return r, nil
}
