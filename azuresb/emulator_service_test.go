package azuresb_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

func TestEmulatorRunService(t *testing.T) {
	client, queue, _, _ := emulatorClient(t)
	for _, scenario := range []string{"pre-canceled", "idle", "deadline", "custom-cause", "graceful", "forced"} {
		t.Run(scenario, func(t *testing.T) {
			parent, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			if scenario == "pre-canceled" {
				cancel(errors.New("pre-canceled"))
			}
			if scenario == "deadline" {
				var stop context.CancelFunc
				parent, stop = context.WithTimeout(context.Background(), 2*time.Second)
				defer stop()
			}
			mode := azservicebus.ReceiveModePeekLock
			if scenario == "forced" {
				mode = azservicebus.ReceiveModeReceiveAndDelete
			}
			if scenario == "graceful" || scenario == "forced" {
				sender, err := client.NewSender(queue, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer closeEmulatorResource(t, sender.Close)
				ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				id := fmt.Sprintf("service-%d", time.Now().UnixNano())
				if err = sender.SendMessage(ctx, &azservicebus.Message{
					MessageID: &id,
				}, nil); err != nil {
					t.Fatal(err)
				}
			}
			receiving := make(chan struct{}, 1)
			config := adapter.ConsumerConfig{
				Source:           "service",
				ReceiveMode:      mode,
				ReceiveTimeout:   5 * time.Second,
				OperationTimeout: 5 * time.Second,
				CloseTimeout:     5 * time.Second,
			}
			if scenario == "forced" {
				config.Shutdown.Mode = broker.ShutdownCancel
			}
			consumer, err := adapter.NewConsumer(func() (adapter.Receiver, error) {
				receiver, e := client.NewReceiverForQueue(queue, &azservicebus.ReceiverOptions{
					ReceiveMode: mode,
				})
				if e != nil {
					return nil, e
				}
				return &notifyingReceiver{
					Receiver:  receiver,
					receiving: receiving,
				}, nil
			}, config)
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() {
				result <- consumer.RunService(parent, broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
					cancel(errors.New("requested stop"))
					if scenario == "forced" {
						<-work.Done()
						return nil, work.Err()
					}
					return broker.Handled{}, nil
				}))
			}()
			if scenario == "idle" || scenario == "custom-cause" {
				select {
				case <-receiving:
					cancel(errors.New("requested stop"))
				case <-time.After(10 * time.Second):
					t.Fatal("receive not started")
				}
			}
			select {
			case err = <-result:
				if err != nil {
					t.Fatalf("RunService: %v", err)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("RunService did not stop")
			}
		})
	}
}

type notifyingReceiver struct {
	*azservicebus.Receiver
	receiving chan<- struct{}
}

func (r *notifyingReceiver) ReceiveMessages(
	ctx context.Context, maxMessages int, options *azservicebus.ReceiveMessagesOptions,
) ([]*azservicebus.ReceivedMessage, error) {
	select {
	case r.receiving <- struct{}{}:
	default:
	}
	return r.Receiver.ReceiveMessages(ctx, maxMessages, options)
}
