# Broker

The "broker" package provides functionality for working with publish/subscribe messaging systems.

The main tasks that this package solves is to simplify the creation of business logic and unify the code for different
implementations.

## Usage example

**Below are examples that demonstrate the general use of the package, without initialization and other details.
See the `examples` directory for specific examples.**

### Message publishing

```go
func Publish(publisher broker.Publisher) {
	message := &broker.Message{
		ID: "my-unique-id",
		Header: map[string]string{
			"additional": "data",
		},
		Body: []byte(`{"my":"event payload"}`),
	}

	err := publisher.Publish("topic-name", message)
	if err != nil {
		fmt.Println("cannot publish message: ", err)
		return
	}
	fmt.Println("message has been published")
	return
}
```

### Message subscribing

```go
func Subscribe(subscriber broker.Subscriber) {
	subscription, err := subscriber.Subscribe(
		"topic-name",
		eventHandler,
		broker.WithDefaultErrorHandler(subscriber, &stubLogger{}),
	)
	if err != nil {
		fmt.Println("cannot subscribe on topic: ", err)
		return
	}

	go func() {
		time.Sleep(15 * time.Second)
		fmt.Println("Unsubscribing ", subscription.Unsubscribe())
	}()
	
	<-subscription.Done()
}

func eventHandler(event broker.Event) error {
	fmt.Printf("[%s] received new message\n", event.Topic())
	msg := event.Message()
	fmt.Println("Headers: ")
	for k, v := range msg.Header {
		fmt.Printf("\t%s = %s\n", k, v)
	}
	fmt.Printf("BODY:\n%s\n", string(msg.Body))

	return nil
}

type stubLogger struct {
}

func (s *stubLogger) Debug(v ...interface{}) {
	fmt.Println("DEBUG:")
	fmt.Println(v...)
}

func (s *stubLogger) Error(v ...interface{}) {
	fmt.Println("ERROR:")
	fmt.Println(v...)
}

func (s *stubLogger) Info(v ...interface{}) {
	fmt.Println("INFO:")
	fmt.Println(v...)
}
```

## Error handling

The package provides a convenient way to handle errors and contains several "out of the box" implementations 
that will be sufficient for most possible use cases.

```go
// ErrorHandler is used in order to handle errors
type ErrorHandler func(err error, sub Subscription)
```

Consider using the `broker.WithDefaultErrorHandler`. This default handler does the following:

* logs error message
* waits 5 seconds
* resubscribes on a topic
  * tries to subscribe every 10 seconds in case if subscription attempt failed

You may want to add additional error handlers. It is possible by combining them using `broker.CombineErrorHandlers`.

## Message acknowledgment

By default, received messages are acknowledged automatically if the Handler run without 
errors and returned nil.
If this behavior is undesirable, 
then it is possible to disable auto-acknowledge using the `broker.DisableAutoAck` option

If this option is applied, then the handler must explicitly "Ack" the message.

```go
func eventHandler(event broker.Event) error {
    //... some event handling business logic
	
	err := event.Ack() // explicit message acknowledgment

	return err
}
```



