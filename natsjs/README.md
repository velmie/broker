# NATS JetStream

Package provides convenient wrapper to work with NATS JetStream and provides compatability with [Velmie's broker package](https://github.com/velmie/broker). Under the hood standard [NATS Go client](https://github.com/nats-io/nats.go) is used.

## Publisher and subscriber initialization
Publisher or subscriber can be initialized as easy as follows:
```go
import (
    "github.com/velmie/broker/natsjs/v2/publisher"
    "github.com/velmie/broker/natsjs/v2/subscriber"
)

const stream = "NATSJS"

pub, _ := publisher.Connect()
sub, _ := subscriber.Connect(stream)
```
Under the hood it performs connection to default NATS url (nats://127.0.0.1:4222) with all connection arguments set as default. Options can be customized according to your own needs as explained below.

### Connection options
Both publisher and subscriber have connection options, some of them are common and other publisher/subscriber specific. Connection can be configured by passing `ConnectionsOptions(...)` functional options on publisher/subscriber initialization. So, pretty fully configured publisher connection can look as follows:
```go
import (
    "github.com/nats-io/nats.go"
	
    "github.com/velmie/broker/natsjs/v2/publisher"
    "github.com/velmie/broker/natsjs/v2/conn"
)

pub, err := publisher.Connect(
    publisher.ConnectionOptions(
        conn.URL(nats.DefaultURL),
        conn.NATSOptions(
            nats.ReconnectWait(time.Second),
            nats.RetryOnFailedConnect(true),
            // here can be other standard NATS connection options 
        ),
        conn.JetStreamContextOptions(
            nats.MaxWait(time.Second),
            // here con be other NAST JetStream Context options
        ),
        conn.NoWaitFailedConnectRetry(),
    ),
)
```
Please, see below some short explanation about options specified above:
* `conn.URL` - specifies NATS server URL;
* `conn.NATSOptions` - represents connection options from standard library. Please, see [nats.Option](https://pkg.go.dev/github.com/nats-io/nats.go#Option) for more details and full list of available options;
* `conn.JetStreamContextOptions` - are used to configure JetStreamContext. Please, see [nats.JSOpt](https://pkg.go.dev/github.com/nats-io/nats.go#JSOpt) for more details and full list of available options;
* `conn.NoWaitFailedConnectRetry` - special option used in conjunction with [nats.RetryOnFailedConnect](https://pkg.go.dev/github.com/nats-io/nats.go#RetryOnFailedConnect). If `RetryOnFailedConnect` options is set and server can't be reached, connection will be set to `RECONNECTING` state and no error returned, `JetStreamContext` will be created as well and all subsequent requests will be buffered until connection is not in `CONNECTED` state or timeout is raised and error is returned. Package overcome this behavior by checking state of connection every `nats.ReconnectWait` and, as soon as state is `CONNECTED`, healthy connection is returned or if state is `CLOSED` (connection timeout) error returned. By specifying `NoWaitFailedConnectRetry` option you disable awaiting behavior and proceed with connection procedure of standard Go client. Please, note that in this case you are responsible for all JetStreams or Consumers initialization and handling any errors occurred because of `RECONNECTING` state. Some actions can be moved, for instance, to `ConnectedCB`. (Please, see ConnectedCB in [nats.Options](https://pkg.go.dev/github.com/nats-io/nats.go#Options)).
* `publisher.UseConnection` / `subscriber.UseConnection` - allows to reuse existing connection on subscriber/publisher initialization. As soon as publisher/subscriber instance is constructed there is a possibility to call `Connection` method for its retrieval.
### Publisher options
There are some options specific to publisher:
```go
import (
    "github.com/nats-io/nats.go"

    "github.com/velmie/broker/natsjs/v2/publisher"
)

const (
	stream = "NATSJS"
	subject = "NATSJS.>"
)

pub, err := publisher.Connect(
    // ... here can be connection options
    publisher.PubOptions(
        nats.AckWait(time.Second),
        nats.RetryWait(time.Second),
        nats.RetryAttempts(10),
    ),
    publisher.InitJetStream(&nats.StreamConfig{
        Name:      stream,
        Subjects:  []string{subject},
        Retention: nats.InterestPolicy,
    }),
)
```
Please, see below some short explanation about options specified above:
* `publisher.PubOptions` - specifies options which are propagated on message publishing (Please, see [nats.PubOpt](https://pkg.go.dev/github.com/nats-io/nats.go#PubOpt));
* `publisher.InitJetStream` - creates JetStream with provided configuration. If JetStream with the same name is already present no actions will be taken.  
  
**Please, note that declaration of broker topology (Streams, Consumers, etc.) on client side is not recommended way to go. The best option is to define all necessary object beforehand and use client to communicate with infrastructure. It gives you more control over topology and allow to avoid unexpected behavior from on the fly declaration.**  
  
### Subscriber options
There are some options specific to subscriber:
```go
import (
    "github.com/nats-io/nats.go"

    "github.com/velmie/broker/natsjs/v2/subscriber"
)

const (
	stream = "NATSJS"
	consumer = "NATSJS-CSM"
)

subFactory := func(subj string, namer subscriber.GroupNamer) subscriber.Subscriptor {
    return subscriber.PullSubscription().
        Subject(subj).
        Durable(namer.Name()).
        SubOptions(
            nats.DeliverLast(),
            nats.AckExplicit(),
            nats.ReplayInstant(),
        )
}

consFactory := func(subj string, namer subscriber.GroupNamer) *nats.ConsumerConfig {
    return &nats.ConsumerConfig{
        Name:          namer.Name(),
        Durable:       namer.Name(),
        DeliverPolicy: nats.DeliverLastPolicy,
        AckPolicy:     nats.AckExplicitPolicy,
    }
}

sub, err := subscriber.Connect(
    stream,
    // ... here can be connection options
    subscriber.SubscriptionFactory(subFactory),
    subscriber.ConsumerFactory(consFactory),
)
```
Please, see below some short explanation about options specified above:
* `subscriber.SubscriptionFactory` - defines the logic for building subscriptions based on stream and subject names or any other user defined criteria. The goal of logic within function is to return `subscriber.Subscriptor` which is used to init subscription. If nothing is specified, `subscriber.DefaultSubscriptionFactory` is used (constructs queue push consumers). Package provides various of built-in subscriptions builders: `subscriber.PullSubscription()`, `subscriber.SyncQueueSubscription()`, `subscriber.AsyncQueueSubscription()`, `subscriber.SyncSubscription()`, `subscriber.AsyncSubscription()`;
* `subscriber.ConsumerFactory` - defines the way consumers are created before subscribing. Again, it is recommended to define topology before communication and don't delegate this responsibility to client. If consumer with the same name is already present no action will be taken. Package doesn't have any default consumer factory, so if nothing is specified here, consumers will be created according to NATS Go client rules on subscription initialization.

## Publishing messages
Message publishing function follows the signature of [Velmie's broker package](https://github.com/velmie/broker) publisher and is easy as follows:
```go
const (
	subject = "NATSJS.subj.created"
)

// ... here is publisher init

err = pub.Publish(subject, &broker.Message{
    ID:   "message-id",
    Body: []byte("Hello World!"),
})
```
If `publisher.PubOptions` option is specified on publisher initialization, it will be propagated on message publishing.

## Subscribing to events
Subscription function follows the signature of [Velmie's broker package](https://github.com/velmie/broker) subscriber and is easy as follows:
```go
const (
    subject = "NATSJS.subj.created"
)

handler := func(evt broker.Event) error {
    data := evt.Message().Body
    log.Println(data)
    return evt.Ack() // manual ack
}

errHandler := func(err error, sub broker.Subscription) {
    log.Println(err)
}

ns, _ := sub.Subscribe(subject, handler, broker.WithErrorHandler(errHandler))
defer ns.Unsubscribe()
```
Please, refer to `broker.SubscribeOption` to configure subscribe side effects and options. It is worth mentioning that `evt.Ack()` is an explicit message ack which is not necessary in default configuration, since by default messages are acked automatically if no error returned from handler. To prevent automatic ack, pass `broker.DisableAutoAck` to `Subscribe` function options.
## Subscription message handling
There different requirements for each application and different types of subscriptions might be required, there are some tips you should be aware of.
* For pull subscription additional goroutine is created which `Fetch` messages with specified batch and process each message with `broker.Handler`. Goroutine is stopped as soon as `Unsubscribe` is called or connection to server is closed;
* There is nothing special about async subscriptions - `broker.Handler` is called as callback function on each message;
* Sync subscription message fetch is fully controlled by client. Package provides special type `subscriber.SyncIterator` to trigger message retrieval and processing:
```go
const (
    stream = "NATSJS"
    subject = "NATSJS.subj.created"
)

subFactory := func(subj string, _ subscriber.GroupNamer) subscriber.Subscriptor {
    return subscriber.SyncSubscription().Subject(subj)
}

sub, _ := subscriber.Connect(
    stream,
    subscriber.SubscriptionFactory(subFactory),
)

handler := func(evt broker.Event) error {
    return evt.Ack()
}

ns, _ := sub.Subscribe(subject, handler)
defer ns.Unsubscribe()

itr, _ := subscriber.NewSyncIterator(ns)
_ = itr.Next(time.Second)
```
Each call to `Next` causes fetch of message and, if successfully retrieved, processing via `broker.Handler` passed to `Subscribe` function.

Since, there are a lot of different subscriptions, pull and async subscriptions are the most useful.
## Examples
Please, refer to `natsjs_test.go` file which contains tests for natsjs package.