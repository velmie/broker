## Usage example
[NATS messaging](http://thinkmicroservices.com/blog/2021/jetstream/nats-jetstream.html)

### Run nats docker container
> docker run --rm -it -p 4222:4222 --name nats nats:latest -js
### Run nats-box docker container
> docker run --network host --rm -it synadia/nats-box
### Run example
> go run natsjs-publish/main.go

```
// list streams
> nats str ls
// get stream info
> nats str info TEST
// Add a stream
> nats str add TEST --subjects "TEST.*" --ack --max-msgs=-1 --max-bytes=-1 --max-age=1y --storage file --retention limits --max-msg-size=-1 --discard old --dupe-window="0s" --replicas 1
// Add a consumer
> nats con add TEST MONITOR --ack none --target monitor.TEST --deliver last --replay instant --filter '' --heartbeat=10s --flow-control
```