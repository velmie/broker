package otelnatsjs_test

import (
	"fmt"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/metric/noop"

	"github.com/velmie/broker/otelbroker/otelnatsjs"
)

func ExampleNewObserver() {
	meter := noop.NewMeterProvider().Meter("orders")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	observer, err := otelnatsjs.NewObserver(meter, logger)
	fmt.Println(err == nil && observer.Record != nil)
	// Output: true
}
