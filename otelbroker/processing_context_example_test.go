package otelbroker_test

import (
	"context"
	"fmt"

	"github.com/velmie/broker/otelbroker"
)

func ExampleCaptureProcessing() {
	ctx := otelbroker.CaptureProcessing(context.Background())
	// Without a processing span, observations use the original context.
	fmt.Println(otelbroker.ProcessingContext(ctx) == ctx)
	// Output: true
}

func ExampleProcessingContext() {
	ctx := otelbroker.CaptureProcessing(context.Background())
	// Pass the returned context to the logger that records settlement.
	fmt.Println(otelbroker.ProcessingContext(ctx) == ctx)
	// Output: true
}
