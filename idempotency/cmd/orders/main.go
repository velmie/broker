// Command orders demonstrates idempotent handling with an in-memory store.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/velmie/idempo"
	"github.com/velmie/idempo/memory"

	"github.com/velmie/broker"
	"github.com/velmie/broker/idempotency"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) (err error) {
	// The store keeps completion markers in memory.
	store := memory.New()
	defer func() { err = errors.Join(err, store.Close()) }()
	deduplicate, err := idempotency.Middleware(idempo.NewEngine(store), idempotency.WithRequireKey(true))
	if err != nil {
		return err
	}

	calls, total := 0, 0
	handler := broker.NewTypedHandler(broker.DecodeJSON[order], func(_ context.Context, value order) error {
		calls++
		total += value.Amount
		return nil
	}).WithMiddleware(deduplicate)

	// The message ID identifies repeated input. No transport is needed here.
	delivery := broker.NewDelivery(broker.Message{
		ID: "order-1", Body: []byte(`{"amount":42}`),
	}, "orders")
	for range 2 {
		if _, err = handler.Handle(ctx, delivery); err != nil {
			return err
		}
	}
	_, err = fmt.Printf("handler calls: %d\ntotal amount: %d\n", calls, total)
	return err
}

type order struct {
	Amount int `json:"amount"`
}
