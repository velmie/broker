package natsjs_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v4"
)

func TestManagedConsumerReceivesFilterSet(t *testing.T) {
	nc, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "MANAGED_FILTERS",
		Subjects: []string{"managed.*"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = js.AddConsumer("MANAGED_FILTERS", &nats.ConsumerConfig{
		Durable:        "worker",
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubjects: []string{"managed.created", "managed.paid"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, subject := range []string{"managed.created", "managed.excluded", "managed.paid"} {
		if _, err = js.Publish(subject, []byte(subject)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, stop := context.WithTimeout(context.Background(), testTimeout)
	defer stop()
	confirmed := 0
	cfg := natsjs.ConsumerConfig{
		Stream:       "MANAGED_FILTERS",
		Consumer:     "worker",
		Subjects:     []string{"managed.paid", "managed.created"},
		FetchTimeout: time.Second,
		Observer: natsjs.Observer{
			Record: func(_ context.Context, event natsjs.Observation) {
				if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
					confirmed++
					if confirmed == 2 {
						stop()
					}
				}
			},
		},
	}
	consumer, err := natsjs.NewConsumer(nc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var bodies []string
	handler := broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		bodies = append(bodies, string(delivery.Message().Body))
		if delivery.Source() != "MANAGED_FILTERS/worker" {
			t.Errorf("source: %s", delivery.Source())
		}
		return broker.Handled{}, nil
	})
	if err = consumer.Run(ctx, handler); !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
	if !reflect.DeepEqual(bodies, []string{"managed.created", "managed.paid"}) || confirmed != 2 {
		t.Fatalf("bodies=%v confirmed=%d", bodies, confirmed)
	}
}

func TestManagedFilterSetConsumerModes(t *testing.T) {
	for _, mode := range []string{"ephemeral-pull", "push", "queue-push"} {
		t.Run(mode, func(t *testing.T) { checkManagedFilterSetMode(t, mode) })
	}
}

//nolint:gocyclo // Verify creation, redelivery, acknowledgements, binding and shutdown in one real session.
func checkManagedFilterSetMode(t *testing.T, mode string) {
	t.Helper()
	nc, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	stream := "MODE_" + strings.ReplaceAll(strings.ToUpper(mode), "-", "_")
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: []string{stream + ".*"},
	})
	if err != nil {
		t.Fatal(err)
	}
	remote := &nats.ConsumerConfig{
		Name:           "worker",
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        time.Second,
		MaxDeliver:     3,
		FilterSubjects: []string{stream + ".created", stream + ".paid"},
	}
	if mode != "ephemeral-pull" {
		remote.Durable = "worker"
	}
	if mode != "ephemeral-pull" {
		remote.DeliverSubject = nats.NewInbox()
	}
	if mode == "queue-push" {
		remote.DeliverGroup = "workers"
	}
	if _, err = js.AddConsumer(stream, remote); err != nil {
		t.Fatal(err)
	}
	for _, subject := range []string{stream + ".created", stream + ".excluded", stream + ".paid"} {
		if _, err = js.Publish(subject, []byte(subject)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, stop := context.WithTimeout(context.Background(), testTimeout)
	defer stop()
	subjects := []string{stream + ".paid", stream + ".created"}
	confirmed := 0
	cfg := natsjs.ConsumerConfig{
		Stream:   stream,
		Consumer: "worker",
		Subjects: subjects,
		Observer: natsjs.Observer{
			Record: func(_ context.Context, event natsjs.Observation) {
				if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
					confirmed++
					if confirmed == 2 {
						stop()
					}
				}
			},
		},
	}
	var consumer *natsjs.Consumer
	if mode == "ephemeral-pull" {
		consumer, err = natsjs.NewPullConsumer(nc, cfg)
	} else {
		consumer, err = natsjs.NewPushConsumer(nc, cfg)
	}
	if err != nil {
		t.Fatal(err)
	}
	subjects[0] = "changed.after.construction"
	attempts := map[string]int{}
	handler := broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		body := string(delivery.Message().Body)
		attempts[body]++
		if delivery.Source() != stream+"/worker" || delivery.(natsjs.SubjectReader).Subject() != body {
			t.Errorf("incorrect metadata for %s", body)
		}
		if body == stream+".created" && attempts[body] == 1 {
			return broker.RetryAfter{}, nil
		}
		return broker.Handled{}, nil
	}, broker.WithRequirements(broker.RedeliveryRequirement{}))
	if err = consumer.Run(ctx, handler); !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
	if attempts[stream+".created"] != 2 || attempts[stream+".paid"] != 1 || len(attempts) != 2 || confirmed != 2 {
		t.Fatalf("attempts=%v confirmed=%d", attempts, confirmed)
	}
	info, err := js.ConsumerInfo(stream, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 0 || info.AckFloor.Stream != 3 || !reflect.DeepEqual(info.Config.FilterSubjects, remote.FilterSubjects) {
		t.Fatalf("remote consumer changed or not acknowledged: %+v", info)
	}
}

func TestManagedFilterSetDriftStopsBeforeCallback(t *testing.T) {
	nc, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "MANAGED_DRIFT",
		Subjects: []string{"drift.*"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = js.AddConsumer("MANAGED_DRIFT", &nats.ConsumerConfig{
		Durable:        "worker",
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubjects: []string{"drift.created", "drift.paid"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish("drift.created", []byte("pending")); err != nil {
		t.Fatal(err)
	}
	for _, requested := range [][]string{
		{"drift.created"},
		{"drift.created", "drift.extra"},
		{"drift.created", "drift.paid", "drift.extra"},
	} {
		cfg := natsjs.ConsumerConfig{
			Stream:   "MANAGED_DRIFT",
			Consumer: "worker",
			Subjects: requested,
		}
		consumer, createErr := natsjs.NewConsumer(nc, cfg)
		if createErr != nil {
			t.Fatal(createErr)
		}
		handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("callback ran with mismatched filters")
			return broker.Handled{}, nil
		})
		runErr := consumer.Run(t.Context(), handler)
		if runErr == nil || !strings.Contains(runErr.Error(), "drift.paid") || !strings.Contains(runErr.Error(), "want") {
			t.Fatalf("filter drift diagnostics: %v", runErr)
		}
	}
	info, err := js.ConsumerInfo("MANAGED_DRIFT", "worker")
	if err != nil {
		t.Fatal(err)
	}
	if info.NumPending != 1 || info.AckFloor.Stream != 0 ||
		!reflect.DeepEqual(info.Config.FilterSubjects, []string{"drift.created", "drift.paid"}) {
		t.Fatalf("drift changed consumer: %+v", info)
	}
}

func TestManagedConsumerRejectsInvalidFilters(t *testing.T) {
	conn, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	for _, tc := range []struct {
		name     string
		subjects []string
		field    string
	}{
		{
			name:  "missing",
			field: "subjects",
		},
		{
			name:     "empty element",
			subjects: []string{"valid.one", ""},
			field:    "subjects[1]",
		},
		{
			name:     "duplicate",
			subjects: []string{"valid.one", "valid.one"},
			field:    "subjects[1]",
		},
		{
			name:     "invalid syntax",
			subjects: []string{"valid.one", "bad.>.more"},
			field:    "subjects[1]",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err = natsjs.NewConsumer(conn, natsjs.ConsumerConfig{
				Stream:   "STREAM",
				Consumer: "worker",
				Subjects: tc.subjects,
			})
			if err == nil || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("invalid filter accepted or no field in error: %v", err)
			}
		})
	}
}
