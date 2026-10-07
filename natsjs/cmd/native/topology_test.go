package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNativeTopologyPreservesExistingConfiguration(t *testing.T) {
	nc := nativeConnection(t)
	js, err := nc.JetStream(nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cleanupErr := cleanup(); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	}()
	info, err := js.StreamInfo(stream)
	if err != nil {
		t.Fatal(err)
	}
	info.Config.Subjects = []string{stream + ".existing"}
	info.Config.Description, info.Config.MaxMsgs, info.Config.MaxAge = "retained policy", 37, time.Hour
	before, err := js.UpdateStream(&info.Config)
	if err != nil {
		t.Fatal(err)
	}
	if err = ensureStream(ctx, js, stream, []string{stream + ".existing", stream + ".added", stream + ".added"}); err != nil {
		t.Fatal(err)
	}
	after, err := js.StreamInfo(stream)
	if err != nil {
		t.Fatal(err)
	}
	want := before.Config
	want.Subjects = []string{stream + ".existing", stream + ".added"}
	if !reflect.DeepEqual(want, after.Config) {
		t.Fatalf("subject update replaced unrelated native configuration: before=%+v after=%+v", want, after.Config)
	}
}

func TestNativeTopologyProfile(t *testing.T) {
	nc := nativeConnection(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := runTopology(ctx, nc); err != nil {
		t.Fatal(err)
	}
	if nc.IsClosed() {
		t.Fatal("topology profile closed borrowed connection")
	}
}
