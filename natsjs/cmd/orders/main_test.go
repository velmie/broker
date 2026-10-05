package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestCancellationDoesNotHideJoinedFailures(t *testing.T) {
	failure := errors.New("settlement failed")
	if onlyCancellation(fmt.Errorf("worker: %w", errors.Join(context.Canceled, failure))) {
		t.Fatal("expected shutdown hid an independent failure")
	}
	if !onlyCancellation(errors.Join(fmt.Errorf("worker: %w", context.Canceled), context.Canceled)) {
		t.Fatal("ordinary shutdown was reported as failure")
	}
}
