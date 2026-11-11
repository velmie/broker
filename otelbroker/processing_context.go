package otelbroker

import (
	"context"
	"sync"
)

type processingContextKey struct{}

type processingCapture struct {
	mu  sync.RWMutex
	ctx context.Context
}

// CaptureProcessing installs delivery-local storage for the context created
// by TraceProcessing. It preserves cancellation ancestry and starts no work.
func CaptureProcessing(ctx context.Context) context.Context {
	return context.WithValue(ctx, processingContextKey{}, &processingCapture{})
}

// ProcessingContext returns the captured processing context, or ctx when none
// was captured. Use it for observations, not SDK operations.
func ProcessingContext(ctx context.Context) context.Context {
	capture, ok := ctx.Value(processingContextKey{}).(*processingCapture)
	if !ok {
		return ctx
	}
	capture.mu.RLock()
	defer capture.mu.RUnlock()
	if capture.ctx == nil {
		return ctx
	}
	return capture.ctx
}

func captureProcessingContext(ctx context.Context) {
	capture, ok := ctx.Value(processingContextKey{}).(*processingCapture)
	if !ok {
		return
	}
	capture.mu.Lock()
	capture.ctx = ctx
	capture.mu.Unlock()
}
