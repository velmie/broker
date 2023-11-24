// Code generated by MockGen. DO NOT EDIT.
// Source: go.opentelemetry.io/otel/propagation (interfaces: TextMapPropagator)
//
// Generated by this command:
//
//	mockgen -destination ./mock/propagator.go -package mock_otelbroker go.opentelemetry.io/otel/propagation TextMapPropagator
//
// Package mock_otelbroker is a generated GoMock package.
package mock_otelbroker

import (
	context "context"
	reflect "reflect"

	propagation "go.opentelemetry.io/otel/propagation"
	gomock "go.uber.org/mock/gomock"
)

// MockTextMapPropagator is a mock of TextMapPropagator interface.
type MockTextMapPropagator struct {
	ctrl     *gomock.Controller
	recorder *MockTextMapPropagatorMockRecorder
}

// MockTextMapPropagatorMockRecorder is the mock recorder for MockTextMapPropagator.
type MockTextMapPropagatorMockRecorder struct {
	mock *MockTextMapPropagator
}

// NewMockTextMapPropagator creates a new mock instance.
func NewMockTextMapPropagator(ctrl *gomock.Controller) *MockTextMapPropagator {
	mock := &MockTextMapPropagator{ctrl: ctrl}
	mock.recorder = &MockTextMapPropagatorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTextMapPropagator) EXPECT() *MockTextMapPropagatorMockRecorder {
	return m.recorder
}

// Extract mocks base method.
func (m *MockTextMapPropagator) Extract(arg0 context.Context, arg1 propagation.TextMapCarrier) context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Extract", arg0, arg1)
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// Extract indicates an expected call of Extract.
func (mr *MockTextMapPropagatorMockRecorder) Extract(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Extract", reflect.TypeOf((*MockTextMapPropagator)(nil).Extract), arg0, arg1)
}

// Fields mocks base method.
func (m *MockTextMapPropagator) Fields() []string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Fields")
	ret0, _ := ret[0].([]string)
	return ret0
}

// Fields indicates an expected call of Fields.
func (mr *MockTextMapPropagatorMockRecorder) Fields() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Fields", reflect.TypeOf((*MockTextMapPropagator)(nil).Fields))
}

// Inject mocks base method.
func (m *MockTextMapPropagator) Inject(arg0 context.Context, arg1 propagation.TextMapCarrier) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Inject", arg0, arg1)
}

// Inject indicates an expected call of Inject.
func (mr *MockTextMapPropagatorMockRecorder) Inject(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Inject", reflect.TypeOf((*MockTextMapPropagator)(nil).Inject), arg0, arg1)
}
