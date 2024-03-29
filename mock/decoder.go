// Code generated by MockGen. DO NOT EDIT.
// Source: decoder.go
//
// Generated by this command:
//
//	mockgen -source decoder.go -destination ./mock/decoder.go
//
// Package mock_broker is a generated GoMock package.
package mock_broker

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockDecoder is a mock of Decoder interface.
type MockDecoder struct {
	ctrl     *gomock.Controller
	recorder *MockDecoderMockRecorder
}

// MockDecoderMockRecorder is the mock recorder for MockDecoder.
type MockDecoderMockRecorder struct {
	mock *MockDecoder
}

// NewMockDecoder creates a new mock instance.
func NewMockDecoder(ctrl *gomock.Controller) *MockDecoder {
	mock := &MockDecoder{ctrl: ctrl}
	mock.recorder = &MockDecoderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDecoder) EXPECT() *MockDecoderMockRecorder {
	return m.recorder
}

// Decode mocks base method.
func (m *MockDecoder) Decode(data []byte, v any) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Decode", data, v)
	ret0, _ := ret[0].(error)
	return ret0
}

// Decode indicates an expected call of Decode.
func (mr *MockDecoderMockRecorder) Decode(data, v any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Decode", reflect.TypeOf((*MockDecoder)(nil).Decode), data, v)
}
