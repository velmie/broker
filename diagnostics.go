package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"
)

const (
	maxErrorDepth          = 8
	maxErrorNodes          = 32
	maxErrorFields         = 16
	maxErrorText           = 128
	diagnosticTypeKey      = "type"
	diagnosticFieldKey     = "field"
	diagnosticOperationKey = "operation"
	diagnosticStageKey     = "stage"
	diagnosticOffsetKey    = "offset"
	diagnosticMessageKey   = "message"
	diagnosticReasonKey    = "reason"
)

// DescribeError projects an error tree into bounded structured data suitable for
// an existing logger or telemetry recorder. A nil error produces nil. It retains
// node types, cause relationships and known Broker/standard-library context;
// arbitrary Error strings, payloads, addresses, URLs and panic values are omitted.
// The input error and its causes remain unchanged.
//
// Each optional describer inspects one original node, without walking its causes.
// The first non-nil result replaces that node's default fields. Describers must
// select application-approved field names and scalar values, support concurrent
// calls, and return promptly without panicking. Use them for native SDK codes and
// business details that the library cannot safely infer. Returning an empty map
// intentionally omits the node's default fields. Unwrapping is still performed.
//
// Fields accept strings, booleans and built-in integer types. Other values
// are omitted rather than invoking custom marshaling or formatting. Strings are
// limited to 128 UTF-8 bytes with control/non-graphic characters removed. There
// are at most 16 selected fields per node, 32 nodes and eight cause levels. Any
// omitted fields or branches mark their containing node as truncated. The keys
// type, cause, causes and truncated are reserved. Bounds prevent oversized output
// and record injection; they do not make arbitrary application text non-sensitive.
func DescribeError(err error, describers ...func(error) map[string]any) map[string]any {
	remaining := maxErrorNodes
	return describeError(err, 0, &remaining, describers)
}

func describeError(err error, depth int, remaining *int, describers []func(error) map[string]any) map[string]any {
	if err == nil {
		return nil
	}
	typeName, typeTruncated := errorText(fmt.Sprintf("%T", err))
	node := map[string]any{diagnosticTypeKey: typeName}
	*remaining--
	if typeTruncated || depth >= maxErrorDepth {
		node["truncated"] = true
	}
	if depth >= maxErrorDepth {
		return node
	}
	var fields map[string]any
	for _, describe := range describers {
		if fields = describe(err); fields != nil {
			break
		}
	}
	if fields == nil {
		fields = errorFields(err)
	}
	appendErrorFields(node, fields)
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		var causes []map[string]any
		for _, cause := range wrapped.Unwrap() {
			if cause == nil {
				continue
			}
			if *remaining == 0 {
				node["truncated"] = true
				break
			}
			causes = append(causes, describeError(cause, depth+1, remaining, describers))
		}
		if len(causes) != 0 {
			node["causes"] = causes
		}
	case interface{ Unwrap() error }:
		if cause := wrapped.Unwrap(); cause != nil {
			if *remaining == 0 {
				node["truncated"] = true
			} else {
				node["cause"] = describeError(cause, depth+1, remaining, describers)
			}
		}
	}
	return node
}

func appendErrorFields(node, fields map[string]any) {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for index, key := range keys {
		if index >= maxErrorFields {
			node["truncated"] = true
			break
		}
		switch key {
		case diagnosticTypeKey, "cause", "causes", "truncated":
			continue
		}
		name, changed := errorText(key)
		if changed || name == "" {
			node["truncated"] = true
			continue
		}
		switch value := fields[key].(type) {
		case string:
			text, truncated := errorText(value)
			node[name] = text
			if truncated {
				node["truncated"] = true
			}
		case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			node[name] = value
		default:
			node["truncated"] = true
		}
	}
}

func errorFields(err error) map[string]any {
	switch value := err.(type) {
	case *FieldError:
		return map[string]any{diagnosticFieldKey: value.Field, diagnosticReasonKey: value.Reason}
	case *RegistrationError:
		return map[string]any{"source": diagnosticSource(value.Name), diagnosticOperationKey: value.Operation}
	case *StageError:
		stage := value.Stage
		switch stage {
		case StageDecode, StageMetadata, StageHandle, StageResolve, StageEncode, StagePublish:
		default:
			stage = "unknown"
		}
		return map[string]any{diagnosticStageKey: stage}
	case *DecodeError:
		return map[string]any{diagnosticStageKey: StageDecode}
	case *PanicError:
		return map[string]any{diagnosticStageKey: stagePanic, "value_type": fmt.Sprintf("%T", value.Value)}
	case *json.UnmarshalTypeError:
		fields := map[string]any{
			diagnosticFieldKey: value.Field, "struct": value.Struct,
			diagnosticOffsetKey: value.Offset, diagnosticReasonKey: "invalid_type",
		}
		if value.Type != nil {
			fields["expected_type"] = value.Type.String()
		}
		return fields
	case *json.SyntaxError:
		return map[string]any{diagnosticOffsetKey: value.Offset, diagnosticReasonKey: "invalid_json"}
	}
	return systemErrorFields(err)
}

func systemErrorFields(err error) map[string]any {
	switch value := err.(type) {
	case *net.OpError:
		return map[string]any{diagnosticOperationKey: value.Op, "network": value.Net}
	case *url.Error:
		return map[string]any{diagnosticOperationKey: value.Op}
	case *os.PathError:
		return map[string]any{diagnosticOperationKey: value.Op}
	case *os.SyscallError:
		return map[string]any{diagnosticOperationKey: value.Syscall}
	case syscall.Errno:
		return map[string]any{"errno": uint64(value), diagnosticMessageKey: value.Error()}
	case *tls.CertificateVerificationError:
		return map[string]any{diagnosticReasonKey: "certificate_verification_failed"}
	case x509.UnknownAuthorityError:
		return map[string]any{diagnosticReasonKey: "unknown_authority"}
	case x509.HostnameError:
		return map[string]any{diagnosticReasonKey: "hostname_mismatch"}
	case x509.CertificateInvalidError:
		return map[string]any{diagnosticReasonKey: "invalid_certificate", "code": int(value.Reason)}
	}
	switch err {
	case context.Canceled:
		return map[string]any{diagnosticReasonKey: "canceled", diagnosticMessageKey: err.Error()}
	case context.DeadlineExceeded:
		return map[string]any{diagnosticReasonKey: "deadline_exceeded", diagnosticMessageKey: err.Error()}
	case io.EOF:
		return map[string]any{diagnosticReasonKey: "eof", diagnosticMessageKey: err.Error()}
	case io.ErrUnexpectedEOF:
		return map[string]any{diagnosticReasonKey: "unexpected_eof", diagnosticMessageKey: err.Error()}
	}
	return nil
}

func errorText(value string) (string, bool) {
	var text strings.Builder
	truncated := false
	for offset, character := range value {
		if offset >= maxErrorText || text.Len()+utf8.RuneLen(character) > maxErrorText {
			truncated = true
			break
		}
		if !unicode.IsGraphic(character) || unicode.IsControl(character) {
			truncated = true
			continue
		}
		text.WriteRune(character)
	}
	return text.String(), truncated
}
