package broker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/velmie/broker"
)

func TestDescribeErrorLogsAssociatedCausesWithoutSensitiveValues(t *testing.T) {
	primary := &json.UnmarshalTypeError{Field: "profile.status", Struct: "Profile", Type: reflect.TypeOf(0),
		Value: "sensitive-input", Offset: 17}
	cleanup := errors.New("cleanup-password")
	err := errors.Join(
		&broker.RegistrationError{Name: "profiles", Operation: "run", Cause: &broker.StageError{Stage: broker.StageDecode, Cause: primary}},
		&broker.RegistrationError{Name: "accounts", Operation: "cleanup", Cause: cleanup},
	)
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
		if cause, ok := attr.Value.Any().(error); ok {
			return slog.Any(attr.Key, broker.DescribeError(cause))
		}
		return attr
	}}))
	logger.Error("consumer failed", slog.Any("error", err))
	var record struct {
		Error struct {
			Causes []struct {
				Source    string
				Operation string
				Cause     struct {
					Type  string
					Stage string
					Cause struct {
						Field        string
						ExpectedType string `json:"expected_type"`
						Offset       int
					}
				}
			}
		}
	}
	if decodeErr := json.Unmarshal(output.Bytes(), &record); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	causes := record.Error.Causes
	if len(causes) != 2 || causes[0].Source != "profiles" || causes[0].Operation != "run" ||
		causes[0].Cause.Stage != "decode" || causes[0].Cause.Cause.Field != "profile.status" ||
		causes[0].Cause.Cause.ExpectedType != "int" || causes[0].Cause.Cause.Offset != 17 ||
		causes[1].Source != "accounts" || causes[1].Operation != "cleanup" || causes[1].Cause.Type != "*errors.errorString" {
		t.Fatalf("lost branch context: %s", output.String())
	}
	for _, secret := range []string{"sensitive-input", "cleanup-password"} {
		if strings.Contains(output.String(), secret) {
			t.Fatalf("sensitive value serialized: %s", output.String())
		}
	}
	var original *json.UnmarshalTypeError
	if !errors.Is(err, cleanup) || !errors.As(err, &original) || original != primary {
		t.Fatal("original causes changed")
	}
}

func TestDescribeErrorApplicationOverrideReceivesOriginalNodes(t *testing.T) {
	cause := errors.New("private account value")
	outer := &broker.FieldError{Field: "private-field", Reason: "invalid", Cause: cause}
	var seen []error
	got := broker.DescribeError(outer, func(err error) map[string]any {
		seen = append(seen, err)
		if err == outer {
			return map[string]any{"field": "account", "reason": "invalid", "type": "cannot replace type", "cause": "cannot replace cause"}
		}
		return nil
	}, func(err error) map[string]any {
		if err == cause {
			return map[string]any{"code": uint16(123), "message": "approved application detail"}
		}
		return nil
	})
	if !reflect.DeepEqual(seen, []error{outer, cause}) || got["field"] != "account" || got["type"] != "*broker.FieldError" {
		t.Fatalf("override context: %#v, seen %#v", got, seen)
	}
	if child := got["cause"].(map[string]any); child["code"] != uint16(123) || child["message"] != "approved application detail" {
		t.Fatalf("application details: %#v", child)
	}
	if got = broker.DescribeError(outer, func(error) map[string]any { return map[string]any{} }); len(got) != 2 {
		t.Fatalf("empty override must retain only type and cause: %#v", got)
	}
	if broker.DescribeError(nil) != nil {
		t.Fatal("nil error should produce nil")
	}
}

func TestDescribeErrorBoundsCyclesBranchesAndFields(t *testing.T) {
	cycle := &cyclicDiagnosticError{}
	if count, truncated := diagnosticNodeCount(broker.DescribeError(cycle)); count > 9 || !truncated {
		t.Fatalf("cycle not depth bounded: nodes=%d truncated=%v", count, truncated)
	}
	branches := make([]error, 100)
	for i := range branches {
		branches[i] = cycle
	}
	if count, truncated := diagnosticNodeCount(broker.DescribeError(errors.Join(branches...))); count > 32 || !truncated {
		t.Fatalf("branch budget exceeded: nodes=%d truncated=%v", count, truncated)
	}
	got := broker.DescribeError(cycle, func(error) map[string]any {
		fields := map[string]any{
			"a": strings.Repeat("я", 100), "b": "before\n\u202eafter", "c\n": "invalid key", "d": forbiddenDiagnosticValue{},
		}
		for i := 0; i < 30; i++ {
			fields[fmt.Sprintf("field%02d", i)] = i
		}
		return fields
	})
	if got["truncated"] != true || len(got) > 19 || !utf8.ValidString(got["a"].(string)) || len(got["a"].(string)) > 128 ||
		got["b"] != "beforeafter" || got["c\n"] != nil || got["d"] != nil {
		t.Fatalf("unbounded or unsafe projection: %#v", got)
	}
	if _, err := json.Marshal(got); err != nil {
		t.Fatalf("projection cannot be encoded: %v", err)
	}
}

func TestDescribeErrorPanicKeepsCauseIdentityWithoutValueOrStack(t *testing.T) {
	cause := errors.New("sensitive panic input")
	err := &broker.PanicError{Value: cause, Stack: []byte("private stack")}
	got := broker.DescribeError(err)
	if got["stage"] != "panic" || got["value_type"] != "*errors.errorString" ||
		got["cause"].(map[string]any)["type"] != "*errors.errorString" || !errors.Is(err, cause) {
		t.Fatalf("panic context lost: %#v", got)
	}
	encoded, marshalErr := json.Marshal(got)
	if marshalErr != nil || strings.Contains(string(encoded), "sensitive") || strings.Contains(string(encoded), "private") {
		t.Fatalf("unsafe panic data: %s, %v", encoded, marshalErr)
	}
}

func TestCoordinatorDiagnosticContextPreservesOriginalFailure(t *testing.T) {
	for _, operation := range []string{"factory", "validate", "run"} {
		t.Run(operation, func(t *testing.T) {
			cause := errors.New("native detail")
			coordinator := broker.NewCoordinator()
			factory := func() (broker.Consumer, error) {
				if operation == "factory" {
					return nil, cause
				}
				return consumer{validate: func() error {
					if operation == "validate" {
						return cause
					}
					return nil
				}, run: func(context.Context) error { return cause }}, nil
			}
			handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				t.Fatal("unexpected callback")
				return nil, nil
			})
			if err := coordinator.Add("profiles", factory, handler); err != nil {
				t.Fatal(err)
			}
			err := coordinator.Run(context.Background())
			var registration *broker.RegistrationError
			if !errors.Is(err, cause) || !errors.As(err, &registration) || registration.Operation != operation || registration.Name != "profiles" {
				t.Fatalf("lost original context: %v", err)
			}
			got := broker.DescribeError(registration)
			if got["source"] != "profiles" || got["operation"] != operation || got["cause"] == nil {
				t.Fatalf("registration cannot be diagnosed: %#v", got)
			}
		})
	}
}

type cyclicDiagnosticError struct{}

func (*cyclicDiagnosticError) Error() string     { panic("must not format arbitrary errors") }
func (err *cyclicDiagnosticError) Unwrap() error { return err }

type forbiddenDiagnosticValue struct{}

func (forbiddenDiagnosticValue) MarshalJSON() ([]byte, error) {
	panic("must not invoke custom marshaler")
}

func diagnosticNodeCount(node map[string]any) (int, bool) {
	count, truncated := 1, node["truncated"] == true
	if cause, ok := node["cause"].(map[string]any); ok {
		children, childTruncated := diagnosticNodeCount(cause)
		count += children
		truncated = truncated || childTruncated
	}
	if causes, ok := node["causes"].([]map[string]any); ok {
		for _, cause := range causes {
			children, childTruncated := diagnosticNodeCount(cause)
			count += children
			truncated = truncated || childTruncated
		}
	}
	return count, truncated
}
