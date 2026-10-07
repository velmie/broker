package broker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/velmie/broker"
)

func TestReplyMetadataConventionsValidateBeforeEffects(t *testing.T) {
	for _, headers := range [][]broker.Header{
		nil,
		{{Name: "Reply-To", Value: []byte("unapproved")}},
		{{Name: "Reply-To", Value: []byte("orders.results")}, {Name: "Reply-To", Value: []byte("orders.results")}},
		{{Name: "reply-to", Value: []byte("orders.results")}},
		{{Name: "Reply-To", Value: []byte("orders.results")}, {Name: "Correlation-Id", Value: []byte{0xff}}},
		{{Name: "Reply-To", Value: []byte("orders.results")}, {Name: "Created-At", Value: []byte("9223372036854775808")}},
	} {
		effects, published := 0, 0
		pub := broker.PublisherFunc(func(context.Context, broker.Message) error { published++; return nil })
		handler := broker.NewReplyHandler(broker.DecodeJSON[struct{}], broker.EncoderFunc(json.Marshal), replyMetadataResolver(pub),
			func(context.Context, struct{}) (struct{}, error) { effects++; return struct{}{}, nil })
		_, err := handler.Handle(context.Background(), replyDelivery{message: broker.Message{
			ID: "request-1", Body: []byte(`{}`), Headers: headers}})
		var stage *broker.StageError
		if !errors.As(err, &stage) || stage.Stage != broker.StageResolve || effects != 0 || published != 0 {
			t.Fatalf("invalid metadata reached effects: %v", err)
		}
	}
}

func TestReplyMetadataRetainsIndependentIdentitiesWithoutMutation(t *testing.T) {
	message := broker.Message{ID: "request-1", Headers: []broker.Header{
		{Name: "Reply-To", Value: []byte("orders.results")}, {Name: "Correlation-Id", Value: []byte("trace-7")},
		{Name: "Created-At", Value: []byte("-1")}, {Name: "Opaque", Value: []byte{0xff}}, {Name: "Opaque", Value: []byte{0x00}},
	}}
	before := message
	before.Headers = make([]broker.Header, len(message.Headers))
	for i, header := range message.Headers {
		before.Headers[i] = broker.Header{Name: header.Name, Value: bytes.Clone(header.Value)}
	}
	resolver := replyMetadataResolver(broker.PublisherFunc(func(context.Context, broker.Message) error { return nil }))
	first, err := resolver(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolver(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	want := broker.MessageMetadata{ID: "request-1.reply", Headers: []broker.Header{
		{Name: "Reply-Message-Id", Value: []byte("request-1")}, {Name: "Correlation-Id", Value: []byte("trace-7")},
	}}
	if !reflect.DeepEqual(first.Metadata, want) || !reflect.DeepEqual(first.Metadata, second.Metadata) {
		t.Fatalf("reply metadata: %+v", first.Metadata)
	}
	if !reflect.DeepEqual(message, before) {
		t.Fatal("resolver mutated ordered input bytes")
	}
}

func TestCreatedAtSecondsPreservesPresenceAndParseCause(t *testing.T) {
	for _, test := range []struct {
		value      string
		seconds    int64
		parseError error
	}{
		{"0", 0, nil}, {"-1", -1, nil}, {"+1", 1, nil}, {"bad", 0, strconv.ErrSyntax}, {"", 0, strconv.ErrSyntax},
		{"9223372036854775808", 0, strconv.ErrRange},
	} {
		got, found, err := createdAtSeconds([]broker.Header{{Name: "Created-At", Value: []byte(test.value)}})
		if !found {
			t.Fatal("present timestamp reported absent")
		}
		if test.parseError == nil {
			if err != nil || got != test.seconds {
				t.Fatalf("timestamp %q: %d %v", test.value, got, err)
			}
		} else {
			var cause *strconv.NumError
			if !errors.As(err, &cause) || !errors.Is(err, test.parseError) {
				t.Fatalf("original parse error lost: %v", err)
			}
		}
	}
	if seconds, found, err := createdAtSeconds(nil); seconds != 0 || found || err != nil {
		t.Fatal("absent timestamp manufactured")
	}
}

func ExampleNewReplyHandler_metadataMigration() {
	// This publisher is already bound to the one destination approved by the resolver.
	results := broker.PublisherFunc(func(_ context.Context, response broker.Message) error {
		requestID, _, readErr := textConvention(response.Headers, "Reply-Message-Id")
		if readErr != nil {
			return readErr
		}
		correlationID, _, readErr := textConvention(response.Headers, "Correlation-Id")
		if readErr != nil {
			return readErr
		}
		fmt.Printf("reply=%s request=%s correlation=%s\n", response.ID, requestID, correlationID)
		return nil
	})
	handler := broker.NewReplyHandler(broker.DecodeJSON[struct{}], broker.EncoderFunc(json.Marshal), replyMetadataResolver(results),
		func(context.Context, struct{}) (struct{}, error) { return struct{}{}, nil })
	request := broker.Message{ID: "request-1", Body: []byte(`{}`), Headers: []broker.Header{
		{Name: "Reply-To", Value: []byte("orders.results")},
		{Name: "Correlation-Id", Value: []byte("trace-7")},
		// Timestamp creation is an explicit application choice, including the Unix epoch.
		{Name: "Created-At", Value: []byte(strconv.FormatInt(time.Unix(0, 0).Unix(), 10))},
	}}
	if _, err := handler.Handle(context.Background(), replyDelivery{message: request}); err != nil {
		panic(err)
	}
	seconds, present, err := createdAtSeconds(request.Headers)
	if err != nil {
		panic(err)
	}
	fmt.Printf("created_at=%d present=%t\n", seconds, present)
	// Output:
	// reply=request-1.reply request=request-1 correlation=trace-7
	// created_at=0 present=true
}

// replyMetadataResolver defines this application's conventions. It does not
// normalize unrelated headers or turn arbitrary Reply-To values into destinations.
func replyMetadataResolver(results broker.Publisher) broker.ReplyResolver {
	return func(_ context.Context, request broker.Message) (broker.ReplyTarget, error) {
		route, found, err := textConvention(request.Headers, "Reply-To")
		if err != nil {
			return broker.ReplyTarget{}, err
		}
		if !found || route != "orders.results" {
			return broker.ReplyTarget{}, errors.New("Reply-To: approved route required")
		}
		if request.ID == "" || !utf8.ValidString(request.ID) {
			return broker.ReplyTarget{}, errors.New("ID: nonempty UTF-8 value required")
		}
		correlation, _, err := textConvention(request.Headers, "Correlation-Id")
		if err != nil {
			return broker.ReplyTarget{}, err
		}
		if _, _, err = createdAtSeconds(request.Headers); err != nil {
			return broker.ReplyTarget{}, err
		}
		headers := []broker.Header{{Name: "Reply-Message-Id", Value: []byte(request.ID)}}
		if correlation != "" {
			headers = append(headers, broker.Header{Name: "Correlation-Id", Value: []byte(correlation)})
		}
		return broker.ReplyTarget{Publisher: results, Metadata: broker.MessageMetadata{ID: request.ID + ".reply", Headers: headers}}, nil
	}
}

func createdAtSeconds(headers []broker.Header) (seconds int64, found bool, err error) {
	value, found, err := textConvention(headers, "Created-At")
	if err != nil || !found {
		return 0, found, err
	}
	seconds, err = strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, true, fmt.Errorf("Created-At: %w", err)
	}
	return seconds, true, nil
}

// Only the selected text convention is validated. Ordered byte headers outside
// that convention retain their original meaning, duplicates and storage.
func textConvention(headers []broker.Header, name string) (value string, found bool, err error) {
	for _, header := range headers {
		if !strings.EqualFold(header.Name, name) {
			continue
		}
		if header.Name != name {
			return "", true, fmt.Errorf("%s: case alias is not allowed", name)
		}
		if found {
			return "", true, fmt.Errorf("%s: duplicate is not allowed", name)
		}
		if !utf8.Valid(header.Value) {
			return "", true, fmt.Errorf("%s: invalid UTF-8", name)
		}
		value, found = string(header.Value), true
	}
	return value, found, nil
}
