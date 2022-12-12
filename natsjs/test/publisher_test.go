package natsjs_test

import (
	"fmt"
	"github.com/velmie/broker"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/velmie/broker/natsjs"
)

func TestPublisherParams(t *testing.T) {
	_, err := natsjs.NewPublisher("PROJECT", "", natsjs.DefaultConnectionFactory())

	if errors.Cause(err) != natsjs.ErrSubjectPrefix {
		t.Fatalf("Did not get the proper error, got %v", err)
	}

	_, err = natsjs.NewPublisher("", "SERVICE1", natsjs.DefaultConnectionFactory())

	if errors.Cause(err) != natsjs.ErrStreamName {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
}

func TestPublisherSendMessage(t *testing.T) {
	var (
		stream     = "project"
		subjPrefix = "service"
		subject    = "test.message"
	)

	jsp, err := natsjs.NewPublisher(stream, subjPrefix, natsjs.DefaultConnectionFactory())
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err == nil || !strings.Contains(err.Error(), "cannot send message") {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
}
