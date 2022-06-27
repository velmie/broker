package sqs

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"github.com/velmie/broker"
	mock_sqs "github.com/velmie/broker/sqs/mock"
)

type subscriberTestSuite struct {
	suite.Suite
	ctrl *gomock.Controller

	subscriber *Subscriber
	srv        *mock_sqs.MockService
}

func (s *subscriberTestSuite) TestHandleUnsubscribedMessages() {
	const (
		eventName   = "some-event-name"
		messageBody = "payload"
	)
	out := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			{
				Body: stringP(messageBody),
			},
			{
				Body: stringP(messageBody),
			},
		},
	}

	urlOut := &sqs.GetQueueUrlOutput{QueueUrl: stringP(eventName)}
	s.srv.EXPECT().GetQueueUrl(gomock.Any()).Return(urlOut, nil)
	s.srv.EXPECT().ReceiveMessage(gomock.Any()).Return(out, nil)

	errHandler := broker.ErrorHandler(func(_ error, sub broker.Subscription) {
		select {
		case <-sub.Done():
			s.FailNow("Error Handler must be called only one time, because subscription was canceled")
		default:
		}
		s.Require().NoError(sub.Unsubscribe())
	})
	alwaysFailHandler := func(event broker.Event) error {
		return errors.New("something went wrong")
	}

	sub, err := s.subscriber.Subscribe(eventName, alwaysFailHandler, broker.WithErrorHandler(errHandler))
	s.Require().NoError(err)

	<-sub.Done()
	time.Sleep(20 * time.Millisecond)
}

func (s *subscriberTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.srv = mock_sqs.NewMockService(s.ctrl)
	s.subscriber = NewSubscriber(s.srv)
}

func (s *subscriberTestSuite) TearDownTest() {
	s.ctrl.Finish()
}

func TestSubscriberTestSuite(t *testing.T) {
	suite.Run(t, new(subscriberTestSuite))
}

func stringP(v string) *string {
	return &v
}
