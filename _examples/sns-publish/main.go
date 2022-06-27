package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sts"

	"github.com/velmie/broker"
	snsBroker "github.com/velmie/broker/sns"
)

func main() {
	httpClient := http.Client{
		Timeout: 10 * time.Second,
	}

	awsSession, err := session.NewSession(&aws.Config{HTTPClient: &httpClient})
	if err != nil {
		fmt.Println("cannot create AWS session: ", err)
		os.Exit(1)
	}

	snsService := sns.New(awsSession)
	awsAccountID := getAWSAccountID(awsSession)

	snsPublisher := snsBroker.NewPublisher(snsService, "some_message_group", awsAccountID)

	err = snsPublisher.Publish("test-sns-1.fifo", &broker.Message{
		ID: fmt.Sprintf("my-unique-id-%d", time.Now().Unix()),
		Header: map[string]string{
			"something": "rational",
		},
		Body: []byte(`{"greeting":"Hello there!"}`),
	})

	if err != nil {
		fmt.Println("failed to publish new message: ", err)
		return
	}
	fmt.Println("message has been successfully sent")
}

func getAWSAccountID(awsSession *session.Session) string {
	stsService := sts.New(awsSession)
	out, err := stsService.GetCallerIdentity(&sts.GetCallerIdentityInput{})
	if err != nil {
		fmt.Println("cannot get AWS account id: ", err)
		os.Exit(1)
	}
	return *out.Account
}
