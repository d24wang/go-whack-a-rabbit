package aws

import (
	"context"
	"fmt"

	"github.com/d24wang/go-whack-a-rabbit/rmq"
)

// LambdaEvent is the event consumed by the lambda
type LambdaEvent struct {
	QueueURL      string `json:"queueURL"`
	Exchange      string `json:"exchange"`
	MessageNumber int    `json:"messageNumber"`
	MessageSize   int    `json:"messageSize"`
	QueueName     string `json:"queueName,omitempty"`
	Summary       string `json:"summary,omitempty"`
	NonFatalError string `json:"nonFatalError,omitempty"`
}

// Handle implements the standard AWS lambda function interface
func Handle(ctx context.Context, evt LambdaEvent) (LambdaEvent, error) {
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	pub := rmq.NewPublisher(cancelCtx, evt.QueueURL, evt.Exchange)
	queueName, err := pub.AssertQueue()
	if err != nil {
		return evt, err
	}
	defer pub.Close()
	evt.QueueName = queueName

	if evt.MessageNumber < 1 {
		return evt, fmt.Errorf("Invalide number of messages is configured: %d", evt.MessageNumber)
	}

	if evt.MessageSize < 1 {
		return evt, fmt.Errorf("Invalide message size is configured: %d", evt.MessageSize)
	}

	stats, err := pub.Publish(evt.MessageNumber, evt.MessageSize)
	if stats != nil {
		evt.Summary = stats.String()
	}
	if err != nil {
		evt.NonFatalError = fmt.Sprintf("%s encountered error during publishing: %s", pub.QueueName(), err.Error())
	}

	return evt, nil
}
