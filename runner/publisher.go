package runner

import (
	"context"
	"fmt"

	"github.com/d24wang/go-whack-a-rabbit/rmq"
)

// PublisherRunner is responsible for launching and managing publishers
type PublisherRunner struct {
	ctx           context.Context
	queueURL      string
	exchange      string
	messageNumber int
	messageSize   int
	totalRunners  int
	publishers    []*rmq.Publisher
}

// CreatePublisherRunner created a PublisherRunner
func CreatePublisherRunner(ctx context.Context, queueURL, exchange string, messageNumber, messageSize, totalRunners int) *PublisherRunner {
	return &PublisherRunner{
		ctx:           ctx,
		queueURL:      queueURL,
		exchange:      exchange,
		messageNumber: messageNumber,
		messageSize:   messageSize,
		totalRunners:  totalRunners,
	}
}

// SetUpPublishers create publishers and setup queues
func (pr *PublisherRunner) SetUpPublishers() ([]string, error) {
	if pr.totalRunners < 1 {
		return nil, fmt.Errorf("Invalide number of runners is configured: %d", pr.totalRunners)
	}

	var queueNames []string
	for i := 0; i < pr.totalRunners; i++ {
		pub := rmq.NewPublisher(pr.ctx, pr.queueURL, pr.exchange)

		queueName, err := pub.AssertQueue()
		if err != nil {
			defer pr.CloseAndCleanUp()
			return nil, err
		}

		queueNames = append(queueNames, queueName)
		pr.publishers = append(pr.publishers, pub)
	}

	return queueNames, nil
}

// Run will start all publishers to publisher messages
func (pr *PublisherRunner) Run() error {
	if pr.messageNumber < 1 {
		return fmt.Errorf("Invalide number of messages is configured: %d", pr.messageNumber)
	}

	if pr.messageSize < 1 {
		return fmt.Errorf("Invalide message size is configured: %d", pr.messageSize)
	}

	pubChan := make(chan *rmq.PublishStats, len(pr.publishers))
	for _, pub := range pr.publishers {
		go func(pub *rmq.Publisher) {
			stats, err := pub.Publish(pr.messageNumber, pr.messageSize)
			if err != nil {
				fmt.Printf("%s encountered error during publishing: %s", pub.QueueName(), err.Error())
			}
			pubChan <- stats
		}(pub)
	}

	for i := 0; i < len(pr.publishers); i++ {
		fmt.Println(<-pubChan)
	}
	close(pubChan)

	return nil
}

// CloseAndCleanUp will close and clean all created publishers and queues
func (pr *PublisherRunner) CloseAndCleanUp() {
	for _, pub := range pr.publishers {
		pub.CloseAndCleanUp()
	}
}

// Close will close all created publishers, created queues are not touched
func (pr *PublisherRunner) Close() {
	for _, pub := range pr.publishers {
		pub.Close()
	}
}
