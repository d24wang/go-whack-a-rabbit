package runner

import (
	"context"
	"fmt"

	"github.com/d24wang/go-whack-a-rabbit/rmq"
)

// ConsumerRunner is responsible for launching and managing consumers
type ConsumerRunner struct {
	ctx           context.Context
	queueURL      string
	messageNumber int
	queueNames    []string
	consumers     []*rmq.Consumer
	cancel        context.CancelFunc
}

// CreateConsumerRunner creates a ConsumerRunner
func CreateConsumerRunner(ctx context.Context, queueURL string, messageNumber int, queueNames []string) *ConsumerRunner {
	return &ConsumerRunner{
		ctx:           ctx,
		queueURL:      queueURL,
		messageNumber: messageNumber,
		queueNames:    queueNames,
	}
}

// SetUpConsumers create consumers and connect to queues
func (cr *ConsumerRunner) SetUpConsumers() error {
	ctxCancel, cancel := context.WithCancel(cr.ctx)
	cr.cancel = cancel

	for _, queueName := range cr.queueNames {
		con := rmq.CreateConsumer(ctxCancel, queueName, cr.queueURL)
		if err := con.AssertQueue(); err != nil {
			defer cr.CloseAndCleanUp()
			return err
		}

		cr.consumers = append(cr.consumers, con)
	}

	return nil
}

// SetRate sets the pulling rate for all consumers
func (cr *ConsumerRunner) SetRate(rate int) {
	for _, con := range cr.consumers {
		con.SetRate(rate)
	}
}

// Run starts draining
func (cr *ConsumerRunner) Run() error {
	defer cr.cancel()

	if cr.messageNumber < 1 {
		return fmt.Errorf("Invalide number of messages is configured: %d", cr.messageNumber)
	}

	conChan := make(chan *rmq.ConsumeStats, len(cr.consumers))

	for _, con := range cr.consumers {
		go func(con *rmq.Consumer) {
			stats, err := con.Consume(cr.messageNumber)
			if err != nil {
				fmt.Printf("%s encountered error during publishing: %s", con.QueueName(), err.Error())
			}
			conChan <- stats
		}(con)
	}

	for i := 0; i < len(cr.consumers); i++ {
		fmt.Println(<-conChan)
	}
	close(conChan)
	return nil
}

// CloseAndCleanUp will close and clean all created queues
func (cr *ConsumerRunner) CloseAndCleanUp() {
	for _, con := range cr.consumers {
		con.CloseAndCleanUp()
	}
}
