package rmq

import (
	"context"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

// Consumer is responsible for draining the messages from a given queue
type Consumer struct {
	ctx       context.Context
	queueURL  string
	conn      *amqp.Connection
	chann     *amqp.Channel
	queueName string
	asserted  bool
}

// CreateConsumer creates a consumer
func CreateConsumer(ctx context.Context, queueName, queueURL string) *Consumer {
	return &Consumer{
		ctx:       ctx,
		queueURL:  queueURL,
		queueName: queueName,
	}
}

// AssertQueue will connect to a RMQ queue by the given queue name
func (consumer *Consumer) AssertQueue() error {
	conn, err := amqp.Dial(consumer.queueURL)
	if err != nil {
		return err
	}
	consumer.conn = conn

	chann, err := conn.Channel()
	if err != nil {
		defer consumer.Close()
		return err
	}
	consumer.chann = chann

	if err = chann.Qos(4, 0, false); err != nil {
		defer consumer.Close()
		return err
	}

	_, err = chann.QueueDeclare(consumer.queueName, true, false, false, false, nil)
	if err != nil {
		defer consumer.Close()
		return err
	}
	consumer.asserted = true

	return nil
}

// Consume starts to consume at most n messages from the queue
func (consumer *Consumer) Consume(n int) (*ConsumeStats, error) {
	messageChan, err := consumer.chann.Consume(consumer.queueName, "", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	cancelCtx, cancel := context.WithCancel(consumer.ctx)
	defer cancel()

	stats := CreateConsumeStats(consumer.queueName)
	defer func() {
		stats.Stop()
	}()

	timer := createTimer(cancelCtx, time.Second*30)
	timeout := timer.start()
	stats.Start()

	var counter, delta int
	for {
		select {
		case <-timeout:
			if delta == 0 {
				return stats, fmt.Errorf("%v has been idle for at least 30 second", consumer.queueName)
			}
			delta = 0
		case <-consumer.ctx.Done():
			return stats, nil
		case <-messageChan:
			counter++
			delta++
			stats.IncrementMessage()
			if counter == n {
				return stats, nil
			}
		}
	}
}

// Close closes channel and connection
func (consumer *Consumer) Close() {
	if consumer.chann != nil {
		consumer.chann.Close()
	}
	if consumer.conn != nil {
		consumer.conn.Close()
	}
}

// CloseAndCleanUp closes channel and connection and delete queues
func (consumer *Consumer) CloseAndCleanUp() {
	if consumer.asserted {
		if _, err := consumer.chann.QueueDelete(consumer.queueName, false, false, true); err != nil {
			fmt.Printf("Error when deleting %s, error: %v/n", consumer.queueName, err)
		}
	}

	if consumer.chann != nil {
		consumer.chann.Close()
	}
	if consumer.conn != nil {
		consumer.conn.Close()
	}
}

// QueueName returns the queueName this consumer is consuming from
func (consumer *Consumer) QueueName() string {
	return consumer.queueName
}

type timer struct {
	ctx     context.Context
	signal  chan bool
	timeout time.Duration
}

func createTimer(ctx context.Context, timeout time.Duration) *timer {
	return &timer{
		ctx:     ctx,
		timeout: timeout,
		signal:  make(chan bool, 1),
	}
}

func (t *timer) start() <-chan bool {
	go func() {
		ticker := time.NewTicker(t.timeout)
		defer ticker.Stop()

		select {
		case <-t.ctx.Done():
		case <-ticker.C:
			t.signal <- true
		}
	}()

	return t.signal
}
