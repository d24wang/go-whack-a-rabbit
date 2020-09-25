package rmq

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// Publisher is responsible for create a queue and publish messages
type Publisher struct {
	ctx       context.Context
	queueURL  string
	exchange  string
	conn      *amqp.Connection
	chann     *amqp.Channel
	queueName string
	binded    bool
}

// NewPublisher creates a new Publisher instance
func NewPublisher(ctx context.Context, queueURL, exchange string) *Publisher {
	return &Publisher{
		ctx:      ctx,
		queueURL: queueURL,
		exchange: exchange,
	}
}

// AssertQueue will create a RMQ queue, and return the queue name
func (pub *Publisher) AssertQueue() (string, error) {
	randomID, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	prefix := "zzzzzzzz-bang"
	queueName := prefix + randomID.String()[len(prefix):]

	conn, err := amqp.Dial(pub.queueURL)
	if err != nil {
		return "", err
	}
	pub.conn = conn

	chann, err := conn.Channel()
	if err != nil {
		defer pub.Close()
		return "", err
	}
	pub.chann = chann

	if err = chann.Confirm(false); err != nil {
		defer pub.Close()
		return "", err
	}

	_, err = chann.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		defer pub.Close()
		return "", err
	}
	pub.queueName = queueName

	if err = chann.QueueBind(queueName, queueName, pub.exchange, false, nil); err != nil {
		defer pub.Close()
		return "", err
	}
	pub.binded = true

	return queueName, nil
}

// Publish publishes message with specified size and a given number of times
func (pub *Publisher) Publish(number, size int) (*PublishStats, error) {
	raw := []byte(strings.Repeat("s", size))
	confirmChan := pub.chann.NotifyPublish(make(chan amqp.Confirmation, 1))

	stats := CreatePublishStats(pub.queueName)
	stats.Start()

	defer func() {
		stats.Stop()
	}()

	for i := 0; i < number; i++ {
		message := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         raw,
			Type:         "Test",
			MessageId:    fmt.Sprintf("%v-%v", pub.queueName, i),
		}

		if err := pub.chann.Publish(pub.exchange, pub.queueName, false, false, message); err != nil {
			return stats, err
		}

		confirmation := <-confirmChan
		if !confirmation.Ack {
			log.Printf("%v-%v is not published", pub.queueName, i)
		}
		stats.IncrementMessage()
	}

	return stats, nil
}

// Close closes connection and channel
func (pub *Publisher) Close() {
	if pub.chann != nil {
		pub.chann.Close()
	}
	if pub.conn != nil {
		pub.conn.Close()
	}
}

// CloseAndCleanUp closes connection and channel and delete the queue
func (pub *Publisher) CloseAndCleanUp() {
	if pub.binded {
		pub.chann.QueueUnbind(pub.queueName, pub.queueName, pub.exchange, nil)
	}
	if pub.queueName != "" {
		pub.chann.QueueDelete(pub.queueName, false, false, false)
	}
	pub.Close()
}

// QueueName returns the queue name this publisher publishes to
func (pub *Publisher) QueueName() string {
	return pub.queueName
}
