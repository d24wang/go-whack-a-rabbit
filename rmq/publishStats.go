package rmq

import (
	"fmt"
	"time"
)

// PublishStats is a collection of publishing metrics
type PublishStats struct {
	queueName    string
	totalMessage int
	duration     time.Duration
	start        time.Time
	rate         float64
}

// CreatePublishStats creates a PublishStats
func CreatePublishStats(queueName string) *PublishStats {
	return &PublishStats{
		queueName: queueName,
	}
}

// IncrementMessage increments message count
func (ps *PublishStats) IncrementMessage() {
	ps.totalMessage++
}

// Start starts the timer
func (ps *PublishStats) Start() {
	ps.start = time.Now()
}

// Stop stops the timer and caculate the duration
func (ps *PublishStats) Stop() {
	ps.duration = time.Since(ps.start)
}

// String returns a short summary
func (ps *PublishStats) String() string {
	ps.rate = float64(ps.totalMessage) / ps.duration.Seconds()

	return fmt.Sprintf("Published %d messages in total at a rate of %.2f/s in %v to %s", ps.totalMessage, ps.rate, ps.duration, ps.queueName)
}
