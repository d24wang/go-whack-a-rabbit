package rmq

import (
	"fmt"
	"time"
)

// ConsumeStats is a collection of publishing metrics
type ConsumeStats struct {
	queueName    string
	totalMessage int
	duration     time.Duration
	start        time.Time
	rate         float64
}

// CreateConsumeStats creates a ConsumeStats
func CreateConsumeStats(queueName string) *ConsumeStats {
	return &ConsumeStats{
		queueName: queueName,
	}
}

// IncrementMessage increments message count
func (cs *ConsumeStats) IncrementMessage() {
	cs.totalMessage++
}

// Start starts the timer
func (cs *ConsumeStats) Start() {
	cs.start = time.Now()
}

// Stop stops the timer and caculate the duration
func (cs *ConsumeStats) Stop() {
	cs.duration = time.Since(cs.start)
}

// String returns a short summary
func (cs *ConsumeStats) String() string {
	cs.rate = float64(cs.totalMessage) / cs.duration.Seconds()

	return fmt.Sprintf("Drained %d messages in total at a rate of %.2f/s in %v from %s", cs.totalMessage, cs.rate, cs.duration, cs.queueName)
}
