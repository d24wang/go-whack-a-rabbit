package main

import (
	"context"
	"fmt"
	"os"

	"github.com/d24wang/go-whack-a-rabbit/runner"
)

func main() {
	queueNames := os.Args[1:]
	if len(queueNames) < 1 {
		panic("Need at least one queue to start with")
	}

	queueURL := "***"
	cr := runner.CreateConsumerRunner(context.Background(), queueURL, 1000, queueNames)
	defer cr.CloseAndCleanUp()

	if err := cr.SetUpConsumers(); err != nil {
		panic(err)
	}

	fmt.Println("Finish setting up consumers")

	if err := cr.Run(); err != nil {
		panic(err)
	}
}
