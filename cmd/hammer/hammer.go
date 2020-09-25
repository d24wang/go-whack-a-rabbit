package main

import (
	"context"
	"fmt"

	"github.com/d24wang/go-whack-a-rabbit/runner"
)

func main() {
	ctx := context.Background()
	queueURL := "***"
	pubRunner := runner.CreatePublisherRunner(ctx, queueURL, "***", 1000, 100, 2)

	queueNames, err := pubRunner.SetUpPublishers()
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("Queues:")

	for _, queueName := range queueNames {
		fmt.Println(queueName)
	}

	if err = pubRunner.Run(); err != nil {
		panic(err.Error())
	}

	pubRunner.Close()

	fmt.Println("Done hammering")
}
