package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/d24wang/go-whack-a-rabbit/aws"
)

func main() {
	lambda.Start(aws.Handle)
}
