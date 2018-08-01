package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/datanormalization/pkg/scheduledServices"
)

func Handler(request events.CloudWatchEvent) {
	scheduledGet.ScheduledGetWithDynamoDB("https://asic.gov.au/Reports/Daily/2018/07/RR20180726-001-SSDailyAggShortPos.csv")
}

func main() {
	lambda.Start(Handler)
}
