package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/datanormalization/pkg/scheduledServices"
)

func Handler(request events.CloudWatchEvent) {
	clients := scheduledGet.GenerateAWSClients("dynamoDB", "s3")
	scheduledGet.WithDynamoDBGetLatest("https://asic.gov.au/Reports/Daily/2018/07/RR20180726-001-SSDailyAggShortPos.csv", "test", clients)
}

func main() {
	//clients := scheduledGet.GenerateAWSClients("dynamoDB", "s3")
	// scheduledGet.WithDynamoDBGetLatest("https://asic.gov.au/Reports/Daily/2018/07/RR20180726-001-SSDailyAggShortPos.csv", "test", clients)
	lambda.Start(Handler)
}
