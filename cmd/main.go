package main

import (
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/datanormalization/internal/sharecodes"
	"github.com/shortedapp/datanormalization/pkg/scheduledServices"
)

func Handler(request events.CloudWatchEvent) {
	clients := scheduledget.GenerateAWSClients("dynamoDB", "s3")
	stockCodes, err := scheduledget.FetchMapFileFromS3("shortedappjmk", "testJson.json", clients, sharecodes.UnmarshalShares)
	fmt.Println(stockCodes.([]*sharecodes.Share)[0], err)
	// clients := scheduledGet.GenerateAWSClients("dynamoDB", "s3")
	scheduledget.WithDynamoDBGetLatest("https://asic.gov.au/Reports/Daily/2018/07/RR20180726-001-SSDailyAggShortPos.csv", "test", clients)
}

func main() {
	Handler(events.CloudWatchEvent{})
	lambda.Start(Handler)
}
