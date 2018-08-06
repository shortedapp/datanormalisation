package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/datanormalization/internal/sharecodes"
	log "github.com/shortedapp/datanormalization/pkg/loggingutil"
	"github.com/shortedapp/datanormalization/pkg/scheduledServices"
)

func Handler(request events.CloudWatchEvent) {
	clients := scheduledget.GenerateAWSClients("dynamoDB", "s3")
	scheduledget.FetchCSVFileFromS3("shortedappjmk", "testCsv.csv", clients, sharecodes.UnmarshalSharesCSV)
	// clients := scheduledGet.GenerateAWSClients("dynamoDB", "s3")
	scheduledget.WithDynamoDBGetLatest("https://asic.gov.au/Reports/Daily/2018/07/RR20180726-001-SSDailyAggShortPos.csv", "test", clients)
}

func main() {
	log.SetAppName("ShortedApp")
	Handler(events.CloudWatchEvent{})
	lambda.Start(Handler)
}
