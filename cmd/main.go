package main

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/datanormalization/internal/sharecodes"
	log "github.com/shortedapp/datanormalization/pkg/loggingutil"
	"github.com/shortedapp/datanormalization/pkg/scheduledServices"
)

func Handler(request events.CloudWatchEvent) {
	clients := scheduledget.GenerateAWSClients("dynamoDB", "s3", "kinesis")
	// scheduledget.FetchCSVFileFromS3("shortedappjmk", "testCsv.csv", clients, sharecodes.UnmarshalSharesCSV)
	// clients := scheduledGet.GenerateAWSClients("dynamoDB", "s3")
	// scheduledget.WithDynamoDBGetLatest("https://asic.gov.au/Reports/Daily/2018/07/RR20180726-001-SSDailyAggShortPos.csv", "test", clients)
	// stream := "shorted"
	s1 := make([]interface{}, 0, 1)
	update := sharecodes.Short{
		Name:  "TES",
		Value: 123.12,
	}

	s1 = append(s1, update)
	res, err := json.Marshal(s1)
	// err := scheduledget.PutKinesisRecords(&stream, s1, []string{"tes"}, clients)
	err = scheduledget.PutFileToS3("shortedappjmk", "newTest.json", clients, res)
	log.Warn("Handler", fmt.Sprintf("%v", err))
}

func main() {
	log.SetAppName("ShortedApp")
	Handler(events.CloudWatchEvent{})
	lambda.Start(Handler)
}
