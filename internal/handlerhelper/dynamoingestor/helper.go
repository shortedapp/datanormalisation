package dynamoingestor

import (
	"time"

	"github.com/shortedapp/shortedfunctions/internal/ingestionutils"
	"github.com/shortedapp/shortedfunctions/internal/sharedata"

	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Dynamoingestor - struct to enable testing
type Dynamoingestor struct {
	Clients awsutils.AwsUtiler
}

// IngestRoutine - function to ingest data into DynamoDB
func (d *Dynamoingestor) IngestRoutine(tableName string) {
	resp, err := d.Clients.FetchJSONFileFromS3("shortedappjmk", "combinedshorts.json", sharedata.UnmarshalCombinedShortsJSON)
	if err != nil {
		log.Info("IngestRoutine", "unable to fetch data from s3")
		return
	}

	//Update table capacity units
	_, writeThroughput := ingestionutils.UpdateDynamoWriteUnits(d.Clients, tableName, 25)

	//Create a list of data to put into dynamo db
	data := resp.([]*sharedata.CombinedShortJSON)
	putRequest := make(chan *sharedata.CombinedShortJSON, len(data))
	for _, short := range data {
		putRequest <- short
	}
	close(putRequest)

	//Define a burst capacity for putting into dynamoDb. Set to write throughput to avoid significant ThroughputExceededErrors
	burstChannel := make(chan *sharedata.CombinedShortJSON, writeThroughput)

	//Create 1 second rate limiter
	limiter := time.Tick(1000 * time.Millisecond)

	timeVal := time.Now().UTC().UnixNano()
	//Continue until no jobs are left
	for len(putRequest) > 0 {
		//fill burst capacity to max or until no jobs are left
		for len(burstChannel) < cap(burstChannel) && len(putRequest) > 0 {
			burstChannel <- <-putRequest
		}
		//Create multiple puts
		for len(burstChannel) > 0 {
			go d.putRecord(<-burstChannel, timeVal)
		}
		<-limiter
	}

	//Update table capacity units
	ingestionutils.UpdateDynamoWriteUnits(d.Clients, tableName, 5)

}

func (d *Dynamoingestor) putRecord(data *sharedata.CombinedShortJSON, unixTime int64) {
	attributes := make(map[string]interface{}, 6)
	attributes["Name"] = data.Name
	attributes["Code"] = data.Code
	attributes["Shorts"] = data.Shorts
	attributes["Total"] = data.Total
	attributes["Percent"] = data.Percent
	attributes["Industry"] = data.Industry
	attributes["Date"] = unixTime

	err := d.Clients.PutDynamoDBItems("testShorts", attributes)
	if err != nil {
		log.Info("putRecord", err.Error())
	}
}
