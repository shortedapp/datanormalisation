package dynamoingestor

import (
	"time"

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

	readUnits, writeUnits := d.Clients.GetDynamoDBTableThroughput(tableName)
	err = d.Clients.UpdateDynamoDBTableCapacity(tableName, readUnits, 25)
	if err != nil {
		log.Warn("IngestRoutine", "unable to update write capacity units")
	}

	_, writeThroughput := d.Clients.GetDynamoDBTableThroughput(tableName)
	data := resp.([]*sharedata.CombinedShortJSON)

	//Create a list of data to put into dynamo db
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

	err = d.Clients.UpdateDynamoDBTableCapacity(tableName, readUnits, writeUnits)
	if err != nil {
		log.Warn("IngestRoutine", "unable to update write capacity units")
	}

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
