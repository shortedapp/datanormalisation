package dynamoingestor

import (
	"strconv"
	"time"

	"github.com/shortedapp/shortedfunctions/internal/sharedata"

	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Dynamoingestor - struct to enable testing
type Dynamoingestor struct {
	Clients awsutil.AwsUtiler
}

// IngestRoutine - function to ingest data into DynamoDB
func (d *Dynamoingestor) IngestRoutine(tableName string) {
	currentTime := time.Now()
	currentDay := currentTime.Format("20060102")
	timeVal, err := strconv.Atoi(currentDay)
	if err != nil {
		log.Error("IngestRoutine", "failed to create int from date")
		return
	}
	resp, err := d.Clients.FetchJSONFileFromS3("shortedappjmk", "testShortedData/"+currentDay+".json", sharedata.UnmarshalCombinedResultJSON)
	if err != nil {
		log.Info("IngestRoutine", "unable to fetch data from s3")
		return
	}

	d.Clients.WriteToDynamoDB("testShorts", resp, CombinedShortJSONMapper, timeVal)
}

// //WriteToDynamoDB - write rows to dynamo and lift base write units to a higher value for ingestion
// func WriteToDynamoDB(Clients awsutil.AwsUtiler, tableName string, data interface{},
// 	mapper func(resp interface{}, date int) []*map[string]interface{}, date int) {
// 	//Update table capacity units
// 	_, writeThroughput := ingestionutils.UpdateDynamoWriteUnits(Clients, tableName, 25)

// 	//Create a list of data to put into dynamo db
// 	dataMapped := mapper(data, date)
// 	putRequest := make(chan *map[string]interface{}, len(dataMapped))
// 	for _, val := range dataMapped {
// 		putRequest <- val
// 	}
// 	close(putRequest)

// 	//Define a burst capacity for putting into dynamoDb. Set to write throughput to avoid significant ThroughputExceededErrors
// 	burstChannel := make(chan *map[string]interface{}, writeThroughput)

// 	//Create 1 second rate limiter
// 	limiter := time.Tick(1000 * time.Millisecond)

// 	//Continue until no jobs are left
// 	for len(putRequest) > 0 {
// 		//fill burst capacity to max or until no jobs are left
// 		for len(burstChannel) < cap(burstChannel) && len(putRequest) > 0 {
// 			burstChannel <- <-putRequest
// 		}
// 		//Create multiple puts
// 		for len(burstChannel) > 0 {
// 			go putRecord(Clients, <-burstChannel, tableName)
// 		}
// 		<-limiter
// 	}

// 	//Update table capacity units
// 	ingestionutils.UpdateDynamoWriteUnits(Clients, tableName, 5)
// }

//Function To map combinedshorts object to dynamo row
func CombinedShortJSONMapper(resp interface{}, date int) []*map[string]interface{} {
	//TODO uplift this to take a slice of additional input data
	dataSet := resp.(sharedata.CombinedResultJSON)
	result := make([]*map[string]interface{}, 0, len(dataSet.Result))
	for _, data := range dataSet.Result {
		attributes := make(map[string]interface{}, 6)
		attributes["Name"] = data.Name
		attributes["Code"] = data.Code
		attributes["Shorts"] = data.Shorts
		attributes["Total"] = data.Total
		attributes["Percent"] = data.Percent
		attributes["Industry"] = data.Industry
		attributes["Date"] = date
		result = append(result, &attributes)
	}
	return result
}

// func putRecord(clients awsutil.AwsUtiler, data *map[string]interface{}, table string) {
// 	err := clients.PutDynamoDBItems(table, *data)
// 	if err != nil {
// 		log.Info("putRecord", err.Error())
// 	}
// }
