package dynamoingestor

import (
	"fmt"
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
func (d *Dynamoingestor) IngestRoutine(tableName string) error {
	currentTime := time.Now()
	currentDay := currentTime.Format("20060102")
	timeVal, err := strconv.Atoi(currentDay)
	if err != nil {
		log.Error("IngestRoutine", "failed to create int from date")
		return err
	}
	resp, err := d.Clients.FetchJSONFileFromS3("shortedappjmk", "testShortedData/"+currentDay+".json", sharedata.UnmarshalCombinedResultJSON)
	if err != nil {
		log.Info("IngestRoutine", "unable to fetch data from s3")
		return err
	}

	return d.Clients.WriteToDynamoDB(tableName, resp, CombinedShortJSONMapper, timeVal)
}

//Function To map combinedshorts object to dynamo row
func CombinedShortJSONMapper(resp interface{}, date int) ([]*map[string]interface{}, error) {
	//TODO uplift this to take a slice of additional input data
	dataSet, ok := resp.(sharedata.CombinedResultJSON)
	if !ok {
		return nil, fmt.Errorf("unable to cast to CombinedResultJSON")
	}
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
	return result, nil
}
