package topshortsingestor

import (
	"sort"
	"time"

	"github.com/shortedapp/shortedfunctions/internal/sharedata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Topshortslist - struct to enable testing
type Topshortsingestor struct {
	Clients awsutil.AwsUtiler
}

//IngestTopShorted - Reads the latest
func (t *Topshortsingestor) IngestTopShorted(tableName string) {

	currentTime := time.Now()
	currentDay := currentTime.Format("20060102")
	resp, err := t.Clients.FetchJSONFileFromS3("shortedappjmk", "testShortedData/"+currentDay+".json", sharedata.UnmarshalCombinedResultJSON)
	if err != nil {
		log.Info("IngestRoutine", "unable to fetch data from s3")
		return
	}

	t.Clients.WriteToDynamoDB(tableName, resp, TopShortJSONMapper, 0)

}

//Function To map topshort object to dynamo row
func TopShortJSONMapper(resp interface{}, date int) []*map[string]interface{} {
	//TODO uplift this to take a slice of additional input data
	data := resp.(sharedata.CombinedResultJSON)
	dataResult := data.Result

	sort.Slice(dataResult, func(i, j int) bool {
		return dataResult[i].Percent > dataResult[j].Percent
	})

	result := make([]*map[string]interface{}, 0, len(dataResult))
	for i, data := range dataResult {
		attributes := make(map[string]interface{}, 3)
		attributes["Position"] = int64(i)
		attributes["Code"] = data.Code
		attributes["Percent"] = data.Percent
		result = append(result, &attributes)
	}
	return result
}

func (t *Topshortsingestor) putRecord(short *sharedata.TopShortJSON, tableName string) {
	attributes := make(map[string]interface{}, 6)
	attributes["Position"] = short.Position
	attributes["Code"] = short.Code
	attributes["Percent"] = short.Percent

	err := t.Clients.PutDynamoDBItems(tableName, attributes)
	if err != nil {
		log.Info("putRecord", err.Error())
	}
}
