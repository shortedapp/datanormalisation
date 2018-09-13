package topshortsingestor

import (
	"sort"
	"time"

	"github.com/shortedapp/shortedfunctions/internal/ingestionutils"
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
	data := resp.(sharedata.CombinedResultJSON)
	dataResult := data.Result

	sort.Slice(dataResult, func(i, j int) bool {
		return dataResult[i].Percent > dataResult[j].Percent
	})

	putRequest := make(chan *sharedata.TopShortJSON, len(dataResult))
	for i, short := range dataResult {
		shortIn := &sharedata.TopShortJSON{
			Position: int64(i),
			Code:     short.Code,
			Percent:  short.Percent,
		}
		putRequest <- shortIn
	}
	close(putRequest)

	//Update table capacity units
	_, writeThroughput := ingestionutils.UpdateDynamoWriteUnits(t.Clients, tableName, 5)

	//Define a burst capacity for putting into dynamoDb. Set to write throughput to avoid significant ThroughputExceededErrors
	burstChannel := make(chan *sharedata.TopShortJSON, writeThroughput)

	//Create 1 second rate limiter
	limiter := time.Tick(1000 * time.Millisecond)

	//Continue until no jobs are left
	for len(putRequest) > 0 {
		//fill burst capacity to max or until no jobs are left
		for len(burstChannel) < cap(burstChannel) && len(putRequest) > 0 {
			burstChannel <- <-putRequest
		}
		//Create multiple puts
		for len(burstChannel) > 0 {
			go t.putRecord(<-burstChannel, tableName)
		}
		<-limiter
	}

	//Update table capacity units
	ingestionutils.UpdateDynamoWriteUnits(t.Clients, tableName, 5)

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
