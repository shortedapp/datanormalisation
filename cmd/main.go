package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/datanormalization/internal/sharedata"
	"github.com/shortedapp/datanormalization/pkg/awsutils"
	log "github.com/shortedapp/datanormalization/pkg/loggingutil"
)

//GetShareCodes - Get Short position data from ASIC
// inputs:
//	- clients: a pointer to the pregenerated AWS clients
//	- shortsReady: a channel to place the goroutine result
func GetShareCodes(clients *awsutils.ClientsStruct, codesReady chan<- map[string]*sharedata.ShareCsv) {
	resp, err := clients.FetchCSVFileFromS3("shortedappjmk", "ASXListedCompanies.csv", sharedata.UnmarshalSharesCSV)
	if err != nil {

	}
	result := resp.([]*sharedata.ShareCsv)
	resultMap := make(map[string]*sharedata.ShareCsv)
	for _, record := range result {
		resultMap[record.Code] = record
	}

	codesReady <- resultMap
}

//GetShortPositions - Get Short position data from ASIC
// inputs:
//	- clients: a pointer to the pregenerated AWS clients
//	- shortsReady: a channel to place the goroutine result
func GetShortPositions(clients *awsutils.ClientsStruct, shortsReady chan<- map[string]*sharedata.AsicShortCsv) {
	resp, err := clients.WithDynamoDBGetLatest("https://asic.gov.au/Reports/Daily/2018/07/RR20180726-001-SSDailyAggShortPos.csv", "test")
	if resp == nil && err == nil {
		//No Update required
		shortsReady <- nil
		return
	}
	b, err := ioutil.ReadAll(resp.Body)
	b = bytes.Replace(b, []byte("\x00"), []byte(""), -1)

	if err != nil {
		fmt.Println(err)
	}
	asicShorts, err := sharedata.UnmarshalAsicShortsCSV(b)

	resultMap := make(map[string]*sharedata.AsicShortCsv)
	for _, short := range asicShorts {
		resultMap[short.Code] = short
	}

	shortsReady <- resultMap
}

//MergeShortData - Merges data from ASIC and ASX
// inputs:
//	- shortsReady: channel to signal shorts data has been retrieved and processed
//	- codesReady: channel to signal codes data has been retrieved and processed
// Output:
//	- Array of CombinedShortJSON pointers
func MergeShortData(shortsReady <-chan map[string]*sharedata.AsicShortCsv, codesReady <-chan map[string]*sharedata.ShareCsv) []*sharedata.CombinedShortJSON {
	shorts := <-shortsReady
	if shorts == nil {
		log.Info("MergeShortData", "No updated short data to merge")
		return nil
	}
	codes := <-codesReady
	result := make([]*sharedata.CombinedShortJSON, 0, len(codes))
	for _, key := range codes {
		val, pres := shorts[key.Code]
		var shortsVal, totalVal int64
		var percentVal float32
		if pres {
			shortsVal = val.Shorts
			totalVal = val.Total
			percentVal = val.Percent
		}
		combinedShort := &sharedata.CombinedShortJSON{
			Code:     key.Code,
			Name:     key.Name,
			Shorts:   shortsVal,
			Total:    totalVal,
			Percent:  percentVal,
			Industry: key.Industry,
		}
		result = append(result, combinedShort)
	}
	return result
}

//Handler - the main function handler, triggered by cloudwatch event
func Handler(request events.CloudWatchEvent) {
	clients := awsutils.GenerateAWSClients("dynamoDB", "s3", "kinesis")

	//Get Share Codes
	codesReady := make(chan map[string]*sharedata.ShareCsv, 1)
	go GetShareCodes(clients, codesReady)

	//Get Short positions
	shortsReady := make(chan map[string]*sharedata.AsicShortCsv, 1)
	go GetShortPositions(clients, shortsReady)

	//Merge two data sets
	mergeShortData := MergeShortData(shortsReady, codesReady)
	if mergeShortData == nil {
		log.Info("Handler", "nil return from mergeShortData")
		return
	}

	//Marshal the data into JSON
	shortDataBytes, err := json.Marshal(mergeShortData)
	if err != nil {
		log.Info("Handler", "unable to marshal short data into JSON")
	}

	//Push to S3
	clients.PutFileToS3("shortedappjmk", "combinedshorts.json", shortDataBytes)
}

func main() {
	log.SetAppName("ShortedApp")
	Handler(events.CloudWatchEvent{})
	lambda.Start(Handler)
}
