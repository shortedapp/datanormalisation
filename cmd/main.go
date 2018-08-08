package main

import (
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/datanormalization/internal/sharecodes"
	log "github.com/shortedapp/datanormalization/pkg/loggingutil"
	"github.com/shortedapp/datanormalization/pkg/scheduledServices"
)

func GetShareCodes(clients *scheduledget.ClientsStruct, codesReady chan<- map[string]*sharecodes.ShareCsv) {
	res, err := scheduledget.FetchCSVFileFromS3("shortedappjmk", "ASXListedCompanies.csv", clients, sharecodes.UnmarshalSharesCSV)
	if err != nil {

	}
	result := res.([]*sharecodes.ShareCsv)
	resultMap := make(map[string]*sharecodes.ShareCsv)
	for _, record := range result {
		resultMap[record.Code] = record
	}

	codesReady <- resultMap
}

func GetShortPositions(clients *scheduledget.ClientsStruct, shortsReady chan<- map[string]*sharecodes.AsicShortCsv) {
	resp, err := scheduledget.WithDynamoDBGetLatest("https://asic.gov.au/Reports/Daily/2018/07/RR20180726-001-SSDailyAggShortPos.csv", "test", clients)
	// fmt.Println(res2.Body)
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
	}
	asicShorts, err := sharecodes.UnmarshalAsicShortsCSV(b)

	resultMap := make(map[string]*sharecodes.AsicShortCsv)
	for _, short := range asicShorts {
		resultMap[short.Code] = short
	}

	shortsReady <- resultMap
}

func MergeShortData(shortsReady <-chan map[string]*sharecodes.AsicShortCsv, codesReady <-chan map[string]*sharecodes.ShareCsv) ([]*sharecodes.CombinedShortJson, error) {
	shorts := <-shortsReady
	codes := <-codesReady
	// fmt.Println(shorts)
	//fmt.Println(codes)
	result := make([]*sharecodes.CombinedShortJson, 0, len(codes))
	for _, key := range codes {
		val, pres := shorts[key.Code]
		var shortsVal, totalVal int64
		var percentVal float32
		if pres {
			shortsVal = val.Shorts
			totalVal = val.Total
			percentVal = val.Percent
		}
		combinedShort := &sharecodes.CombinedShortJson{
			Code:     key.Code,
			Name:     key.Name,
			Shorts:   shortsVal,
			Total:    totalVal,
			Percent:  percentVal,
			Industry: key.Industry,
		}
		result = append(result, combinedShort)
	}
	return result, nil
}

func Handler(request events.CloudWatchEvent) {
	clients := scheduledget.GenerateAWSClients("dynamoDB", "s3", "kinesis")

	//Get Share Codes
	codesReady := make(chan map[string]*sharecodes.ShareCsv, 1)
	go GetShareCodes(clients, codesReady)

	//Get Short positions
	shortsReady := make(chan map[string]*sharecodes.AsicShortCsv, 1)
	go GetShortPositions(clients, shortsReady)

	_, err := MergeShortData(shortsReady, codesReady)
	log.Warn("Handler", fmt.Sprintf("%v", err))
}

func main() {
	log.SetAppName("ShortedApp")
	Handler(events.CloudWatchEvent{})
	lambda.Start(Handler)
}
