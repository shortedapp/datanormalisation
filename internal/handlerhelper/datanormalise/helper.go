package datanormalise

import (
	"bytes"
	"encoding/json"
	"io/ioutil"

	"github.com/shortedapp/shortedfunctions/internal/sharedata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Datanormalise - struct to enable testing
type Datanormalise struct {
	Clients awsutil.AwsUtiler
}

//NormaliseRoutine - Runs a routine to generate the short data and upload to s3
func (d Datanormalise) NormaliseRoutine() {
	//Get Share Codes
	codesReady := make(chan map[string]*sharedata.ShareCsv, 1)
	go d.GetShareCodes(codesReady)

	//Get Short positions
	shortsReady := make(chan map[string]*sharedata.AsicShortCsv, 1)
	go d.GetShortPositions(shortsReady)

	//Merge two data sets
	mergeShortData := d.MergeShortData(shortsReady, codesReady)
	if mergeShortData != nil {
		//Upload JSON document
		d.UploadData(mergeShortData)
	}
}

//GetShareCodes - Get Short position data from ASIC
// inputs:
//	- clients: a pointer to the pregenerated AWS clients
//	- shortsReady: a channel to place the goroutine result
func (d Datanormalise) GetShareCodes(codesReady chan<- map[string]*sharedata.ShareCsv) {
	resp, err := d.Clients.FetchCSVFileFromS3("shortedappjmk", "ASXListedCompanies.csv", sharedata.UnmarshalSharesCSV)
	if err != nil {
		codesReady <- nil
		return
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
func (d Datanormalise) GetShortPositions(shortsReady chan<- map[string]*sharedata.AsicShortCsv) {
	resp, err := d.Clients.WithDynamoDBGetLatest("https://asic.gov.au/Reports/Daily/2018/07/RR20180726-001-SSDailyAggShortPos.csv", "test")
	if resp == nil || err != nil {
		if err != nil {
			log.Info("GetShortPositions", err.Error())
		}
		//No Update required
		shortsReady <- nil
		return
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Info("GetShortPositions", err.Error())
	}

	b = bytes.Replace(b, []byte("\x00"), []byte(""), -1)
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
func (d Datanormalise) MergeShortData(shortsReady <-chan map[string]*sharedata.AsicShortCsv, codesReady <-chan map[string]*sharedata.ShareCsv) []*sharedata.CombinedShortJSON {
	shorts := <-shortsReady
	codes := <-codesReady
	if shorts == nil {
		log.Info("MergeShortData", "No updated short data to merge")
		return nil
	}
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

//UploadData - Marshal Object to JSON and Push to S3
func (d Datanormalise) UploadData(data []*sharedata.CombinedShortJSON) {
	//Marshal the data into JSON
	shortDataBytes, err := json.Marshal(data)
	if err != nil {
		log.Info("UploadData", "unable to marshal short data into JSON")
		return
	}

	//Push to S3
	err = d.Clients.PutFileToS3("shortedappjmk", "combinedshorts.json", shortDataBytes)
	if err != nil {
		log.Info("UploadData", "unable to upload to S3")
	}
}
