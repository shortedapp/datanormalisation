package bulknormalise

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/shortedapp/shortedfunctions/internal/sharedata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
	"github.com/shortedapp/shortedfunctions/pkg/timeslotutil"
)

//Datanormalise - struct to enable testing
type Bulknormalise struct {
	Clients awsutil.AwsUtiler
}

//NormaliseRoutine - Runs a routine to generate the short data and upload to s3
func (b Bulknormalise) NormaliseRoutine(previousMonth int) {

	resp, err := b.Clients.GetItemByPartDynamoDB(&awsutil.DynamoDBItemQuery{TableName: "lastUpdate", PartitionName: "latestDate", PartitionKey: "name_id"})
	if err != nil {
		//TODO determine what to do with error logic here
		return
	}
	latestDynamoDate := *resp["date"].S

	//Get Share Codes
	codes := b.GetShareCodes()
	if codes == nil {
		panic("unable to get codes")
	}

	//Get Short positions
	tNow := timeslotutil.BackDateBusinessDays(time.Now(), 4)
	latestDate := timeslotutil.GetPreviousDateMinusMonthsString(-previousMonth, tNow)

	tStart := tNow.AddDate(0, -(previousMonth + 1), 0)
	dateString := timeslotutil.GetPreviousDateMinusMonthsString((previousMonth + 1), tNow)
	var maxDate string
	i := 1
	for dateString != latestDate {
		//Can't do a gp routine or connections will be throttled
		result := b.MergeAndUploadShorts(codes, dateString)
		time.Sleep(time.Second)
		dateString = timeslotutil.GetDatePlusDaysString(i, tStart)

		//Track the most current file uploaded
		if result {
			maxDate = dateString
		}
		i++
	}

	//TODO add error handling for atoi here
	latestDynamoInt, _ := strconv.Atoi(latestDynamoDate)
	maxDateInt, _ := strconv.Atoi(maxDate)

	if latestDynamoInt < maxDateInt {
		b.Clients.PutDynamoDBLastModified("lastUpdate", "latestDate", maxDate)
	}

}

func (b Bulknormalise) MergeAndUploadShorts(codes map[string]*sharedata.ShareCsv, dateString string) bool {
	shorts := b.GetShortPositions(dateString)
	if shorts == nil {
		log.Info("Could not get shorts for: ", dateString)
		return false
	}
	//Merge two data sets
	mergeShortData := b.MergeShortData(shorts, codes)
	if mergeShortData != nil {
		//Upload JSON document
		err := b.UploadData(mergeShortData, dateString)
		if err == nil {
			return true
		}
	}
	return false
}

//GetShareCodes - Get Short position data from ASIC
// inputs:
//	- clients: a pointer to the pregenerated AWS clients
//	- shortsReady: a channel to place the goroutine result
func (b Bulknormalise) GetShareCodes() map[string]*sharedata.ShareCsv {
	resp, err := b.Clients.FetchCSVFileFromS3("shortedappjmk", "ASXListedCompanies.csv", sharedata.UnmarshalSharesCSV)
	if err != nil {
		return nil
	}
	result := resp.([]*sharedata.ShareCsv)
	resultMap := make(map[string]*sharedata.ShareCsv)
	for _, record := range result {
		resultMap[record.Code] = record
	}

	return resultMap
}

//GetShortPositions - Get Short position data from ASIC
// inputs:
//	- clients: a pointer to the pregenerated AWS clients
//	- shortsReady: a channel to place the goroutine result
func (b Bulknormalise) GetShortPositions(timeString string) map[string]*sharedata.AsicShortCsv {

	resp, err := http.Get("https://asic.gov.au/Reports/Daily/" + timeString[0:4] + "/" +
		timeString[4:6] + "/RR" + timeString + "-001-SSDailyAggShortPos.csv")
	if resp == nil || err != nil {
		if err != nil {
			log.Info("GetShortPositions", err.Error())
		}
		//No Update required
		return nil
	}

	bData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Info("GetShortPositions", err.Error())
		return nil
	}

	bData = bytes.Replace(bData, []byte("\x00"), []byte(""), -1)
	asicShorts, err := sharedata.UnmarshalAsicShortsCSV(bData)

	resultMap := make(map[string]*sharedata.AsicShortCsv)
	for _, short := range asicShorts {
		resultMap[short.Code] = short
	}

	return resultMap
}

//MergeShortData - Merges data from ASIC and ASX
// inputs:
//	- shorts: slice of shorts
//	- codes: slice of codes
// Output:
//	- Slice of CombinedShortJSON pointers
func (b Bulknormalise) MergeShortData(shorts map[string]*sharedata.AsicShortCsv, codes map[string]*sharedata.ShareCsv) []*sharedata.CombinedShortJSON {

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
func (b Bulknormalise) UploadData(data []*sharedata.CombinedShortJSON, dateString string) error {
	//add result key
	result := sharedata.CombinedResultJSON{Result: data}
	log.Debug("UploadData", "uploading data...")
	//Marshal the data into JSON
	shortDataBytes, err := json.Marshal(result)
	if err != nil {
		log.Info("UploadData", "unable to marshal short data into JSON")
		return fmt.Errorf("unable to marshal data for date: " + dateString)
	}
	//Push to S3
	err = b.Clients.PutFileToS3("shortedappjmk", "testShortedData/"+dateString+".json", shortDataBytes)
	if err != nil {
		log.Info("UploadData", "unable to upload to S3 for date: "+dateString)
		return fmt.Errorf("unable to upload to S3 for date: " + dateString)
	}
	return nil
}
