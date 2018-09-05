package timeseriesutil

import (
	"strconv"

	"github.com/shortedapp/shortedfunctions/internal/searchutil"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
)

//DatePercent - Struct to store Date and Percent KVs
type DatePercent struct {
	Date    int
	Percent float64
}

//FetchTimeSeries - Function To fetch a time series based off passed duration
func FetchTimeSeries(clients awsutil.AwsUtiler, tableName string, code string, period searchutil.SearchPeriod) (string, []DatePercent) {
	if period == searchutil.Latest {
		return "", nil
	}
	low, high := searchutil.GetSearchWindow(clients, "", "", period)
	query := awsutil.DynamoDBRangeQuery{
		TableName:     tableName,
		PartitionName: "Code",
		PartitionKey:  code,
		SortName:      "Date",
		Low:           low,
		High:          high,
	}
	//ENHANCEMENT: create custom retry logic
	res, _ := clients.TimeRangeQueryDynamoDB(&query)
	timeSeries := make([]DatePercent, 0, len(res))
	for _, timespot := range res {
		date, err := strconv.Atoi(*timespot["Date"].N)
		if err != nil {
			//skip element on error
			continue
		}
		percent, _ := strconv.ParseFloat(*timespot["Percent"].N, 64)
		if err != nil {
			//skip element on error
			continue
		}
		timeSeries = append(timeSeries, DatePercent{Date: date, Percent: percent})
	}
	return code, timeSeries
}
