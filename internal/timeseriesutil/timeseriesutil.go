package timeseriesutil

import (
	"strconv"

	"github.com/shortedapp/shortedfunctions/internal/searchutils"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
)

//DatePercent - Struct to store Date and Percent KVs
type DatePercent struct {
	Date    int
	Percent float64
}

//FetchTimeSeries - Function To fetch a time series based off passed duration
func FetchTimeSeries(clients awsutils.AwsUtiler, tableName string, code string, period searchutils.SearchPeriod) (string, []DatePercent) {
	if period == searchutils.Latest {
		return "", nil
	}
	low, high := searchutils.GetSearchWindow(clients, "", "", period)
	query := awsutils.DynamoDBRangeQuery{
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
