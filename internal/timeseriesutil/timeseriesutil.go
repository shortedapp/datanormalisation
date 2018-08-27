package timeseriesutil

import (
	"github.com/shortedapp/shortedfunctions/internal/searchutils"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
)

func FetchTimeSeries(clients awsutils.AwsUtiler, tableName string, code string, period searchutils.SearchPeriod) (string, [][2]string) {
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
	res := clients.TimeRangeQueryDynamoDB(&query)
	timeSeries := make([][2]string, 0, len(res))
	for _, timespot := range res {
		timeSeries = append(timeSeries, [2]string{*timespot["Date"].N, *timespot["Percent"].N})
	}
	return code, timeSeries
}
