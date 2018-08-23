package timeseriesutil

import (
	"fmt"

	"github.com/shortedapp/shortedfunctions/internal/searchutils"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
)

func FetchTimeSeries(clients awsutils.AwsUtiler, tableName string, code string, period searchutils.SearchPeriod) {
	if period == searchutils.Latest {
		return
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
	fmt.Println(clients.TimeRangeQueryDynamoDB(&query))

}
