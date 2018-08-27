package topshortseries

import (
	"github.com/shortedapp/shortedfunctions/internal/searchutils"
	"github.com/shortedapp/shortedfunctions/internal/timeseriesutil"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
)

type Topshortseries struct {
	client awsutils.AwsUtiler
}

func (t *Topshortseries) fetchTopShortedSeries(top int, tableName string, period searchutils.SearchPeriod) {
	//Work In Progress
	interSlice := make([]interface{}, top)
	for i := 0; i < top; i++ {
		interSlice[i] = i
	}
	res, err := t.client.BatchGetItemsDynamoDB(tableName, "Position", interSlice)

	if err != nil {
		return
	}

	for _, item := range res {
		code := *item["code"].S
		go timeseriesutil.FetchTimeSeries(t.client, tableName, code, period)
	}

}
