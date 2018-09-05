package topshortseries

import (
	"sync"

	"github.com/shortedapp/shortedfunctions/internal/searchutil"
	"github.com/shortedapp/shortedfunctions/internal/timeseriesutil"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
)

type Topshortseries struct {
	Clients awsutil.AwsUtiler
}

type TopSeries struct {
	Code       string
	DateValues []timeseriesutil.DatePercent
}

func (t *Topshortseries) FetchTopShortedSeries(topShortsTable string, timeSeriesTable string, top int, period searchutil.SearchPeriod) map[string][]timeseriesutil.DatePercent {
	//Work In Progress
	interSlice := make([]interface{}, top)
	for i := 0; i < top; i++ {
		interSlice[i] = i
	}
	res, err := t.Clients.BatchGetItemsDynamoDB(topShortsTable, "Position", interSlice)

	if err != nil {
		return nil
	}

	seriesChannel := make(chan TopSeries, top)

	//create waitgroup to syncronise the fetch of time series
	wg := sync.WaitGroup{}
	wg.Add(top)

	for _, item := range res {
		code := *item["Code"].S
		go t.getCodeSeries(timeSeriesTable, code, period, seriesChannel, &wg)
	}

	//Wait here and close channel once done
	wg.Wait()
	close(seriesChannel)

	return generateSeriesMap(seriesChannel, top)

}

func (t *Topshortseries) getCodeSeries(table string, code string,
	period searchutil.SearchPeriod, seriesChannel chan TopSeries, wg *sync.WaitGroup) {
	code, res := timeseriesutil.FetchTimeSeries(t.Clients, table, code, period)
	seriesChannel <- TopSeries{Code: code, DateValues: res}
	wg.Done()
}

func generateSeriesMap(seriesChannel chan TopSeries, top int) map[string][]timeseriesutil.DatePercent {
	resMap := make(map[string][]timeseriesutil.DatePercent, top)
	for item := range seriesChannel {
		resMap[item.Code] = item.DateValues
	}
	return resMap
}
