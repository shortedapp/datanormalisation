package topshortseries

import (
	"sync"
	"testing"

	"github.com/shortedapp/shortedfunctions/internal/searchutils"

	"github.com/shortedapp/shortedfunctions/internal/timeseriesutil"
	"github.com/stretchr/testify/assert"
)

func TestGenerateSeriesMap(t *testing.T) {
	seriesChannel := make(chan TopSeries, 1)
	dateValueTest := make([]timeseriesutil.DatePercent, 0, 1)
	dateValueTest = append(dateValueTest, timeseriesutil.DatePercent{20180810, 12.0123})
	seriesChannel <- TopSeries{Code: "test", DateValues: dateValueTest}

	close(seriesChannel)
	res := generateSeriesMap(seriesChannel, 1)
	assert.True(t, res["test"] != nil)
}

func TestGetCodeSeries(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	tc := Topshortseries{}
	seriesChannel := make(chan TopSeries, 1)
	tc.getCodeSeries("test", "code", searchutils.Latest, seriesChannel, &wg)
	res := <-seriesChannel
	assert.True(t, res.Code == "")
}
