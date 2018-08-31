package topshortseries

import (
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/shortedapp/shortedfunctions/internal/searchutils"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"

	"github.com/shortedapp/shortedfunctions/internal/timeseriesutil"
	"github.com/stretchr/testify/assert"
)

type mockAwsUtilClients struct {
	TestOption int
	awsutils.AwsUtiler
}

func (m mockAwsUtilClients) BatchGetItemsDynamoDB(table string, key string, values []interface{}) ([]map[string]*dynamodb.AttributeValue, error) {
	if m.TestOption == 0 {
		return nil, fmt.Errorf("test failure")
	}
	res := make([]map[string]*dynamodb.AttributeValue, 0, 1)
	fakeCode := "test"
	res = append(res, map[string]*dynamodb.AttributeValue{"Code": &dynamodb.AttributeValue{S: &fakeCode}})

	return res, nil
}

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

func TestFetchTopShortedSeries(t *testing.T) {

	testCases := []struct {
		testOption int
		result     bool
	}{
		{0, true},
		{1, false},
	}

	for _, testCase := range testCases {
		client := mockAwsUtilClients{testCase.testOption, nil}
		ts := Topshortseries{Clients: client}
		res := ts.FetchTopShortedSeries("test", "code", 1, searchutils.Latest)
		assert.Equal(t, testCase.result, len(res) == 0)
	}
}
