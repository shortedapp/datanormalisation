package timeseriesutil

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/shortedapp/shortedfunctions/internal/searchutils"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
	"github.com/stretchr/testify/assert"
)

type mockAwsUtilClients struct {
	awsutils.AwsUtiler
}

func (m mockAwsUtilClients) TimeRangeQueryDynamoDB(query *awsutils.DynamoDBRangeQuery) ([]map[string]*dynamodb.AttributeValue, error) {
	res := make([]map[string]*dynamodb.AttributeValue, 0, 1)
	fakeDate := "20180712"
	fakePercent := "1.0123"
	res = append(res, map[string]*dynamodb.AttributeValue{"Date": &dynamodb.AttributeValue{N: &fakeDate},
		"Percent": &dynamodb.AttributeValue{N: &fakePercent}})

	return res, nil
}

func TestFetchTimeSeries(t *testing.T) {

	testCases := []struct {
		searchPeriod searchutils.SearchPeriod
		code         string
		result       bool
	}{
		{searchutils.Latest, "", true},
		{searchutils.Month, "test", false},
	}

	for _, testCase := range testCases {
		client := mockAwsUtilClients{nil}
		code, res := FetchTimeSeries(client, "test", "test", testCase.searchPeriod)
		assert.Equal(t, testCase.code, code)
		assert.Equal(t, testCase.result, res == nil)
	}
}
