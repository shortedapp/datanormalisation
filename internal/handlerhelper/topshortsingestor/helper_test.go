package topshortsingestor

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/shortedapp/shortedfunctions/pkg/testingutil"
	"github.com/stretchr/testify/assert"

	"github.com/shortedapp/shortedfunctions/internal/sharedata"

	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

type mockAwsUtilClients struct {
	TestOption int
	awsutils.AwsUtiler
}

func (m mockAwsUtilClients) PutDynamoDBItems(tableName string, values map[string]interface{}) error {
	if m.TestOption == 0 {
		return fmt.Errorf("test failure")
	}

	return nil
}

func (m mockAwsUtilClients) FetchJSONFileFromS3(bucket string, key string, f func([]byte) (interface{}, error)) (interface{}, error) {
	if m.TestOption == 0 {
		return nil, fmt.Errorf("err")
	}
	b, _ := ioutil.ReadFile("../../../test/data/combinedshortdatatest.json")
	res, _ := sharedata.UnmarshalCombinedShortsJSON(b)
	return res, nil
}

func (m mockAwsUtilClients) GetDynamoDBTableThroughput(tableName string) (int64, int64) {
	return 2, 2
}

func (m mockAwsUtilClients) UpdateDynamoDBTableCapacity(tableName string, readCap int64, writeCap int64) error {
	return nil
}

func TestPutRecord(t *testing.T) {
	log.Logger.Vlogging = true
	log.Logger.Level = 1

	testCases := []struct {
		testOption int
		val        string
	}{
		{0, "putRecord"},
		{1, ""},
	}

	for _, testCase := range testCases {
		client := mockAwsUtilClients{testCase.testOption, nil}
		d := Topshortsingestor{Clients: client}
		data := sharedata.TopShortJSON{Position: 1, Code: "TST", Percent: 0.5}
		res := testingutil.CaptureStandardErr(func() { d.putRecord(&data, "test") }, log.Logger.StdLogger)
		assert.Equal(t, true, strings.Contains(res, testCase.val))
	}
}

func TestIngestRoutine(t *testing.T) {
	log.Logger.Vlogging = true
	log.Logger.Level = 1
	testCases := []struct {
		testOption int
		val        string
	}{
		{0, "unable to fetch data from s3"},
		{1, ""},
	}

	for _, testCase := range testCases {
		client := mockAwsUtilClients{testCase.testOption, nil}
		d := Topshortsingestor{Clients: client}

		res := testingutil.CaptureStandardErr(func() { d.IngestTopShorted("test") }, log.Logger.StdLogger)
		assert.Equal(t, true, strings.Contains(res, testCase.val))
	}

}
