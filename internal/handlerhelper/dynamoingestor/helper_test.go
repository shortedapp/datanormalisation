package dynamoingestor

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

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
	testTime := time.Now().UTC().UnixNano()
	testCases := []struct {
		testOption int
		val        string
	}{
		{0, "putRecord"},
		{1, ""},
	}

	for _, testCase := range testCases {
		client := mockAwsUtilClients{testCase.testOption, nil}
		d := Dynamoingestor{Clients: client}
		data := sharedata.CombinedShortJSON{Name: "test", Code: "TST", Shorts: 10, Total: 20, Percent: 0.5, Industry: "TEST"}
		res := testingutil.CaptureStandardErr(func() { d.putRecord(&data, testTime) }, log.Logger.StdLogger)
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
		d := Dynamoingestor{Clients: client}

		res := testingutil.CaptureStandardErr(func() { d.IngestRoutine("test") }, log.Logger.StdLogger)
		assert.Equal(t, true, strings.Contains(res, testCase.val))
	}

}
