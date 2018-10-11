package dynamoingestor

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	"github.com/stretchr/testify/assert"

	"github.com/shortedapp/shortedfunctions/internal/sharedata"
)

type mockAwsUtilClients struct {
	TestOption int
	awsutil.AwsUtiler
}

func (m mockAwsUtilClients) FetchJSONFileFromS3(bucket string, key string, f func([]byte) (interface{}, error)) (interface{}, error) {
	if m.TestOption == 0 {
		return nil, fmt.Errorf("err")
	}
	b, _ := ioutil.ReadFile("../../../test/data/combinedshortdatatest.json")
	res, _ := sharedata.UnmarshalCombinedShortsJSON(b)
	return res, nil
}

func (m mockAwsUtilClients) WriteToDynamoDB(tableName string, data interface{},
	mapper func(resp interface{}, date int) ([]*map[string]interface{}, error), date int) error {
	return nil
}

func (m mockAwsUtilClients) PutDynamoDBLastModified(string, string, string) error {
	return nil
}

func TestCombinedShortJSONMapper(t *testing.T) {
	testCases := []struct {
		input interface{}
		len   int
		code  string
		err   bool
	}{
		{sharedata.CombinedResultJSON{Result: []*sharedata.CombinedShortJSON{
			&sharedata.CombinedShortJSON{Name: "test", Code: "TST", Shorts: 10, Total: 20, Percent: 0.5, Industry: "TEST"}}}, 1, "TST", false},
		{1, 1, "TST", true},
	}

	for _, test := range testCases {
		res, err := CombinedShortJSONMapper(test.input, 0)
		if err != nil {
			assert.Equal(t, test.err, err != nil)
		} else {
			assert.Equal(t, test.len, len(res))
			assert.Equal(t, test.code, (*res[0])["Code"])
		}
	}

}

func TestIngestRoutine(t *testing.T) {
	testCases := []struct {
		testOption int
		err        bool
	}{
		{0, true},
		{1, false},
	}

	for _, test := range testCases {
		client := mockAwsUtilClients{TestOption: test.testOption}
		d := Dynamoingestor{Clients: client}
		res := d.IngestRoutine("test")
		assert.Equal(t, test.err, res != nil)
	}

}
