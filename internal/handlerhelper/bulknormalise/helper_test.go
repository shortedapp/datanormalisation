package bulknormalise

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/shortedapp/shortedfunctions/internal/sharedata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	"github.com/stretchr/testify/assert"
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

func (m mockAwsUtilClients) FetchCSVFileFromS3(bucketName string, key string, f func(s [][]string) (interface{}, error)) (interface{}, error) {
	if m.TestOption == 0 {
		return nil, fmt.Errorf("test failure")
	}
	sharesample := sharedata.ShareCsv{Name: "test"}

	return []*sharedata.ShareCsv{&sharesample}, nil
}

func (m mockAwsUtilClients) PutFileToS3(string, string, []byte) error {
	if m.TestOption == 0 {
		return fmt.Errorf("test failure")
	}
	return nil
}

func (m mockAwsUtilClients) WriteToDynamoDB(tableName string, data interface{},
	mapper func(resp interface{}, date int) ([]*map[string]interface{}, error), date int) error {
	return nil
}

func (m mockAwsUtilClients) PutDynamoDBLastModified(string, string, string) error {
	return nil
}

func (m mockAwsUtilClients) GetItemByPartDynamoDB(*awsutil.DynamoDBItemQuery) (map[string]*dynamodb.AttributeValue, error) {
	mapMock := make(map[string]*dynamodb.AttributeValue)
	fakeDate := "20180901"
	mapMock["date"] = &dynamodb.AttributeValue{S: &fakeDate}
	return mapMock, nil
}

func TestGetShareCodes(t *testing.T) {

	sharesample := sharedata.ShareCsv{Name: "test", Code: "TST"}
	testCases := []struct {
		testOption int
		val        *sharedata.ShareCsv
	}{
		{0, nil},
		{1, &sharesample},
	}
	for _, test := range testCases {
		client := mockAwsUtilClients{TestOption: test.testOption}
		b := Bulknormalise{Clients: client}
		output := b.GetShareCodes()
		for _, val := range output {
			fmt.Println(*val)
			assert.Equal(t, test.val.Name, val.Name)
		}
	}
}

func TestUploadData(t *testing.T) {

	testCases := []struct {
		testOption int
		data       []*sharedata.CombinedShortJSON
		err        bool
	}{
		{0, []*sharedata.CombinedShortJSON{{Name: "test", Code: "TST", Industry: "TEST"}}, false},
		{1, []*sharedata.CombinedShortJSON{{Name: "test", Code: "TST", Industry: "TEST"}}, true},
		{0, nil, false},
	}
	for _, test := range testCases {
		client := mockAwsUtilClients{TestOption: test.testOption}
		b := Bulknormalise{Clients: client}
		result := b.UploadData(test.data, "20180901")
		assert.Equal(t, test.err, result == nil)
	}
}
