package bulknormalise

import (
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/shortedapp/shortedfunctions/internal/sharedata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
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

func (m mockAwsUtilClients) GetItemByPartDynamoDB(*awsutil.DynamoDBItemQuery) (map[string]*dynamodb.AttributeValue, error) {
	mapMock := make(map[string]*dynamodb.AttributeValue)
	fakeDate := "20180901"
	mapMock["date"] = &dynamodb.AttributeValue{S: &fakeDate}
	return mapMock, nil
}
