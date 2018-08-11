package awsutils

import (
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/stretchr/testify/assert"
)

type mockS3DownloadClient struct {
	s3manageriface.DownloaderAPI
}

type mockS3UploadClient struct {
	s3manageriface.UploaderAPI
}

type mockKinesisClient struct {
	kinesisiface.KinesisAPI
}

type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
}

type fakeKinesisDoc struct {
	name  string `json:"name"`
	value string `json:"value"`
}

func (m *mockS3DownloadClient) Download(w io.WriterAt, input *s3.GetObjectInput, functions ...func(*s3manager.Downloader)) (int64, error) {
	switch *input.Bucket {
	case "testJsonFetch":
		file, _ := ioutil.ReadFile("../../test/data/sharedatatest.json")
		w.WriteAt(file, 0)
	case "testCsvFetch":
		file, _ := ioutil.ReadFile("../../test/data/fetchcsvfile.csv")
		w.WriteAt(file, 0)
	}
	// mock response/functionality
	return 100, nil
}

func (m *mockS3UploadClient) Upload(s *s3manager.UploadInput, functions ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	if s.Body == nil {
		return nil, fmt.Errorf("missing body")
	}
	fakeVersionID := "1"
	return &s3manager.UploadOutput{VersionID: &fakeVersionID}, nil
}

func (m *mockKinesisClient) PutRecords(record *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	return nil, nil
}

func (m *mockDynamoDBClient) PutItem(item *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return nil, nil
}

func (m *mockDynamoDBClient) GetItem(item *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	if item.TableName == "test" {
		return nil, nil
	}
	mapAttr := make(map[string]*dynamodb.AttributeValue)
	time := "2018/08/10 22:09:55.166"
	attr := dynamodb.AttributeValue{S: &time}
	mapAttr["date"] = &attr
	result := dynamodb.GetItemOutput{Item: mapAttr}
	return &result, nil
}
func TestGenerateAWSClients(t *testing.T) {
	testCsvs := []struct {
		clients []string
		isNil   []bool
	}{
		{[]string{"s3", "dynamoDB"}, []bool{false, false}},
		{[]string{"s3"}, []bool{false, true}},
		{[]string{}, []bool{true, true}},
	}
	for _, testCase := range testCsvs {
		clients := GenerateAWSClients(testCase.clients...)
		assert.Equal(t, testCase.isNil[0], clients.s3DownloadClient == nil)
		assert.Equal(t, testCase.isNil[1], clients.dynamoClient == nil)
	}
}

func TestFetchJSONFileFromS3(t *testing.T) {
	mockS3Client := mockS3DownloadClient{}
	client := ClientsStruct{s3DownloadClient: &mockS3Client}
	res, err := client.FetchJSONFileFromS3("testJsonFetch", "a", func(b []byte) (interface{}, error) {
		return string(b), nil
	})

	assert.True(t, err == nil)
	assert.Equal(t, "[{\"name\":\"abc\", \"code\":\"ABC\", \"industry\": \"test\"},"+
		"{\"name\":\"def\", \"code\":\"DEF\", \"industry\": \"test2\"}]", res)

}

func TestFetchCSVFileFromS3(t *testing.T) {
	mockS3Client := mockS3DownloadClient{}
	client := ClientsStruct{s3DownloadClient: &mockS3Client}
	res, err := client.FetchCSVFileFromS3("testCsvFetch", "a", func(s [][]string) (interface{}, error) {
		return s, nil
	})

	assert.True(t, err == nil)
	for i, str := range res.([][]string) {
		assert.Equal(t, []string{"test" + string(i), "test" + string(i)}, str)
	}

}

func TestPutFileToS3(t *testing.T) {
	mockS3Client := mockS3UploadClient{}
	client := ClientsStruct{s3UploadClient: &mockS3Client}
	testCases := []struct {
		bucketName string
		key        string
		data       []byte
		err        bool
	}{
		{"", "", nil, true},
		{"test", "test", []byte{1, 2, 20}, false},
	}
	for _, test := range testCases {
		err := client.PutFileToS3(test.bucketName, test.key, test.data)
		assert.Equal(t, test.err, err != nil)
	}

}

func TestPutKinesisRecords(t *testing.T) {
	mockKinesisClient := mockKinesisClient{}
	client := ClientsStruct{kinesisClient: &mockKinesisClient}
	stringMock := []string{"abc", "bcd"}
	fakeJSONMock := []fakeKinesisDoc{{"test", "test"}}
	testCases := []struct {
		stream        string
		partitionKeys []string
		err           bool
	}{
		{"test", []string{"test", "test2"}, false},
		{"test", []string{"test"}, false},
	}
	for i, test := range testCases {
		var data []interface{}
		if i == 0 {
			data = make([]interface{}, len(stringMock))
			for i := 0; i < len(data); i++ {
				data[i] = stringMock[i]
			}
		} else if i == 1 {
			data = make([]interface{}, len(fakeJSONMock))
			for i := 0; i < len(data); i++ {
				data[i] = fakeJSONMock[i]
			}
		}

		err := client.PutKinesisRecords(&test.stream, data, test.partitionKeys)
		assert.Equal(t, test.err, err != nil)
	}
}

func TestPutDynamoDBLastModified(t *testing.T) {
	mockDynamoClient := mockDynamoDBClient{}
	client := ClientsStruct{dynamoClient: &mockDynamoClient}
	testCases := []struct {
		table string
		key   string
		time  string
		err   bool
	}{
		{"test", "test", "", true},
		{"test", "test", "2018/08/10 22:09:55.166", false},
	}
	for _, test := range testCases {
		err := client.PutDynamoDBLastModified(test.table, test.key, test.time)
		assert.Equal(t, test.err, err != nil)
	}
}

func TestFetchDynamoDBLastModified(t *testing.T) {
	mockDynamoClient := mockDynamoDBClient{}
	client := ClientsStruct{dynamoClient: &mockDynamoClient}
	testCases := []struct {
		table string
		key   string
		time  string
		err   error
	}{
		{"test", "test", "", fmt.Errorf("test")},
		{"test2", "test", "2018/08/10 22:09:55.166", nil},
	}
	for _, test := range testCases {
		res, err := client.FetchDynamoDBLastModified(test.table, test.key)
		assert.Equal(t, test.err, err != nil)
		assert.Equal(t, test.time, res)
	}
}
