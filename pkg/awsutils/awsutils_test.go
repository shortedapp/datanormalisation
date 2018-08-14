package awsutils

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
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

type FakeKinesisDoc struct {
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
	if *item.TableName == "test2" {
		return nil, fmt.Errorf("table does not exist")
	}
	return nil, nil
}

func (m *mockDynamoDBClient) GetItem(item *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	if *item.TableName == "test" {
		return nil, fmt.Errorf("table does not exist")
	} else if *item.Key["name_id"].S == "testInvalid" {
		//Test WithDynamoDBGetLatest invalid key
		return nil, fmt.Errorf("key does not exist")
	} else if *item.Key["name_id"].S == "testValid" {
		//Test WithDynamoDBGetLatest valid return time
		mapAttr := make(map[string]*dynamodb.AttributeValue)
		time := "2018-08-11T13:22:41+00:00"
		attr := dynamodb.AttributeValue{S: &time}
		mapAttr["date"] = &attr
		result := dynamodb.GetItemOutput{Item: mapAttr}
		return &result, nil
	}

	//default return
	mapAttr := make(map[string]*dynamodb.AttributeValue)
	time := "2018/08/10 22:09:55.166"
	attr := dynamodb.AttributeValue{S: &time}
	mapAttr["date"] = &attr
	result := dynamodb.GetItemOutput{Item: mapAttr}
	return &result, nil
}

type testHttp struct{}

func (t testHttp) RoundTrip(request *http.Request) (*http.Response, error) {
	//Test  WithDynamoDBGetLatest valid head last modified time
	if request.URL.String() == "127.0.0.1" {
		header := http.Header{"Last-Modified": []string{"Sat, 11 Aug 2018 09:46:37 GMT"}}
		return &http.Response{
			Status:     "200 OK",
			StatusCode: 200,
			Header:     header,
		}, nil
		//Test  WithDynamoDBGetLatest invalid head last modified time
	} else if request.URL.String() == "127.0.0.2" {
		header := http.Header{"Last-Modified": []string{"GMT"}}
		return &http.Response{
			Status:     "200 OK",
			StatusCode: 200,
			Header:     header,
		}, nil
	} else if request.URL.String() == "127.0.0.3" {
		header := http.Header{"Last-Modified": []string{"Sun, 12 Aug 2018 09:46:37 GMT"}}
		return &http.Response{
			Status:     "200 OK",
			StatusCode: 200,
			Header:     header,
		}, nil
	}
	return nil, fmt.Errorf("could not reach url")
}

func TestGenerateAWSClients(t *testing.T) {
	testCsvs := []struct {
		clients []string
		isNil   []bool
	}{
		{[]string{"s3", "dynamoDB", "kinesis"}, []bool{false, false, false}},
		{[]string{"s3", "dynamoDB"}, []bool{false, false, true}},
		{[]string{"s3"}, []bool{false, true, true}},
		{[]string{}, []bool{true, true, true}},
	}
	for _, testCase := range testCsvs {
		clients := GenerateAWSClients(testCase.clients...)
		assert.Equal(t, testCase.isNil[0], clients.s3DownloadClient == nil)
		assert.Equal(t, testCase.isNil[1], clients.dynamoClient == nil)
		assert.Equal(t, testCase.isNil[2], clients.kinesisClient == nil)
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

	res, err = client.FetchJSONFileFromS3("testJsonFetch", "a", func(b []byte) (interface{}, error) {
		return nil, fmt.Errorf("error fetching")
	})
	assert.True(t, err != nil)

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

	res, err = client.FetchCSVFileFromS3("testCsvFetch", "a", func(s [][]string) (interface{}, error) {
		return nil, fmt.Errorf("error fetching")
	})

	assert.True(t, err != nil)

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
	fakeJSONMock := []FakeKinesisDoc{{"test", "test"}}
	testCases := []struct {
		stream        string
		partitionKeys []string
		err           bool
	}{
		{"test", []string{"test", "test2"}, false},
		{"test", []string{"test"}, false},
		{"test", nil, false},
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
		{"test2", "test", "2018/08/10 22:09:55.166", true},
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
		err   bool
	}{
		{"test", "test", "", true},
		{"test2", "test", "2018/08/10 22:09:55.166", false},
	}
	for _, test := range testCases {
		res, err := client.FetchDynamoDBLastModified(test.table, test.key)
		assert.Equal(t, test.err, err != nil)
		assert.Equal(t, test.time, res)
	}
}

func TestWithDynamoDBGetLatest(t *testing.T) {
	//setup http for testing
	savedClient := http.DefaultClient
	http.DefaultClient = &http.Client{
		Transport: testHttp{},
	}
	mockDynamoClient := mockDynamoDBClient{}
	client := ClientsStruct{dynamoClient: &mockDynamoClient}

	testCases := []struct {
		url  string
		key  string
		err  bool
		resp bool
	}{
		{"127.0.0.2", "test", true, false},
		{"127.0.0.1", "testInvalid", true, false},
		{"127.0.0.1", "testValid", false, false},
		{"127.0.0.3", "testValid", false, true},
	}
	for _, test := range testCases {
		res, err := client.WithDynamoDBGetLatest(test.url, test.key)
		assert.Equal(t, test.err, err != nil)
		assert.Equal(t, test.resp, res != nil)
	}

	//return to default http
	http.DefaultClient = savedClient
}
