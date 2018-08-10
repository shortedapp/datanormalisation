package awsutils

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/stretchr/testify/assert"
)

type mockS3DownloadClient struct {
	s3manageriface.DownloaderAPI
}

func (m *mockS3DownloadClient) Download(w io.WriterAt, input *s3.GetObjectInput, functions ...func(*s3manager.Downloader)) (int64, error) {
	switch *input.Bucket {
	case "testJsonFetch":
		file, _ := ioutil.ReadFile("../../test/data/sharedatatest.json")
		w.WriteAt(file, 0)
	}
	// mock response/functionality
	return 100, nil
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
		assert.Equal(t, testCase.isNil[1], clients.dynamoclient == nil)
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
