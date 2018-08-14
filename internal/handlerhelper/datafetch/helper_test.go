package datafetch

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/shortedapp/shortedfunctions/pkg/testingutil"

	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
	"github.com/stretchr/testify/assert"
)

type mockAwsUtilClients struct {
	TestOption int
}

func (m mockAwsUtilClients) FetchCSVFileFromS3(bucketName string, key string, f func(s [][]string) (interface{}, error)) (interface{}, error) {

	return nil, nil
}

func (m mockAwsUtilClients) WithDynamoDBGetLatest(string, string) (*http.Response, error) {
	return nil, nil
}
func (m mockAwsUtilClients) FetchDynamoDBLastModified(string, string) (string, error) {
	return "", nil
}
func (m mockAwsUtilClients) PutDynamoDBLastModified(string, string, string) error {
	return nil
}
func (m mockAwsUtilClients) PutKinesisRecords(*string, []interface{}, []string) error {
	return nil
}
func (m mockAwsUtilClients) FetchJSONFileFromS3(string, string, func([]byte) (interface{}, error)) (interface{}, error) {
	return nil, nil
}
func (m mockAwsUtilClients) PutFileToS3(string, string, []byte) error {
	if m.TestOption == 0 {
		return fmt.Errorf("unable to put to s3")
	}
	return nil
}

type testHttp struct {
	testCase int
}

func (t testHttp) RoundTrip(request *http.Request) (*http.Response, error) {
	if t.testCase == 0 {
		return nil, fmt.Errorf("failed")
	}
	b, _ := ioutil.ReadFile("../../../test/data/datafetchhelper.csv")
	ioutil.NopCloser(bytes.NewReader(b))
	//Test  WithDynamoDBGetLatest valid head last modified time
	return &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
		Header:     nil,
		Body:       ioutil.NopCloser(bytes.NewReader(b)),
	}, nil
}

func TestFetchRoutine(t *testing.T) {
	//Test function tpo pass out information via a channel
	chanTest := make(chan int, 1)
	f := func() {
		chanTest <- 1
	}
	d := Datafetch{}
	d.FetchRoutine(f)

	//check the routine ran
	assert.Equal(t, 1, <-chanTest)
}

func TestAsxCodeFetch(t *testing.T) {
	log.Logger.Vlogging = true
	log.Logger.Level = 1
	savedClient := http.DefaultClient
	testCases := []struct {
		testOption int
		contains   string
	}{
		{0, "unable to fetch"},
		{1, "completed put"},
	}
	for _, testCase := range testCases {
		http.DefaultClient = &http.Client{
			Transport: testHttp{testCase.testOption},
		}
		d := Datafetch{mockAwsUtilClients{testCase.testOption}}
		str := testingutil.CaptureStandardErr(func() { d.AsxCodeFetch() }, log.Logger.StdLogger)
		assert.True(t, strings.Contains(str, testCase.contains))
	}
	http.DefaultClient = savedClient
}

func TestFilterLines(t *testing.T) {
	b, _ := ioutil.ReadFile("../../../test/data/datafetchhelper.csv")
	resp := http.Response{}
	resp.Body = ioutil.NopCloser(bytes.NewReader(b))
	result := filterLines(&resp)
	b2, _ := ioutil.ReadFile("../../../test/data/datafetchresult.csv")
	assert.Equal(t, result, b2)
}
