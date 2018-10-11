package bulknormalise

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/shortedapp/shortedfunctions/internal/sharedata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	"github.com/shortedapp/shortedfunctions/pkg/testingutil"
	"github.com/stretchr/testify/assert"
)

type mockAwsUtilClients struct {
	TestOption int
	awsutil.AwsUtiler
}

type testHttp struct {
	testOption int
}

func (t testHttp) RoundTrip(request *http.Request) (*http.Response, error) {
	//Test  WithDynamoDBGetLatest valid head last modified time
	if t.testOption == 0 {
		return &http.Response{
			Status:     "200 OK",
			StatusCode: 200,
		}, nil
		//Test  WithDynamoDBGetLatest invalid head last modified time
	} else if t.testOption == 1 {
		resp := &http.Response{}
		file, _ := ioutil.ReadFile("../test/data/mainasicshorttest.csv")
		resp.Body = ioutil.NopCloser(bytes.NewReader(file))
		resp.ContentLength = -1
		return resp, nil
	}
	return nil, fmt.Errorf("could not reach url")
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
	if m.TestOption == 1 {
		return nil, fmt.Errorf("error")
	}
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

func TestGetShortPositions(t *testing.T) {
	savedClient := http.DefaultClient
	client := mockAwsUtilClients{}
	b := Bulknormalise{Clients: client}
	testCases := []struct {
		testOption int
		isNil      bool
	}{
		{1, false},
		{2, true},
	}
	for _, test := range testCases {
		http.DefaultClient = &http.Client{
			Transport: testHttp{testOption: test.testOption},
		}
		result := b.GetShortPositions("20180109")
		assert.Equal(t, test.isNil, result == nil)
	}

	http.DefaultClient = savedClient
}

func TestMergeShortData(t *testing.T) {

	testCases := []struct {
		shorts map[string]*sharedata.AsicShortCsv
		codes  map[string]*sharedata.ShareCsv
		data   []*sharedata.CombinedShortJSON
	}{
		{
			map[string]*sharedata.AsicShortCsv{"TST": &sharedata.AsicShortCsv{Name: "TEST", Code: "TST", Shorts: 10, Total: 20, Percent: 0.5}},
			map[string]*sharedata.ShareCsv{"TST": &sharedata.ShareCsv{Name: "TEST", Code: "TST", Industry: "Test industries"}},
			[]*sharedata.CombinedShortJSON{{Name: "TEST", Code: "TST", Shorts: 10, Total: 20, Percent: 0.5, Industry: "Test industries"}},
		},
		{
			nil,
			map[string]*sharedata.ShareCsv{"TST": &sharedata.ShareCsv{Name: "TEST", Code: "TST", Industry: "Test industries"}},
			nil,
		},
		{
			nil,
			nil,
			nil,
		},
	}
	for _, test := range testCases {
		b := Bulknormalise{}
		result := b.MergeShortData(test.shorts, test.codes)
		assert.Equal(t, test.data, result)
	}
}

func TestNormaliseRoutine(t *testing.T) {
	log.Logger.Vlogging = true
	log.Logger.Level = 1
	testCases := []struct {
		option int
		msg    string
	}{
		{1, "Unable to get last updated data, aborting"},
		{0, "Unable to get last ASX codes, aborting"},
		{2, "finishing routine"},
	}
	for _, test := range testCases {
		mockClient := mockAwsUtilClients{TestOption: test.option}
		b := Bulknormalise{Clients: mockClient}

		result := testingutil.CaptureStandardErr(func() { b.NormaliseRoutine(0, 1) }, log.Logger.StdLogger)
		assert.True(t, strings.Contains(result, test.msg))
	}
}
