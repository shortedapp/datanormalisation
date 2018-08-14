package datanormalise

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/shortedapp/shortedfunctions/internal/sharedata"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
	"github.com/shortedapp/shortedfunctions/pkg/testingutil"
	"github.com/stretchr/testify/assert"
)

type mockAwsUtilClients struct {
	TestOption int
}

func (m mockAwsUtilClients) FetchCSVFileFromS3(bucketName string, key string, f func(s [][]string) (interface{}, error)) (interface{}, error) {
	if m.TestOption == 0 {
		return nil, fmt.Errorf("test failure")
	}
	sharesample := sharedata.ShareCsv{Name: "test"}

	return []*sharedata.ShareCsv{&sharesample}, nil
}

func (m mockAwsUtilClients) WithDynamoDBGetLatest(string, string) (*http.Response, error) {
	if m.TestOption == 0 {
		return nil, fmt.Errorf("test failure")
	}
	resp := &http.Response{}
	//Create new readcloser with fake data for response
	file, _ := ioutil.ReadFile("../test/data/mainasicshorttest.csv")
	resp.Body = ioutil.NopCloser(bytes.NewReader(file))
	resp.ContentLength = -1

	//Return response
	return resp, nil
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

func TestGetShareCodes(t *testing.T) {

	sharesample := sharedata.ShareCsv{Name: "test"}
	codesReady := make(chan map[string]*sharedata.ShareCsv, 1)
	testCases := []struct {
		testOption int
		val        *sharedata.ShareCsv
	}{
		{0, nil},
		{1, &sharesample},
	}
	for _, test := range testCases {
		client := mockAwsUtilClients{TestOption: test.testOption}
		d := Datanormalise{Clients: client}
		d.GetShareCodes(codesReady)
		output := <-codesReady
		for _, val := range output {
			assert.Equal(t, test.val, val)
		}
	}
}

func TestGetShortPositions(t *testing.T) {
	shortsReady := make(chan map[string]*sharedata.AsicShortCsv, 1)
	testCases := []struct {
		testOption int
		val        string
	}{
		{0, ""},
		{1, "1-PAGE LTD ORDINARY"},
	}
	for _, test := range testCases {
		client := mockAwsUtilClients{TestOption: test.testOption}
		d := Datanormalise{Clients: client}
		d.GetShortPositions(shortsReady)
		output := <-shortsReady
		for _, val := range output {
			assert.Equal(t, test.val, val.Name)
			break
		}
	}
}
func TestMergeShortData(t *testing.T) {
	client := mockAwsUtilClients{}
	d := Datanormalise{Clients: client}
	shortsReady := make(chan map[string]*sharedata.AsicShortCsv, 1)
	codesReady := make(chan map[string]*sharedata.ShareCsv, 1)

	fakeCode := sharedata.ShareCsv{Name: "test", Code: "TST", Industry: "TEST"}
	fakeCodes := make(map[string]*sharedata.ShareCsv, 1)
	fakeCodes[fakeCode.Code] = &fakeCode

	fakeShort := sharedata.AsicShortCsv{Name: "test", Code: "TST", Shorts: 10, Total: 20, Percent: 10}
	fakeShorts := make(map[string]*sharedata.AsicShortCsv, 1)
	fakeShorts[fakeShort.Code] = &fakeShort

	testCases := []struct {
		shortInput map[string]*sharedata.AsicShortCsv
		codeInput  map[string]*sharedata.ShareCsv
		result     []*sharedata.CombinedShortJSON
	}{
		{nil, nil, []*sharedata.CombinedShortJSON(nil)},
		{make(map[string]*sharedata.AsicShortCsv, 1), fakeCodes, []*sharedata.CombinedShortJSON{{Name: "test", Code: "TST", Industry: "TEST"}}},
		{fakeShorts, fakeCodes, []*sharedata.CombinedShortJSON{{Name: "test", Code: "TST", Industry: "TEST", Shorts: 10, Total: 20, Percent: 10}}},
	}
	for _, test := range testCases {
		shortsReady <- test.shortInput
		codesReady <- test.codeInput
		result := d.MergeShortData(shortsReady, codesReady)
		assert.Equal(t, test.result, result)
	}
}

func TestUploadData(t *testing.T) {
	log.Logger.Vlogging = true
	log.Logger.Level = 1
	testCases := []struct {
		testOption int
		input      []*sharedata.CombinedShortJSON
		output     string
	}{
		{0, []*sharedata.CombinedShortJSON{{Name: "test", Code: "TST", Industry: "TEST"}}, "unable to upload to S3"},
		{1, []*sharedata.CombinedShortJSON{{Name: "test2", Code: "TST", Industry: "TEST"}}, ""},
	}
	for _, test := range testCases {
		client := mockAwsUtilClients{TestOption: test.testOption}
		d := Datanormalise{Clients: client}
		output := testingutil.CaptureStandardErr(func() { d.UploadData(test.input) }, log.Logger.StdLogger)
		assert.True(t, strings.Contains(output, test.output))
	}
}
