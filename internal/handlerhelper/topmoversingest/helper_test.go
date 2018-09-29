package topmoversingest

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/shortedapp/shortedfunctions/internal/moversdata"
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

func (m mockAwsUtilClients) SendAthenaQuery(query string, database string) ([]*athena.ResultSet, error) {
	return nil, nil
}

func mockOrderedTopMoversAthena(option int) *athena.ResultSet {
	strings := []string{}
	switch option {
	case 0:
		strings = []string{"99", "EXR", "10.1", "MEU", "0.0", "CVF", "0.3", "CSL", "99"}
	case 1:
		strings = []string{"99", "EXR", "asd.1", "MEU", "0.0", "CVF", "0.3", "EXR", "99"}
	}
	data := make([]*athena.Datum, 0)
	for i := range strings {
		data = append(data, &athena.Datum{VarCharValue: &strings[i]})
	}
	row := &athena.Row{Data: data}
	rows := []*athena.Row{row}
	return &athena.ResultSet{Rows: rows}
}

func mockCodedTopMoversAthena(option int) *athena.ResultSet {
	strings := []string{}
	switch option {
	case 0:
		strings = []string{"EXR", "10.1", "0.0", "0.3", "1.3"}
	case 1:
		strings = []string{"EXR", "10.1", "0.0", "", "asd"}
	}

	data := make([]*athena.Datum, 0)
	for _, s := range strings {
		data = append(data, &athena.Datum{VarCharValue: &s})
	}
	row := &athena.Row{Data: data}
	rows := []*athena.Row{row}
	return &athena.ResultSet{Rows: rows}
}

func TestOrderedTopMoversMapper(t *testing.T) {
	realStruct := moversdata.OrderedTopMovers{Order: 1, DayCode: "tst", DayChange: 0.123, WeekCode: "tst",
		WeekChange: 0.123, MonthCode: "tst", MonthChange: 1.5423, YearCode: "tst", YearChange: 3.452}
	interfaceSlice := make([]*interface{}, 0, 1)
	realStructInterface := reflect.ValueOf(realStruct).Interface()
	interfaceSlice = append(interfaceSlice, &realStructInterface)

	testCases := []struct {
		data interface{}
		err  bool
	}{
		{interfaceSlice, false},
		{realStructInterface, true},
	}
	for _, test := range testCases {
		result, err := OrderedTopMoversMapper(test.data, 0)
		assert.Equal(t, test.err, err != nil)
		if err == nil {
			res := *result[0]
			assert.True(t, res["Position"].(int) == 1)
		}
	}

}

func TestCodedTopMoversMapper(t *testing.T) {
	realStruct := moversdata.CodedTopMovers{Code: "tst", DayChange: 0.123, WeekChange: 0.123, MonthChange: 1.5423, YearChange: 3.452}
	interfaceSlice := make([]*interface{}, 0, 1)
	realStructInterface := reflect.ValueOf(realStruct).Interface()
	interfaceSlice = append(interfaceSlice, &realStructInterface)

	testCases := []struct {
		data interface{}
		err  bool
	}{
		{interfaceSlice, false},
		{realStructInterface, true},
	}
	for _, test := range testCases {
		result, err := CodedTopMoversMapper(test.data, 0)
		assert.Equal(t, test.err, err != nil)
		if err == nil {
			res := *result[0]
			assert.True(t, res["Code"].(string) == "tst")
		}
	}

}

func TestConvertListOfResults(t *testing.T) {
	resultList := make([]*athena.ResultSet, 0)
	for i := 0; i < 2; i++ {
		resultList = append(resultList, mockOrderedTopMoversAthena(0))
	}
	result := convertListOfResults(resultList, athenaToTopMovers)
	convertedResult := result.([]*interface{})
	assert.True(t, len(convertedResult) == 2)
}

func TestAthenaToMoversByCode(t *testing.T) {
	testCases := []struct {
		option int
		err    bool
	}{
		{0, false},
		{1, true},
	}
	for _, test := range testCases {
		_, err := athenaToMoversByCode(mockCodedTopMoversAthena(test.option).Rows[0])
		assert.Equal(t, test.err, err != nil)
	}
}

func TestAthenaToTopMovers(t *testing.T) {
	testCases := []struct {
		option int
		err    bool
	}{
		{0, false},
		{1, true},
	}
	for _, test := range testCases {
		_, err := athenaToTopMovers(mockOrderedTopMoversAthena(test.option).Rows[0])
		assert.Equal(t, test.err, err != nil)
	}
}

func TestGenerateViews(t *testing.T) {
	client := mockAwsUtilClients{TestOption: 0}
	tm := Topmoversingestor{Clients: client}
	tm.generateViews()
	//ensure no panics occure here
	assert.True(t, true)
}
