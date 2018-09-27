package topmoversquery

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/shortedapp/shortedfunctions/internal/moversdata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	"github.com/stretchr/testify/assert"
)

type mockTopMoversQuery struct {
	TestOption int
	awsutil.AwsUtiler
}

func (m mockTopMoversQuery) BatchGetItemsDynamoDB(tableName string, field string, keys []interface{}) ([]map[string]*dynamodb.AttributeValue, error) {
	if tableName == "fail" {
		return nil, fmt.Errorf("An Error Occurred")
	}
	result := make([]map[string]*dynamodb.AttributeValue, 0, 2)
	position := []string{"1", "0"}
	dayChange := []string{"0.32", "0.62"}
	weekChange := []string{"1.32", "1.62"}
	monthChange := []string{"2.32", "2.62"}
	yearChange := []string{"3.32", "3.62"}
	dayCode := []string{"TLS", "TLS"}
	weekCode := []string{"TST", "SLS"}
	monthCode := []string{"ABC", "RES"}
	yearCode := []string{"STA", "TRS"}
	result = append(result, map[string]*dynamodb.AttributeValue{"Position": &dynamodb.AttributeValue{N: &position[0]},
		"DayChange":   &dynamodb.AttributeValue{N: &dayChange[0]},
		"WeekChange":  &dynamodb.AttributeValue{N: &weekChange[0]},
		"MonthChange": &dynamodb.AttributeValue{N: &monthChange[0]},
		"YearChange":  &dynamodb.AttributeValue{N: &yearChange[0]},
		"DayCode":     &dynamodb.AttributeValue{S: &dayCode[0]},
		"WeekCode":    &dynamodb.AttributeValue{S: &weekCode[0]},
		"MonthCode":   &dynamodb.AttributeValue{S: &monthCode[0]},
		"YearCode":    &dynamodb.AttributeValue{S: &yearCode[0]}})
	result = append(result, map[string]*dynamodb.AttributeValue{"Position": &dynamodb.AttributeValue{N: &position[1]},
		"DayChange":   &dynamodb.AttributeValue{N: &dayChange[1]},
		"WeekChange":  &dynamodb.AttributeValue{N: &weekChange[1]},
		"MonthChange": &dynamodb.AttributeValue{N: &monthChange[1]},
		"YearChange":  &dynamodb.AttributeValue{N: &yearChange[1]},
		"DayCode":     &dynamodb.AttributeValue{S: &dayCode[1]},
		"WeekCode":    &dynamodb.AttributeValue{S: &weekCode[1]},
		"MonthCode":   &dynamodb.AttributeValue{S: &monthCode[1]}})
	return result, nil
}

func TestQueryOrderedTopMovers(t *testing.T) {
	testCases := []struct {
		tableName string
		res       []int
	}{
		{"fail", nil},
		{"test", []int{0, 1}},
	}
	tm := TopMoversQuery{mockTopMoversQuery{}}

	for _, test := range testCases {
		res := tm.QueryOrderedTopMovers(test.tableName, 5)
		if res == nil {
			assert.True(t, test.res == nil)
		} else {
			for i, elem := range res {
				assert.Equal(t, test.res[i], elem.Order)
			}
		}

	}
}

func TestAddNumElements(t *testing.T) {

	correctNumString := "1"
	badNumString := "asd"
	testCases := []struct {
		input map[string]*dynamodb.AttributeValue
		err   bool
	}{
		{map[string]*dynamodb.AttributeValue{"Position": &dynamodb.AttributeValue{N: &correctNumString},
			"DayChange": &dynamodb.AttributeValue{N: &correctNumString}, "WeekChange": &dynamodb.AttributeValue{N: &correctNumString},
			"MonthChange": &dynamodb.AttributeValue{N: &correctNumString}, "YearChange": &dynamodb.AttributeValue{N: &correctNumString}}, false},
		{map[string]*dynamodb.AttributeValue{"Position": &dynamodb.AttributeValue{N: &correctNumString},
			"DayChange": &dynamodb.AttributeValue{N: &correctNumString}, "WeekChange": &dynamodb.AttributeValue{N: &correctNumString},
			"MonthChange": &dynamodb.AttributeValue{N: &correctNumString}}, true},
		{map[string]*dynamodb.AttributeValue{"Position": &dynamodb.AttributeValue{N: &badNumString},
			"DayChange": &dynamodb.AttributeValue{N: &correctNumString}, "WeekChange": &dynamodb.AttributeValue{N: &correctNumString},
			"MonthChange": &dynamodb.AttributeValue{N: &correctNumString}, "YearChange": &dynamodb.AttributeValue{N: &correctNumString}}, true},
	}

	for _, test := range testCases {
		mover := moversdata.OrderedTopMovers{}
		error := addNumElements(test.input, &mover)
		assert.Equal(t, test.err, error != nil)
	}
}

func TestAddStringElements(t *testing.T) {

	codeString := "TST"
	testCases := []struct {
		input map[string]*dynamodb.AttributeValue
		err   bool
	}{
		{map[string]*dynamodb.AttributeValue{"DayCode": &dynamodb.AttributeValue{S: &codeString}, "WeekCode": &dynamodb.AttributeValue{S: &codeString},
			"MonthCode": &dynamodb.AttributeValue{S: &codeString}, "YearCode": &dynamodb.AttributeValue{S: &codeString}}, false},
		{map[string]*dynamodb.AttributeValue{"DayCode": &dynamodb.AttributeValue{S: &codeString}, "WeekCode": &dynamodb.AttributeValue{S: &codeString},
			"MonthCode": &dynamodb.AttributeValue{S: &codeString}}, true},
	}

	for _, test := range testCases {
		mover := moversdata.OrderedTopMovers{}
		error := addStringElements(test.input, &mover)
		assert.Equal(t, test.err, error != nil)
	}
}
