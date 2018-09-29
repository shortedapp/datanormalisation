package codedmoversquery

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/shortedapp/shortedfunctions/internal/moversdata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	"github.com/stretchr/testify/assert"
)

type mockCodedMoversQuery struct {
	TestOption int
	awsutil.AwsUtiler
}

func (m mockCodedMoversQuery) GetItemByPartDynamoDB(query *awsutil.DynamoDBItemQuery) (map[string]*dynamodb.AttributeValue, error) {
	if query.TableName == "fail" {
		return nil, fmt.Errorf("An Error Occurred")
	}

	dayChange := []string{"0.32", "0.62"}
	weekChange := []string{"1.32", "1.62"}
	monthChange := []string{"2.32", "2.62"}
	yearChange := []string{"3.32", "3.62"}
	return map[string]*dynamodb.AttributeValue{"DayChange": &dynamodb.AttributeValue{N: &dayChange[0]},
		"WeekChange":  &dynamodb.AttributeValue{N: &weekChange[0]},
		"MonthChange": &dynamodb.AttributeValue{N: &monthChange[0]},
		"YearChange":  &dynamodb.AttributeValue{N: &yearChange[0]}}, nil
}

func TestQueryOrderedTopMovers(t *testing.T) {
	testCases := []struct {
		tableName string
		res       float64
	}{
		{"fail", 0.0},
		{"test", 0.32},
	}
	tm := CodedMoversQuery{mockCodedMoversQuery{}}

	for _, test := range testCases {
		res := tm.QueryCodedTopMovers(test.tableName, "TST")
		if res == nil {
			assert.True(t, true)
		} else {
			assert.Equal(t, test.res, res.DayChange)
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
		{map[string]*dynamodb.AttributeValue{"DayChange": &dynamodb.AttributeValue{N: &correctNumString}, "WeekChange": &dynamodb.AttributeValue{N: &correctNumString},
			"MonthChange": &dynamodb.AttributeValue{N: &correctNumString}, "YearChange": &dynamodb.AttributeValue{N: &correctNumString}}, false},
		{map[string]*dynamodb.AttributeValue{
			"DayChange": &dynamodb.AttributeValue{N: &correctNumString}, "WeekChange": &dynamodb.AttributeValue{N: &correctNumString},
			"MonthChange": &dynamodb.AttributeValue{N: &correctNumString}}, true},
		{map[string]*dynamodb.AttributeValue{"Position": &dynamodb.AttributeValue{N: &badNumString},
			"DayChange": &dynamodb.AttributeValue{N: &correctNumString}, "WeekChange": &dynamodb.AttributeValue{N: &correctNumString},
			"MonthChange": &dynamodb.AttributeValue{N: &correctNumString}, "YearChange": &dynamodb.AttributeValue{N: &badNumString}}, true},
	}

	for _, test := range testCases {
		mover := moversdata.CodedTopMovers{}
		error := addNumElements(test.input, &mover)
		assert.Equal(t, test.err, error != nil)
	}
}
