package topshortsquery

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	"github.com/stretchr/testify/assert"
)

type mockAwsUtilClients struct {
	TestOption int
	awsutil.AwsUtiler
}

func (m mockAwsUtilClients) BatchGetItemsDynamoDB(tableName string, field string, keys []interface{}) ([]map[string]*dynamodb.AttributeValue, error) {
	if tableName == "fail" {
		return nil, fmt.Errorf("An Error Occurred")
	}
	result := make([]map[string]*dynamodb.AttributeValue, 0, 2)
	codes := []string{"TST", "TST2"}
	position := []string{"1", "0"}
	percent := []string{"0.32", "0.62"}
	result = append(result, map[string]*dynamodb.AttributeValue{"Code": &dynamodb.AttributeValue{S: &codes[0]},
		"Position": &dynamodb.AttributeValue{N: &position[0]},
		"Percent":  &dynamodb.AttributeValue{N: &percent[0]}})
	result = append(result, map[string]*dynamodb.AttributeValue{"Code": &dynamodb.AttributeValue{S: &codes[1]},
		"Position": &dynamodb.AttributeValue{N: &position[1]},
		"Percent":  &dynamodb.AttributeValue{N: &percent[1]}})
	return result, nil
}

func TestQueryTopShorted(t *testing.T) {
	testCases := []struct {
		tableName string
		res       []int64
	}{
		{"fail", nil},
		{"test", []int64{0, 1}},
	}
	tc := Topshortsquery{mockAwsUtilClients{}}

	for _, test := range testCases {
		res := tc.QueryTopShorted(test.tableName, 5)
		if res == nil {
			assert.True(t, test.res == nil)
		} else {
			for i, elem := range res {
				assert.Equal(t, test.res[i], elem.Position)
			}
		}

	}
}
