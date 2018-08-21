package searchutils

import (
	"fmt"
	"testing"
	"time"

	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
	"github.com/stretchr/testify/assert"
)

type Searchutilclient struct {
	awsutils.AwsUtiler
}

func (s Searchutilclient) FetchDynamoDBLastModified(tableName string, keyName string) (string, error) {
	if tableName == "test" {
		return "", fmt.Errorf("error")
	}
	return "2006-01-02T15:04:05Z07:00", nil
}

func TestGetSearchWindow(t *testing.T) {

	client := Searchutilclient{}
	testCases := []struct {
		table  string
		period SearchPeriod
		result int64
	}{
		{"test", Year, 31536000000000000},
		{"test", Month, 2592000000000000},
		{"test", Week, 604800000000000},
		{"test", Day, 86400000000000},
		{"test", Latest, 0},
		{"test2", Year, 31536000000000000},
		{"test2", Month, 2592000000000000},
		{"test2", Week, 604800000000000},
		{"test2", Day, 86400000000000},
		{"test2", Latest, time.Now().UnixNano()},
	}

	for _, test := range testCases {
		res, res2 := GetSearchWindow(client, test.table, "", test.period)
		if test.period == Latest && test.table != "test" {
			assert.True(t, test.result > (res2-res))
		} else {
			assert.Equal(t, test.result, res2-res)
		}
	}
}
