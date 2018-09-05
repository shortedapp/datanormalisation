package ingestionutils

import (
	"fmt"
	"testing"

	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
	"github.com/shortedapp/shortedfunctions/pkg/testingutil"
)

type Ingestionutilclient struct {
	awsutil.AwsUtiler
}

func (i Ingestionutilclient) UpdateDynamoDBTableCapacity(table string, read int64, write int64) error {
	if table == "fail" {
		return fmt.Errorf("error")
	}
	return nil
}

func (i Ingestionutilclient) GetDynamoDBTableThroughput(table string) (int64, int64) {
	return 5, 5
}

func TestUpdateDynamoWriteUnits(t *testing.T) {
	client := Ingestionutilclient{}
	testCases := []struct {
		table string
		err   string
	}{
		{"fail", "IngestRoutine"},
		{"test", ""},
	}
	for _, test := range testCases {
		testingutil.CaptureStandardErr(func() { UpdateDynamoWriteUnits(client, test.table, 5) }, log.Logger.StdLogger)
	}
}
