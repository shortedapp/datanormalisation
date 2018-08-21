package ingestionutils

import (
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

func UpdateDynamoWriteUnits(clients awsutils.AwsUtiler, tableName string, write int64) (int64, int64) {
	readUnits, writeUnits := clients.GetDynamoDBTableThroughput(tableName)
	err := clients.UpdateDynamoDBTableCapacity(tableName, readUnits, write)
	if err != nil {
		log.Warn("IngestRoutine", "unable to update write capacity units")
		return readUnits, writeUnits
	}

	readThroughput, writeThroughput := clients.GetDynamoDBTableThroughput(tableName)
	return readThroughput, writeThroughput
}
