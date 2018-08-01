package scheduledGet

import (
	"testing"
)

func TestScheduledGetWithDynamoDB(t *testing.T) {
	ScheduledGetWithDynamoDB("https://asic.gov.au/Reports/Daily/2018/07/RR20180726-001-SSDailyAggShortPos.csv")
}
