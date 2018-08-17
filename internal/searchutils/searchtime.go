package searchutils

import (
	"time"

	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
)

type SearchPeriod int

const (
	//Year - generate a Yearly average
	Year SearchPeriod = iota
	//Month - generate a Monthly average
	Month
	//Week - generate a Weekly average
	Week
	//Day - generate a Daily average
	Day
	//Hour - generate an Hourly average
	Hour
	//Latest - generate an Hourly average
	Latest
)

func GetSearchWindow(a awsutils.AwsUtiler, tableName string, keyName string, period SearchPeriod) (int64, int64) {
	var duration time.Duration
	now := time.Now()
	switch period {
	case 0:
		duration = now.Sub(time.Unix(31536000, 0))
	case 1:
		//Update this to do exact month subtraction
		duration = now.Sub(time.Unix(2592000, 0))
	case 2:
		duration = now.Sub(time.Unix(604800, 0))
	case 3:
		duration = now.Sub(time.Unix(86400, 0))
	case 4:
		duration = now.Sub(time.Unix(3600, 0))
	case 5:
		//TODO update this value later
		res, err := a.FetchDynamoDBLastModified("lastUpdate", "test")
		timeRes, err := time.Parse(time.RFC3339, res)
		if err != nil {

		}
		duration = time.Duration(timeRes.UnixNano())
	}
	return time.Unix(0, duration.Nanoseconds()).UnixNano(), now.UnixNano()
}
