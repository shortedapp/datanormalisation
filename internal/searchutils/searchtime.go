package searchutils

import (
	"fmt"
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
		//TODO update this value later
		res, err := a.FetchDynamoDBLastModified(tableName, keyName)
		if err != nil {
			return -1, -1
		}
		timeRes, err := time.Parse(time.RFC3339, res)
		if err != nil {
			fmt.Println(err.Error())
			return -1, -1
		}
		duration = time.Duration(timeRes.UnixNano())
	}
	return time.Unix(0, duration.Nanoseconds()).UnixNano(), now.UnixNano()
}
