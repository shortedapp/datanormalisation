package searchutil

import (
	"strconv"
	"strings"
	"time"

	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	"github.com/shortedapp/shortedfunctions/pkg/timeslotutil"
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

func GetSearchWindow(a awsutil.AwsUtiler, tableName string, keyName string, period SearchPeriod) (int64, int64) {
	var duration int
	now := time.Now()
	nowDate, _ := strconv.Atoi(now.UTC().Format("20060102"))
	switch period {
	case 0, 1, 2, 3:
		duration = timeslotutil.GetPreviousDate(int(period), now)
	case 4:
		res, err := a.FetchDynamoDBLastModified(tableName, keyName)
		if err != nil {
			return -1, -1
		}
		timeRes, err := time.Parse(time.RFC3339, res)
		if err != nil {
			return -1, -1
		}
		duration, _ = strconv.Atoi(timeRes.UTC().Format("20060102"))
	}
	return int64(duration), int64(nowDate)
}

func StringToSearchPeriod(s string) SearchPeriod {
	switch strings.ToLower(s) {
	case "day":
		return Day
	case "week":
		return Week
	case "month":
		return Month
	case "year":
		return Year
	default:
		return Week
	}
}
