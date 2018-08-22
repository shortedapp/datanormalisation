package searchutils

import (
	"fmt"
	"strconv"
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
	var duration int
	now := time.Now()
	nowDate, _ := strconv.Atoi(now.UTC().Format("20060102"))
	switch period {
	case 0:
		duration, _ = strconv.Atoi(now.AddDate(-1, 0, 0).UTC().Format("20060102"))
	case 1:
		duration, _ = strconv.Atoi(now.AddDate(0, -1, 0).UTC().Format("20060102"))
	case 2:
		duration, _ = strconv.Atoi(now.AddDate(0, 0, -7).UTC().Format("20060102"))
	case 3:
		duration, _ = strconv.Atoi(now.AddDate(0, 0, -1).UTC().Format("20060102"))
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
		duration, _ = strconv.Atoi(timeRes.UTC().Format("20060102"))
	}
	return int64(duration), int64(nowDate)
}
