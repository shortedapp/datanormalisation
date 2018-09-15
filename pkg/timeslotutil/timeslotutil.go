package timeslotutil

import (
	"strconv"
	"time"
)

//GetPreviousDate Returns a int data number based of passed in time and option
// input option: select how far in the past you would like to calculate
// options include: 0 (1 year), 1 (1 month), 2 (1 week), 3 (1 day)
func GetPreviousDate(option int, now time.Time) int {
	var duration int
	switch option {
	case 0:
		duration, _ = strconv.Atoi(now.AddDate(-1, 0, 0).UTC().Format("20060102"))
	case 1:
		duration, _ = strconv.Atoi(now.AddDate(0, -1, 0).UTC().Format("20060102"))
	case 2:
		duration, _ = strconv.Atoi(now.AddDate(0, 0, -7).UTC().Format("20060102"))
	case 3:
		duration, _ = strconv.Atoi(now.AddDate(0, 0, -1).UTC().Format("20060102"))
	case 4:
		duration, _ = strconv.Atoi(now.UTC().Format("20060102"))
	}
	return duration
}
