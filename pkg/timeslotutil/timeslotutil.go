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

func GetPreviousWeekdayDate(option int, now time.Time) int {
	var duration int
	switch option {
	case 0:
		now = now.AddDate(-1, 0, 0)
		now = BackDateToWeekday(now)
		duration, _ = strconv.Atoi(now.Format("20060102"))
	case 1:
		now = now.AddDate(0, -1, 0)
		now = BackDateToWeekday(now)
		duration, _ = strconv.Atoi(now.Format("20060102"))
	case 2:
		now = now.AddDate(0, 0, -7)
		now = BackDateToWeekday(now)
		duration, _ = strconv.Atoi(now.Format("20060102"))
	case 3:
		now = now.AddDate(0, 0, -1)
		now = BackDateToWeekday(now)
		duration, _ = strconv.Atoi(now.Format("20060102"))
	case 4:
		now = BackDateToWeekday(now)
		duration, _ = strconv.Atoi(now.Format("20060102"))
	}
	return duration
}

func BackDateToWeekday(t time.Time) time.Time {
	dayOfWeek := t.Weekday()
	if dayOfWeek == time.Saturday {
		return t.AddDate(0, 0, -1)
	} else if dayOfWeek == time.Sunday {
		return t.AddDate(0, 0, -2)
	}
	return t

}
func GetPreviousDateMinusDaysString(days int, now time.Time) string {
	return now.AddDate(0, 0, -days).Format("20060102")
}

func GetPreviousDateMinusMonthsString(months int, now time.Time) string {
	return now.AddDate(0, -months, 0).Format("20060102")
}

func GetPreviousDateMinusYearsString(years int, now time.Time) string {
	return now.AddDate(-years, 0, 0).Format("20060102")
}

func GetDatePlusDaysString(days int, date time.Time) string {
	return date.AddDate(0, 0, days).Format("20060102")
}
