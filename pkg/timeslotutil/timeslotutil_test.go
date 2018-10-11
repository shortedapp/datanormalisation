package timeslotutil

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetPreviousDate(t *testing.T) {
	now := time.Now()
	nowDate, _ := strconv.Atoi(now.UTC().Format("20060102"))
	testCases := []struct {
		option int
		result int
	}{
		{0, 1},
		{1, 1},
		{2, 7},
		{3, 1},
	}

	for _, test := range testCases {
		res := GetPreviousDate(test.option, now)
		if test.option == 0 {
			assert.True(t, test.result <= (nowDate/10000-res/10000))
		} else if test.option == 1 {
			assert.True(t, test.result <= nowDate-res)
		} else if test.option == 2 {
			diff := nowDate - res
			assert.True(t, (test.result == diff || diff > 21))
		} else if test.option == 3 {
			diff := nowDate%100 - res%100
			assert.True(t, (test.result == diff || diff > 27))
		}
	}
}

func TestGetPreviousWeekdayDate(t *testing.T) {
	now, _ := time.Parse(time.RFC1123, "Sun, 07 Oct 2018 12:04:05 AEST")
	testCases := []struct {
		option int
		result int
	}{
		{0, 20171006},
		{1, 20180907},
		{2, 20180928},
		{3, 20181005},
	}

	for _, test := range testCases {
		res := GetPreviousWeekdayDate(test.option, now)
		assert.Equal(t, test.result, res)
	}
}

func TestBackDateBusinessDays(t *testing.T) {
	now, _ := time.Parse(time.RFC1123, "Sun, 07 Oct 2018 12:04:05 AEST")
	testCases := []struct {
		days   int
		result int
	}{
		{1, 4},
		{3, 2},
	}

	for _, test := range testCases {
		res := BackDateBusinessDays(now, test.days)
		assert.Equal(t, test.result, res.Day())
	}
}

func TestGetPreviousDateMinusBusinessDaysString(t *testing.T) {
	now, _ := time.Parse(time.RFC1123, "Sun, 07 Oct 2018 12:04:05 AEST")
	testCases := []struct {
		days   int
		result string
	}{
		{1, "20181004"},
		{3, "20181002"},
	}

	for _, test := range testCases {
		res := GetPreviousDateMinusBusinessDaysString(now, test.days)
		assert.Equal(t, test.result, res)
	}
}

func TestGetPreviousDateMinusDaysString(t *testing.T) {
	now, _ := time.Parse(time.RFC1123, "Sun, 07 Oct 2018 12:04:05 AEST")
	testCases := []struct {
		days   int
		result string
	}{
		{1, "20181006"},
		{3, "20181004"},
	}

	for _, test := range testCases {
		res := GetPreviousDateMinusDaysString(test.days, now)
		assert.Equal(t, test.result, res)
	}
}

func TestGetPreviousDateMinusMonthsString(t *testing.T) {
	now, _ := time.Parse(time.RFC1123, "Sun, 07 Oct 2018 12:04:05 AEST")
	testCases := []struct {
		months int
		result string
	}{
		{1, "20180907"},
		{3, "20180707"},
	}

	for _, test := range testCases {
		res := GetPreviousDateMinusMonthsString(test.months, now)
		assert.Equal(t, test.result, res)
	}
}

func TestGetPreviousDateMinusYearsString(t *testing.T) {
	now, _ := time.Parse(time.RFC1123, "Sun, 07 Oct 2018 12:04:05 AEST")
	testCases := []struct {
		years  int
		result string
	}{
		{1, "20171007"},
		{3, "20151007"},
	}

	for _, test := range testCases {
		res := GetPreviousDateMinusYearsString(test.years, now)
		assert.Equal(t, test.result, res)
	}
}

func TestGetDatePlusDaysString(t *testing.T) {
	now, _ := time.Parse(time.RFC1123, "Sun, 07 Oct 2018 12:04:05 AEST")
	testCases := []struct {
		days   int
		result string
	}{
		{1, "20181008"},
		{3, "20181010"},
	}

	for _, test := range testCases {
		res := GetDatePlusDaysString(test.days, now)
		assert.Equal(t, test.result, res)
	}
}
