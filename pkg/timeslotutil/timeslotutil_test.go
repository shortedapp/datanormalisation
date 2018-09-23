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
