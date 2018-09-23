package csvutil

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadCSVBytesNoChecks(t *testing.T) {
	testCsvs := []struct {
		file      string
		delimiter rune
		expected  [][]string
	}{
		{"../../test/data/csvutiltest.csv", ',', [][]string{{"test", "abc"}, {"test2", "dfg"}}},
		{"../../test/data/csvutiltest2.csv", ' ', [][]string{{"test", "abc"}, {"test2", "dfg"}}},
	}
	for _, testCase := range testCsvs {
		dat, err := ioutil.ReadFile(testCase.file)
		if err != nil {
			t.Error("failed to read test csv")
		}
		result, err := ReadCSVBytesNoChecks(dat, testCase.delimiter)
		if err != nil {
			t.Error("failed to convert csv")
		}
		for i, line := range result {
			for j, str := range line {
				assert.Equal(t, testCase.expected[i][j], str)
			}
		}
	}

}
