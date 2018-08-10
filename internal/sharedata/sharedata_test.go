package sharedata

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestError(t *testing.T) {
	errorTest := lengthError{1}
	result := errorTest.Error()
	assert.Equal(t, "len is too short: 1", result)
}

func TestParseShareCsv(t *testing.T) {
	shareTest := ShareCsv{}
	testCases := []struct {
		strings []string
		isError bool
	}{
		{[]string{"a", "b"}, false},
		{[]string{"a", "b", "c"}, true},
	}
	for _, test := range testCases {
		err := shareTest.Parse(test.strings)
		assert.Equal(t, err == nil, test.isError)
	}
}

func TestParseAsicShortCsv(t *testing.T) {
	asicShortTest := AsicShortCsv{}
	testCases := []struct {
		strings []string
		isError bool
	}{
		{[]string{"a", "b", "c", "d", "e"}, false},
		{[]string{"a", "b", "c"}, false},
		{[]string{"a", "b", "10", "20", "10.9232"}, true},
	}
	for _, test := range testCases {
		err := asicShortTest.Parse(test.strings)
		assert.Equal(t, err == nil, test.isError)
	}
}

func TestUnmarshalSharesCSV(t *testing.T) {
	testCases := []struct {
		csv [][]string
		len int
	}{
		{[][]string{{"a", "b", "c"}}, 1},
		{[][]string{{"a", "b", "c"}, {"d", "f"}}, 1},
		{[][]string{{"a", "b", "c"}, {"d", "f", "g"}}, 2},
	}
	for _, test := range testCases {
		res, err := UnmarshalSharesCSV(test.csv)
		assert.Equal(t, err == nil, true)
		typedResult := res.([]*ShareCsv)
		assert.Equal(t, test.len, len(typedResult))
	}
}

func TestUnmarshalAsicShortsCSV(t *testing.T) {
	testCsvs := []struct {
		file      string
		delimiter rune
		expected  int
	}{
		{"../../test/data/sharedatashortstest.csv", '\t', 2},
	}
	for _, test := range testCsvs {
		b, err := ioutil.ReadFile(test.file)
		if err != nil {
			t.Error("unable to read file")
		}
		res, err := UnmarshalAsicShortsCSV(b)
		assert.Equal(t, err == nil, true)
		assert.Equal(t, test.expected, len(res))
	}
}

func TestUnmarshalSharesJSON(t *testing.T) {
	testCsvs := []struct {
		file     string
		expected int
	}{
		{"../../test/data/sharedatatest.json", 2},
	}
	for _, test := range testCsvs {
		b, err := ioutil.ReadFile(test.file)
		if err != nil {
			t.Error("unable to read file")
		}
		res, err := UnmarshalSharesJSON(b)
		result := res.([]*ShareJSON)
		assert.Equal(t, err == nil, true)
		assert.Equal(t, test.expected, len(result))
	}
}

func TestUnmarshalShortsJSON(t *testing.T) {
	testCsvs := []struct {
		file     string
		expected int
	}{
		{"../../test/data/asicshortdatatest.json", 2},
	}
	for _, test := range testCsvs {
		b, err := ioutil.ReadFile(test.file)
		if err != nil {
			t.Error("unable to read file")
		}
		res, err := UnmarshalShortsJSON(b)
		result := res.([]*AsicShortJSON)
		assert.Equal(t, err == nil, true)
		assert.Equal(t, test.expected, len(result))
	}
}
