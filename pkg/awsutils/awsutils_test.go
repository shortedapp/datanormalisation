package awsutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateAWSClients(t *testing.T) {
	testCsvs := []struct {
		clients []string
		isNil   []bool
	}{
		{[]string{"s3", "dynamoDB"}, []bool{false, false}},
		{[]string{"s3"}, []bool{false, true}},
		{[]string{}, []bool{true, true}},
	}
	for _, testCase := range testCsvs {
		clients := GenerateAWSClients(testCase.clients...)
		assert.Equal(t, testCase.isNil[0], clients.s3DownloadClient == nil)
		assert.Equal(t, testCase.isNil[1], clients.dynamoclient == nil)
	}

}
