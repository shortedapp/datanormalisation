package topmoversingest

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/shortedapp/shortedfunctions/internal/sharedata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	"github.com/stretchr/testify/assert"
)

type mockAwsUtilClients struct {
	TestOption int
	awsutil.AwsUtiler
}

func (m mockAwsUtilClients) FetchJSONFileFromS3(bucket string, key string, f func([]byte) (interface{}, error)) (interface{}, error) {
	if m.TestOption == 0 {
		return nil, fmt.Errorf("err")
	}
	b, _ := ioutil.ReadFile("../../../test/data/combinedshortdatatest.json")
	res, _ := sharedata.UnmarshalCombinedShortsJSON(b)
	return res, nil
}

func mockOrderedTopMoversAthena() *athena.ResultSet {
	strings := []string{"99", "EXR", "10.1", "MEU", "0.0", "CVF", "0.3", "EXR", "1.3"}
	data := make([]*athena.Datum, 0)
	for _, s := range strings {
		data = append(data, &athena.Datum{VarCharValue: &s})
	}
	row := &athena.Row{Data: data}
	rows := []*athena.Row{row}
	return &athena.ResultSet{Rows: rows}
}

func mockCodedTopMoversAthena(option int) *athena.ResultSet {
	strings := []string{}
	switch option {
	case 0:
		strings = []string{"EXR", "10.1", "0.0", "0.3", "1.3"}
	case 1:
		strings = []string{"EXR", "10.1", "0.0", "", "asd"}
	}

	data := make([]*athena.Datum, 0)
	for _, s := range strings {
		data = append(data, &athena.Datum{VarCharValue: &s})
	}
	row := &athena.Row{Data: data}
	rows := []*athena.Row{row}
	return &athena.ResultSet{Rows: rows}
}

func TestAthenaToMoversByCode(t *testing.T) {
	testCases := []struct {
		option int
		err    bool
	}{
		{0, false},
		{1, true},
	}
	for _, test := range testCases {
		_, err := athenaToMoversByCode(mockCodedTopMoversAthena(test.option).Rows[0])
		assert.Equal(t, test.err, err != nil)
	}
}
