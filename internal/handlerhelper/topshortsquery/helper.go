package topshortsquery

import (
	"sort"
	"strconv"

	"github.com/shortedapp/shortedfunctions/internal/sharedata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
)

//Topshortsquery - struct to enable testing
type Topshortsquery struct {
	Clients awsutils.AwsUtiler
}

//QueryTopShorted - Reads the latest
func (t *Topshortsquery) QueryTopShorted(tableName string, number int) []*sharedata.TopShortJSON {
	interSlice := make([]interface{}, number)
	for i := 0; i < number; i++ {
		interSlice[i] = i
	}
	res, err := t.Clients.BatchGetItemsDynamoDB(tableName, "Position", interSlice)

	if err != nil {
		return nil
	}

	result := make([]*sharedata.TopShortJSON, 0, number)
	for _, item := range res {
		i, _ := strconv.ParseInt(*item["Position"].N, 10, 64)
		j, _ := strconv.ParseFloat(*item["Percent"].N, 32)
		result = append(result, &sharedata.TopShortJSON{Position: i, Code: *item["Code"].S, Percent: float32(j)})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Position < result[j].Position
	})

	return result
}
