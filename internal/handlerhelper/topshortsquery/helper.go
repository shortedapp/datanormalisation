package topshortsquery

import (
	"strconv"

	"github.com/shortedapp/shortedfunctions/internal/sharedata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
)

//Topshortsquery - struct to enable testing
type Topshortsquery struct {
	Clients awsutils.AwsUtiler
}

//QueryTopShorted - Reads the latest
func (t *Topshortsquery) QueryTopShorted(tableName string, number int) {
	interSlice := make([]interface{}, number)
	for i := 0; i < number; i++ {
		interSlice[i] = i
	}
	res, err := t.Clients.BatchGetItemsDynamoDB(tableName, "Position", interSlice)

	if err != nil {
		return
	}

	result := make([]*sharedata.TopShortJSON, 0, number)
	for _, item := range res {
		i, _ := strconv.ParseInt(*item["Posistion"].N, 10, 64)
		j, _ := strconv.ParseFloat(*item["Percent"].S, 32)
		result = append(result, &sharedata.TopShortJSON{Position: i, Code: *item["Code"].S, Percent: float32(j)})
	}
}
