package topmoversquery

import (
	"sort"
	"strconv"

	"github.com/shortedapp/shortedfunctions/internal/moversdata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
)

//TopMoversQuery - struct to enable testing
type TopMoversQuery struct {
	Clients awsutil.AwsUtiler
}

//QueryOrderedTopShorted - Return the top x movers
func (t *TopMoversQuery) QueryOrderedTopMovers(tableName string, number int) []*moversdata.OrderedTopMovers {
	interSlice := make([]interface{}, number)
	for i := 0; i < number; i++ {
		interSlice[i] = i
	}
	res, err := t.Clients.BatchGetItemsDynamoDB(tableName, "Position", interSlice)

	if err != nil {
		return nil
	}

	result := make([]*moversdata.OrderedTopMovers, 0, number)
	for _, item := range res {
		pos, _ := strconv.ParseInt(*item["Position"].N, 10, 64)
		dayChange, _ := strconv.ParseFloat(*item["DayChange"].N, 64)
		weekChange, _ := strconv.ParseFloat(*item["WeekChange"].N, 64)
		monthChange, _ := strconv.ParseFloat(*item["MonthChange"].N, 64)
		yearChange, _ := strconv.ParseFloat(*item["YearChange"].N, 64)
		dayCode := *item["DayCode"].S
		weekCode := *item["WeekCode"].S
		monthCode := *item["MonthCode"].S
		yearCode := *item["YearCode"].S
		result = append(result, &moversdata.OrderedTopMovers{Order: int(pos), DayCode: dayCode, DayChange: dayChange,
			WeekCode: weekCode, WeekChange: weekChange, MonthCode: monthCode, MonthChange: monthChange,
			YearCode: yearCode, YearChange: yearChange})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Order < result[j].Order
	})

	return result
}
