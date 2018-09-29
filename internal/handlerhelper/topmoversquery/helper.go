package topmoversquery

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
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
		mover := moversdata.OrderedTopMovers{}
		addNumElements(item, &mover)
		addStringElements(item, &mover)
		result = append(result, &mover)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Order < result[j].Order
	})

	return result
}

func addStringElements(item map[string]*dynamodb.AttributeValue, data *moversdata.OrderedTopMovers) error {
	dayCode, presDay := item["DayCode"]
	weekCode, presWeek := item["WeekCode"]
	monthCode, presMonth := item["MonthCode"]
	yearCode, presYear := item["YearCode"]

	if !presDay || !presWeek ||
		!presMonth || !presYear {
		return fmt.Errorf("missing a required key")
	}
	data.DayCode = *dayCode.S
	data.WeekCode = *weekCode.S
	data.MonthCode = *monthCode.S
	data.YearCode = *yearCode.S

	return nil
}

func addNumElements(item map[string]*dynamodb.AttributeValue, data *moversdata.OrderedTopMovers) error {
	p, presPos := item["Position"]
	day, presDay := item["DayChange"]
	week, presWeek := item["WeekChange"]
	month, presMonth := item["MonthChange"]
	year, presYear := item["YearChange"]

	if !presPos || !presDay || !presWeek ||
		!presMonth || !presYear {
		return fmt.Errorf("missing a required key")
	}

	pos, errPos := strconv.ParseInt(*p.N, 10, 64)
	dayChange, errDayChange := strconv.ParseFloat(*day.N, 64)
	weekChange, errWeekChange := strconv.ParseFloat(*week.N, 64)
	monthChange, errMonthChange := strconv.ParseFloat(*month.N, 64)
	yearChange, errYearChange := strconv.ParseFloat(*year.N, 64)

	if errPos != nil || errDayChange != nil || errWeekChange != nil ||
		errMonthChange != nil || errYearChange != nil {
		return fmt.Errorf("missing a required key")
	}

	data.Order = int(pos)
	data.DayChange = dayChange
	data.WeekChange = weekChange
	data.MonthChange = monthChange
	data.YearChange = yearChange

	return nil
}
