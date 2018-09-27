package codedmoversquery

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/shortedapp/shortedfunctions/internal/moversdata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
)

//CodedMoversQuery - struct to enable testing
type CodedMoversQuery struct {
	Clients awsutil.AwsUtiler
}

//QueryCodedTopMovers - Return the movement of x ASX code
func (t *CodedMoversQuery) QueryCodedTopMovers(tableName string, code string) *moversdata.CodedTopMovers {
	mover := moversdata.CodedTopMovers{Code: code}
	query := &awsutil.DynamoDBItemQuery{TableName: tableName, PartitionKey: "Code", PartitionName: code}
	res, err := t.Clients.GetItemByPartDynamoDB(query)

	if err != nil {
		return nil
	}
	if len(res) == 0 {
		return nil
	}

	err = addNumElements(res, &mover)
	if err != nil {
		return nil
	}
	return &mover

}

func addNumElements(item map[string]*dynamodb.AttributeValue, data *moversdata.CodedTopMovers) error {
	day, presDay := item["DayChange"]
	week, presWeek := item["WeekChange"]
	month, presMonth := item["MonthChange"]
	year, presYear := item["YearChange"]

	if !presDay || !presWeek ||
		!presMonth || !presYear {
		return fmt.Errorf("missing a required key")
	}

	dayChange, errDayChange := strconv.ParseFloat(*day.N, 64)
	weekChange, errWeekChange := strconv.ParseFloat(*week.N, 64)
	monthChange, errMonthChange := strconv.ParseFloat(*month.N, 64)
	yearChange, errYearChange := strconv.ParseFloat(*year.N, 64)

	if errDayChange != nil || errWeekChange != nil ||
		errMonthChange != nil || errYearChange != nil {
		return fmt.Errorf("missing a required key")
	}

	data.DayChange = dayChange
	data.WeekChange = weekChange
	data.MonthChange = monthChange
	data.YearChange = yearChange

	return nil
}
