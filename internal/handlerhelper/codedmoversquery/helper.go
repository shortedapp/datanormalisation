package codedmoversquery

import (
	"strconv"

	"github.com/shortedapp/shortedfunctions/internal/moversdata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
)

//CodedMoversQuery - struct to enable testing
type CodedMoversQuery struct {
	Clients awsutil.AwsUtiler
}

//QueryCodedTopMovers - Return the movement of x ASX code
func (t *CodedMoversQuery) QueryCodedTopMovers(tableName string, code string) *moversdata.CodedTopMovers {
	query := &awsutil.DynamoDBItemQuery{TableName: tableName, PartitionKey: "Code", PartitionName: code}
	res, err := t.Clients.GetItemByPartDynamoDB(query)

	if err != nil {
		return nil
	}
	if len(res) == 0 {
		return nil
	}

	dayChange, _ := strconv.ParseFloat(*res["DayChange"].N, 64)
	weekChange, _ := strconv.ParseFloat(*res["WeekChange"].N, 64)
	monthChange, _ := strconv.ParseFloat(*res["MonthChange"].N, 64)
	yearChange, _ := strconv.ParseFloat(*res["YearChange"].N, 64)
	return &moversdata.CodedTopMovers{Code: code, DayChange: dayChange, WeekChange: weekChange, MonthChange: monthChange, YearChange: yearChange}

}
