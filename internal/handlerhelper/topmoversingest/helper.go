package topmoversingest

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/shortedapp/shortedfunctions/internal/moversdata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	"github.com/shortedapp/shortedfunctions/pkg/timeslotutil"
)

//Topmoversingestor - struct to enable testing
type Topmoversingestor struct {
	Clients awsutil.AwsUtiler
}

//IngestMovement - Calaculate the movement and store in dynamoDB
func (t *Topmoversingestor) IngestMovement(tableName string) {
	t.generateViews()

	//Generate queries and uploads in go routines
	// orderedTopMoversQuery := `WITH daydata AS
	// (SELECT latest.code, COALESCE(latest.percent-day.percent,-99999999) as diff, ROW_NUMBER() OVER (ORDER BY ABS(latest.percent-day.percent)) as ordernum
	// from "test"."latest"
	// left join "test"."day" on "latest".code = "day".code),
	// weekdata AS
	// (SELECT latest.code, COALESCE(latest.percent-week.percent,-99999999) as diff, ROW_NUMBER() OVER (ORDER BY ABS(latest.percent-week.percent)) as ordernum
	// from "test"."latest"
	// left join "test"."week" on "latest".code = "week".code),
	// monthdata AS
	// (SELECT latest.code, COALESCE(latest.percent-month.percent,-99999999) as diff, ROW_NUMBER() OVER (ORDER BY ABS(latest.percent-month.percent)) as ordernum
	// from "test"."latest"
	// left join "test"."month" on "latest".code = "month".code),
	// yeardata AS
	// (SELECT latest.code, COALESCE(latest.percent-year.percent,-99999999) as diff, ROW_NUMBER() OVER (ORDER BY ABS(latest.percent-year.percent)) as ordernum
	// from "test"."latest"
	// left join "test"."year" on "latest".code = "year".code)
	// SELECT daydata.ordernum, daydata.code, daydata.diff, weekdata.code, weekdata.diff, monthdata.code, monthdata.diff, yeardata.code, yeardata.diff
	// FROM daydata
	// left join weekdata on weekdata.ordernum = daydata.ordernum
	// left join monthdata on monthdata.ordernum = daydata.ordernum
	// left join yeardata on yeardata.ordernum = daydata.ordernum
	// WHERE daydata.ordernum < 100
	// ORDER BY daydata.ordernum ASC`

	// codedTopMoversQuery := `WITH daydata AS
	// (SELECT latest.code, COALESCE(latest.percent-day.percent,-99999999) as daydiff
	// from "test"."latest"
	// left join "test"."day" on "latest".code = "day".code),
	// weekdata AS
	// (SELECT latest.code, COALESCE(latest.percent-week.percent,-99999999) as weekdiff
	// from "test"."latest"
	// left join "test"."week" on "latest".code = "week".code),
	// monthdata AS
	// (SELECT latest.code, COALESCE(latest.percent-month.percent,-99999999) as monthdiff
	// from "test"."latest"
	// left join "test"."month" on "latest".code = "month".code),
	// yeardata AS
	// (SELECT latest.code, COALESCE(latest.percent-year.percent,-99999999) as yeardiff
	// from "test"."latest"
	// left join "test"."year" on "latest".code = "year".code)
	// SELECT daydata.code, daydata.daydiff, weekdata.weekdiff, monthdata.monthdiff, yeardata.yeardiff
	// FROM daydata
	// left join weekdata on weekdata.code = daydata.code
	// left join monthdata on monthdata.code = daydata.code
	// left join yeardata on yeardata.code = daydata.code`

	// orderedDone := make(chan bool)
	// go t.queryAndUploadToDynamoDB(orderedTopMoversQuery, "test", "OrderedTopMovers", athenaToTopMovers, OrderedTopMoversMapper, orderedDone)

	// codedDone := make(chan bool)
	// go t.queryAndUploadToDynamoDB(codedTopMoversQuery, "test", "CodedTopMovers", athenaToTopMovers, CodedTopMoversMapper, codedDone)
	// <-orderedDone
	// <-codedDone

	//TODO add code to drop stale entries
}

func (t *Topmoversingestor) queryAndUploadToDynamoDB(query string, athenaTable string, dynamoTable string,
	athenaFn func(*athena.Row) (interface{}, error), dynamoFn func(resp interface{}, date int) ([]*map[string]interface{}, error), done chan bool) {
	result := t.generateQueryResults(query, athenaTable, athenaFn)
	t.uploadToDynamoDB(dynamoTable, result, dynamoFn)
	done <- true
}

func (t *Topmoversingestor) uploadToDynamoDB(table string, data interface{}, fn func(resp interface{}, date int) ([]*map[string]interface{}, error)) {
	t.Clients.WriteToDynamoDB(table, data, fn, 0)
}

//OrderedTopMoversMapper - Map OrderedTopMover to go map for dynamo ingestion
func OrderedTopMoversMapper(resp interface{}, date int) ([]*map[string]interface{}, error) {
	//TODO uplift this to take a slice of additional input data
	data, ok := resp.([]*interface{})
	if !ok {
		return nil, fmt.Errorf("unable to cast to CombinedResultJSON")
	}
	result := make([]*map[string]interface{}, 0, len(data))
	for _, moverInter := range data {
		mover := (*moverInter).(moversdata.OrderedTopMovers)
		attributes := make(map[string]interface{}, 9)
		attributes["Position"] = mover.Order
		attributes["DayCode"] = mover.DayCode
		attributes["DayChange"] = mover.DayChange
		attributes["WeekCode"] = mover.WeekCode
		attributes["WeekChange"] = mover.WeekChange
		attributes["MonthCode"] = mover.MonthCode
		attributes["MonthChange"] = mover.MonthChange
		attributes["YearCode"] = mover.YearCode
		attributes["YearChange"] = mover.YearChange
		result = append(result, &attributes)
	}
	return result, nil
}

//CodedTopMoversMapper - Map CodedTopMover to go map for dynamo ingestion
func CodedTopMoversMapper(resp interface{}, date int) ([]*map[string]interface{}, error) {
	//TODO uplift this to take a slice of additional input data
	data, ok := resp.([]*interface{})
	if !ok {
		return nil, fmt.Errorf("unable to cast to CombinedResultJSON")
	}
	result := make([]*map[string]interface{}, 0, len(data))
	for _, moverInter := range data {
		mover := (*moverInter).(moversdata.CodedTopMovers)
		attributes := make(map[string]interface{}, 9)
		attributes["Code"] = mover.Code
		attributes["DayChange"] = mover.DayChange
		attributes["WeekChange"] = mover.WeekChange
		attributes["MonthChange"] = mover.MonthChange
		attributes["YearChange"] = mover.YearChange
		result = append(result, &attributes)
	}
	return result, nil
}

func (t *Topmoversingestor) generateQueryResults(query string, database string, fn func(*athena.Row) (interface{}, error)) interface{} {
	//Capture the results
	result, _ := t.Clients.SendAthenaQuery(query, database)
	//Convert and return slice
	return convertListOfResults(result, fn)
}

func convertListOfResults(results []*athena.ResultSet, translate func(*athena.Row) (interface{}, error)) interface{} {
	resultList := make([]*interface{}, 0)

	//Create channel for translated results
	items := make(chan *interface{}, len(results)*1000)

	//Create channel to indicated the topMovers slice is complete
	done := make(chan bool)

	//Process all results in multiple threads and use one go routine to update slice for thread safety
	var wg sync.WaitGroup
	for _, result := range results {
		wg.Add(1)
		go func(items chan *interface{}) {
			defer wg.Done()
			for _, row := range result.Rows {
				item, err := translate(row)
				if err != nil {
					continue
				}
				items <- &item
			}
		}(items)
	}

	//go routine to update the map
	go func(items chan *interface{}, done chan bool) {
		for {
			item, more := <-items
			if !more {
				break
			}
			resultList = append(resultList, item)
		}
		done <- true
	}(items, done)

	// All results transformed and channel closed
	wg.Wait()
	close(items)

	//All results written into the slice
	<-done
	return resultList
}

func athenaToTopMovers(row *athena.Row) (interface{}, error) {
	stockMovement := moversdata.OrderedTopMovers{}
	//Calculate the order
	if row.Data[0].VarCharValue != nil {
		order, err := strconv.Atoi(*row.Data[0].VarCharValue)
		if err != nil {
			return stockMovement, err
		}
		stockMovement.Order = order
	} else {
		return stockMovement, fmt.Errorf("no order")
	}
	//Get the codes
	if row.Data[1].VarCharValue != nil && row.Data[3].VarCharValue != nil &&
		row.Data[5].VarCharValue != nil && row.Data[7].VarCharValue != nil {
		stockMovement.DayCode = *row.Data[1].VarCharValue
		stockMovement.WeekCode = *row.Data[3].VarCharValue
		stockMovement.MonthCode = *row.Data[5].VarCharValue
		stockMovement.YearCode = *row.Data[7].VarCharValue
	} else {
		return stockMovement, fmt.Errorf("no codes")
	}

	//Get the percentages
	percentages := make([]float64, 0, 3)
	for i := 2; i <= 8; i += 2 {
		if row.Data[i].VarCharValue != nil {
			percent, err := strconv.ParseFloat(*row.Data[i].VarCharValue, 64)
			if err == nil {
				percentages = append(percentages, percent)
			}
		} else {
			percentages = append(percentages, 0.)
		}
	}
	if len(percentages) != 4 {
		return stockMovement, fmt.Errorf("not percentage data")
	}
	stockMovement.DayChange = percentages[0]
	stockMovement.WeekChange = percentages[1]
	stockMovement.MonthChange = percentages[2]
	stockMovement.YearChange = percentages[3]
	return stockMovement, nil
}

func athenaToMoversByCode(row *athena.Row) (interface{}, error) {
	stockMovement := moversdata.CodedTopMovers{}

	//Get the Code
	if row.Data[0].VarCharValue != nil {
		stockMovement.Code = *row.Data[0].VarCharValue
	} else {
		return stockMovement, fmt.Errorf("no codes")
	}

	//Get the percentages
	percentages := make([]float64, 0, 3)
	for i := 1; i <= 4; i++ {
		if row.Data[i].VarCharValue != nil {
			percent, err := strconv.ParseFloat(*row.Data[i].VarCharValue, 64)
			if err == nil {
				percentages = append(percentages, percent)
			}
		} else {
			percentages = append(percentages, 0.)
		}
	}
	if len(percentages) != 4 {
		return stockMovement, fmt.Errorf("not percentage data")
	}
	stockMovement.DayChange = percentages[0]
	stockMovement.WeekChange = percentages[1]
	stockMovement.MonthChange = percentages[2]
	stockMovement.YearChange = percentages[3]
	return stockMovement, nil
}

func (t *Topmoversingestor) generateViews() {
	timeSlots := make([]int, 0, 4)
	names := []string{"year", "month", "week", "day", "latest"}
	//back date at least 4 days (ensure it is on a week day first)
	now := timeslotutil.BackDateToWeekday(time.Now())
	now = now.AddDate(0, 0, -4)
	for i := 0; i <= 4; i++ {
		//Ensure the timeslot lands on a weekdayn with data
		timeSlots = append(timeSlots, timeslotutil.GetPreviousWeekdayDate(i, now))
	}

	for i, timeVal := range timeSlots {
		//TODO break this out to a more resilent view creation process (look to combine into one query)
		go t.Clients.SendAthenaQuery(`CREATE OR REPLACE VIEW "`+names[i]+`" AS
	SELECT regexp_extract("$path",
			 '(\d*)(?=\.json$)') AS dateTime, stock.code AS code, stock.percent AS percent
		FROM "test"."testshorts", unnest(result) t(stock)
		WHERE regexp_extract("$path", '(\d*)(?=\.json$)')='`+strconv.Itoa(timeVal)+`'`, "test")
	}

}
