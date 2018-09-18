package topmoversingest

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	"github.com/shortedapp/shortedfunctions/pkg/timeslotutil"
)

//Topmoversingestor - struct to enable testing
type Topmoversingestor struct {
	Clients awsutil.AwsUtiler
}

//OrderedTopMovers
type OrderedTopMovers struct {
	Order       int
	DayCode     string
	DayChange   float64
	WeekCode    string
	WeekChange  float64
	MonthCode   string
	MonthChange float64
	YearCode    string
	YearChange  float64
}

//MoversByCode
type MoversByCode struct {
	Code        string
	DayChange   float64
	WeekChange  float64
	MonthChange float64
	YearChange  float64
}

//IngestMovement - Calaculate the movement and store in dynamoDB
func (t *Topmoversingestor) IngestMovement(tableName string) {
	//TODO put this back in
	//t.generateViews()

	//TODO kick off these tasks in seperate go routines
	orderedMovement := t.generateTopMoversInOrder()
	codedMovement := t.generateMovementByCode()

	//TODO Add channel logic to this task to ingest on each tasks completion
	t.uploadToDynamoDB(orderedMovement, codedMovement)

}

func (t *Topmoversingestor) uploadToDynamoDB(ordedMovement []OrderedTopMovers, codeMovement []MoversByCode) {
	//TODO add ingestion
}

func (t *Topmoversingestor) generateQueryResults(query string, database string, fn func(*athena.Row) (interface{}, error)) []*interface{} {
	//Capture the results
	result, _ := t.Clients.SendAthenaQuery(query, "test")
	//Convert and return slice
	return convertListOfResults(result, fn)
}

func (t *Topmoversingestor) generateMovementByCode() []MoversByCode {
	//TODO load these queries from s3 or use named queries within athena
	query := `WITH daydata AS
	(SELECT latest.code, latest.percent-day.percent as daydiff
	from "test"."latest"
	inner join "test"."day" on "latest".code = "day".code),
	weekdata AS
	(SELECT latest.code, latest.percent-week.percent as weekdiff
	from "test"."latest"
	inner join "test"."week" on "latest".code = "week".code),
	monthdata AS
	(SELECT latest.code, latest.percent-month.percent as monthdiff
	from "test"."latest"
	inner join "test"."month" on "latest".code = "month".code),
	yeardata AS
	(SELECT latest.code, latest.percent-year.percent as yeardiff
	from "test"."latest"
	inner join "test"."year" on "latest".code = "year".code)
	SELECT daydata.code, daydata.daydiff, weekdata.weekdiff, monthdata.monthdiff, yeardata.yeardiff
	FROM daydata
	left join weekdata on weekdata.code = daydata.code
	left join monthdata on monthdata.code = daydata.code
	left join yeardata on yeardata.code = daydata.code`

	//run query and return results
	movers := t.generateQueryResults(query, "test", athenaToMoversByCode)

	//Convert from interace to required type
	//TODO see if there is a better way to do this
	results := make([]MoversByCode, 0, len(movers))
	for _, mover := range movers {
		results = append(results, (*mover).(MoversByCode))
	}
	fmt.Println(results)
	return results

}

func (t *Topmoversingestor) generateTopMoversInOrder() []OrderedTopMovers {
	//TODO load these queries from s3 or use named queries within athena
	query := `WITH daydata AS
	(SELECT latest.code, latest.percent-day.percent as diff, ROW_NUMBER() OVER (ORDER BY latest.percent-day.percent) as ordernum
	from "test"."latest"
	left join "test"."day" on "latest".code = "day".code),
	weekdata AS
	(SELECT latest.code, latest.percent-week.percent as diff, ROW_NUMBER() OVER (ORDER BY latest.percent-week.percent) as ordernum
	from "test"."latest"
	left join "test"."week" on "latest".code = "week".code),
	monthdata AS
	(SELECT latest.code, latest.percent-month.percent as diff, ROW_NUMBER() OVER (ORDER BY latest.percent-month.percent) as ordernum
	from "test"."latest"
	left join "test"."month" on "latest".code = "month".code),
	yeardata AS
	(SELECT latest.code, latest.percent-year.percent as diff, ROW_NUMBER() OVER (ORDER BY latest.percent-year.percent) as ordernum
	from "test"."latest"
	left join "test"."year" on "latest".code = "year".code)
	SELECT daydata.ordernum, daydata.code, daydata.diff, weekdata.code, weekdata.diff, monthdata.code, monthdata.diff, yeardata.code, yeardata.diff
	FROM daydata
	left join weekdata on weekdata.ordernum = daydata.ordernum
	left join monthdata on monthdata.ordernum = daydata.ordernum
	left join yeardata on yeardata.ordernum = daydata.ordernum
	WHERE daydata.ordernum < 100
	ORDER BY daydata.ordernum ASC`

	//run query and return results
	movers := t.generateQueryResults(query, "test", athenaToTopMovers)

	//Convert from interace to required type
	//TODO see if there is a better way to do this
	results := make([]OrderedTopMovers, 0, len(movers))
	for _, mover := range movers {
		results = append(results, (*mover).(OrderedTopMovers))
	}
	fmt.Println(results)
	return results
}

func convertListOfResults(results []*athena.ResultSet, translate func(*athena.Row) (interface{}, error)) []*interface{} {
	resultList := make([]*interface{}, 0)

	//Create channel for translated movers
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

	// All results transfromed and channel closed
	wg.Wait()
	close(items)

	//All results written into the slice
	<-done
	return resultList
}

func athenaToTopMovers(row *athena.Row) (interface{}, error) {
	stockMovement := OrderedTopMovers{}
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
	stockMovement := MoversByCode{}

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
	now := time.Now()
	for i := 0; i <= 4; i++ {
		timeSlots = append(timeSlots, timeslotutil.GetPreviousDate(i, now))
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
