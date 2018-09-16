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

//IngestMovement - Calaculate the movement and store in dynamoDB
func (t *Topmoversingestor) IngestMovement(tableName string) {
	//t.generateViews()
	//TODO kick off these tasks in seperate go routines
	t.generateTopMoversInOrder()
	//t.generateMovementByCode()

	//TODO Add channel logic to this task to ingest on each tasks completion
	t.uploadToDynamoDB()

}

func (t *Topmoversingestor) uploadToDynamoDB() {
	//TODO add ingestion
}

func (t *Topmoversingestor) generateTopMoversInOrder() {
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
	WHERE daydata.ordernum < 10
	ORDER BY daydata.ordernum ASC`

	result, _ := t.Clients.SendAthenaQuery(query, "test")

	//convert result to object for dynamo ingestion
	convertOrderedTopMovers(result)
	//return the list
}

func convertOrderedTopMovers(results []*athena.ResultSet) []*OrderedTopMovers {
	topMovers := make([]*OrderedTopMovers, 0)

	//Create channel for translated movers
	movers := make(chan *OrderedTopMovers, len(results)*1000)
	//Create channel to indicated the topMovers slice is complete
	done := make(chan bool)

	//Process all results in multiple threads and use one go routine to update slice for thread safety
	var wg sync.WaitGroup
	for _, result := range results {
		wg.Add(1)
		go func(movers chan *OrderedTopMovers) {
			defer wg.Done()
			for _, row := range result.Rows {
				stockMovement, err := athenaToTopMovers(row)
				if err != nil {
					continue
				}
				movers <- &stockMovement
			}
		}(movers)
	}

	//go routine to update the map
	go func(movers chan *OrderedTopMovers, done chan bool) {
		for {
			mover, more := <-movers
			if !more {
				break
			}
			topMovers = append(topMovers, mover)
		}
		done <- true
	}(movers, done)

	// All results transfromed and channel closed
	wg.Wait()
	close(movers)

	//All results written into the slice
	<-done
	return topMovers
}

func athenaToTopMovers(row *athena.Row) (OrderedTopMovers, error) {
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
	return stockMovement, nil
}

func (t *Topmoversingestor) generateMovementByCode() {
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

	//Capture the results
	t.Clients.SendAthenaQuery(query, "test")

	//convert to objects for dynamo ingestion

	//return objects
}

func (t *Topmoversingestor) generateViews() {
	timeSlots := make([]int, 0, 4)
	names := []string{"year", "month", "week", "day", "latest"}
	now := time.Now()
	for i := 0; i <= 4; i++ {
		timeSlots = append(timeSlots, timeslotutil.GetPreviousDate(i, now))
	}

	for i, timeVal := range timeSlots {
		//TODO break this out to a more resilent view creation process
		go t.Clients.SendAthenaQuery(`CREATE OR REPLACE VIEW "`+names[i]+`" AS
	SELECT regexp_extract("$path",
			 '(\d*)(?=\.json$)') AS dateTime, stock.code AS code, stock.percent AS percent
		FROM "test"."testshorts", unnest(result) t(stock)
		WHERE regexp_extract("$path", '(\d*)(?=\.json$)')='`+strconv.Itoa(timeVal)+`'`, "test")
	}

}
