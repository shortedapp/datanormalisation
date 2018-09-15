package topmoversingest

import (
	"strconv"
	"time"

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
	//TODO kick off these tasks in seperate go threads
	t.generateTopMoversInOrder()
	t.generateMovementByCode()

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
	inner join "test"."day" on "latest".code = "day".code),
	weekdata AS
	(SELECT latest.code, latest.percent-week.percent as diff, ROW_NUMBER() OVER (ORDER BY latest.percent-week.percent) as ordernum
	from "test"."latest"
	inner join "test"."week" on "latest".code = "week".code),
	monthdata AS
	(SELECT latest.code, latest.percent-month.percent as diff, ROW_NUMBER() OVER (ORDER BY latest.percent-month.percent) as ordernum
	from "test"."latest"
	inner join "test"."month" on "latest".code = "month".code),
	yeardata AS
	(SELECT latest.code, latest.percent-year.percent as diff, ROW_NUMBER() OVER (ORDER BY latest.percent-year.percent) as ordernum
	from "test"."latest"
	inner join "test"."year" on "latest".code = "year".code)
	SELECT daydata.ordernum, daydata.code, daydata.diff, weekdata.code, weekdata.diff, monthdata.code, monthdata.diff, yeardata.code, yeardata.diff
	FROM daydata
	left join weekdata on weekdata.ordernum = daydata.ordernum
	left join monthdata on monthdata.ordernum = daydata.ordernum
	left join yeardata on yeardata.ordernum = daydata.ordernum
	WHERE daydata.ordernum < 100
	ORDER BY daydata.ordernum ASC`

	// result, err :=
	t.Clients.SendAthenaQuery(query, "test")

	//convert result to object for dynamo ingestion

	//return the list
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
