package topmoversingest

import "github.com/shortedapp/shortedfunctions/pkg/awsutil"

//Topmoversingestor - struct to enable testing
type Topmoversingestor struct {
	Clients awsutil.AwsUtiler
}

//IngestMovement - Calaculate the movement and store in dynamoDB
func (t *Topmoversingestor) IngestMovement(tableName string) {

	t.Clients.SendAthenaQuery(`WITH data AS 
	(SELECT new.code, new.percent-old.percent as dayDiff, ROW_NUMBER() OVER (ORDER BY new.percent-old.percent) as dayOrder, new.percent-old2.percent as monthDiff, ROW_NUMBER() OVER (ORDER BY new.percent-old2.percent) as monthOrder
	from "test"."new"
	inner join "test"."old" on "new".code = "old".code
	inner join "test"."old2" on "new".code = "old2".code)
	SELECT *
	FROM data
	WHERE data.dayOrder < 100 OR data.monthOrder < 100`, "test")
	//run Athena Query
	// "CREATE EXTERNAL TABLE IF NOT EXISTS test.testShorts (
	// 		result array<struct<code:string,
	// 		name:string,
	// 		shorts:BIGINT,
	// 		total:BIGINT,
	// 		percent:float,
	// 		industry:string>>
	// )
	// ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' LOCATION 's3://shortedappjmk/testShortedData/' TBLPROPERTIES ('has_encrypted_data'='false');"

	// EXAMPLE QUERY
	// WITH stocks AS
	//     (SELECT "$path" AS dateTime,
	//          stock.code AS code,
	//          stock.percent AS percent
	//     FROM "test"."testshorts", unnest(result) t(stock) )
	// SELECT *
	// FROM stocks
	// ORDER BY stocks.percent DESC limit 10;

	//THE ABOVE QUERY NEEDS REGEX FOR THE FILE NAME

	// "CREATE OR REPLACE VIEW "new" AS
	// SELECT regexp_extract("$path",
	// 		 '(\d*)(?=\.json$)') AS dateTime, stock.code AS code, stock.percent AS percent
	// 	FROM "test"."testshorts", unnest(result) t(stock)
	// 	WHERE regexp_extract("$path", '(\d*)(?=\.json$)')='20180913'"

	// SELECT new.code, new.percent-old.percent as dayDiff, ROW_NUMBER() OVER (ORDER BY new.percent-old.percent) as dayOrder, new.percent-old2.percent as monthDiff, ROW_NUMBER() OVER (ORDER BY new.percent-old2.percent) as monthOrder
	// from "test"."new"
	// inner join "test"."old" on "new".code = "old".code
	// inner join "test"."old2" on "new".code = "old2".code

	//COMBO QUERY
	// 	WITH daydata AS
	// (SELECT new.code, new.percent-old.percent as diff, ROW_NUMBER() OVER (ORDER BY new.percent-old.percent) as ordernum
	// from "test"."new"
	// inner join "test"."old" on "new".code = "old".code),
	// monthdata AS
	// (SELECT new.code, new.percent-old2.percent as diff, ROW_NUMBER() OVER (ORDER BY new.percent-old2.percent) as ordernum
	// from "test"."new"
	// inner join "test"."old2" on "new".code = "old2".code),
	// yeardata AS
	// (SELECT new.code, new.percent-old2.percent as diff, ROW_NUMBER() OVER (ORDER BY new.percent-old2.percent) as ordernum
	// from "test"."new"
	// inner join "test"."old2" on "new".code = "old2".code)
	// SELECT daydata.ordernum, daydata.code, daydata.diff, monthdata.code, monthdata.diff, yeardata.code, yeardata.diff
	// FROM daydata
	// inner join monthdata on monthdata.ordernum = daydata.ordernum
	// inner join yeardata on yeardata.ordernum = daydata.ordernum
	// WHERE daydata.ordernum < 100
	// ORDER BY daydata.ordernum ASC

	//TODO MODIFY COMBINED SHORTS TO HAVE A KEY FOR ATHENA

}
