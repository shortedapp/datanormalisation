package topmoversingest

//Topmoversingestor - struct to enable testing
type Topmoversingestor struct {
	Clients awsutils.AwsUtiler
}

//IngestMovement - Calaculate the movement and store in dynamoDB
func (t *Topmoversingestor) IngestMovement(tableName string) {
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

	//TODO MODIFY COMBINED SHORTS TO HAVE A KEY FOR ATHENA

}
