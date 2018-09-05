package topmoversingest

//Topmoversingestor - struct to enable testing
type Topmoversingestor struct {
	Clients awsutils.AwsUtiler
}

//IngestMovement - Calaculate the movement and store in dynamoDB
func (t *Topmoversingestor) IngestMovement(tableName string) {
	//run Athena Query
	// "CREATE EXTERNAL TABLE IF NOT EXISTS test.testShorts (
	// 	code string,
	// 	name string,
	// 	shorts int,
	// 	total int,
	// 	percent float,
	// 	industry string
	//   )
	//   ROW FORMAT DELIMITED
	//   FIELDS TERMINATED BY ','
	//   ESCAPED BY '\\'
	//   LINES TERMINATED BY '\n'
	//   LOCATION 's3://shortedappjmk/testShortedData/'
	//   TBLPROPERTIES ('has_encrypted_data'='false')"

}
