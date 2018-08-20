package awsutils

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"

	"net/http"
)

//AwsUtiler - interface to define aws util functions
type AwsUtiler interface {
	WithDynamoDBGetLatest(string, string) (*http.Response, error)
	FetchDynamoDBLastModified(string, string) (string, error)
	PutDynamoDBLastModified(string, string, string) error
	PutKinesisRecords(*string, []interface{}, []string) error
	FetchJSONFileFromS3(string, string, func([]byte) (interface{}, error)) (interface{}, error)
	FetchCSVFileFromS3(string, string, func([][]string) (interface{}, error)) (interface{}, error)
	PutFileToS3(string, string, []byte) error
	GetDynamoDBTableThroughput(string) (int64, int64)
	PutDynamoDBItems(string, map[string]interface{}) error
	UpdateDynamoDBTableCapacity(string, int64, int64) error
	BatchGetItemsDynamoDB(string, string, []interface{}) ([]map[string]*dynamodb.AttributeValue, error)
}

//ClientsStruct - Structure to hold the various AWS clients
type ClientsStruct struct {
	dynamoClient     dynamodbiface.DynamoDBAPI
	s3DownloadClient s3manageriface.DownloaderAPI
	s3UploadClient   s3manageriface.UploaderAPI
	kinesisClient    kinesisiface.KinesisAPI
}

// GenerateAWSClients generates new AWS clients based on string array
func GenerateAWSClients(clients ...string) *ClientsStruct {
	sess := session.Must(session.NewSession())
	clientStruct := new(ClientsStruct)
	for _, client := range clients {
		switch client {
		case "s3":
			clientStruct.s3DownloadClient = s3manager.NewDownloader(sess)
			clientStruct.s3UploadClient = s3manager.NewUploader(sess)
		case "dynamoDB":
			clientStruct.dynamoClient = dynamodb.New(sess)
		case "kinesis":
			clientStruct.kinesisClient = kinesis.New(sess)
		}
	}
	return clientStruct
}

// WithDynamoDBGetLatest Checks the last updated time of the url, checks dynamoDB for last recorded record update
// if stale, fetchs record and updates the dynamoDB key
// inputs:
//	url: url for the request
//	item: dynamoDB item to be updated
func (client *ClientsStruct) WithDynamoDBGetLatest(url string, key string) (*http.Response, error) {
	resp, err := http.Head(url)
	if err != nil {
		log.Info("WithDynamoDBGetLatest", "unable to get information from target url")
		return nil, err
	}

	//Get last modified date from the source location
	lastModified := resp.Header.Get("Last-Modified")
	lastModifiedTime, err := time.Parse(time.RFC1123, lastModified)
	if err != nil {
		log.Info("WithDynamoDBGetLatest", "unable to parse last modified data")
		return nil, err
	}

	//check the dynamodb table and then decide whether to continue or not
	dynamoLastModified, err := client.FetchDynamoDBLastModified("lastUpdate", key)
	if err != nil {
		log.Info("WithDynamoDBGetLatest", "unable to get information from target url")
		return nil, err
	}

	dynamoLastModifiedTime, err := time.Parse(time.RFC3339, dynamoLastModified)
	if err != nil {
		log.Info("WithDynamoDBGetLatest", "unable to parse dynamo last modified date")
		return nil, err
	}

	if lastModifiedTime.UTC().Unix() > dynamoLastModifiedTime.UTC().Unix() {
		updateTime := lastModifiedTime.Format(time.RFC3339)
		client.PutDynamoDBLastModified("lastUpdate", key, updateTime)
		resp, err = http.Get(url)
		return resp, err
	}

	return nil, nil
}

// FetchDynamoDBLastModified pulls latest field update date
func (client *ClientsStruct) FetchDynamoDBLastModified(tableName string, keyName string) (string, error) {
	resp, err := client.dynamoClient.GetItem(&dynamodb.GetItemInput{
		Key:       map[string]*dynamodb.AttributeValue{"name_id": &dynamodb.AttributeValue{S: &keyName}},
		TableName: &tableName,
	})

	if err != nil {
		log.Info("FetchDynamoDBLastModified",
			fmt.Sprintf("failed to fetch value from dynamodb table %s, key %s", tableName, keyName))
		return "", err
	}
	return *resp.Item["date"].S, err
}

// PutDynamoDBLastModified updates latest field update date
// inputs:
//	- tableName: the name of the table to write into
//	- keyName: the key to write to
//	- time: the time to update the record to
//	- client: a point to the client structure
func (client *ClientsStruct) PutDynamoDBLastModified(tableName string, keyName string, time string) error {
	if time == "" {
		return fmt.Errorf("no time provided")
	}
	res, err := client.dynamoClient.PutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{"name_id": &dynamodb.AttributeValue{S: &keyName},
			"date": &dynamodb.AttributeValue{S: &time}},
		TableName: &tableName,
	})

	if err == nil {
		log.Info("PutDynamoDBLastModified",
			fmt.Sprintf("put item: %v", res))
	}

	return err
}

// PutKinesisRecords puts records into a kinesis stream
// inputs:
//	- stream: the name of the stream to write into
//	- blobData: an array of blob data (must be a struct that can be json encoded)
//	- valueStruct: an structure to unmarshal the data into
func (client *ClientsStruct) PutKinesisRecords(stream *string, blobData []interface{}, partitionKeys []string) error {
	var streamRecord kinesis.PutRecordsRequestEntry
	recordsList := make([]*kinesis.PutRecordsRequestEntry, 0, len(blobData))
	//Create Records
	for i, record := range blobData {
		jsonRecord, err := json.Marshal(record)
		if err != nil {
			log.Warn("PutKinesisRecords", "failed to convert struct into []byte")
			return err
		}
		streamRecord = kinesis.PutRecordsRequestEntry{
			Data:         jsonRecord,
			PartitionKey: &partitionKeys[i],
		}
		recordsList = append(recordsList, &streamRecord)
	}

	log.Info("PutKinesisRecords", fmt.Sprintf("putting records to stream %v", *stream))
	_, err := client.kinesisClient.PutRecords(&kinesis.PutRecordsInput{
		Records:    recordsList,
		StreamName: stream,
	})
	return err
}

// FetchJSONFileFromS3 loads a json mapping file to generate a key value reference
// inputs:
//	- bucketName: the name of the bucket the file is being retrieved from
//	- key: the key for the s3 object
//	- f: a function to unmarshal the data
func (client *ClientsStruct) FetchJSONFileFromS3(bucketName string, key string,
	f func([]byte) (interface{}, error)) (interface{}, error) {

	//create a buffer to write content
	buf := aws.NewWriteAtBuffer([]byte{})

	//Get the file from s3
	n, err := client.s3DownloadClient.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})

	if err != nil {
		log.Info("FetchJSONFileFromS3",
			fmt.Sprintf("downloads file %v/%v, size %v Bytes", bucketName, key, n))
		return nil, err
	}

	result, err := f(buf.Bytes())

	if err != nil {
		log.Info("FetchJSONFileFromS3", "failed to unmarshal the s3 object")
	}

	return result, err
}

// FetchCSVFileFromS3 loads a csv mapping file to generate a key value reference
// inputs:
//	- bucketName: the name of the bucket the file is being retrieved from
//	- key: the key for the s3 object
//	- client: client structure containing relevant s3 clients
//	- f: a function to unmarshal the data
func (client *ClientsStruct) FetchCSVFileFromS3(bucketName string, key string,
	f func([][]string) (interface{}, error)) (interface{}, error) {

	//create a buffer to write content
	buf := aws.NewWriteAtBuffer([]byte{})

	//Get the file from s3
	n, err := client.s3DownloadClient.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})

	log.Info("FetchJSONFileFromS3",
		fmt.Sprintf("downloads file %v/%v, size %v Bytes", bucketName, key, n))
	if err != nil {
		log.Info("FetchJSONFileFromS3",
			fmt.Sprintf("failed to download file %v/%v", bucketName, key))
		return nil, err
	}

	//Convert the byte array into a reader
	reader := csv.NewReader(bytes.NewReader(buf.Bytes()))
	reader.FieldsPerRecord = -1
	res, err := reader.ReadAll()
	if err != nil {
		log.Info("FetchJSONFileFromS3", "failed to read the csv file")
		return nil, err
	}

	//convert the file into the result struct
	result, err := f(res)

	if err != nil {
		log.Info("FetchJSONFileFromS3", "failed to unmarshal the s3 object")
	}
	return result, err
}

// PutFileToS3 puts a file up to s3
// inputs:
//	- bucketName: the name of the bucket the file is being put to
//	- key: the key for the s3 object
//	- client: client structure containing relevant s3 clients
//	- data: a []byte array of the data
func (client *ClientsStruct) PutFileToS3(bucketName string, key string, data []byte) error {
	if data == nil {
		log.Info("PutFileToS3", "missing data")
		return fmt.Errorf("missing data")
	}

	//Create the file to upload to s3
	res, err := client.s3UploadClient.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})

	if err == nil {
		log.Info("PutFileToS3", fmt.Sprintf("file successfully uploaded to %v", res.Location))
	}

	return err
}

// GetDynamoDBTableThroughput returns the read and write capacity units for a table
// inputs:
//	- tableName: the name of the dynamoDB table
func (client *ClientsStruct) GetDynamoDBTableThroughput(tableName string) (int64, int64) {
	table := dynamodb.DescribeTableInput{
		TableName: &tableName,
	}
	result, err := client.dynamoClient.DescribeTable(&table)
	if err != nil {
		log.Info("GetDynamoDBTableThroughput", "unable to get table details")
		return 5, 5
	}
	tableRead := result.Table.ProvisionedThroughput.ReadCapacityUnits
	tableWrite := result.Table.ProvisionedThroughput.WriteCapacityUnits
	return int64(*tableRead), int64(*tableWrite)
}

// GetDynamoDBFromRange returns records from dynamoDB from a given range
// inputs:
//	- tableName: the name of the dynamoDB table
func (client *ClientsStruct) GetDynamoDBFromRange(tableName string, startTime string) []map[string]*dynamodb.AttributeValue {
	scanInput := dynamodb.ScanInput{
		TableName:        &tableName,
		FilterExpression: aws.String("#dateR > :dateTime"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":dateTime": {
				N: &startTime,
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#dateR": aws.String("Date"),
		},
	}
	result, err := client.dynamoClient.Scan(&scanInput)
	if err != nil {
		log.Info("GetDynamoDBTableThroughput", "unable to get table details")
	}

	return result.Items
}

// PutDynamoDBItems - puts items into a dynamodb table
// inputs:
//	- tableName: the name of the dynamoDB table
//	- values: a map of keys and values for attributes
func (client *ClientsStruct) PutDynamoDBItems(tableName string, values map[string]interface{}) error {
	mapDynamo := make(map[string]*dynamodb.AttributeValue, len(values))
	for key, val := range values {
		mapDynamo[key] = mapAttributeValue(val)
	}

	_, err := client.dynamoClient.PutItem(&dynamodb.PutItemInput{
		Item:      mapDynamo,
		TableName: &tableName,
	})

	if err == nil {
		log.Info("PutDynamoDBItems",
			fmt.Sprintf("put code: %v", *mapDynamo["Code"].S))
	}

	return err
}

// UpdateDynamoDBTableCapacity - updates the tables read and write capacity
// inputs:
//	- tableName: the name of the dynamoDB table
// 	- writeCap: the write capacity units
//	- readCap: the read capacity units
func (client *ClientsStruct) UpdateDynamoDBTableCapacity(tableName string, readCap int64, writeCap int64) error {

	_, err := client.dynamoClient.UpdateTable(&dynamodb.UpdateTableInput{
		TableName:             &tableName,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{WriteCapacityUnits: &writeCap, ReadCapacityUnits: &readCap},
	})

	if err != nil {
		log.Info("UpdateDynamoDBTableCapacity", fmt.Sprintf("failed to provision change to table capacity err %v", err.Error()))
		return err
	}

	ticker := time.NewTicker(1000 * time.Millisecond)
	for range ticker.C {
		log.Info("UpdateDynamoDBTableCapacity", "checking aws")
		table, _ := client.dynamoClient.DescribeTable(&dynamodb.DescribeTableInput{TableName: &tableName})
		if *table.Table.TableStatus != "UPDATING" {
			break
		}
	}
	ticker.Stop()

	return nil
}

// UpdateDynamoDBTableCapacity - updates the tables read and write capacity
// inputs:
//	- tableName: the name of the dynamoDB table
// 	- writeCap: the write capacity units
//	- readCap: the read capacity units
func (client *ClientsStruct) BatchGetItemsDynamoDB(tableName string, field string, keys []interface{}) ([]map[string]*dynamodb.AttributeValue, error) {

	keysMap := make([]map[string]*dynamodb.AttributeValue, 0, len(keys))
	for _, key := range keys {
		keyAttributeMap := make(map[string]*dynamodb.AttributeValue, 1)
		keyAttributeMap[field] = mapAttributeValue(key)
		keysMap = append(keysMap, keyAttributeMap)
	}
	requestItems := make(map[string]*dynamodb.KeysAndAttributes, 1)
	requestItems[tableName] = &dynamodb.KeysAndAttributes{Keys: keysMap}
	res, err := client.dynamoClient.BatchGetItem(&dynamodb.BatchGetItemInput{
		RequestItems: requestItems,
	})

	if err != nil {
		log.Info("BatchGetItemsDynamoDB", err.Error())
		return nil, err
	}

	return res.Responses[tableName], nil
}

//mapAttributeValue - map values to their attribute type in dynamodb
func mapAttributeValue(val interface{}) *dynamodb.AttributeValue {
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.String:
		strVal := val.(string)
		return &dynamodb.AttributeValue{S: &strVal}
	case reflect.Int:
		intVal := fmt.Sprintf("%d", val.(int))
		return &dynamodb.AttributeValue{N: &intVal}
	case reflect.Int64:
		intVal := fmt.Sprintf("%d", val.(int64))
		return &dynamodb.AttributeValue{N: &intVal}
	case reflect.Float32:
		floatVal := fmt.Sprintf("%f", val.(float32))
		return &dynamodb.AttributeValue{N: &floatVal}
	}
	return nil
}
