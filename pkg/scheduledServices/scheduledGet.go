package scheduledget

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"time"

	log "github.com/shortedapp/datanormalization/pkg/loggingutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"net/http"
)

type clientsStruct struct {
	dynamoclient     *dynamodb.DynamoDB
	s3DownloadClient *s3manager.Downloader
	s3UploadClient   *s3manager.Uploader
	kinesisClient    *kinesis.Kinesis
}

var logger log.LoggerImpl

// GenerateAWSClients generates new AWS clients based on string array
func GenerateAWSClients(clients ...string) *clientsStruct {
	sess := session.Must(session.NewSession())
	clientStruct := new(clientsStruct)
	for _, client := range clients {
		switch client {
		case "s3":
			clientStruct.s3DownloadClient = s3manager.NewDownloader(sess)
			clientStruct.s3UploadClient = s3manager.NewUploader(sess)
		case "dynamoDB":
			clientStruct.dynamoclient = dynamodb.New(sess)
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
func WithDynamoDBGetLatest(url string, key string, client *clientsStruct) (*http.Response, error) {
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

	//TODO check the dynamodb table and then decide whether to continue or not
	dynamoLastModified, err := FetchDynamoDBLastModified("lastUpdate", key, client)
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
		PutDynamoDBLastModified("lastUpdate", key, updateTime, client)
		resp, err := http.Get(url)
		return resp, err
	}
	return nil, err
}

// FetchDynamoDBLastModified pulls latest field update date
func FetchDynamoDBLastModified(tableName string, keyName string, client *clientsStruct) (string, error) {
	resp, err := client.dynamoclient.GetItem(&dynamodb.GetItemInput{
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
func PutDynamoDBLastModified(tableName string, keyName string, time string, client *clientsStruct) error {
	res, err := client.dynamoclient.PutItem(&dynamodb.PutItemInput{
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
func PutKinesisRecords(stream *string, blobData []interface{}, partitionKeys []string, client *clientsStruct) error {
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
//	- client: client structure containing relevant s3 clients
//	- f: a function to unmarshal the data
func FetchJSONFileFromS3(bucketName string, key string, client *clientsStruct,
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
func FetchCSVFileFromS3(bucketName string, key string, client *clientsStruct,
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
func PutFileToS3(bucketName string, key string, client *clientsStruct, data []byte) error {

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
