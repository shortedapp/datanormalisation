package scheduledget

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"net/http"
)

type clientsStruct struct {
	dynamoclient     *dynamodb.DynamoDB
	s3DownloadClient *s3manager.Downloader
	s3UploadClient   *s3manager.Uploader
}

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
		}
	}
	return clientStruct
}

// WithDynamoDBGetLatest
// Checks the last updated time of the url, checks dynamoDB for last recorded record update
// if stale, fetchs record and updates the dynamoDB stat
// inputs:
//	url: url for the request
//	item: dynamoDB item to be updated
func WithDynamoDBGetLatest(url string, item string, client *clientsStruct) {
	resp, err := http.Head(url)

	if err != nil {
		return
	}

	lastModified := resp.Header.Get("Last-Modified")
	fmt.Println(lastModified)
	//TODO check the dynamodb table and then decide whether to continue or not
	FetchDynamoDBLastModified("lastUpdate", item, client)
	PutDynamoDBLastModified("lastUpdate", item, "testTime2", client)

}

// FetchDynamoDBLastModified pulls latest field update date
func FetchDynamoDBLastModified(tableName string, keyName string, client *clientsStruct) (string, error) {
	resp, err := client.dynamoclient.GetItem(&dynamodb.GetItemInput{
		Key:       map[string]*dynamodb.AttributeValue{"name_id": &dynamodb.AttributeValue{S: &keyName}},
		TableName: &tableName,
	})

	if err != nil {
		log.Printf("failed to fetch value from dynamodb table %s, key %s\n", tableName, keyName)
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
		log.Printf("put item: %v\n", res)
	}

	return err
}

// FetchMapFileFromS3 loads a csv mapping file to generate a key value reference
// inputs:
//	- bucketName: the name of the bucket the file is being retrieved from
//	- key: the key for the s3 object
//	- client: client structure containing relevant s3 clients
//	- valueStruct: an structure to unmarshal the data into
func FetchMapFileFromS3(bucketName string, key string, client *clientsStruct,
	f func([]byte) (interface{}, error)) (interface{}, error) {

	//create a buffer to write content
	buf := aws.NewWriteAtBuffer([]byte{})

	//Get the file from s3
	n, err := client.s3DownloadClient.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})

	log.Printf("downloads file %v/%v, size %v Bytes\n", bucketName, key, n)
	if err != nil {
		log.Printf("failed to download file %v/%v\n", bucketName, key)
		return nil, err
	}

	result, err := f(buf.Bytes())

	if err != nil {
		log.Println("failed to unmarshal the s3 object")
	}

	return result, err
}
