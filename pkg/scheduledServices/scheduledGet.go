package scheduledGet

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"fmt"
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
	FetchDynamoDBLastModified("lastUpdate", item, client)
	PutDynamoDBLastModified("lastUpdate", item, "testTime2", client)
	// FetchMapFileFromS3("shortedapp", "testCsv.csv", client, "")
}

// FetchDynamoDBLastModified pulls latest field update date
func FetchDynamoDBLastModified(tableName string, keyName string, client *clientsStruct) (string, error) {
	resp, err := client.dynamoclient.GetItem(&dynamodb.GetItemInput{
		Key:       map[string]*dynamodb.AttributeValue{"name_id": &dynamodb.AttributeValue{S: &keyName}},
		TableName: &tableName,
	})

	if err != nil {
		return "", err
	}

	return *resp.Item["date"].S, nil
}

// PutDynamoDBLastModified updates latest field update date
func PutDynamoDBLastModified(tableName string, keyName string, time string, client *clientsStruct) error {
	_, err := client.dynamoclient.PutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{"name_id": &dynamodb.AttributeValue{S: &keyName},
			"date": &dynamodb.AttributeValue{S: &time}},
		TableName: &tableName,
	})

	return err
}

// FetchMapFileFromS3 loads a csv mapping file to generate a key value reference
// inputs:
//	- bucketName: the name of the bucket the file is being retrieved from
//	- key: the key for the s3 object
//	- valueStruct: an example of the file structrue to use
//map[string]interface{}
func FetchMapFileFromS3(bucketName string, key string, client *clientsStruct, valueStruct interface{}) {

	//create a buffer to write content
	buf := aws.NewWriteAtBuffer([]byte{})
	//Get the file from s3
	n, err := client.s3DownloadClient.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	fmt.Println(n, err)
	fmt.Println(buf)

}
