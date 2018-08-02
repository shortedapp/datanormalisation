package scheduledGet

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"fmt"
	"net/http"
)

var dynamoclient *dynamodb.DynamoDB

// WithDynamoDBGetLatest
// Checks the last updated time of the url, checks dynamoDB for last recorded record update
// if stale, fetchs record and updates the dynamoDB stat
// inputs:
//	url: url for the request
//	item: dynamoDB item to be updated
func WithDynamoDBGetLatest(url string, item string) {
	resp, err := http.Head(url)

	if err != nil {
		return
	}

	lastModified := resp.Header.Get("Last-Modified")

	fmt.Println(lastModified)
	FetchDynamoDBLastModified("lastUpdate", item)
	PutDynamoDBLastModified("lastUpdate", item, "testTime2")
}

// GenerateAWSClients generates new AWS clients based on string array
func GenerateAWSClients(clients ...string) {
	sess := session.Must(session.NewSession())
	for _, client := range clients {
		switch client {
		case "s3":
			fmt.Println("TODO")
		case "dynamoDB":
			dynamoclient = dynamodb.New(sess)
		}
	}
}

// FetchDynamoDBLastModified pulls latest field update date
func FetchDynamoDBLastModified(tableName string, keyName string) (string, error) {
	resp, err := dynamoclient.GetItem(&dynamodb.GetItemInput{
		Key:       map[string]*dynamodb.AttributeValue{"name_id": &dynamodb.AttributeValue{S: &keyName}},
		TableName: &tableName,
	})
	if err != nil {
		return "", err
	}
	return resp.Item[keyName].String(), nil
}

// PutDynamoDBLastModified updates latest field update date
func PutDynamoDBLastModified(tableName string, keyName string, time string) error {
	_, err := dynamoclient.PutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{"name_id": &dynamodb.AttributeValue{S: &keyName},
			"date": &dynamodb.AttributeValue{S: &time}},
		TableName: &tableName,
	})

	return err
}
