package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/shortedfunctions/internal/handlerhelper/dynamoingestor"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Handler - the main function handler, triggered by a SNS event
func Handler(request events.SNSEvent) {
	//Generate Clients
	clients := awsutils.GenerateAWSClients("dynamoDB", "s3")
	//Create dynamoingestor object
	d := dynamoingestor.Dynamoingestor{Clients: clients}
	//Run the Ingest routine
	tableName := "testShorts"
	d.IngestRoutine(tableName)
}

func main() {
	log.SetAppName("ShortedApp")
	lambda.Start(Handler)
}
