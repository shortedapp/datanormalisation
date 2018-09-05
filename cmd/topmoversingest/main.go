package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/shortedfunctions/internal/handlerhelper/topmoversingest"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Handler - the main function handler, triggered by a SNS event
func Handler(request events.SNSEvent) {
	//Generate Clients
	clients := awsutil.GenerateAWSClients("dynamoDB")
	//Create topmoversingestor struct
	t := topmoversingest.Topmoversingestor{Clients: clients}
	//Run the Ingest routine
	tableName := "testMovers"
	t.IngestMovement(tableName)
}

func main() {
	log.SetAppName("ShortedApp")
	lambda.Start(Handler)
}
