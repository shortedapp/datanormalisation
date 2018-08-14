package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/shortedfunctions/internal/handlerhelper/datafetch"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Handler - the main function handler, triggered by cloudwatch event
func Handler(request events.CloudWatchEvent) {
	//Generate Clients
	clients := awsutils.GenerateAWSClients("s3")
	//Create datanormalise object
	d := datafetch.Datafetch{Clients: clients}
	//Run the normalise routine
	d.FetchRoutine(d.AsxCodeFetch)
}

func main() {
	log.SetAppName("ShortedApp")
	lambda.Start(Handler)
}
