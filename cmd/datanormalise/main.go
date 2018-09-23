package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/shortedfunctions/internal/handlerhelper/datanormalise"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Handler - the main function handler, triggered by cloudwatch event
func Handler(request events.CloudWatchEvent) {
	//Generate Clients
	clients := awsutil.GenerateAWSClients("dynamoDB", "s3", "kinesis")
	//Create datanormalise struct
	d := datanormalise.Datanormalise{Clients: clients}
	//Run the normalise routine
	d.NormaliseRoutine()
}

func main() {
	log.SetAppName("ShortedApp")
	Handler(events.CloudWatchEvent{})
	lambda.Start(Handler)
}
