package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/datanormalization/internal/handlerhelper/datanormalise"
	"github.com/shortedapp/datanormalization/pkg/awsutils"
	log "github.com/shortedapp/datanormalization/pkg/loggingutil"
)

//Handler - the main function handler, triggered by cloudwatch event
func Handler(request events.CloudWatchEvent) {
	//Generate Clients
	clients := awsutils.GenerateAWSClients("dynamoDB", "s3", "kinesis")
	//Create datanormalise object
	d := datanormalise.Datanormalise{Clients: clients}
	//Run the normalise routine
	d.NormaliseRoutine()
}

func main() {
	log.SetAppName("ShortedApp")
	lambda.Start(Handler)
}
