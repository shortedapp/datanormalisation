package main

import (
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/shortedfunctions/internal/handlerhelper/bulknormalise"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Handler - the main function handler, triggered by sns event
func Handler(request events.SNSEvent) {
	//Generate Clients
	clients := awsutil.GenerateAWSClients("dynamoDB", "s3")
	//Create datanormalise struct
	b := bulknormalise.Bulknormalise{Clients: clients}
	//Get the required month from the SNS message
	msg := request.Records[0].SNS.Message
	month, err := strconv.Atoi(msg)
	if err != nil {
		log.Error("Handler", "unable to convert month from msg string")
		return
	}

	//Run the normalise routine with 1 second delays between days
	b.NormaliseRoutine(month, 1000)
}

func main() {
	log.SetAppName("ShortedApp")
	recordSlice := make([]events.SNSEventRecord, 0, 1)
	recordSlice = append(recordSlice, events.SNSEventRecord{SNS: events.SNSEntity{Message: "1"}})
	Handler(events.SNSEvent{Records: recordSlice})
	lambda.Start(Handler)
}
