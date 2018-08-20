package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/shortedfunctions/internal/handlerhelper/topshortsquery"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Handler - the main function handler, triggered by a SNS event
func Handler(request events.APIGatewayProxyRequest) {
	//Generate Clients
	clients := awsutils.GenerateAWSClients("dynamoDB")

	//Create topshortslist object
	t := topshortsquery.Topshortsquery{Clients: clients}

	//Run the topshorts fetch routine
	t.QueryTopShorted("testTopShorts", 5)
}

func main() {
	log.SetAppName("ShortedApp")
	Handler(events.APIGatewayProxyRequest{})
	lambda.Start(Handler)
}
