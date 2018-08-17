package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/shortedfunctions/internal/handlerhelper/topshortslist"
	"github.com/shortedapp/shortedfunctions/internal/searchutils"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Handler - the main function handler, triggered by a API Gateway Request event
func Handler(request events.APIGatewayProxyRequest) {
	//Generate Clients
	clients := awsutils.GenerateAWSClients("dynamoDB")

	//Create topshortslist object
	d := topshortslist.Topshortslist{Clients: clients}

	//Run the topshorts fetch routine
	d.FetchTopShorts(searchutils.Latest)
}

func main() {
	log.SetAppName("ShortedApp")
	Handler(events.APIGatewayProxyRequest{})
	lambda.Start(Handler)
}
