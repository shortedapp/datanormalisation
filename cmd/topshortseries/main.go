package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/shortedapp/shortedfunctions/internal/handlerhelper/topshortseries"
	"github.com/shortedapp/shortedfunctions/internal/searchutils"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Validator - Validates the input has all the correct data
func Validator(request events.APIGatewayProxyRequest) (bool, string, int) {
	if request.HTTPMethod != "GET" {
		return false, "{\"msg\": \"only HTTP GET is allowed on this resource\"}", -1
	}
	number, pres := request.QueryStringParameters["number"]
	if !pres {
		number = "10"
	}
	num, _ := strconv.Atoi(number)
	return true, "", num
}

//Handler - the main function handler, triggered by a API Gateway Proxy Request event
func Handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// swagger:operation GET /topshortseries Handler
	//
	// Returns the time series for the top X shorted ASX stocks
	//
	// ---
	// produces:
	// - application/json
	// parameters:
	// - name: number
	//   in: query
	//   description: maximum number of results to return
	//   required: false
	//   type: integer
	//   format: int32
	// responses:
	//   '200':
	//     description: Fetch Success
	//   '400':
	//     description: result
	//     type: string

	//Validate the request
	// valid, msg, num := Validator(request)
	// if !valid {
	// 	return events.APIGatewayProxyResponse{
	// 		StatusCode: 400,
	// 		Body:       msg,
	// 	}, nil
	// }

	//Generate Clients
	clients := awsutils.GenerateAWSClients("dynamoDB")

	//Create topshortseries object
	t := topshortseries.Topshortseries{Clients: clients}

	//Run the topshortseries fetch routine
	res := t.FetchTopShortedSeries("testTopShorts", "testShorts", 10, searchutils.Month)

	//Marshal the response and send back to the client
	respJSON, err := json.Marshal(res)
	return events.APIGatewayProxyResponse{
		StatusCode:      200,
		Headers:         nil,
		Body:            string(respJSON),
		IsBase64Encoded: true,
	}, err

}

func main() {
	log.SetAppName("ShortedApp")
	fmt.Println(Handler(events.APIGatewayProxyRequest{}))
	lambda.Start(Handler)
}
