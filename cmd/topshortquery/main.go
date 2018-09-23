package main

import (
	"encoding/json"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/shortedfunctions/internal/handlerhelper/topshortsquery"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
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
	num, err := strconv.Atoi(number)
	if err != nil || num < 1 || num > 100 {
		return false, "{\"msg\": \"number query parameter must be a number between 1 and 100\"}", -1
	}
	return true, "", num
}

//Handler - the main function handler, triggered by a SNS event
func Handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// swagger:operation GET /topshortquery Handler
	//
	// Returns the top X shorted ASX stocks
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
	//     schema:
	//       type: array
	//       items:
	//         "$ref": "#/definitions/TopShortJSON"
	//   '400':
	//     description: result
	//     type: string

	//Validate the request
	valid, msg, num := Validator(request)
	if !valid {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       msg,
		}, nil
	}

	//Generate Clients
	clients := awsutil.GenerateAWSClients("dynamoDB")

	//Create topshortslist struct
	t := topshortsquery.Topshortsquery{Clients: clients}

	//Run the topshorts fetch routine
	res := t.QueryTopShorted("testTopShorts", num)

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
	lambda.Start(Handler)
}
