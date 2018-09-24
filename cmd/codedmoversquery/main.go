package main

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/shortedfunctions/internal/handlerhelper/codedmoversquery"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Validator - Validates the input has all the correct data
func Validator(request events.APIGatewayProxyRequest) (bool, string, string) {
	if request.HTTPMethod != "GET" {
		return false, "{\"msg\": \"only HTTP GET is allowed on this resource\"}", ""
	}
	code, pres := request.QueryStringParameters["code"]
	if !pres {
		return false, "{\"msg\": \"an ASX code must be provided\"}", ""
	}

	return true, "", code
}

//Handler - the main function handler, triggered by a SNS event
func Handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// swagger:operation GET /codedmovers Handler
	//
	// Returns the top X movers
	//
	// ---
	// produces:
	// - application/json
	// parameters:
	// - name: code
	//   in: query
	//   description: code to return movement about
	//   required: false
	//   type: string
	// responses:
	//   '200':
	//     description: Fetch Success
	//     schema:
	//        "$ref": "#/definitions/CodedTopMovers"
	//   '400':
	//     description: result
	//     type: string

	//Validate the request
	valid, msg, code := Validator(request)
	if !valid {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       msg,
		}, nil
	}

	//Generate Clients
	clients := awsutil.GenerateAWSClients("dynamoDB")

	//Create moversquery struct
	t := codedmoversquery.CodedMoversQuery{Clients: clients}

	//Run the topshorts fetch routine
	res := t.QueryCodedTopMovers("CodedTopMovers", code)

	if res == nil {
		return events.APIGatewayProxyResponse{
			StatusCode:      404,
			Headers:         nil,
			Body:            "{\"msg\": \"could not find code\"}",
			IsBase64Encoded: true,
		}, nil
	}

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
	queryStrings := make(map[string]string)
	queryStrings["code"] = "TLS"
	res, _ := Handler(events.APIGatewayProxyRequest{HTTPMethod: "GET", QueryStringParameters: queryStrings})
	fmt.Println(res)
	lambda.Start(Handler)
}
