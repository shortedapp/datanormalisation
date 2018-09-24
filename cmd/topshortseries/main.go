package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/shortedapp/shortedfunctions/internal/handlerhelper/topshortseries"
	"github.com/shortedapp/shortedfunctions/internal/searchutil"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//ConvertDurationToSearchPeriod - Convert String search duration to search period
//defaults if an invalid selection is provided
func ConvertDurationToSearchPeriod(duration string) searchutil.SearchPeriod {
	switch strings.ToLower(duration) {
	case "week":
		return searchutil.Week
	case "month":
		return searchutil.Month
	case "year":
		return searchutil.Year
	}
	return searchutil.Week
}

//Validator - Validates the input has all the correct data
func Validator(request events.APIGatewayProxyRequest) (bool, string, int, searchutil.SearchPeriod) {
	if request.HTTPMethod != "GET" {
		return false, "{\"msg\": \"only HTTP GET is allowed on this resource\"}", -1, searchutil.Day
	}
	number, pres := request.QueryStringParameters["number"]
	if !pres {
		number = "10"
	}
	num, _ := strconv.Atoi(number)
	if num <= 0 {
		return false, "{\"msg\": \"number nmust be greater than 0\"}", -1, searchutil.Day
	}
	duration, _ := request.QueryStringParameters["duration"]
	return true, "", num, ConvertDurationToSearchPeriod(duration)
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
	//   required: true
	//   type: integer
	//   format: int32
	// - name: duration
	//   in: query
	//   description: timeframe of the series
	//   required: true
	//   type: string
	//   format: string
	// responses:
	//   '200':
	//     description: Fetch Success
	//   '400':
	//     description: result
	//     type: string

	//Validate the request
	valid, msg, num, duration := Validator(request)
	if !valid {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       msg,
		}, nil
	}

	//Generate Clients
	clients := awsutil.GenerateAWSClients("dynamoDB")

	//Create topshortseries struct
	t := topshortseries.Topshortseries{Clients: clients}

	//Run the topshortseries fetch routine
	res := t.FetchTopShortedSeries("testTopShorts", "testShorts", num, duration)

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
	fmt.Println(Handler(events.APIGatewayProxyRequest{HTTPMethod: "GET", QueryStringParameters: map[string]string{"number": "10", "duration": "month"}}))
	lambda.Start(Handler)
}
