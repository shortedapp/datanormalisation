package main

import (
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/shortedapp/shortedfunctions/internal/searchutil"

	"github.com/stretchr/testify/assert"
)

func TestConvertDurationToSearchPeriod(t *testing.T) {
	testCases := []struct {
		strVal string
		result searchutil.SearchPeriod
	}{
		{strVal: "week", result: searchutil.Week},
		{strVal: "wEek", result: searchutil.Week},
		{strVal: "week!", result: searchutil.Week},
		{strVal: "month", result: searchutil.Month},
		{strVal: "Month", result: searchutil.Month},
		{strVal: "year", result: searchutil.Year},
		{strVal: "yeaR", result: searchutil.Year},
	}

	for _, test := range testCases {
		res := ConvertDurationToSearchPeriod(test.strVal)
		assert.Equal(t, res, test.result)
	}
}

func TestValidator(t *testing.T) {
	testCases := []struct {
		request events.APIGatewayProxyRequest
		valid   bool
	}{
		{events.APIGatewayProxyRequest{
			HTTPMethod: "GET",
		}, true},
		{events.APIGatewayProxyRequest{
			HTTPMethod:            "GET",
			QueryStringParameters: map[string]string{"number": "10"},
		}, true},
		{events.APIGatewayProxyRequest{
			HTTPMethod:            "GET",
			QueryStringParameters: map[string]string{"number": "0"},
		}, false},
	}

	for _, test := range testCases {
		valid, _, _, _ := Validator(test.request)
		assert.True(t, valid == test.valid)
	}
}

func TestHandler(t *testing.T) {
	testCases := []struct {
		request events.APIGatewayProxyRequest
		Code    int
	}{
		{events.APIGatewayProxyRequest{
			HTTPMethod:            "GET",
			QueryStringParameters: map[string]string{"number": "-1"},
		}, 400},
		{events.APIGatewayProxyRequest{
			HTTPMethod:            "GET",
			QueryStringParameters: map[string]string{"number": "0"},
		}, 400},
	}

	for _, test := range testCases {
		res, _ := Handler(test.request)
		assert.Equal(t, test.Code, res.StatusCode)
	}
}
