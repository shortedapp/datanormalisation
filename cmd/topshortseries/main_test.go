package main

import (
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/shortedapp/shortedfunctions/internal/searchutils"

	"github.com/stretchr/testify/assert"
)

func TestConvertDurationToSearchPeriod(t *testing.T) {
	testCases := []struct {
		strVal string
		result searchutils.SearchPeriod
	}{
		{strVal: "week", result: searchutils.Week},
		{strVal: "wEek", result: searchutils.Week},
		{strVal: "week!", result: searchutils.Week},
		{strVal: "month", result: searchutils.Month},
		{strVal: "Month", result: searchutils.Month},
		{strVal: "year", result: searchutils.Year},
		{strVal: "yeaR", result: searchutils.Year},
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
