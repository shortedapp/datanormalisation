package main

import (
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/stretchr/testify/assert"
)

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
			QueryStringParameters: map[string]string{"number": "101"},
		}, false},
		{events.APIGatewayProxyRequest{
			HTTPMethod:            "GET",
			QueryStringParameters: map[string]string{"number": "0"},
		}, false},
		{events.APIGatewayProxyRequest{
			HTTPMethod:            "POST",
			QueryStringParameters: map[string]string{"number": "101"},
		}, false},
	}

	for _, test := range testCases {
		valid, _, _ := Validator(test.request)
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
			QueryStringParameters: map[string]string{"number": "101"},
		}, 400},
		{events.APIGatewayProxyRequest{
			HTTPMethod:            "GET",
			QueryStringParameters: map[string]string{"number": "0"},
		}, 400},
		{events.APIGatewayProxyRequest{
			HTTPMethod:            "POST",
			QueryStringParameters: map[string]string{"number": "101"},
		}, 400},
	}

	for _, test := range testCases {
		res, _ := Handler(test.request)
		assert.Equal(t, test.Code, res.StatusCode)
	}
}
