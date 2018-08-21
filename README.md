# shortedfunctions
[![Go Report Card](https://goreportcard.com/badge/github.com/shortedapp/shortedfunctions)](https://goreportcard.com/report/github.com/shortedapp/shortedfunctions)

Golang services to provide shorted functionality

These services are deployed using the Serverless Framework. 
For installation instructions: https://serverless.com/framework/docs/getting-started/

## To package
make 

## To clean
make clean

## To deploy
make deploy

## To Generate Swagger Spec
swagger generate spec -m -b ./{path to main function directory}  -o ./api/{function}-swagger.json

Example: swagger generate spec -m -b ./cmd/topshortquery/  -o ./api/topshortquery-swagger.json