# shortedfunctions
[![Go Report Card](https://goreportcard.com/badge/github.com/shortedapp/shortedfunctions)](https://goreportcard.com/report/github.com/shortedapp/shortedfunctions)

Golang services to provide shorted functionality

These services are deployed using the Serverless Framework. 
For installation instructions: https://serverless.com/framework/docs/getting-started/


# Make File Commands
## To clean
make clean

## To package and generate OpenAPI specs
make build

## To deploy
make deploy

# Current functions
## Data fetching and normalisation
- datafetch: Refresh long term data such as stock codes
- datanormalise: Normalises the data from ASIC/ASX/etc. and uploads to S3.
bulknormalise: Bulk normalises and ingest data into S3.

## Ingestion
- dynamoingestor: Ingest latest short data into dynamodb by code
- topmoversingest: Ingest movement statistics for eash stock
- topshortsingestor: Ingest the top shorts for the day based on order

## Querys
- codedmoversquery: Query the movement statistics for a particular stock code
- topmoversquery: Query the movement statistics for the top X stocks
- topshortseries: Query to retreive the time series data for the top X stocks
- topshortsquery: Query the top X shorts for the day based on order