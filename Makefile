.PHONY: clean
clean:
	rm -rf ./bin go.sum

.PHONY: build
build:
	env GOOS=linux go build -ldflags="-s -w" -o bin/datanormalise cmd/datanormalise/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/dynamoingestor cmd/dynamoingestor/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/bulknormalise cmd/bulknormalise/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/codedmoversquery cmd/codedmoversquery/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/datafetch cmd/datafetch/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/topmoversingest cmd/topmoversingest/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/topmoversquery cmd/topmoversquery/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/topshortingestor cmd/topshortingestor/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/topshortquery cmd/topshortquery/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/topshortseries cmd/topshortseries/main.go
	swagger generate spec -m -b ./cmd/topshortquery/  -o ./api/topshortquery-swagger.json
	swagger generate spec -m -b ./cmd/topmoversquery/  -o ./api/topmoversquery-swagger.json
	swagger generate spec -m -b ./cmd/codedmoversquery/  -o ./api/codedmoversquery-swagger.json

.PHONY: deploy
deploy: clean build
	serverless deploy --verbose

.PHONY: remove
deploy: 
	serverless remove --verbose
