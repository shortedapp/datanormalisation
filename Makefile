build:
	env GOOS=linux vgo build -ldflags="-s -w" -o bin/datanormalize cmd/main.go

.PHONY: clean
clean:
	rm -rf ./bin go.sum

.PHONY: deploy
deploy: clean build
	serverless deploy --verbose
