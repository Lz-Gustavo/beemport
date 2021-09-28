all: build

build:
	go build -v

test:
	go test

coverage:
	go test -v -coverprofile cover.out
	go tool cover -func cover.out

coverage-report:
	go test -coverprofile=cover.out
	go tool cover -html=cover.out
