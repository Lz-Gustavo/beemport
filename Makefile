all: build

build:
	go build -v

test:
	go test -race

coverage:
	go test -v -race -coverprofile cover.out
	go tool cover -func cover.out

coverage-report:
	go test -race -coverprofile=cover.out
	go tool cover -html=cover.out
