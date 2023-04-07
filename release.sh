#!/bin/bash -e
go test ./...
go fmt
go build -o bin/ibsen
docker build -t ibsen .