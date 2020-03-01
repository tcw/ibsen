#!/bin/bash -e

protoc --proto_path=api/golang grpcApi.proto --go_out=plugins=grpc:api/golang
go fmt
go build -o bin/logTool ./cmd/logTool
go build -o bin/dosky ./cmd/doskyClient
go build -o bin/dostoevsky
