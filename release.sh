#!/bin/bash -e

protoc --proto_path=api/grpc/golang ibsen.proto --go_out=plugins=grpc:api/grpc/golang
go fmt
go build -o bin/ibsenTool ./cmd/unix/tool
go build -o bin/ibsenClient ./cmd/unix/client
go build -o bin/ibsen
