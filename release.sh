#!/bin/bash -e

protoc --proto_path=api/grpc/golangApi ibsen.proto --go_out=plugins=grpc:./
go fmt
go build -o bin/ibsenTool ./cmd/unix/tool
go build -o bin/ibsenClient ./cmd/unix/client
go build -o bin/ibsen
