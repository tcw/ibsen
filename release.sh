#!/bin/bash -e

FILE=api/grpc/golangApi/ibsen.pb.go
if [ ! -f "$FILE" ]; then
    echo "creating $FILE"
    protoc --proto_path=api/grpc/golangApi ibsen.proto --go_out=plugins=grpc:./
fi
go test ./...
go fmt
go build -o bin/ibsen