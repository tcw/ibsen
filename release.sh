#!/bin/bash -e

FILE=api/grpcApi/ibsen.pb.go
if [ ! -f "$FILE" ]; then
    echo "creating $FILE"
    protoc --proto_path=api/grpcApi ibsen.proto --go_out=plugins=grpc:./
fi
go test ./...
go fmt
go build -o bin/ibsen
docker build -t ibsen .