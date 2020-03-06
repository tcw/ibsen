#!/bin/bash -e

FILE=api/grpc/golangApi/ibsen.pb.go
if [ ! -f "$FILE" ]; then
    echo "creating $FILE"
    protoc --proto_path=api/grpc/golangApi ibsen.proto --go_out=plugins=grpc:./
fi

go fmt
go build -o bin/ibsenTool ./cmd/unix/tool
go build -o bin/ibsenTestClient ./cmd/unix/testClient
go build -o bin/ibsenClient ./cmd/unix/client
go build -o bin/ibsen
