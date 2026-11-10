#!/usr/bin/env bash
# Regenerates the Go gRPC code from api/grpcApi/ibsen.proto: the server's copy in
# adapter/driver/grpcapi and the Go client's in clients/go/api/grpcApi. The generated files are
# committed, so a plain go build needs none of this.
#
# Needs protoc, and the two Go plugins at the versions below:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
set -euo pipefail
cd "$(dirname "$0")/.."

gen() {
	local out=$1 pkg=$2
	protoc --proto_path=api/grpcApi \
		--go_out="$out" --go_opt=paths=source_relative --go_opt="Mibsen.proto=$pkg" \
		--go-grpc_out="$out" --go-grpc_opt=paths=source_relative --go-grpc_opt="Mibsen.proto=$pkg" \
		ibsen.proto
}

gen adapter/driver/grpcapi "github.com/tcw/ibsen/adapter/driver/grpcapi;grpcapi"
gen clients/go/api/grpcApi "github.com/tcw/ibsen/clients/go/api/grpcApi;grpcApi"
