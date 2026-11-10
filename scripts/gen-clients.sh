#!/usr/bin/env bash
# Regenerates every piece of code generated from api/grpcApi/ibsen.proto: the server's Go stubs
# in adapter/driver/grpcapi, the Go client's in clients/go/api/grpcApi, and the clients
# `ibsen generate client` hands out, in adapter/driver/cli/clientgen/clients/<lang>/, with the
# proto copied beside them. The hand-written
# wrappers, examples and build files live in the same folders and are left alone. Everything
# generated is committed, so a plain `go build` needs none of this; CI runs it and fails if
# the committed files differ, so they never go stale against the proto.
#
# Every generator is pinned, and fetched into $IBSEN_GEN_TOOLS (default .cache/gen-tools):
#   protoc 29.0 (Maven Central, as 4.29.0), and the same protoc as bundled by grpcio-tools 1.71.0 (PyPI)
#   for Python, whose gRPC generator only ships inside it
#   protoc-gen-go 1.36.12, protoc-gen-go-grpc 1.6.2 (Go module proxy)
#   protoc-gen-grpc-java 1.68.1 (Maven Central)
#   ts-proto 2.18.4 (npm)
# It needs python3, go, mvn and npm.
set -euo pipefail
cd "$(dirname "$0")/.."

PROTOC=4.29.0 # protoc 29.0, as Maven Central names it
GRPCIO_TOOLS=1.71.0
PROTOC_GEN_GO=v1.36.12
PROTOC_GEN_GO_GRPC=v1.6.2
GRPC_JAVA=1.68.1
TS_PROTO=2.18.4

tools=$(mkdir -p "${IBSEN_GEN_TOOLS:-.cache/gen-tools}" && cd "${IBSEN_GEN_TOOLS:-.cache/gen-tools}" && pwd)
out=adapter/driver/cli/clientgen/clients
proto=api/grpcApi/ibsen.proto

# --- the tools, once ---------------------------------------------------------------------------
if [ ! -x "$tools/venv/bin/python" ]; then
	python3 -m venv "$tools/venv"
fi
"$tools/venv/bin/pip" install -q --disable-pip-version-check "grpcio-tools==$GRPCIO_TOOLS"
GOBIN="$tools/bin" GOFLAGS= go install "google.golang.org/protobuf/cmd/protoc-gen-go@$PROTOC_GEN_GO"
GOBIN="$tools/bin" GOFLAGS= go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@$PROTOC_GEN_GO_GRPC"
protoc_bin="$tools/bin/protoc-$PROTOC"
if [ ! -x "$protoc_bin" ]; then
	mvn -q dependency:copy "-Dartifact=com.google.protobuf:protoc:$PROTOC:exe:linux-x86_64" \
		"-DoutputDirectory=$tools/java" >/dev/null
	cp "$tools/java/protoc-$PROTOC-linux-x86_64.exe" "$protoc_bin"
	chmod +x "$protoc_bin"
fi
java_plugin="$tools/bin/protoc-gen-grpc-java-$GRPC_JAVA"
if [ ! -x "$java_plugin" ]; then
	mvn -q dependency:copy "-Dartifact=io.grpc:protoc-gen-grpc-java:$GRPC_JAVA:exe:linux-x86_64" \
		"-DoutputDirectory=$tools/java" >/dev/null
	cp "$tools/java/protoc-gen-grpc-java-$GRPC_JAVA-linux-x86_64.exe" "$java_plugin"
	chmod +x "$java_plugin"
fi
if [ ! -x "$tools/ts/node_modules/.bin/protoc-gen-ts_proto" ]; then
	npm install --silent --no-audit --no-fund --prefix "$tools/ts" "ts-proto@$TS_PROTO" >/dev/null
fi

protoc() {
	"$protoc_bin" --proto_path=api/grpcApi "$@" ibsen.proto
}

python_protoc() {
	"$tools/venv/bin/python" -m grpc_tools.protoc --proto_path=api/grpcApi "$@" ibsen.proto
}

# --- the server's and the Go client's stubs ---------------------------------------------------------
go_stubs() {
	local dir=$1 pkg=$2
	PATH="$tools/bin:$PATH" protoc \
		--go_out="$dir" --go_opt=paths=source_relative --go_opt="Mibsen.proto=$pkg" \
		--go-grpc_out="$dir" --go-grpc_opt=paths=source_relative --go-grpc_opt="Mibsen.proto=$pkg"
}
go_stubs adapter/driver/grpcapi "github.com/tcw/ibsen/adapter/driver/grpcapi;grpcapi"
go_stubs clients/go/api/grpcApi "github.com/tcw/ibsen/clients/go/api/grpcApi;grpcApi"

# --- the proto itself, for `ibsen generate proto` ------------------------------------------------
cp "$proto" adapter/driver/cli/clientgen/ibsen.proto

# --- Go: stubs in package ibsenpb, kept as .go.tmpl so the main module does not build them -------
go_out=$(mktemp -d)
PATH="$tools/bin:$PATH" protoc \
	--go_out="$go_out" --go_opt=paths=source_relative --go_opt="Mibsen.proto=example.com/ibsen-client/ibsenpb;ibsenpb" \
	--go-grpc_out="$go_out" --go-grpc_opt=paths=source_relative --go-grpc_opt="Mibsen.proto=example.com/ibsen-client/ibsenpb;ibsenpb"
mkdir -p "$out/go/ibsenpb"
cp "$go_out/ibsen.pb.go" "$out/go/ibsenpb/ibsen.pb.go.tmpl"
cp "$go_out/ibsen_grpc.pb.go" "$out/go/ibsenpb/ibsen_grpc.pb.go.tmpl"
rm -rf "$go_out"
# the wrapper is the repository's own Go client, pointed at the stubs beside it
mkdir -p "$out/go/ibsen"
sed -e 's#"github.com/tcw/ibsen/clients/go/api/grpcApi"#grpcApi "@@MODULE@@/ibsenpb"#' \
	-e 's#// generated gRPC stubs in clients/go/api/grpcApi.#// generated gRPC stubs in package ibsenpb.#' \
	-e 's#// docs/USER_GUIDE.md\#safe-retries.#// the README beside this folder.#' \
	clients/go/ibsen/client.go >"$out/go/ibsen/client.go.tmpl"

# --- Java: messages from protoc, stubs from grpc-java ---------------------------------------------
java_out="$out/java/src/main/java"
rm -rf "$java_out/github"
protoc --java_out="$java_out" --plugin=protoc-gen-grpc-java="$java_plugin" --grpc-java_out="$java_out"

# --- Python: messages, type stubs and grpc stubs, imported relative to the package ---------------
py_out="$out/python/__package__"
rm -f "$py_out"/ibsen_pb2*.py "$py_out"/ibsen_pb2*.pyi
python_protoc --python_out="$py_out" --pyi_out="$py_out" --grpc_python_out="$py_out"
sed -i 's/^import ibsen_pb2 as ibsen__pb2$/from . import ibsen_pb2 as ibsen__pb2/' "$py_out/ibsen_pb2_grpc.py"

# --- TypeScript: ts-proto, with grpc-js services ----------------------------------------------------
protoc --plugin=protoc-gen-ts_proto="$tools/ts/node_modules/.bin/protoc-gen-ts_proto" \
	--ts_proto_out="$out/typescript/src" \
	--ts_proto_opt=outputServices=grpc-js,esModuleInterop=true,env=node,useOptionals=messages

echo "generated clients in $out"
