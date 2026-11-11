#!/usr/bin/env bash
# Proves a bundled client works: generates it with the given ibsen binary, starts an in-memory
# server, builds the folder with that language's own toolchain, runs its example, and checks
# what it printed. CI runs it once per language, each in a container holding only that
# language's toolchain:
#   scripts/client-example.sh go|java|python|typescript path/to/ibsen
set -euo pipefail

language=$1
ibsen=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
work=$(mktemp -d)
port=${IBSEN_EXAMPLE_PORT:-54321}

"$ibsen" server -p "$port" >"$work/server.log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true; rm -rf "$work"' EXIT
for _ in $(seq 1 100); do
	"$ibsen" client list -p "$port" >/dev/null 2>&1 && break
	sleep 0.2
done

"$ibsen" generate client "$language" -o "$work/client"
cd "$work/client"
target="localhost:$port"
case "$language" in
go)
	go mod tidy
	go run ./example "$target" >"$work/out"
	;;
java)
	mvn -q -B compile exec:java "-Dexec.args=$target" >"$work/out"
	;;
python)
	python3 -m venv .venv
	.venv/bin/pip install -q .
	.venv/bin/python example.py "$target" >"$work/out"
	;;
typescript)
	npm install --no-audit --no-fund --silent
	npm run -s build
	node dist/example.js "$target" >"$work/out"
	;;
*)
	echo "unknown language $language" >&2
	exit 2
	;;
esac

cat "$work/out"
grep -q $'^0\thello$' "$work/out" && grep -q $'^1\tworld$' "$work/out" && grep -q 'example' "$work/out" || {
	echo "the $language example did not write and read back what it should have" >&2
	exit 1
}
echo "the $language client works"
