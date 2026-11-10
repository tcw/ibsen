#!/usr/bin/env bash
# The README's quick start, run against a built binary. Used on the release snapshot in CI,
# and by hand on anything you are about to hand somebody:
#   scripts/smoke-test.sh path/to/ibsen
set -euo pipefail

bin=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

expect() {
	if [ "$1" != "$2" ]; then
		printf 'smoke test: %s\n  want: %q\n  got:  %q\n' "$3" "$2" "$1" >&2
		exit 1
	fi
}

"$bin" --version

mkdir -p "$work/data"
printf 'hello\nworld\n' | "$bin" append -d "$work/data" greetings
expect "$("$bin" cat -d "$work/data" greetings)" $'hello\nworld' "cat"
expect "$("$bin" cat -d "$work/data" greetings 1 --offsets)" $'1\tworld' "cat from offset 1"
expect "$("$bin" topics -d "$work/data")" "greetings" "topics"

# the server half: in memory, on a port of its own
port=54399
"$bin" server -p "$port" > "$work/server.log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true; rm -rf "$work"' EXIT
for _ in $(seq 1 50); do
	"$bin" client list -p "$port" > /dev/null 2>&1 && break
	sleep 0.2
done
printf 'hello\nworld\n' | "$bin" client write -p "$port" greetings > /dev/null
# read follows the log until it is stopped, so it is given a few seconds and then cut off
read_out=$(timeout 3 "$bin" client read -p "$port" greetings || true)
expect "$read_out" $'0\thello\n1\tworld' "client read"

echo "smoke test passed"
