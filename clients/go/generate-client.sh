#!/usr/bin/env bash
# The Go client's code is generated with the server's, from one place.
exec "$(dirname "$0")/../../scripts/generate-proto.sh"
