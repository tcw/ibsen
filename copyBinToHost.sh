#!/bin/bash -e
target=$1

containerId=$(docker ps -aqf "name=ibsen")

docker cp "$containerId:/bin/ibsen" "$target/ibsen"
docker cp "$containerId:/bin/ibsenTool" "$target/ibsenTool"
docker cp "$containerId:/bin/ibsenClient" "$target/ibsenClient"