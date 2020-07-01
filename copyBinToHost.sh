#!/bin/bash -e
target=$1

containerId=$(docker ps -aqf "name=ibsen")

docker cp "$containerId:/bin/ibsen" "$target/ibsen"
