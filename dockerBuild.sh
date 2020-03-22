#!/bin/bash -e

go build -o /bin/ibsenTool /ibsen/build/cmd/unix/tool
go build -o /bin/ibsenTestClient /ibsen/build/cmd/unix/testClient
go build -o /bin/ibsenClient /ibsen/build/cmd/unix/client
go build -o /bin/ibsen /ibsen/build/