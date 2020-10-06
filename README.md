## Create grpc api

```shell script
protoc --proto_path=api/grpc/golangApi ibsen.proto --go_out=plugins=grpc:./

protoc --proto_path=api/grpc/JavaApi --java_out=api/grpc/JavaApi ibsen.proto
```

## Create ssl certs

```shell script
openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 -keyout key.pem -out cert.pem
```

## Run with profiling

```shell script
ibsen -s <path> -cpu=cpu.pprof -mem=mem.pprof
```

## gRPC logging
```shell script

GRPC_GO_LOG_VERBOSITY_LEVEL=99 GRPC_GO_LOG_SEVERITY_LEVEL=info 

```

## Docker 

```shell script
docker build -t ibsen .

```

```shell script
docker run --name ibsen_peer -e IBSEN_HTTP=true -p 5001:5001 ibsen

```

```shell script
docker run --name ibsen_solveig -p 50001:50001 ibsen

```





## Todo

* Java client
* More tests
* Readme
* In-memory 
* Easy embeddable
