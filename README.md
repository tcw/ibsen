# Ibsen (under development)

## Install

### GO

```shell script
go get github.com/tcw/ibsen@latest
```

### Docker

#### Create image

```shell
./release.sh
```



#### With Golang


#### With Docker

```shell
docker run --name ibsen_solveig -p 50001:50001 ibsen
```

In memory only
```shell
docker run --name ibsen_solvei -e IBSEN_IN_MEMORY_ONLY='true' -p 50001:50001 ibsen
```

## Usage

### GRPC

clients are under development

## Development

### Create grpc api

#### GO

```shell
protoc --proto_path=api/grpcApi ibsen.proto --go_out=plugins=grpc:./
```

#### Java

```shell
protoc --proto_path=api/grpc/JavaApi --java_out=api/grpc/JavaApi ibsen.proto
```

### Profiling

```shell script
ibsen server -d <path> -z cpu.pprof -y mem.pprof
go tool pprof cpu.pprof
> weblist ibsen
> evince
go tool pprof mem.pprof
> evince
```

## gRPC logging

```shell script

GRPC_GO_LOG_VERBOSITY_LEVEL=99 GRPC_GO_LOG_SEVERITY_LEVEL=info 

```

## Security

### Create ssl certs (for grpc)

```shell script
openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 -keyout key.pem -out cert.pem
```

## Todo

- better command completion
- improved error recovery/analysis
- block compression (Zstandard,snappy) ?

## Benchmarks

create data tmp directory

```shell

```

Start server
```shell
ibsen server -d "/tmp/ibsen/data"
```

To free pagecache in linux:
```shell
echo 1 > /proc/sys/vm/drop_caches 
```