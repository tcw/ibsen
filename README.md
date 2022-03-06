# Ibsen (under development)

## Install

### GO

```shell script
go get -u github.com/tcw/ibsen
```

### Docker

#### Create image

```shell script
./release.sh
```

#### Run Ibsen server as docker container

```shell script
docker run --name ibsen_solveig -p 50001:50001 ibsen
```

In memory only
```shell script
docker run --name ibsen_solvei -e IBSEN_IN_MEMORY_ONLY='true' -p 50001:50001 ibsen
```

## Usage

### GRPC

clients are under development

## Development

### Create grpc api

#### GO

```shell script
protoc --proto_path=api/grpcApi ibsen.proto --go_out=plugins=grpc:./
```

#### Java

```shell script
protoc --proto_path=api/grpc/JavaApi --java_out=api/grpc/JavaApi ibsen.proto
```

### Profiling

```shell script
ibsen server <path> -z cpu.pprof -y mem.pprof
go tool pprof cpu.pprof
> weblist ibsen
> pdf
go tool pprof mem.pprof
> pdf
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
- topic aliasing
- improved error recovery/analysis
- block compression (Zstandard,snappy) ?

## Benchmarks

To free pagecache in linux:
```shell script
echo 1 > /proc/sys/vm/drop_caches 
```