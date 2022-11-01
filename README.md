# Ibsen (under development)

## Install

### GO

```shell script
go install github.com/tcw/ibsen@latest
```

### Docker

#### Create image

```shell
./release.sh
```



#### With Golang


#### With Docker

Build
```shell
docker build -t ibsen .
```

Run
```shell
docker run --name ibsen_solveig -p 50001:50001 ibsen
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

```shell

openssl req -x509 -newkey rsa:4096 -days 365 -nodes -keyout ca-key.pem -out ca-cert.pem 
openssl x509 -in ca-cert.pem -noout -text
openssl req -newkey rsa:4096 -nodes -keyout server-key.pem -out server-req.pem 
openssl x509 -req -in server-req.pem -days 180 -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem -extfile server-ext.cnf
openssl x509 -in server-cert.pem -noout -text
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

## OpenTelemetry

docker run -v collector-gateway.yaml:/etc/otelcol/config.yaml otel/opentelemetry-collector:0.54.0
