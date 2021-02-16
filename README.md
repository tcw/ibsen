# Ibsen (under development)

## Install

### GO
```shell script
go get github.com/tcw/ibsen
```

### Docker 

#### Create image
```shell script
docker build -t ibsen .

```


#### Run grpc server version (recommended)
```shell script
docker run --name ibsen_solveig -p 50001:50001 ibsen

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
go tool pprof cpu.proff
> weblist ibsen
> pdf
go tool pprof mem.proff
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

For darwin bench
```shell script
ibsen client bench read <topic>
```

## Todo

- index cache files
- humanized error messages
- better command completion
- infinite read streaming (no pull) ?
- embedded etcd for clustering ?
- topic aliasing
- improved error recovery/analysis
- io separated tests (better interfaces)
- allow reading from the lowest block number
- block compression (Zstandard,snappy) ?

## Benchmarks

To free pagecache:
echo 1 > /proc/sys/vm/drop_caches
To free reclaimable slab objects (includes dentries and inodes):
echo 2 > /proc/sys/vm/drop_caches
To free slab objects and pagecache:
echo 3 > /proc/sys/vm/drop_caches

## Dir types

$ sudo debugfs /dev/partition
$ htree /
htree: Not a hash-indexed directory