## Create grpc api

```shell script
protoc -I api/ api/ibsen.proto --go_out=plugins=grpc:api
```

## Create ssl certs

```shell script
openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 -keyout key.pem -out cert.pem
```

## Run with profiling

```shell script
dostoevsky -s -cpu=cpu-profile.out -mem=mem-profile.out
```
