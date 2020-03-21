## Create grpc api

```shell script
protoc --proto_path=api/grpc/golangApi ibsen.proto --go_out=plugins=grpc:./
```

## Create ssl certs

```shell script
openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 -keyout key.pem -out cert.pem
```

## Run with profiling

```shell script
dostoevsky -s -cpu=cpu-profile.out -mem=mem-profile.out
```

GRPC_GO_LOG_VERBOSITY_LEVEL=99 GRPC_GO_LOG_SEVERITY_LEVEL=info


if os.Getenv("DEBUG") == "true" {
		log.Printf("worker [%d] - created hash [%d] from word [%s]\n", id, h.Sum32(), word)
	}