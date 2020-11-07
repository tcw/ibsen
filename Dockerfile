############################
# STEP 1 build executable binary
############################
FROM golang:buster AS builder
COPY . /build
WORKDIR /build
ENV CGO_ENABLED=0 GO_LDFLAGS="-extldflags='-static'"
RUN go build -v -o app/ibsen . && mkdir -p app/data && chmod 600 app/data

# STEP 2 build a small image
############################
FROM scratch
COPY --from=builder /build/app/* /app/
COPY --from=builder /build/app/data /app/data
CMD ["app/ibsen","server","/app/data", "1>server.log", "2>server.log"]
