FROM golang:1.18.3-buster AS builder
COPY . /build
WORKDIR /build
ENV CGO_ENABLED=0 GO_LDFLAGS="-extldflags='-static'"
RUN go build -v -o app/ibsen . && mkdir -p app/data && chmod 600 app/data

FROM scratch
COPY --from=builder /build/app/* /app/
COPY --from=builder /build/app/data /app/data
CMD ["app/ibsen","server","-d", "/app/data"]
