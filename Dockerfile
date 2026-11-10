FROM golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/ibsen . && mkdir -p /out/data

FROM scratch
COPY --from=builder /out/ibsen /app/ibsen
COPY --from=builder /out/data /app/data
EXPOSE 54321
ENTRYPOINT ["/app/ibsen"]
CMD ["server", "-d", "/app/data"]
