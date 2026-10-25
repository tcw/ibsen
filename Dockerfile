# The Go version is the one go.mod names; keep the two in step.
FROM golang:1.27.3 AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ibsen . \
 && mkdir -p /out/data \
 && chmod 700 /out/data

FROM scratch
COPY --from=builder /out/ibsen /app/ibsen
COPY --from=builder --chown=65532:65532 /out/data /app/data
USER 65532:65532
EXPOSE 54321
ENTRYPOINT ["/app/ibsen"]
CMD ["server", "-d", "/app/data"]
