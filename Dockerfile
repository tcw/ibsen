# The Go version is the go directive in go.mod. CI builds this image with
# --build-arg GO_VERSION taken from there, so the default below only serves a hand-run build.
ARG GO_VERSION=1.27.3

FROM golang:${GO_VERSION}-trixie AS builder
WORKDIR /build
# dependencies first, so a source change does not download them again
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ibsen . \
 && mkdir -m 700 /out/data

FROM scratch
COPY --from=builder /out/ibsen /app/ibsen
# owned by the user the server runs as, since the log and its write lease live here
COPY --from=builder --chown=65532:65532 /out/data /data
USER 65532:65532
EXPOSE 54321
VOLUME /data
ENTRYPOINT ["/app/ibsen"]
CMD ["server", "-d", "/data"]
