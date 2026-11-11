# Builds an image from source. BINARY chooses which: ibsen (the default) or ibsen-k8s, which
# adds the Kubernetes Lease lock (docs/OPERATIONS.md#running-on-kubernetes):
#   docker build -t ibsen .
#   docker build -t ibsen-k8s --build-arg BINARY=ibsen-k8s .
# The Go version is the one go.mod names; keep the two in step.
FROM golang:1.27.3 AS builder
ARG BINARY=ibsen
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN if [ "$BINARY" = "ibsen-k8s" ]; then pkg=./cmd/ibsen-k8s; else pkg=.; fi \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ibsen "$pkg" \
 && mkdir -p /out/data \
 && chmod 700 /out/data

FROM scratch
COPY --from=builder /out/ibsen /app/ibsen
COPY --from=builder --chown=65532:65532 /out/data /app/data
USER 65532:65532
EXPOSE 54321
ENTRYPOINT ["/app/ibsen"]
CMD ["server", "-d", "/app/data"]
