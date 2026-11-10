# Builds an image from source. BINARY chooses which: ibsen (the default) or ibsen-k8s, which
# adds the Kubernetes Lease lock (docs/OPERATIONS.md#running-on-kubernetes):
#   docker build -t ibsen .
#   docker build -t ibsen-k8s --build-arg BINARY=ibsen-k8s .
FROM golang:1.26-bookworm AS builder
ARG BINARY=ibsen
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN if [ "$BINARY" = "ibsen-k8s" ]; then pkg=./cmd/ibsen-k8s; else pkg=.; fi && \
    go build -trimpath -ldflags="-s -w" -o /out/ibsen "$pkg" && mkdir -p /out/data

FROM scratch
COPY --from=builder /out/ibsen /app/ibsen
COPY --from=builder /out/data /app/data
EXPOSE 54321
ENTRYPOINT ["/app/ibsen"]
CMD ["server", "-d", "/app/data"]
