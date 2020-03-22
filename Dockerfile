############################
# STEP 1 build executable binary
############################
FROM golang:buster AS builder
COPY . /ibsen/build
WORKDIR /ibsen/build
ENV CGO_ENABLED=0 GO_LDFLAGS="-extldflags='-static'"
RUN ./dockerBuild.sh && mkdir -p /data && chmod 600 /data

# STEP 2 build a small image
############################
FROM scratch
COPY --from=builder /bin/ibsen /bin/ibsen
COPY --from=builder /data /data

CMD ["/bin/ibsen","-s","/data"]