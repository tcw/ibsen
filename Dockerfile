############################
# STEP 1 build executable binary
############################
FROM golang:buster AS builder
COPY . /ibsen/build
WORKDIR /ibsen/build
RUN ./dockerBuild.sh
# STEP 2 build a small image
############################
FROM scratch
COPY --from=builder /ibsen/build/bin/* /bin/
RUN mkdir -p ~/data
CMD ["/bin/ibsen", "-s", "~/data"]