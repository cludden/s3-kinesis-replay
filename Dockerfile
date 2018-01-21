# build stage
FROM golang:1.9 as build

# install dep
RUN curl -fsSL -o /usr/local/bin/dep https://github.com/golang/dep/releases/download/v0.3.2/dep-linux-amd64 && chmod +x /usr/local/bin/dep

# create src directory
RUN mkdir -p /go/src/s3-kinesis-replay
WORKDIR /go/src/s3-kinesis-replay

# copy dependency manifest and install dependencies
COPY Gopkg.toml Gopkg.lock ./
RUN dep ensure --vendor-only

COPY . .

RUN make

# final stage
FROM alpine

RUN apk add --update ca-certificates mysql-client

RUN mkdir -p /etc/s3-kinesis-replay
COPY --from=build /go/src/s3-kinesis-replay/bin/s3-kinesis-replay /usr/local/bin/s3-kinesis-replay

ENTRYPOINT ["s3-kinesis-replay"]