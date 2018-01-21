FROM golang:1.9 AS build

RUN curl -fsSL -o /usr/local/bin/dep https://github.com/golang/dep/releases/download/v0.3.2/dep-linux-amd64 && chmod +x /usr/local/bin/dep

RUN mkdir -p /go/src/s3-kinesis-replay
WORKDIR /go/src/s3-kinesis-replay

COPY Gopkg.toml Gopkg.lock ./
RUN dep ensure -vendor-only

CMD make help