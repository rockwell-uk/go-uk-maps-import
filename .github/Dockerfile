FROM ubuntu:22.04

WORKDIR /go-uk-maps-import

RUN apt-get update && apt-get install -y libgeos-dev curl golang-go git
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v1.52.1
RUN mv bin/golangci-lint /usr/local/bin
RUN rm -rf bin

CMD go version && golangci-lint version && golangci-lint run --timeout 3m && echo "done"
