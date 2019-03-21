FROM golang:1.11

ENV GO111MODULE on
RUN mkdir -p /go/src/github.com/tmlbl/promscylla
WORKDIR /go/src/github.com/tmlbl/promscylla

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .
RUN go build -o /promscylla

FROM debian:stretch

COPY --from=0 /promscylla /bin/promscylla

ENTRYPOINT [ "/bin/promscylla" ]
