FROM golang:latest

WORKDIR /application

COPY src/go.mod .
COPY src/go.sum .

RUN go mod download
COPY src .

RUN go build -race

CMD ["./src"]