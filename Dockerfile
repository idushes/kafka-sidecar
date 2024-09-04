FROM golang:1.21-alpine AS base

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o main

EXPOSE 8000

CMD ["/build/main"]