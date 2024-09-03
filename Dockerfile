FROM golang:1.22.4-alpine as build

WORKDIR /app

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod ./
COPY *.go ./

RUN go mod download && go mod verify
RUN go build -o main .

FROM scratch
COPY --from=build /app/main /app/

CMD [ "/app/main" ]

EXPOSE 8080
