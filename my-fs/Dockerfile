FROM golang:1.16.4-alpine3.13 as builder
WORKDIR /opt
ENV GOPROXY https://goproxy.io
COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .
RUN CGO_ENABLE=0 GOARCH=arm64 GOOS=darwin go build -a -o myfs main.go

FROM alpine:3.10 AS final
WORKDIR /opt
COPY --from=builder /opt/. /opt
CMD /opt/myfs
