FROM golang:alpine as builder

RUN apk update; apk add curl netcat-openbsd

COPY . /root

WORKDIR /root
RUN GO111MODULE=on GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /usr/local/bin/hyena-controller-manager -mod=vendor ./cmd/controller-manager/main.go

# hyena-operator
FROM debian:jessie-slim

COPY --from=builder usr/local/bin/hyena-controller-manager usr/local/bin/hyena-controller-manager
CMD ["/usr/local/bin/hyena-controller-manager"]