FROM golang:1.24 as builder

WORKDIR /app

# Копируем файлы зависимостей из корня проекта
COPY ../go.mod ../go.sum ./

RUN go mod download

COPY . .
RUN go build -o app cmd/server/main.go

# ---

# FROM gcr.io/distroless/base-debian11
FROM ubuntu:22.04

WORKDIR /app
COPY --from=builder /app/app .

EXPOSE 8080

ENTRYPOINT ["/app/app"]
