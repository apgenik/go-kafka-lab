#!/bin/bash

echo "Создание структуры проекта"

# Создаем основную структуру каталогов
mkdir -p ./{internal/{config,delivery/http,repository/kafka,domain},scripts,api,docs,deploy}

# Создаем основные файлы
touch ./internal/config/config.go
touch ./internal/delivery/http/handler.go
touch ./internal/repository/kafka/producer.go
touch ./internal/repository/kafka/consumer.go
touch ./internal/domain/models.go
