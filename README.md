# go-kafka-lab

запуск kafka
```
    docker-compose up -d

```
    docker-compose -f ./docker-compose/docker-compose-kafka.yml up -d

```
    docker-compose -f ./docker-compose/docker-compose-kafka.yml down

```
    docker build -t go-kafka-lab -f dockerfile/Dockerfile .

```
    docker run -d --name app -p 8081:8081 go-kafka-lab:latest