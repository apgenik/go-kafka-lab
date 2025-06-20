package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/apgenik/go-kafka-lab/api"
	"github.com/apgenik/go-kafka-lab/kafka"
)

type Event struct {
	Message string `json:"message"`
}

func main() {

	// r := gin.Default()

	// r.POST("/createmessage", api.CreateMessage)
	// r.GET("/readmessage", api.ReadMessage)

	// r.Run(":8081")

	kafkaBroker := []string{"localhost:9092"}
	topicName := "AR.APGENIK.V1"

	// Инициализация один раз при старте
	consumer := kafka.NewConsumer(kafkaBroker, topicName)
	defer consumer.Close()

	// Инициализация Kafka Producer
	producer := kafka.NewProducer(kafkaBroker, topicName)
	defer producer.Close()

	// Инициализация HTTP сервера
	server := api.NewServer(producer, consumer)

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := server.Start("8081"); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	<-quit
	log.Println("Shutting down server...")
	server.Stop()
}
