package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	Message string `json:"message"`
}

func main() {
	brockers := "localhost:9092"
	topic := "AR.APGENIK.V1"

	// Подключение к Kafka
	// ---------- Producer ----------
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brockers},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	// Формируем сообщение
	event := Event{Message: "Hello, Kafka!"}
	value, err := json.Marshal(event)
	if err != nil {
		log.Fatalf("Ошибка сериализации: %v", err)
	}

	// Отправка сообщения
	msg := kafka.Message{
		Key:   []byte("key1"),
		Value: value,
		Time:  time.Now(),
	}

	ctx := context.Background()
	err = writer.WriteMessages(ctx, msg)
	if err != nil {
		log.Fatalf("Ошибка отправки в Kafka: %v", err)
	}

	fmt.Println("✅ Сообщение отправлено в Kafka")
}
