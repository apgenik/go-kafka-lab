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
	brokers := "localhost:9092"
	topic := "AR.APGENIK.V1"

	// Подключение к Kafka
	// ---------- Producer ----------
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brokers},
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

	fmt.Println("✅ Читаем сообщение из Kafka")
	// ---------- Reader ----------
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokers},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,    // минимальный размер пакета
		MaxBytes:  10e6, // максимальный размер пакета
	})
	defer reader.Close()

	// Пропускаем до конца, если есть старые сообщения
	reader.SetOffset(kafka.LastOffset - 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	message, err := reader.ReadMessage(ctx)
	if err != nil {
		log.Fatalf("Ошибка чтения из Kafka: %v", err)
	}

	fmt.Printf("📨 Получено сообщение: %s\n", string(message.Value))
	fmt.Printf("📌 Offset: %d | Partition: %d | Topic: %s\n",
		message.Offset, message.Partition, message.Topic)
}
