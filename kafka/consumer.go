package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

func ReadLastMessage() (string, int, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       topicName,
		Partition:   0,
		StartOffset: kafka.LastOffset - 1,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m, err := reader.ReadMessage(ctx)
	if err != nil {
		return "", 0, err
	}

	return string(m.Value), m.Partition, nil
}

func getLastOffset() (int64, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       topicName,
		Partition:   0,
		StartOffset: kafka.LastOffset - 1,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	m, err := reader.ReadMessage(ctx)
	if err != nil {
		return 0, err
	}

	return m.Offset, nil
}

//
//

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: 0,                      // Читаем только из партиции 0
			MinBytes:  1,                      // Минимальный размер для немедленного ответа
			MaxBytes:  10e6,                   // 10MB
			MaxWait:   100 * time.Millisecond, // Максимальное время ожидания
		}),
	}
}

func (c *Consumer) ReadMessage() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return "", err
	}

	return string(msg.Value), nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
