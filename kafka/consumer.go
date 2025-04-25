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
