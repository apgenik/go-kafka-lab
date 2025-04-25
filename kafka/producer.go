package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker = "localhost:9092"
	topicName   = "AR.APGENIK.V1"
)

func PublishMessage(message string) (int64, error) {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	msg := kafka.Message{
		Key:   []byte("key"),
		Value: []byte(message),
		Time:  time.Now(),
	}

	err := writer.WriteMessages(context.Background(), msg)
	if err != nil {
		return 0, err
	}

	return getLastOffset()
}
