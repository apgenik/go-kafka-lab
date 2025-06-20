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

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
			Async:    true, // Асинхронная запись для производительности
		},
	}
}

func (p *Producer) Publish(message string) (int64, error) {
	err := p.writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte(message),
		},
	)
	if err != nil {
		return 0, err
	}
	// В этой реализации offset не возвращается напрямую
	// Можно изменить логику, если нужно точное значение
	return 0, nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
