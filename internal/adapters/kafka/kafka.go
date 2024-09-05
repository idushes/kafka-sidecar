package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	brokers         []string
	topics          []string
	consumerGroupId string
	consumers       map[string]*kafka.Reader
	producer        *kafka.Writer
}

func New(brokers, topics []string, consumerGroupId string) *Kafka {
	k := &Kafka{
		brokers:         brokers,
		topics:          topics,
		consumerGroupId: consumerGroupId,
		consumers:       make(map[string]*kafka.Reader, len(topics)),
		producer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: brokers,
		}),
	}
	for _, topic := range topics {
		k.consumers[topic] = kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: consumerGroupId,
		})
	}

	return k
}

func (k *Kafka) Listen(ctx context.Context) (<-chan kafka.Message, <-chan error) {
	once := sync.Once{}
	errCh := make(chan error)
	messageCh := make(chan kafka.Message)

	for topic, consumer := range k.consumers {
		go func(topic string, consumer *kafka.Reader) {
			defer once.Do(func() {
				close(errCh)
				close(messageCh)
			})

			for {
				select {
				case <-ctx.Done():
					errCh <- errors.New("cancelled by context")
					return
				default:
					m, err := consumer.FetchMessage(ctx)
					if err != nil {
						errCh <- fmt.Errorf("fetch message from topic %q error: %w", topic, err)
					} else {
						messageCh <- m
					}
				}
			}
		}(topic, consumer)
	}

	return messageCh, errCh
}

func (k *Kafka) CommitMessage(ctx context.Context, m kafka.Message) error {
	return k.consumers[m.Topic].CommitMessages(ctx, m)
}

func (k *Kafka) Send(ctx context.Context, m kafka.Message) error {
	return k.producer.WriteMessages(ctx, m)
}

func (k *Kafka) Close() error {
	for topic := range k.consumers {
		if k.consumers[topic] != nil {
			if err := k.consumers[topic].Close(); err != nil {
				return fmt.Errorf("close consumer %q error: %w", topic, err)
			}
		}
	}
	if k.producer != nil {
		if err := k.producer.Close(); err != nil {
			return fmt.Errorf("close producer error: %w", err)
		}
	}

	return nil
}
