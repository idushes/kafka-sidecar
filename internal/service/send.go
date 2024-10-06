package service

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-sidecar/internal/config"
	"kafka-sidecar/internal/helpers"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

type sendMessage struct {
	Topic   string            `json:"topic"`
	Headers map[string]string `json:"headers"`
	Key     string            `json:"key"`
	Value   json.RawMessage   `json:"value"`
}

func (s *Service) send(ctx context.Context, msg []sendMessage) error {
	var err error

	kafkaMessages := make([]kafka.Message, len(msg))
	for i, re := range msg {
		if len(config.Config.AllowedTopics) > 0 && !helpers.InArrayString(config.Config.AllowedTopics, re.Topic) {
			return fmt.Errorf(
				"topic %q is not allowed",
				re.Topic,
			)
		}
		kafkaMessages[i].Topic = re.Topic
		kafkaMessages[i].Key = []byte(re.Key)
		for s2, s3 := range re.Headers {
			kafkaMessages[i].Headers = append(kafkaMessages[i].Headers, kafka.Header{Key: s2, Value: []byte(s3)})
		}
		kafkaMessages[i].Value, err = s.SchemaRegistry.Encode(re.Topic, re.Value)
		if err != nil {
			log.Debug().
				Str("topic", kafkaMessages[i].Topic).
				Any("headers", kafkaMessages[i].Headers).
				Bytes("value", kafkaMessages[i].Value).
				Msg("send message error")
			return fmt.Errorf(
				"pack message error for topic %s: value: %v, error: %w",
				re.Topic,
				string(re.Value),
				err,
			)
		}
	}

	for _, m := range kafkaMessages {
		log.Debug().
			Str("topic", m.Topic).
			Str("key", string(m.Key)).
			Msg("send message")

		if err := s.KafkaSender.Send(ctx, m); err != nil {
			return fmt.Errorf("send message error: %w", err)
		}
	}

	return nil
}
