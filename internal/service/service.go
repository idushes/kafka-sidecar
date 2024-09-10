package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/segmentio/kafka-go"
)

type SchemaRegistry interface {
	Encode(topic string, value []byte) ([]byte, error)
	Decode(topic string, value []byte) ([]byte, error)
}

type RemoteServer interface {
	Send(ctx context.Context, topic string, headers map[string]string, key, value []byte, timestamp time.Time, offset int64) ([]byte, error)
}

type Kafka interface {
	Listen(ctx context.Context) (<-chan kafka.Message, <-chan error)
	CommitMessage(ctx context.Context, m kafka.Message) error
	Send(ctx context.Context, m kafka.Message) error
}

type Service struct {
	Kafka            Kafka
	SchemaRegistry   SchemaRegistry
	RemoteServer     RemoteServer
	CommitOnSuccess  bool
	TerminateOnError bool
}

func (s *Service) Run(ctx context.Context) {
	messageCh, errorCh := s.Kafka.Listen(ctx)

	go func() {
		for err := range errorCh {
			log.Error().Err(err).Msg("listen error")
			if s.TerminateOnError {
				os.Exit(1)
			}
		}
	}()

	for m := range messageCh {
		log.Debug().
			Str("topic", m.Topic).
			Str("key", string(m.Key)).
			Time("timestamp", m.Time).
			Int64("offset", m.Offset).
			Msg("new message")

		needExit := false
		err := s.processing(ctx, m)
		if err != nil {
			log.Error().Err(err).Msg("processing error")
			needExit = s.TerminateOnError
		}
		if err == nil && s.CommitOnSuccess {
			log.Debug().Msgf("Committing message with offset: %d", m.Offset)
			if err = s.Kafka.CommitMessage(ctx, m); err != nil {
				log.Error().Err(err).Msg("commit error")
				needExit = needExit || s.TerminateOnError
			}
		}

		if needExit {
			os.Exit(1)
		}
	}
}

func (s *Service) processing(ctx context.Context, msg kafka.Message) error {
	value, err := s.SchemaRegistry.Decode(msg.Topic, msg.Value)
	if err != nil {
		return fmt.Errorf(
			"failed to decode message from topic %s: raw_value: %v, error: %w",
			msg.Topic,
			string(msg.Value),
			err,
		)
	}

	headers := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}

	data, err := s.RemoteServer.Send(
		ctx,
		msg.Topic,
		headers,
		msg.Key,
		value,
		msg.Time,
		msg.Offset,
	)
	if err != nil {
		return fmt.Errorf(
			"request to remote server error for topic %s: key: %v, value: %v, error: %w",
			msg.Topic,
			msg.Key,
			value,
			err,
		)
	}

	var res []struct {
		Topic   string            `json:"topic"`
		Headers map[string]string `json:"headers"`
		Key     string            `json:"key"`
		Value   json.RawMessage   `json:"value"`
	}

	if err := json.Unmarshal(data, &res); err != nil {
		return fmt.Errorf(
			"unmarshal response error for data: %v, error: %w",
			data,
			err,
		)
	}

	for _, re := range res {
		var m kafka.Message
		m.Topic = re.Topic
		m.Key = []byte(re.Key)
		for s2, s3 := range re.Headers {
			m.Headers = append(m.Headers, kafka.Header{Key: s2, Value: []byte(s3)})
		}
		m.Value, err = s.SchemaRegistry.Encode(re.Topic, re.Value)
		if err != nil {
			return fmt.Errorf(
				"pack message error for topic %s: value: %v, error: %w",
				re.Topic,
				string(re.Value),
				err,
			)
		}

		log.Debug().
			Str("topic", m.Topic).
			Str("key", string(m.Key)).
			Time("timestamp", m.Time).
			Int64("offset", m.Offset).
			Msg("send message")

		if err := s.Kafka.Send(ctx, m); err != nil {
			return fmt.Errorf("send message error: %w", err)
		}
	}

	return nil
}
