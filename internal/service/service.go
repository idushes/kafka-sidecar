package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
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

type HttpServer interface {
	Listen(ctx context.Context) (<-chan []byte, <-chan error)
}

type KafkaListener interface {
	Listen(ctx context.Context) (<-chan kafka.Message, <-chan error)
	CommitMessage(ctx context.Context, m kafka.Message) error
}

type KafkaSender interface {
	Send(ctx context.Context, m kafka.Message) error
}

type Service struct {
	KafkaListener    KafkaListener
	KafkaSender      KafkaSender
	HttpServer       HttpServer
	SchemaRegistry   SchemaRegistry
	RemoteServer     RemoteServer
	CommitOnSuccess  bool
	TerminateOnError bool
}

func (s *Service) Run(ctx context.Context) {
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		if s.KafkaListener == nil {
			return
		}

		messageCh, errorCh := s.KafkaListener.Listen(ctx)

		go func() {
			for err := range errorCh {
				log.Error().Err(err).Msg("kafka listen error")
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
				Msg("new message from kafka")

			needExit := false
			err := s.kafkaProcessing(ctx, m)
			if err != nil {
				log.Error().Err(err).Msg("kafka processing error")
				needExit = s.TerminateOnError
			}
			if err == nil && s.CommitOnSuccess {
				log.Debug().Msgf("Committing message with offset: %d", m.Offset)
				if err = s.KafkaListener.CommitMessage(ctx, m); err != nil {
					log.Error().Err(err).Msg("commit error")
					needExit = needExit || s.TerminateOnError
				}
			}

			if needExit {
				os.Exit(1)
			}
		}
	}()

	go func() {
		defer wg.Done()
		if s.HttpServer == nil {
			return
		}

		messageCh, errorCh := s.HttpServer.Listen(ctx)

		go func() {
			for err := range errorCh {
				log.Error().Err(err).Msg("kafka listen error")
				if s.TerminateOnError {
					os.Exit(1)
				}
			}
		}()

		for m := range messageCh {
			log.Debug().
				Bytes("message", m).
				Msg("new message from http")

			err := s.httpServerProcessing(ctx, m)
			if err != nil {
				log.Error().Err(err).Msg("http server processing error")
				os.Exit(1)
			}
		}
	}()

	wg.Wait()
}

func (s *Service) kafkaProcessing(ctx context.Context, msg kafka.Message) error {
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

	var res []sendMessage

	if err := json.Unmarshal(data, &res); err != nil {
		return fmt.Errorf(
			"unmarshal response error for data: %v, error: %w",
			data,
			err,
		)
	}

	for i := range res {
		if res[i].Headers == nil {
			res[i].Headers = map[string]string{}
		}
		res[i].Headers["processed_topic"] = msg.Topic
	}

	return s.send(ctx, res)
}

func (s *Service) httpServerProcessing(ctx context.Context, msg []byte) error {
	var res []sendMessage

	if err := json.Unmarshal(msg, &res); err != nil {
		return fmt.Errorf(
			"unmarshal message error for data: %s, error: %w",
			string(msg),
			err,
		)
	}

	return s.send(ctx, res)
}
