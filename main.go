package main

import (
	"context"
	"kafka-sidecar/internal/adapters/kafka"
	"kafka-sidecar/internal/adapters/registry"
	"kafka-sidecar/internal/adapters/remoteServer"
	"kafka-sidecar/internal/config"
	"kafka-sidecar/internal/service"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	ctx, doneFunc := context.WithCancel(context.Background())
	defer doneFunc()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if config.Config.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	kafkaInst := kafka.New(config.Config.KafkaBrokers, config.Config.KafkaTopics, config.Config.KafkaConsumerGroupId)
	defer func() {
		if err := kafkaInst.Close(); err != nil {
			log.Error().Err(err).Msg("close kafka error")
		}
	}()

	srv := &service.Service{
		Kafka:            kafkaInst,
		SchemaRegistry:   registry.New(config.Config.SchemaRegistryUrl),
		RemoteServer:     remoteServer.New(config.Config.HttpRoute),
		CommitOnSuccess:  config.Config.CommitOnSuccess,
		TerminateOnError: config.Config.TerminateOnError,
	}

	srv.Run(ctx)
}
