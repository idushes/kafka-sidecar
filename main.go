package main

import (
	"context"
	"kafka-sidecar/internal/adapters/httpServer"
	"kafka-sidecar/internal/adapters/kafka"
	"kafka-sidecar/internal/adapters/registry"
	"kafka-sidecar/internal/adapters/remoteServer"
	"kafka-sidecar/internal/config"
	"kafka-sidecar/internal/service"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/rs/zerolog"
)

func main() {
	if config.Config.StartupDelay > 0 {
		log.Info().Msgf("Startup delay %d seconds", config.Config.StartupDelay)
		time.Sleep(time.Duration(config.Config.StartupDelay) * time.Second)
	}

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
		KafkaSender:      kafkaInst,
		SchemaRegistry:   registry.New(config.Config.SchemaRegistryUrl, config.Config.AvroSchemaRefreshInterval),
		RemoteServer:     remoteServer.New(config.Config.HttpRoute),
		CommitOnSuccess:  config.Config.CommitOnSuccess,
		TerminateOnError: config.Config.TerminateOnError,
	}

	if len(config.Config.KafkaTopics) > 0 {
		srv.KafkaListener = kafkaInst
	}

	if config.Config.HttpPort > 0 {
		srv.HttpServer = httpServer.New(config.Config.HttpPort)
	}

	srv.Run(ctx)
}
