package main

import (
	"context"
	"net/http"
	"os"

	"kafka-sidecar/internal/adapters/kafka"
	"kafka-sidecar/internal/adapters/registry"
	"kafka-sidecar/internal/adapters/remoteServer"
	"kafka-sidecar/internal/config"
	"kafka-sidecar/internal/service"

	"github.com/labstack/echo/v4"
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

	kafkaInst := kafka.New(config.Config.KafkaBrokers, config.Config.KafkaTopics)
	defer func() {
		if err := kafkaInst.Close(); err != nil {
			log.Error().Err(err).Msg("close kafka error")
		}
	}()

	srv := &service.Service{
		Kafka:           kafkaInst,
		SchemaRegistry:  registry.New(config.Config.SchemaRegistryUrl),
		RemoteServer:    remoteServer.New(config.Config.HttpRoute),
		CommitOnSuccess: config.Config.CommitOnSuccess,
	}

	for err := range srv.Run(ctx) {
		log.Error().Err(err).Msg("service error")
		if config.Config.TerminateOnError {
			os.Exit(1)
		}
	}

	e := echo.New()
	e.Debug = config.Config.Debug
	e.GET("/health/check", func(c echo.Context) error {
		return c.HTML(http.StatusOK, "ok")
	})
	if err := e.Start("localhost:8080"); err != nil {
		log.Fatal().Err(err).Msg("start router error")
		os.Exit(1)
	}
}
