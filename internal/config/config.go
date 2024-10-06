package config

import (
	"kafka-sidecar/internal/helpers"
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

type conf struct {
	Debug                     bool
	KafkaBrokers              []string
	KafkaTopics               []string
	KafkaConsumerGroupId      string
	AllowedTopics             []string
	SchemaRegistryUrl         string
	HttpRoute                 string
	HttpPort                  int
	TerminateOnError          bool
	CommitOnSuccess           bool
	StartupDelay              int
	AvroSchemaRefreshInterval int
}

var Config conf

func init() {
	Config.Debug, _ = strconv.ParseBool(getEnv("DEBUG", "false"))
	Config.KafkaBrokers = helpers.RemoveEmptyStrings(strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","))
	Config.KafkaTopics = helpers.RemoveEmptyStrings(strings.Split(getEnv("KAFKA_TOPICS", ""), ","))
	Config.KafkaConsumerGroupId = getEnv("KAFKA_CONSUMER_GROUP_ID", "")
	Config.AllowedTopics = helpers.RemoveEmptyStrings(strings.Split(getEnv("ALLOWED_TOPICS", ""), ","))
	Config.SchemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	Config.HttpRoute = getEnv("HTTP_ROUTE", "")
	Config.HttpPort, _ = strconv.Atoi(getEnv("HTTP_PORT", ""))
	Config.TerminateOnError, _ = strconv.ParseBool(getEnv("TERMINATE_ON_ERROR", "true"))
	Config.CommitOnSuccess, _ = strconv.ParseBool(getEnv("COMMIT_ON_SUCCESS", "true"))
	Config.StartupDelay, _ = strconv.Atoi(getEnv("STARTUP_DELAY", "0"))
	Config.AvroSchemaRefreshInterval, _ = strconv.Atoi(getEnv("AVRO_SCHEMA_REFRESH_INTERVAL", "10"))

	if len(Config.HttpRoute) == 0 && Config.HttpPort < 1 {
		log.Fatal().Msg("myst specify HTTP_ROUTE or HTTP_PORT")
	}

	if len(Config.HttpRoute) > 0 && (len(Config.KafkaConsumerGroupId) == 0 || len(Config.KafkaTopics) == 0) {
		log.Fatal().Msg("KAFKA_CONSUMER_GROUP_ID and KAFKA_TOPICS are required when HTTP_ROUTE is filled in")
	}

	log.Info().Strs("Brokers", Config.KafkaBrokers).
		Strs("Topics", Config.KafkaTopics).
		Str("GroupID", Config.KafkaConsumerGroupId).
		Str("HttpRoute", Config.HttpRoute).
		Msg("Consumer configuration")
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
