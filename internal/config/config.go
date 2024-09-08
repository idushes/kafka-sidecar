package config

import (
	"github.com/rs/zerolog/log"
	"os"
	"strconv"
	"strings"
)

type conf struct {
	Debug                bool
	KafkaBrokers         []string
	KafkaTopics          []string
	KafkaConsumerGroupId string
	SchemaRegistryUrl    string
	HttpRoute            string
	TerminateOnError     bool
	CommitOnSuccess      bool
	StartupDelay         int
}

var Config conf

func init() {
	Config.Debug, _ = strconv.ParseBool(getEnv("DEBUG", "false"))
	Config.KafkaBrokers = strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	Config.KafkaTopics = strings.Split(getRequiredEnv("KAFKA_TOPICS"), ",")
	Config.KafkaConsumerGroupId = getRequiredEnv("KAFKA_CONSUMER_GROUP_ID")
	Config.SchemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	Config.HttpRoute = getRequiredEnv("HTTP_ROUTE")
	Config.TerminateOnError, _ = strconv.ParseBool(getEnv("TERMINATE_ON_ERROR", "true"))
	Config.CommitOnSuccess, _ = strconv.ParseBool(getEnv("COMMIT_ON_SUCCESS", "true"))
	Config.StartupDelay, _ = strconv.Atoi(getEnv("STARTUP_DELAY", "0"))

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

func getRequiredEnv(key string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	log.Error().Msgf("Required environment variable %s is not set", key)
	os.Exit(1)
	return ""
}
