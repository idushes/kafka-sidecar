package config

import (
	"os"
	"strconv"
	"strings"
)

type conf struct {
	Debug             bool
	KafkaBrokers      []string
	KafkaTopics       []string
	SchemaRegistryUrl string
	HttpRoute         string
	TerminateOnError  bool
	CommitOnSuccess   bool
}

var Config conf

func init() {
	Config.Debug, _ = strconv.ParseBool(os.Getenv("DEBUG"))
	Config.KafkaBrokers = strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	Config.KafkaTopics = strings.Split(os.Getenv("KAFKA_TOPICS"), ",")
	Config.SchemaRegistryUrl = os.Getenv("SCHEMA_REGISTRY_URL")
	Config.HttpRoute = os.Getenv("HTTP_ROUTE")
	Config.TerminateOnError, _ = strconv.ParseBool(os.Getenv("TERMINATE_ON_ERROR"))
	Config.CommitOnSuccess, _ = strconv.ParseBool(os.Getenv("COMMIT_ON_SUCCESS"))
}
