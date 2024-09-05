package registry

import (
	"fmt"

	"github.com/riferrei/srclient"
)

type Registry struct {
	client *srclient.SchemaRegistryClient
}

func New(url string) *Registry {
	return &Registry{
		client: srclient.NewSchemaRegistryClient(url),
	}
}

func (r *Registry) Encode(topic string, value []byte) ([]byte, error) {
	schema, err := r.client.GetLatestSchema(topic + "-value")
	if err != nil {
		return nil, fmt.Errorf("get schema error: %w", err)
	}

	native, _, err := schema.Codec().NativeFromTextual(value)
	if err != nil {
		return nil, fmt.Errorf("text to native error: %w", err)
	}
	valueBytes, err := schema.Codec().BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("native to binary error: %w", err)
	}

	return valueBytes, nil
}

func (r *Registry) Decode(topic string, value []byte) ([]byte, error) {
	schema, err := r.client.GetLatestSchema(topic + "-value")
	if err != nil {
		return nil, fmt.Errorf("get schema error: %w", err)
	}
	native, _, err := schema.Codec().NativeFromBinary(value)
	if err != nil {
		return nil, fmt.Errorf("binary to native error: %w", err)
	}
	text, err := schema.Codec().TextualFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("native to text error: %w", err)
	}

	return text, nil
}
