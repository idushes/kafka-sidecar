package registry

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/riferrei/srclient"
)

type Registry struct {
	client *srclient.SchemaRegistryClient

	mu      sync.Mutex
	schemas map[string]map[uint32]*srclient.Schema
}

func New(url string) *Registry {
	r := &Registry{
		client:  srclient.NewSchemaRegistryClient(url),
		schemas: map[string]map[uint32]*srclient.Schema{},
	}

	r.client.CodecJsonEnabled(true)
	r.client.CodecCreationEnabled(true)

	return r
}

func (r *Registry) Encode(topic string, value []byte) ([]byte, error) {
	schema, err := r.getSchema(topic+"-value", nil)
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

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))

	recordValue := make([]byte, 0, 1+len(schemaIDBytes)+len(valueBytes))
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, valueBytes...)

	return recordValue, nil
}

func (r *Registry) Decode(topic string, value []byte) ([]byte, error) {
	schemaID := binary.BigEndian.Uint32(value[1:5])
	schema, err := r.getSchema(topic+"-value", &schemaID)
	if err != nil {
		return nil, fmt.Errorf("get schema error: %w", err)
	}

	native, _, err := schema.Codec().NativeFromBinary(value[5:])
	if err != nil {
		return nil, fmt.Errorf("binary to native error: %w", err)
	}
	text, err := schema.Codec().TextualFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("native to text error: %w", err)
	}

	return text, nil
}

func (r *Registry) getSchema(topic string, id *uint32) (*srclient.Schema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	var schema *srclient.Schema

	if m := r.schemas[topic]; m != nil {
		if id != nil {
			schema = m[*id]
		} else if len(m) > 0 {
			ids := make([]int, 0, len(m))
			for u := range m {
				ids = append(ids, int(u))
			}
			sort.Ints(ids)
			schema = m[uint32(ids[len(ids)-1])]
		}
	}

	if schema != nil {
		return schema, nil
	}

	if id != nil {
		schema, err = r.client.GetSchema(int(*id))
		if err != nil {
			return nil, fmt.Errorf("get schema by id %d error: %w", id, err)
		}
	} else {
		schema, err = r.client.GetLatestSchema(topic)
		if err != nil {
			return nil, fmt.Errorf("get latest schema from topic %q error: %w", topic, err)
		}
		uintId := uint32(schema.ID())
		id = &uintId
	}

	if r.schemas[topic] == nil {
		r.schemas[topic] = map[uint32]*srclient.Schema{}
	}
	r.schemas[topic][*id] = schema

	return schema, nil
}
