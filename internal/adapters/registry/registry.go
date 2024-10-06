package registry

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/riferrei/srclient"
)

type schemaBuf struct {
	Schema   *srclient.Schema
	Deadline time.Time
}

type Registry struct {
	client                    *srclient.SchemaRegistryClient
	avroSchemaRefreshInterval int

	mu      sync.Mutex
	schemas map[string]map[uint32]*schemaBuf
}

func New(url string, avroSchemaRefreshInterval int) *Registry {
	r := &Registry{
		client:                    srclient.NewSchemaRegistryClient(url),
		avroSchemaRefreshInterval: avroSchemaRefreshInterval,
		schemas:                   map[string]map[uint32]*schemaBuf{},
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

	var s schemaStruct
	if err := json.Unmarshal([]byte(schema.Codec().Schema()), &s); err != nil {
		return nil, fmt.Errorf("unmarshal schema error: %w", err)
	}

	value, err = r.deleteUnnecessaryFields(s, value)
	if err != nil {
		return nil, fmt.Errorf("delete unnecessary fields error: %w", err)
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
		buf := &schemaBuf{}
		if id != nil {
			buf = m[*id]
		} else if len(m) > 0 {
			ids := make([]int, 0, len(m))
			for u := range m {
				ids = append(ids, int(u))
			}
			sort.Ints(ids)
			buf = m[uint32(ids[len(ids)-1])]

		}
		if buf.Deadline.After(time.Now()) {
			schema = buf.Schema
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
		r.schemas[topic] = map[uint32]*schemaBuf{}
	}
	r.schemas[topic][*id] = &schemaBuf{
		schema,
		time.Now(),
	}

	return schema, nil
}

type schemaStruct struct {
	Fields []struct {
		Name string          `json:"name"`
		Type json.RawMessage `json:"type"`
	} `json:"fields"`
}

func (r *Registry) deleteUnnecessaryFields(s schemaStruct, b []byte) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("unmarshal message error: %w", err)
	}

	needDelete := make([]string, 0, len(m))
	for fieldName := range m {
		ok := false
		for i := 0; i < len(s.Fields); i++ {
			if s.Fields[i].Name == fieldName {
				var fs schemaStruct
				if err := json.Unmarshal(s.Fields[i].Type, &fs); err == nil {
					m[fieldName], err = r.deleteUnnecessaryFields(fs, m[fieldName])
					if err != nil {
						return nil, fmt.Errorf("delete from subschema %q error: %w", fieldName, err)
					}
				}
				ok = true
				break
			}
		}
		if !ok {
			needDelete = append(needDelete, fieldName)
		}
	}

	for _, fieldName := range needDelete {
		delete(m, fieldName)
	}
	var err error
	b, err = json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal message error: %w", err)
	}

	return b, nil
}
