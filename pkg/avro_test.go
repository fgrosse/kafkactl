package pkg

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAvroDecoder(t *testing.T) {
	schemaRegistry := TestingSchemaRegistry{
		1: `{
			"type": "record",
			"namespace": "test",
			"name" : "Key",
			"fields" : [
			  { "name" : "id" , "type" : "string" }
			] 
		}`,
		2: `{
			"type": "record",
			"namespace": "test",
			"name" : "Record",
			"fields" : [
			  { "name" : "Name" , "type" : "string" },
			  { "name" : "Age" , "type" : "int" }
			] 
		}`,
	}

	encodedKey := encodeAvro(t, schemaRegistry[1], 1, map[string]any{
		"id": "e2901280-d602-11ed-99dc-9c2dcd849371",
	})
	encodedValue := encodeAvro(t, schemaRegistry[2], 2, map[string]any{
		"Name": "John Doe",
		"Age":  42,
	})

	msg := &sarama.ConsumerMessage{
		Key:       encodedKey,
		Value:     encodedValue,
		Topic:     "test-topic",
		Partition: 1,
		Offset:    42,
		Timestamp: time.Date(2023, 4, 7, 0, 0, 0, 0, time.UTC),
		Headers: []*sarama.RecordHeader{
			{Key: []byte("a"), Value: []byte("b")},
			{Key: []byte("x"), Value: []byte("y")},
		},
	}

	expected := &Message{
		Key:       json.RawMessage(`{"id":"e2901280-d602-11ed-99dc-9c2dcd849371"}`),
		Topic:     "test-topic",
		Partition: 1,
		Offset:    42,
		Timestamp: msg.Timestamp,
		Headers:   map[string][]string{"a": {"b"}, "x": {"y"}},
	}
	expectedValue := `{ "Name":"John Doe", "Age":42 }`

	d := NewAvroDecoder(schemaRegistry)
	actual, err := d.Decode(msg)
	require.NoError(t, err)

	assert.JSONEq(t, expectedValue, string(actual.Value.(json.RawMessage)))

	actual.Value = nil // checked already separately above
	assert.Equal(t, expected, actual)
}

func encodeAvro(t *testing.T, schema string, id uint32, value any) []byte {
	t.Helper()

	codec, err := goavro.NewCodec(schema)
	require.NoError(t, err)

	encoded, err := codec.BinaryFromNative(nil, value)
	require.NoError(t, err)

	idBuf := make([]byte, 5)
	binary.BigEndian.PutUint32(idBuf[1:], id)

	return append(idBuf, encoded...)
}

type TestingSchemaRegistry map[int]string

func (r TestingSchemaRegistry) Schema(id int) (string, error) {
	schema, ok := r[id]
	if !ok {
		return "", fmt.Errorf("unknown schema %d", id)
	}

	return schema, nil
}
