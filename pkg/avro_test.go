package pkg

import (
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAvroDecoder(t *testing.T) {
	schema := `{
		"type": "record",
		"namespace": "test",
		"name" : "Record",
		"fields" : [
		  { "name" : "Name" , "type" : "string" },
		  { "name" : "Age" , "type" : "int" }
		] 
	}`

	encoded := encodeAvro(t, schema, 123, map[string]any{
		"Name": "John Doe",
		"Age":  42,
	})

	msg := &sarama.ConsumerMessage{
		Key:       []byte("123"),
		Value:     encoded,
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
		Key:       "123",
		Topic:     "test-topic",
		Partition: 1,
		Offset:    42,
		Timestamp: msg.Timestamp,
		Headers:   map[string][]string{"a": {"b"}, "x": {"y"}},
	}
	expectedValue := `{ "Name":"John Doe", "Age":42 }`

	d := NewAvroDecoder(TestingSchemaRegistry{schema}, false)
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
	binary.BigEndian.PutUint32(idBuf, id)

	return append(idBuf, encoded...)
}

type TestingSchemaRegistry struct {
	schema string
}

func (r TestingSchemaRegistry) Schema(int) (string, error) {
	return r.schema, nil
}
