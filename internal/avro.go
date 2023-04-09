package internal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
)

// AvroDecoder is a Decoder implementation that supports the Apache Avro format.
type AvroDecoder struct {
	registry SchemaRegistry
}

// NewAvroDecoder creates a new AvroDecoder instance.
func NewAvroDecoder(r SchemaRegistry) *AvroDecoder {
	return &AvroDecoder{
		registry: r,
	}
}

// Decode a message from Kafka into our own Message type.
func (d *AvroDecoder) Decode(kafkaMsg *sarama.ConsumerMessage) (*Message, error) {
	key, err := d.decode(kafkaMsg.Key)
	if err != nil {
		return nil, fmt.Errorf("decoding key: %w", err)
	}

	value, err := d.decode(kafkaMsg.Value)
	if err != nil {
		return nil, fmt.Errorf("decoding value: %w", err)
	}

	msg := NewMessage(kafkaMsg)
	msg.Key = key
	msg.Value = value

	return msg, nil
}

func (d *AvroDecoder) decode(value []byte) (json.RawMessage, error) {
	if len(value) < 5 {
		return nil, fmt.Errorf("need at least 5 bytes to decode Avro messages (got %d)", len(value))
	}

	const magicByte byte = 0x0
	if value[0] != magicByte {
		return nil, fmt.Errorf("unknown magic byte")
	}

	schemaID := int(binary.BigEndian.Uint32(value[1:5]))
	data := value[5:]

	schema, err := d.registry.Schema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("registry: %w", err)
	}

	codec, err := goavro.NewCodecForStandardJSONFull(schema)
	if err != nil {
		return nil, fmt.Errorf("codec: %w", err)
	}

	decoded, _, err := codec.NativeFromBinary(data)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	avroJSON, err := codec.TextualFromNative(nil, decoded)
	if err != nil {
		return nil, fmt.Errorf("failed to convert value to avro JSON: %w", err)
	}

	return avroJSON, nil
}
