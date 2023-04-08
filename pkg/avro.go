package pkg

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
)

// AvroDecoder is a Decoder implementation that supports the Apache Avro format.
type AvroDecoder struct {
	registry        SchemaRegistry
	printAvroSchema bool
}

// NewAvroDecoder creates a new AvroDecoder instance.
func NewAvroDecoder(r SchemaRegistry) *AvroDecoder {
	return &AvroDecoder{
		registry:        r,
		printAvroSchema: false,
	}
}

// UseAvroJSON configures the AvroDecoder to serialize the decoded data using
// avro-json which is still valid JSON, but encodes fields in a more verbose way
// and has special handling for union types.
func (d *AvroDecoder) UseAvroJSON() {
	d.printAvroSchema = true
}

// Decode a message from Kafka into our own Message type.
func (d *AvroDecoder) Decode(msg *sarama.ConsumerMessage) (*Message, error) {
	return newMessage(msg, func(value []byte) (any, error) {
		if len(value) < 5 {
			return nil, fmt.Errorf("need at least 5 bytes to decode Avro messages (got %d)", len(value))
		}

		schemaID := int(binary.BigEndian.Uint32(value[1:5]))
		data := value[5:]

		schema, err := d.registry.Schema(schemaID)
		if err != nil {
			return nil, fmt.Errorf("registry: %w", err)
		}

		var codec *goavro.Codec
		if d.printAvroSchema {
			codec, err = goavro.NewCodec(schema)
		} else {
			codec, err = goavro.NewCodecForStandardJSONFull(schema)
		}

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

		return json.RawMessage(avroJSON), nil
	})
}
