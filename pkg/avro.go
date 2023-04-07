package pkg

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
)

type AvroDecoder struct {
	registry        SchemaRegistry
	printAvroSchema bool
}

type SchemaRegistry interface {
	Schema(id int) (string, error)
}

func NewAvroDecoder(r SchemaRegistry, printAvroSchema bool) *AvroDecoder {
	return &AvroDecoder{
		registry:        r,
		printAvroSchema: printAvroSchema,
	}
}

func (d *AvroDecoder) Decode(msg *sarama.ConsumerMessage) (*Message, error) {
	return newMessage(msg, func(value []byte) (any, error) {
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
