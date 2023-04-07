package pkg

import (
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Headers   map[string][]string
	Timestamp time.Time
	Key       string
	Value     any
}

type Decoder interface {
	Decode(*sarama.ConsumerMessage) (*Message, error)
}

func NewTopicDecoder(topic string, conf Configuration) (Decoder, error) {
	topicConf, err := conf.TopicConfig(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to load topic configuration: %w", err)
	}

	switch {
	case topicConf == nil:
		return new(StringDecoder), nil
	case topicConf.Schema.Avro.RegistryURL != "":
		r, err := NewKafkaSchemaRegistry(topicConf.Schema.Avro.RegistryURL)
		if err != nil {
			return nil, err
		}

		return NewAvroDecoder(r, topicConf.Schema.Avro.PrintAvroSchema), nil

	case topicConf.Schema.Proto.Type != "":
		for i, s := range conf.Proto.Includes {
			conf.Proto.Includes[i] = os.ExpandEnv(s)
		}

		return NewProtoDecoder(ProtoConfig{
			Includes: conf.Proto.Includes,
			File:     topicConf.Schema.Proto.File,
			Type:     topicConf.Schema.Proto.Type,
		})
	default:
		return new(StringDecoder), nil
	}
}

// The StringDecoder assumes that the values of all consumed messages are unicode strings.
type StringDecoder struct{}

func (d *StringDecoder) Decode(msg *sarama.ConsumerMessage) (*Message, error) {
	return newMessage(msg, func(value []byte) (any, error) {
		return string(value), nil
	})
}

func newMessage(m *sarama.ConsumerMessage, decodeValue func([]byte) (any, error)) (*Message, error) {
	decoded, err := decodeValue(m.Value)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Key:       string(m.Key),
		Value:     decoded,
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Timestamp: m.Timestamp,
	}

	msg.Headers = map[string][]string{}
	for _, h := range m.Headers {
		key := string(h.Key)
		msg.Headers[key] = append(msg.Headers[key], string(h.Value))
	}

	return msg, nil
}
