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
	Key       any
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

	registry, _ := NewSchemaRegistry(conf)

	switch {
	case topicConf == nil:
		return new(StringDecoder), nil

	case topicConf.Schema.Type == "avro":
		if registry == nil {
			return nil, fmt.Errorf(`topic schema type is "avro"" but schema registry config is missing in context configuration`)
		}

		dec := NewAvroDecoder(registry)

		return dec, nil

	case topicConf.Schema.Type == "proto":
		return nil, fmt.Errorf("reading proto schema from the Schema Registy is not yet supported. Please configure the proto type directly in the topic schema configuration")

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

func (d *StringDecoder) Decode(kafkaMsg *sarama.ConsumerMessage) (*Message, error) {
	msg := NewMessage(kafkaMsg)
	msg.Key = string(kafkaMsg.Key)
	msg.Value = string(kafkaMsg.Value)
	return msg, nil
}

// NewMessage creates a new Message from a given Kafka message.
// The Key and Value are copied into the Message as is (i.e. without decoding).
func NewMessage(m *sarama.ConsumerMessage) *Message {
	msg := &Message{
		Key:       m.Key,
		Value:     m.Value,
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

	return msg
}
