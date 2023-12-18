package internal

import (
	"fmt"
	"os"

	"github.com/Shopify/sarama"
)

type Encoder interface {
	Encode(msg string) (sarama.Encoder, error)
}

func NewTopicEncoder(topic string, conf Configuration) (Encoder, error) {
	topicConf, err := conf.TopicConfig(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to load topic configuration: %w", err)
	}

	switch {
	case topicConf == nil:
		return new(StringEncoder), nil
	case topicConf.Schema.Proto.Type != "":
		for i, s := range conf.Proto.Includes {
			conf.Proto.Includes[i] = os.ExpandEnv(s)
		}

		return NewProtoEncoder(ProtoConfig{
			Includes: conf.Proto.Includes,
			Key:      topicConf.Schema.Key,
			Value: SchemaConfig{
				File: topicConf.Schema.Proto.File,
				Type: topicConf.Schema.Proto.Type,
			},
		})
	default:
		return new(StringEncoder), nil
	}
}

type StringEncoder struct{}

func (e *StringEncoder) Encode(msg string) (sarama.Encoder, error) {
	return sarama.StringEncoder(msg), nil
}
