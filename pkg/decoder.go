package pkg

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/Shopify/sarama"
)

type Message struct {
	Topic     string          `json:"topic"`
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
	Value     json.RawMessage `json:"value"`
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
		return new(RawDecoder), nil
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
		return new(RawDecoder), nil
	}
}

type RawDecoder struct{}

func (d *RawDecoder) Decode(msg *sarama.ConsumerMessage) (*Message, error) {
	return &Message{
		Value:     msg.Value,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}, nil
}
