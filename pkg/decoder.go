package pkg

import (
	"encoding/json"
	"os"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
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

type RawDecoder struct{}

func NewTopicDecoder(topic string, conf Configuration) (Decoder, error) {
	topicConf, err := conf.TopicConfig(topic)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load topic configuration")
	}

	switch {
	case topicConf == nil:
		return new(RawDecoder), nil
	case topicConf.Decode.Proto.Type != "":
		for i, s := range conf.Proto.Includes {
			conf.Proto.Includes[i] = os.ExpandEnv(s)
		}

		return NewProtoDecoder(ProtoDecoderConfig{
			Includes: conf.Proto.Includes,
			File:     topicConf.Decode.Proto.File,
			Type:     topicConf.Decode.Proto.Type,
		})
	default:
		return new(RawDecoder), nil
	}
}

func (d *RawDecoder) Decode(msg *sarama.ConsumerMessage) (*Message, error) {
	return &Message{
		Value:     msg.Value,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}, nil
}
