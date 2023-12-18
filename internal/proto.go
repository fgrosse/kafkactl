package internal

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/jsonpb"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
)

type ProtoDecoder struct {
	key   *desc.MessageDescriptor
	value *desc.MessageDescriptor
}

type ProtoEncoder struct {
	typ *desc.MessageDescriptor
}

type ProtoConfig struct {
	Includes []string
	Key      SchemaConfig
	Value    SchemaConfig
}

func NewProtoDecoder(conf ProtoConfig) (*ProtoDecoder, error) {
	p := protoparse.Parser{
		ImportPaths: conf.Includes,
	}

	files := []string{conf.Value.File}
	if conf.Key.File != "" {
		files = append(files, conf.Key.File)
	}

	dd, err := p.ParseFiles(files...)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto: %w", err)
	}

	dec := &ProtoDecoder{}
	for _, descr := range dd {
		if dec.value == nil {
			dec.value = descr.FindMessage(conf.Value.Type)
		}
		if dec.key == nil {
			dec.key = descr.FindMessage(conf.Key.Type)
		}
	}

	if dec.value == nil {
		return nil, fmt.Errorf("could not find message %q", conf.Value.Type)
	}

	return dec, nil
}

func (d *ProtoDecoder) Decode(kafkaMsg *sarama.ConsumerMessage) (*Message, error) {
	msg := NewMessage(kafkaMsg)
	val, err := d.decode(d.value, kafkaMsg.Value)
	if err != nil {
		return nil, fmt.Errorf("value: %w", err)
	}

	msg.Value = val

	if d.key == nil {
		return msg, nil
	}

	key, err := d.decode(d.key, kafkaMsg.Key)
	if err != nil {
		return nil, fmt.Errorf("key: %w", err)
	}

	msg.Key = key

	return msg, nil
}

func (*ProtoDecoder) decode(descriptor *desc.MessageDescriptor, value []byte) (json.RawMessage, error) {
	protoMsg := dynamic.NewMessage(descriptor)
	err := protoMsg.Unmarshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode message as proto %s: %w", descriptor.GetFullyQualifiedName(), err)
	}

	marshaler := &jsonpb.Marshaler{
		OrigName:     true,
		EnumsAsInts:  false,
		EmitDefaults: true,
		Indent:       "  ",
	}

	val, err := protoMsg.MarshalJSONPB(marshaler)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode message as JSON: %w", err)
	}

	return val, nil
}

func NewProtoEncoder(conf ProtoConfig) (*ProtoEncoder, error) {
	p := protoparse.Parser{
		ImportPaths: conf.Includes,
	}

	dd, err := p.ParseFiles(conf.Value.File)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse proto")
	}

	enc := &ProtoEncoder{}
	for _, descr := range dd {
		enc.typ = descr.FindMessage(conf.Value.Type)
		if enc.typ != nil {
			break
		}
	}

	if enc.typ == nil {
		return nil, errors.Errorf("could not find message %q", conf.Value.Type)
	}

	return enc, nil
}

func (d *ProtoEncoder) Encode(msg string) (sarama.Encoder, error) {
	m := dynamic.NewMessage(d.typ)
	err := m.UnmarshalJSON([]byte(msg))
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal proto message from JSON")
	}

	data, err := m.Marshal()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal message as proto")
	}

	return sarama.ByteEncoder(data), nil
}
