package pkg

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/jsonpb"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
)

type ProtoDecoder struct {
	typ *desc.MessageDescriptor
}

type ProtoEncoder struct {
	typ *desc.MessageDescriptor
}

type ProtoConfig struct {
	Includes []string
	File     string
	Type     string
}

func NewProtoDecoder(conf ProtoConfig) (*ProtoDecoder, error) {
	p := protoparse.Parser{
		ImportPaths: conf.Includes,
	}

	dd, err := p.ParseFiles(conf.File)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto: %w", err)
	}

	dec := &ProtoDecoder{}
	for _, descr := range dd {
		dec.typ = descr.FindMessage(conf.Type)
		if dec.typ != nil {
			break
		}
	}

	if dec.typ == nil {
		return nil, fmt.Errorf("could not find message %q", conf.Type)
	}

	return dec, nil
}

func (d *ProtoDecoder) Decode(msg *sarama.ConsumerMessage) (*Message, error) {
	protoMsg := dynamic.NewMessage(d.typ)
	err := protoMsg.Unmarshal(msg.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode message as proto %s: %w", d.typ.GetName(), err)
	}

	marshaler := &jsonpb.Marshaler{
		OrigName:     true,
		EnumsAsInts:  false,
		EmitDefaults: true,
		Indent:       "  ",
	}
	value, err := protoMsg.MarshalJSONPB(marshaler)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode message as JSON: %w", err)
	}

	return &Message{
		Value:     value,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}, nil
}

func NewProtoEncoder(conf ProtoConfig) (*ProtoEncoder, error) {
	p := protoparse.Parser{
		ImportPaths: conf.Includes,
	}

	dd, err := p.ParseFiles(conf.File)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse proto")
	}

	enc := &ProtoEncoder{}
	for _, descr := range dd {
		enc.typ = descr.FindMessage(conf.Type)
		if enc.typ != nil {
			break
		}
	}

	if enc.typ == nil {
		return nil, errors.Errorf("could not find message %q", conf.Type)
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
