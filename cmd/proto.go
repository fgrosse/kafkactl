package cmd

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/jsonpb"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
)

type ProtoDecoder struct {
	typ *desc.MessageDescriptor
}

type ProtoDecoderConfig struct {
	Includes []string
	File     string
	Type     string
}

func NewProtoDecoder(conf ProtoDecoderConfig) (*ProtoDecoder, error) {
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
