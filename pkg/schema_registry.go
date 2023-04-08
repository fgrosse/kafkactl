package pkg

import (
	"fmt"

	"github.com/landoop/schema-registry"
)

type SchemaRegistry interface {
	Schema(id int) (string, error)
}

type KafkaSchemaRegistry struct {
	client  *schemaregistry.Client
	schemas map[int]string
}

func NewKafkaSchemaRegistry(baseURL string) (*KafkaSchemaRegistry, error) {
	client, err := schemaregistry.NewClient(baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema registry client: %w", err)
	}

	return &KafkaSchemaRegistry{
		client:  client,
		schemas: map[int]string{},
	}, nil
}

func (r *KafkaSchemaRegistry) Schema(id int) (string, error) {
	schema, ok := r.schemas[id]
	if ok {
		return schema, nil
	}

	schema, err := r.client.GetSchemaByID(id)
	if err != nil {
		return "", err
	}

	r.schemas[id] = schema
	return schema, nil
}
