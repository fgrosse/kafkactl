package internal

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

type SchemaRegistry interface {
	Schema(id int) (string, error)
}

type ConfluentSchemaRegistry struct {
	client schemaregistry.Client
}

func NewSchemaRegistry(conf Configuration) (SchemaRegistry, error) {
	contextConfig, err := conf.Context(conf.CurrentContext)
	if err != nil {
		return nil, fmt.Errorf("get context: %w", err)
	}

	if contextConfig.SchemaRegistry.URL == "" {
		return nil, fmt.Errorf("missing schema registry base URL")
	}

	return NewConfluentSchemaRegistry(contextConfig.SchemaRegistry)
}

func NewConfluentSchemaRegistry(config SchemaRegistryConfiguration) (*ConfluentSchemaRegistry, error) {
	var conf *schemaregistry.Config
	if config.Username != "" {
		conf = schemaregistry.NewConfigWithAuthentication(config.URL, config.Username, config.Password)
	} else {
		conf = schemaregistry.NewConfig(config.URL)
	}

	client, err := schemaregistry.NewClient(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema registry client: %w", err)
	}

	return &ConfluentSchemaRegistry{client: client}, nil
}

func (r *ConfluentSchemaRegistry) Schema(id int) (string, error) {
	schema, err := r.client.GetBySubjectAndID("", id)
	if err != nil {
		return "", err
	}

	return schema.Schema, nil
}
