package search

import (
	"log"

	"github.com/IBM/sarama"
	"github.com/spf13/cobra"

	"github.com/fgrosse/kafkactl/internal"
)

type command struct {
	BaseCommand
	*cobra.Command
	logger *log.Logger
}

type BaseCommand interface {
	Configuration() *internal.Configuration
	SaramaConfig() (*sarama.Config, error)
	ConnectClient(*sarama.Config) (sarama.Client, error)
}

func Command(base BaseCommand, logger *log.Logger) *cobra.Command {
	cmd := &command{
		BaseCommand: base,
		logger:      logger,
	}

	cmd.Command = cmd.SearchCmd()
	return cmd.Command
}
