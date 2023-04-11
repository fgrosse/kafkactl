package replay

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/kafkactl/internal"
	"github.com/spf13/cobra"
)

type command struct {
	BaseCommand
	*cobra.Command
	logger *log.Logger
	debug  *log.Logger
}

type BaseCommand interface {
	Configuration() *internal.Configuration
	SaramaConfig() (*sarama.Config, error)
	ConnectClient(*sarama.Config) (sarama.Client, error)
}

func Command(base BaseCommand, logger, debug *log.Logger) *cobra.Command {
	cmd := &command{
		BaseCommand: base,
		logger:      logger,
		debug:       debug,
	}

	cmd.Command = cmd.ReplayCmd()
	return cmd.Command
}
