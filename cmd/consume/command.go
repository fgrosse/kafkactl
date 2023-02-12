package consume

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/kafkactl/pkg"
	"github.com/spf13/cobra"
)

type command struct {
	BaseCommand
	*cobra.Command
	logger *log.Logger
	debug  *log.Logger
}

type BaseCommand interface {
	Configuration() *pkg.Configuration
	SaramaConfig() *sarama.Config
	ConnectClient(*sarama.Config) (sarama.Client, error)
}

func Command(base BaseCommand, logger, debug *log.Logger) *cobra.Command {
	cmd := &command{
		BaseCommand: base,
		logger:      logger,
		debug:       debug,
	}

	cmd.Command = cmd.ConsumeCmd()
	return cmd.Command
}
