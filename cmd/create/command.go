package create

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

type command struct {
	BaseCommand
	*cobra.Command
	logger *log.Logger
	debug  *log.Logger
}

type BaseCommand interface {
	SaramaConfig() (*sarama.Config, error)
	ConnectClient(*sarama.Config) (sarama.Client, error)
}

func Command(base BaseCommand, logger, debug *log.Logger) *cobra.Command {
	cmd := &command{
		BaseCommand: base,
		logger:      logger,
		debug:       debug,
		Command: &cobra.Command{
			Use:     "create",
			GroupID: "resources",
			Short:   "Create resources in the Kafka cluster",
		},
	}

	cmd.AddCommand(cmd.CreateTopicCmd())

	return cmd.Command
}
