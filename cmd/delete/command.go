package delete

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
	ConnectAdmin() (sarama.ClusterAdmin, error)
}

func Command(base BaseCommand, logger, debug *log.Logger) *cobra.Command {
	cmd := &command{
		BaseCommand: base,
		logger:      logger,
		debug:       debug,
		Command: &cobra.Command{
			Use:     "delete",
			GroupID: "resources",
			Short:   "Delete resources in the Kafka cluster",
			Example: `
  # Delete topic
  kafkactl delete topic [TOPIC_NAME]`,
		},
	}

	cmd.AddCommand(cmd.DeleteTopicCmd())

	return cmd.Command
}
