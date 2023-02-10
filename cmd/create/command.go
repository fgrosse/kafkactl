package create

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
	ConnectAdmin() (sarama.ClusterAdmin, error)
}

func Command(base BaseCommand, logger, debug *log.Logger) *cobra.Command {
	cmd := &command{
		BaseCommand: base,
		logger:      logger,
		debug:       debug,
		Command: &cobra.Command{
			Use:   "create",
			Short: "Create resources in the Kafka cluster",
			Example: `
# Create topics 
kafkactl create topic --partitions=1 --replicas=1 --retention=7d [TOPIC_NAME]`,
		},
	}

	cmd.AddCommand(cmd.CreateTopicCmd())

	return cmd.Command
}
