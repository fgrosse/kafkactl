package get

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/kafkactl/pkg"
	"github.com/spf13/cobra"
)

type Command struct {
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

func NewCommand(base BaseCommand, logger, debug *log.Logger) *cobra.Command {
	cmd := &Command{
		BaseCommand: base,
		logger:      logger,
		debug:       debug,
		Command: &cobra.Command{
			Use:   "get",
			Short: "Display one or many resources in the Kafka cluster",
			Example: `
# List all topics in a Kafka cluster
kafkactl get topics

# Get all brokers as JSON
kafkactl get brokers --output=json`,
		},
	}

	cmd.PersistentFlags().StringP("output", "o", "table", "Output format. One of json|yaml|table")

	cmd.AddCommand(cmd.GetBrokersCmd())
	cmd.AddCommand(cmd.GetTopicsCmd())
	cmd.AddCommand(cmd.GetMessageCmd())
	cmd.AddCommand(cmd.GetConsumerGroupsCmd())
	cmd.AddCommand(cmd.GetConfigCmd())

	return cmd.Command
}
