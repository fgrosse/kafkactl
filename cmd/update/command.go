package update

import (
	"log"

	"github.com/IBM/sarama"
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
	ConnectAdmin() (sarama.ClusterAdmin, error)
}

func Command(base BaseCommand, logger, debug *log.Logger) *cobra.Command {
	cmd := &command{
		BaseCommand: base,
		logger:      logger,
		debug:       debug,
		Command: &cobra.Command{
			Use:     "update",
			GroupID: "resources",
			Short:   "Update resources in the Kafka cluster",
		},
	}

	cmd.AddCommand(cmd.UpdateTopicCmd())

	return cmd.Command
}
