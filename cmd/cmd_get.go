package cmd

import (
	"github.com/spf13/cobra"
)

func (cmd *Kafkactl) GetCmd() *cobra.Command {
	getCmd := &cobra.Command{
		Use:   "get",
		Short: "Display one or many resources in the Kafka cluster",
		Example: `
# List all topics in a Kafka cluster
kafkactl get topics

# Get all brokers as JSON
kafkactl get brokers --output=json`,
	}

	getCmd.PersistentFlags().StringP("output", "o", "table", "Output format. One of json|yaml|table")

	getCmd.AddCommand(cmd.GetBrokersCmd())
	getCmd.AddCommand(cmd.GetTopicsCmd())
	getCmd.AddCommand(cmd.GetConsumerGroupsCmd())
	getCmd.AddCommand(cmd.GetConfigCmd())

	return getCmd
}
