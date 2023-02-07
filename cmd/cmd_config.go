package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cmd *Kafkactl) ConfigCmd() *cobra.Command {
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Manage the kafkactl configuration",
	}

	addContextCmd := &cobra.Command{
		Use:   "add-context <NAME>",
		Args:  cobra.ExactArgs(1),
		Short: "Add a new Kafka cluster to your configuration",
		RunE:  cmd.runConfigAddContextCmd,
	}

	addContextCmd.Flags().StringSliceP("broker", "b", nil, "Kafka Broker address")
	addContextCmd.MarkFlagRequired("broker")

	configCmd.AddCommand(addContextCmd)

	return configCmd
}

func (cmd *Kafkactl) runConfigAddContextCmd(_ *cobra.Command, args []string) error {
	name := args[0]
	brokers := viper.GetStringSlice("broker")
	err := cmd.conf.AddContext(name, brokers...)
	if err != nil {
		return err
	}

	return cmd.saveConfiguration()
}
