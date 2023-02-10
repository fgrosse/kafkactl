package config

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cmd *Command) ConfigAddContextCmd() *cobra.Command {
	addContextCmd := &cobra.Command{
		Use:   "add <CONTEXT_NAME>",
		Args:  cobra.ExactArgs(1),
		Short: "Add a new Kafka cluster configuration context to your kafkactl config file",
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			brokers := viper.GetStringSlice("broker")
			return cmd.addContext(name, brokers)
		},
	}

	addContextCmd.Flags().StringSliceP("broker", "b", nil, "Kafka Broker address")
	addContextCmd.MarkFlagRequired("broker")

	return addContextCmd
}

func (cmd *Command) addContext(name string, brokers []string) error {
	conf := cmd.Configuration()
	err := conf.AddContext(name, brokers...)
	if err != nil {
		return err
	}

	err = cmd.SaveConfiguration()
	if err != nil {
		return err
	}

	cmd.logger.Printf("Successfully added new configuration context %q", name)

	return nil
}
