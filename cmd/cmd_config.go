package cmd

import (
	"fmt"

	"github.com/fgrosse/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cmd *Kafkactl) ConfigCmd() *cobra.Command {
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Manage the kafkactl configuration",
	}

	printCmd := &cobra.Command{
		Use:   "print",
		Args:  cobra.NoArgs,
		Short: "Print the Kafkactl config file to stdout",
		RunE: func(_ *cobra.Command, args []string) error {
			encoding := viper.GetString("output")
			switch encoding {
			case "json", "yaml", "yml":
				return cli.Print(encoding, cmd.conf)
			default:
				return fmt.Errorf("invalid output encoding %q", encoding)
			}
		},
	}

	printCmd.Flags().StringP("output", "o", "yaml", `Output format. Either "json" or "yaml"`)

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

	deleteContextCmd := &cobra.Command{
		Use:   "delete <CONTEXT_NAME>",
		Args:  cobra.ExactArgs(1),
		Short: "Delete a configuration context from your kafkactl config file",
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			return cmd.deleteContext(name)
		},
	}

	renameContextCmd := &cobra.Command{
		Use:   "rename <OLD_CONTEXT_NAME> <NEW_CONTEXT_NAME>",
		Args:  cobra.ExactArgs(2),
		Short: "Rename a configuration context in your kafkactl config file",
		RunE: func(_ *cobra.Command, args []string) error {
			oldName := args[0]
			newName := args[1]
			return cmd.renameContext(oldName, newName)
		},
	}

	configCmd.AddCommand(printCmd)
	configCmd.AddCommand(addContextCmd)
	configCmd.AddCommand(deleteContextCmd)
	configCmd.AddCommand(renameContextCmd)

	return configCmd
}

func (cmd *Kafkactl) addContext(name string, brokers []string) error {
	err := cmd.conf.AddContext(name, brokers...)
	if err != nil {
		return err
	}

	err = cmd.saveConfiguration()
	if err != nil {
		return err
	}

	cmd.logger.Printf("Successfully added new configuration context %q", name)

	return nil
}

func (cmd *Kafkactl) deleteContext(name string) error {
	err := cmd.conf.DeleteContext(name)
	if err != nil {
		return err
	}

	err = cmd.saveConfiguration()
	if err != nil {
		return err
	}

	cmd.logger.Printf("Successfully deleted configuration context %q", name)
	return nil
}

func (cmd *Kafkactl) renameContext(oldName, newName string) error {
	err := cmd.conf.RenameContext(oldName, newName)
	if err != nil {
		return err
	}

	err = cmd.saveConfiguration()
	if err != nil {
		return err
	}

	cmd.logger.Printf("Successfully renamed configuration context %q to %q", oldName, newName)
	return nil
}
