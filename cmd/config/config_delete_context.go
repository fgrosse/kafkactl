package config

import (
	"github.com/spf13/cobra"
)

func (cmd *Command) ConfigDeleteContextCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <CONTEXT_NAME>",
		Args:  cobra.ExactArgs(1),
		Short: "Delete a configuration context from your kafkactl config file",
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			return cmd.deleteContext(name)
		},
	}
}

func (cmd *Command) deleteContext(name string) error {
	conf := cmd.Configuration()
	err := conf.DeleteContext(name)
	if err != nil {
		return err
	}

	err = cmd.SaveConfiguration()
	if err != nil {
		return err
	}

	cmd.logger.Printf("Successfully deleted configuration context %q", name)
	return nil
}
