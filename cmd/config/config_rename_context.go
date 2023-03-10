package config

import (
	"github.com/spf13/cobra"
)

func (cmd *command) ConfigRenameContextCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rename <old-name> <new-name>",
		Args:  cobra.ExactArgs(2),
		Short: "Rename a configuration context in your kafkactl config file",
		RunE: func(_ *cobra.Command, args []string) error {
			oldName := args[0]
			newName := args[1]
			return cmd.renameContext(oldName, newName)
		},
	}
}

func (cmd *command) renameContext(oldName, newName string) error {
	conf := cmd.Configuration()
	err := conf.RenameContext(oldName, newName)
	if err != nil {
		return err
	}

	err = cmd.SaveConfiguration()
	if err != nil {
		return err
	}

	cmd.logger.Printf("Successfully renamed configuration context %q to %q", oldName, newName)
	return nil
}
