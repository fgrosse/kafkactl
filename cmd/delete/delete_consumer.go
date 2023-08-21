package delete

import (
	"errors"

	"github.com/spf13/cobra"
)

func (cmd *command) DeleteConsumerGroupCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "consumer-group <name>",
		Aliases: []string{"consumer"},
		Args:    cobra.ArbitraryArgs,
		Short:   "Delete one or many consumer groups",
		Example: `
  # Delete a single consumer group called "foo"
  kafkactl delete consumer-group foo
  
  # Delete three consumer groups "foo", "bar" and "baz"
  kafkactl delete consumer-group foo bar baz`,
		RunE: func(_ *cobra.Command, args []string) error {
			topics := args
			return cmd.deleteConsumerGroup(topics)
		},
	}
}

func (cmd *command) deleteConsumerGroup(names []string) error {
	if len(names) == 0 {
		return errors.New("you need to pass the consumer group name as argument")
	}

	admin, err := cmd.ConnectAdmin()
	if err != nil {
		return err
	}

	defer admin.Close()

	for _, name := range names {
		err := admin.DeleteConsumerGroup(name)
		if err != nil {
			cmd.logger.Printf("Error deleting consumer group %q: %v", name, err)
		} else {
			cmd.logger.Printf("Consumer group %q deleted successfully", name)
		}
	}

	return nil
}
