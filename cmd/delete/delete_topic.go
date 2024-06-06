package delete

import (
	"errors"
	"fmt"
	"regexp"
	"sort"

	"github.com/IBM/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cmd *command) DeleteTopicCmd() *cobra.Command {
	deleteTopicCmd := &cobra.Command{
		Use:     "topic <name>",
		Aliases: []string{"topics"},
		Args:    cobra.ArbitraryArgs,
		Short:   "Delete one or many topics",
		Example: `
  # Delete a single topic called "foo"
  kafkactl delete topic foo
  
  # Delete three topics "foo", "bar" and "baz"
  kafkactl delete topics foo bar baz
  
  # Delete all topics that contain the term "foo"
  kafkactl delete topics --regex=foo
  
  # Delete all topics that match a regular expression
  kafkactl delete topics --regex '^fo+bar$'`,
		RunE: func(_ *cobra.Command, args []string) error {
			topics := args
			regex := viper.GetString("regex")
			return cmd.deleteTopic(topics, regex)
		},
	}

	flags := deleteTopicCmd.Flags()
	flags.String("regex", "", "delete all topics that match this regular expression")

	return deleteTopicCmd
}

func (cmd *command) deleteTopic(topics []string, regex string) error {
	if len(topics) == 0 && regex == "" {
		return errors.New("you need to pass the topic names as arguments or use the --regex flag")
	}

	if len(topics) > 0 && regex != "" {
		return errors.New("you cannot pass the topic names as arguments and also use the --regex flag at the same time")
	}

	admin, err := cmd.ConnectAdmin()
	if err != nil {
		return err
	}

	defer admin.Close()

	if regex != "" {
		var err error
		topics, err = cmd.findTopics(admin, regex)
		if err != nil {
			return err
		}
	}

	for _, topic := range topics {
		err := admin.DeleteTopic(topic)
		if err != nil {
			cmd.logger.Printf("Error deleting topic %q: %v", topic, err)
		} else {
			cmd.logger.Printf("Topic %q deleted successfully", topic)
		}
	}

	return nil
}

func (cmd *command) findTopics(admin sarama.ClusterAdmin, regex string) ([]string, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return nil, fmt.Errorf("failed to compile regular expression: %w", err)
	}

	topics, err := admin.ListTopics()
	if err != nil {
		return nil, err
	}

	var matches []string
	for name := range topics {
		if re.MatchString(name) {
			matches = append(matches, name)
		}
	}

	sort.Strings(matches)
	return matches, nil
}
