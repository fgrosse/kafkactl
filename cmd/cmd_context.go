package cmd

import (
	"fmt"
	"sort"
	"strings"

	"github.com/fatih/color"
	"github.com/fgrosse/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cmd *Kafkactl) ContextCmd() *cobra.Command {
	contextCmd := &cobra.Command{
		Use:   "context",
		Args:  cobra.MaximumNArgs(1),
		Short: "Switch between different configuration contexts (e.g. prod, staging, local)",
		Long: `Switch between different configurations contexts (e.g. prod, staging, local).

The kafkactl context feature is conceptionally similar to how kubectl is typically
configured. You can setup one or more Kafka clusters configurations so you can
conveniently switch between those contexts without having to remember or lookup
the broker addresses all the time.

The 'kafkactl context' command is what the kubectx script is for kubectl. You can 
add or remove new configuration contexts using the 'kafkactl config add-context' command.
`,
		Example: `
  # Print the all available configs and highlight the one that is currently used
  kafkactl context
  
  # Print all configuration names and the corresponding brokers as JSON
  kafkactl context --output=json
  
  # Switch to another configuration
  kafkactl context some-cluster
  
  # Toggle between your current config and your previous config
  kafkactl context -
`,
		RunE: cmd.runContextCommand,
	}

	contextCmd.Flags().StringP("output", "o", "short", "Output format. One of json|yaml|table|raw|short")

	contextCmd.PreRunE = cmd.requireConfiguredContext

	return contextCmd
}

func (cmd *Kafkactl) runContextCommand(cc *cobra.Command, args []string) error {
	output := viper.GetString("output")
	setContext := len(args) == 1

	type Context struct {
		Name          string   `table:"NAME"`
		BrokersString string   `table:"BROKERS" json:"-" yaml:"-"`
		Brokers       []string `table:"-"`
	}

	var contexts []Context
	for _, c := range cmd.conf.Contexts {
		contexts = append(contexts, Context{
			Name:          c.Name,
			Brokers:       c.Brokers,
			BrokersString: strings.Join(c.Brokers, ", "),
		})
	}

	if setContext {
		err := cmd.conf.SetContext(args[0])
		if err != nil {
			return err
		}

		err = cmd.saveConfiguration()
		if err != nil {
			return err
		}
	}

	sort.Slice(contexts, func(i, j int) bool {
		return contexts[i].Name < contexts[j].Name
	})

	if output == "short" {
		colored := color.New(color.FgYellow, color.Bold)
		// emulate a format similar to kubectx
		for _, c := range contexts {
			if c.Name == cmd.conf.CurrentContext {
				_, _ = colored.Println("* " + c.Name)
			} else {
				_, _ = fmt.Println("  " + c.Name)
			}
		}
		return nil
	}

	return cli.Print(output, contexts)
}
