package context

import (
	"fmt"
	"sort"
	"strings"

	"github.com/fatih/color"
	"github.com/fgrosse/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Context struct {
	Name          string   `table:"NAME"`
	BrokersString string   `table:"BROKERS" json:"-" yaml:"-"`
	Brokers       []string `table:"-"`
}

func (cmd *command) ContextCmd() *cobra.Command {
	contextCmd := &cobra.Command{
		Use:   "context [CONTEXT_NAME]",
		Args:  cobra.MaximumNArgs(1),
		Short: "Switch between different configuration contexts",
		Long: `Switch between different configurations contexts (e.g. prod, staging, local).

The kafkactl context feature is conceptionally similar to how kubectl manages cluster configuration.
You can setup one or more Kafka clusters configurations so you can conveniently switch between those
contexts without having to remember or lookup the broker addresses all the time.

The 'kafkactl context' command is what the kubectx script is for kubectl. You can 
add or remove new configuration contexts using the 'kafkactl config' sub commands.
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
		RunE: func(_ *cobra.Command, args []string) error {
			var contextName string
			if len(args) > 0 {
				contextName = args[0]
			}
			encoding := viper.GetString("output")
			return cmd.runContextCmd(contextName, encoding)
		},
	}

	contextCmd.Flags().StringP("output", "o", "short", "Output format. One of json|yaml|table|raw|short")

	return contextCmd
}

func (cmd *command) runContextCmd(contextName, encoding string) error {
	conf := cmd.Configuration()
	if len(conf.Contexts) == 0 {
		return fmt.Errorf("this command requires at least one configured context. Please use kafkactl config --help to get started")
	}

	var contexts []Context
	for _, c := range conf.Contexts {
		contexts = append(contexts, Context{
			Name:          c.Name,
			Brokers:       c.Brokers,
			BrokersString: strings.Join(c.Brokers, ", "),
		})
	}

	if contextName != "" {
		err := conf.SetContext(contextName)
		if err != nil {
			return err
		}

		err = cmd.SaveConfiguration()
		if err != nil {
			return err
		}
	}

	sort.Slice(contexts, func(i, j int) bool {
		return contexts[i].Name < contexts[j].Name
	})

	if encoding == "short" {
		colored := color.New(color.FgYellow, color.Bold)
		// emulate a format similar to kubectx
		for _, c := range contexts {
			if c.Name == conf.CurrentContext {
				_, _ = colored.Println("* " + c.Name)
			} else {
				_, _ = fmt.Println("  " + c.Name)
			}
		}
		return nil
	}

	return cli.Print(encoding, contexts)
}
