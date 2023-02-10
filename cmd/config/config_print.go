package config

import (
	"fmt"

	"github.com/fgrosse/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cmd *Command) ConfigPrintCmd() *cobra.Command {
	printCmd := &cobra.Command{
		Use:   "print",
		Args:  cobra.NoArgs,
		Short: "Print the Kafkactl config file to stdout",
		RunE: func(_ *cobra.Command, args []string) error {
			encoding := viper.GetString("output")
			switch encoding {
			case "json", "yaml", "yml":
				conf := cmd.Configuration()
				return cli.Print(encoding, conf)
			default:
				return fmt.Errorf("invalid output encoding %q", encoding)
			}
		},
	}

	printCmd.Flags().StringP("output", "o", "yaml", `Output format. Either "json" or "yaml"`)

	return printCmd
}
