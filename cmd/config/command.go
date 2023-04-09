package config

import (
	"log"

	"github.com/fgrosse/kafkactl/internal"
	"github.com/spf13/cobra"
)

type command struct {
	BaseCommand
	*cobra.Command
	logger *log.Logger
	debug  *log.Logger
}

type BaseCommand interface {
	Configuration() *internal.Configuration
	SaveConfiguration() error
}

func Command(base BaseCommand, logger *log.Logger, debug *log.Logger) *cobra.Command {
	cmd := &command{
		BaseCommand: base,
		logger:      logger,
		debug:       debug,
		Command: &cobra.Command{
			Use:     "config",
			GroupID: "config",
			Short:   "Manage the kafkactl configuration",
			Long:    `Manage the kafkactl configuration and cluster configuration contexts.`,
		},
	}

	cmd.AddCommand(cmd.ConfigPrintCmd())
	cmd.AddCommand(cmd.ConfigAddContextCmd())
	cmd.AddCommand(cmd.ConfigDeleteContextCmd())
	cmd.AddCommand(cmd.ConfigRenameContextCmd())

	return cmd.Command
}
