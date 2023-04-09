package context

import (
	"github.com/fgrosse/kafkactl/internal"
	"github.com/spf13/cobra"
)

type command struct {
	BaseCommand
	*cobra.Command
}

type BaseCommand interface {
	Configuration() *internal.Configuration
	SaveConfiguration() error
}

func Command(base BaseCommand) *cobra.Command {
	cmd := &command{BaseCommand: base}
	return cmd.ContextCmd()
}
