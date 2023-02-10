package context

import (
	"github.com/fgrosse/kafkactl/pkg"
	"github.com/spf13/cobra"
)

type command struct {
	BaseCommand
	*cobra.Command
}

type BaseCommand interface {
	Configuration() *pkg.Configuration
	SaveConfiguration() error
}

func Command(base BaseCommand) *cobra.Command {
	cmd := &command{BaseCommand: base}
	return cmd.ContextCmd()
}
