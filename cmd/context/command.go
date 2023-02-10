package context

import (
	"github.com/fgrosse/kafkactl/pkg"
	"github.com/spf13/cobra"
)

type Command struct {
	BaseCommand
	*cobra.Command
}

type BaseCommand interface {
	Configuration() *pkg.Configuration
	SaveConfiguration() error
}

func NewCommand(base BaseCommand) *cobra.Command {
	cmd := &Command{BaseCommand: base}
	return cmd.ContextCmd()
}
