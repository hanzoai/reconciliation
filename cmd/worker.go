package cmd

import (
	"github.com/spf13/cobra"
)

func newWorkerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Run reconciliation workers",
	}

	cmd.AddCommand(newWorkerIngestionCommand())
	cmd.AddCommand(newWorkerMatchingCommand())

	return cmd
}
