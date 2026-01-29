package cmd

import (
	"github.com/formancehq/go-libs/v3/bun/bunmigrate"
	"github.com/formancehq/go-libs/v3/service"
	"github.com/spf13/cobra"
)

var (
	ServiceName = "reconciliation"
	Version     = "develop"
	BuildDate   = "-"
	Commit      = "-"
)

const (
	stackFlag             = "stack"
	stackURLFlag          = "stack-url"
	stackClientIDFlag     = "stack-client-id"
	stackClientSecretFlag = "stack-client-secret"
	kafkaTopicsFlag       = "kafka-topics"
	listenFlag            = "listen"
	workerFlag            = "worker"
	matchingWorkersFlag   = "matching-workers"
	MatchingWorkersEnvVar = "MATCHING_WORKERS"
	reportScheduleFlag    = "report-schedule"
	ReportScheduleEnvVar  = "REPORT_SCHEDULE"
)

func NewRootCommand() *cobra.Command {
	cmd := &cobra.Command{}

	cobra.EnableTraverseRunHooks = true

	serveCmd := newServeCommand(Version)
	addAutoMigrateCommand(serveCmd)
	cmd.AddCommand(serveCmd)
	workerCmd := newWorkerCommand()
	cmd.AddCommand(workerCmd)
	versionCmd := newVersionCommand()
	cmd.AddCommand(versionCmd)
	migrate := newMigrate()
	cmd.AddCommand(migrate)
	return cmd
}

func Execute() {
	service.Execute(NewRootCommand())
}

func addAutoMigrateCommand(cmd *cobra.Command) {
	cmd.Flags().Bool(autoMigrateFlag, false, "Auto migrate database")
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		autoMigrate, _ := cmd.Flags().GetBool(autoMigrateFlag)
		if autoMigrate {
			return bunmigrate.Run(cmd, args, Migrate)
		}
		return nil
	}
}
