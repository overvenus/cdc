package cmd

import (
	"github.com/spf13/cobra"
)

// NewCDCommand return a full backup subcommand.
func NewCDCommand() *cobra.Command {
	bp := &cobra.Command{
		Use:   "cdc",
		Short: "capture change data of a TiKV cluster",
	}
	bp.AddCommand(
		newWatchRegionCommand(),
		// newRegionCommand(),
	)
	return bp
}

// newWatchRegionCommand return a full backup subcommand.
func newWatchRegionCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "region",
		Short: "capture the changed data of the given region",
		RunE: func(command *cobra.Command, _ []string) error {
			regionID, err := command.Flags().GetUint64("region")
			if err != nil {
				return err
			}
			err = defaultCDClient.WatchRegion(regionID)
			if err != nil {
				return err
			}
			return nil
		},
	}
	command.Flags().Uint64P("region", "r", 0,
		"watch region and prints its events")
	command.MarkFlagRequired("region")
	return command
}
