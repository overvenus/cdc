package cmd

import (
	"context"
	"sync"

	"github.com/overvenus/cdc/pkg/cdc"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

var (
	initOnce       = sync.Once{}
	defaultContext context.Context

	defaultCDClient *cdc.CDClient
)

const (
	// FlagTiKV is the name of url flag.
	FlagTiKV = "tikv"
)

// AddFlags adds flags to the given cmd.
func AddFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringP(FlagTiKV, "u", "127.0.0.1:2379", "PD address")
	cmd.MarkFlagRequired(FlagTiKV)
}

// Init ...
func Init(ctx context.Context, cmd *cobra.Command) (err error) {
	initOnce.Do(func() {
		defaultContext = ctx
		var addr string
		addr, err = cmd.Flags().GetString(FlagTiKV)
		if err != nil {
			return
		}
		if addr == "" {
			err = errors.Errorf("pd address can not be empty")
			return
		}
		defaultCDClient, err = cdc.NewCDClient(defaultContext, addr)
		if err != nil {
			return
		}
	})
	return
}
