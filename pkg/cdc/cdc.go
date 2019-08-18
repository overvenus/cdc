package cdc

import (
	"context"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	dialTimeout = 5 * time.Second
)

// CDClient the client of a TiDB/TiKV CDC service.
type CDClient struct {
	ctx    context.Context
	client cdcpb.ChangeDataClient
}

// NewCDClient creates a new CDClient.
func NewCDClient(ctx context.Context, tikvAddr string) (*CDClient, error) {
	log.Info("connect tikv", zap.String("tikv", tikvAddr))
	opt := grpc.WithInsecure()
	dailCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	keepAlive := 10
	keepAliveTimeout := 3
	conn, err := grpc.DialContext(
		dailCtx,
		tikvAddr,
		opt,
		grpc.WithBackoffMaxDelay(time.Second*3),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Duration(keepAlive) * time.Second,
			Timeout:             time.Duration(keepAliveTimeout) * time.Second,
			PermitWithoutStream: true,
		}),
	)
	cancel()
	log.Info("conn state", zap.Reflect("state", conn.GetState()))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	client := cdcpb.NewChangeDataClient(conn)

	return &CDClient{
		ctx:    ctx,
		client: client,
	}, nil
}

// WatchRegion watchs a region and prints events
func (cli *CDClient) WatchRegion(regionID uint64) error {
	req := &cdcpb.ChangeDataRequest{
		RegionId: regionID,
	}
	ctx, cancel := context.WithCancel(cli.ctx)
	defer cancel()
	client, err := cli.client.EventFeed(ctx, req)
	if err != nil {
		return errors.Trace(err)
	}
	for {
		events, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				log.Info("watch region finished", zap.Uint64("RegionID", regionID))
				break
			}
		}
		log.Info("region events", zap.Reflect("events", events))
	}
	return nil
}
