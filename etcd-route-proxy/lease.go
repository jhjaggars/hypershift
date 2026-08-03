package etcdrouteproxy

import (
	"context"
	"io"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// leaseProxy implements the etcd Lease gRPC service.
// All lease operations are forwarded to the default backend.
//
// KAS's leaseManager creates leases via Grant and attaches them to keys
// via the LeaseID in Put operations. Since a lease is a cluster-level
// concept (not per-key), we route all lease operations to the default
// backend. The LeaseID returned by Grant is then included in Put requests,
// which get routed to the appropriate backend by the KV proxy.
//
// This means a lease granted on the default backend may be referenced by
// keys stored on shard backends. This works because etcd only checks
// lease existence at Put time on the cluster that owns the key — the
// lease ID is stored as an opaque int64 with the key. The shard backend
// won't auto-expire keys when the default backend's lease expires, but
// KAS TTL-bearing objects (events) are short-lived and cleaned up by
// controllers anyway.
type leaseProxy struct {
	router *Router
	logger *zap.Logger
	pb.UnimplementedLeaseServer
}

func newLeaseProxy(router *Router, logger *zap.Logger) pb.LeaseServer {
	return &leaseProxy{
		router: router,
		logger: logger,
	}
}

func (p *leaseProxy) LeaseGrant(ctx context.Context, r *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	client := p.router.DefaultClient()
	resp, err := client.Lease.Grant(ctx, r.TTL)
	if err != nil {
		return nil, err
	}
	return &pb.LeaseGrantResponse{
		Header: resp.ResponseHeader,
		ID:     int64(resp.ID),
		TTL:    resp.TTL,
		Error:  resp.Error,
	}, nil
}

func (p *leaseProxy) LeaseRevoke(ctx context.Context, r *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	client := p.router.DefaultClient()
	resp, err := client.Lease.Revoke(ctx, clientv3.LeaseID(r.ID))
	if err != nil {
		return nil, err
	}
	return (*pb.LeaseRevokeResponse)(resp), nil
}

func (p *leaseProxy) LeaseTimeToLive(ctx context.Context, r *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	client := p.router.DefaultClient()
	var opts []clientv3.LeaseOption
	if r.Keys {
		opts = append(opts, clientv3.WithAttachedKeys())
	}
	resp, err := client.Lease.TimeToLive(ctx, clientv3.LeaseID(r.ID), opts...)
	if err != nil {
		return nil, err
	}
	return &pb.LeaseTimeToLiveResponse{
		Header:     resp.ResponseHeader,
		ID:         int64(resp.ID),
		TTL:        resp.TTL,
		GrantedTTL: resp.GrantedTTL,
		Keys:       resp.Keys,
	}, nil
}

func (p *leaseProxy) LeaseLeases(ctx context.Context, r *pb.LeaseLeasesRequest) (*pb.LeaseLeasesResponse, error) {
	client := p.router.DefaultClient()
	resp, err := client.Lease.Leases(ctx)
	if err != nil {
		return nil, err
	}
	leases := make([]*pb.LeaseStatus, len(resp.Leases))
	for i := range resp.Leases {
		leases[i] = &pb.LeaseStatus{ID: int64(resp.Leases[i].ID)}
	}
	return &pb.LeaseLeasesResponse{
		Header: resp.ResponseHeader,
		Leases: leases,
	}, nil
}

func (p *leaseProxy) LeaseKeepAlive(stream pb.Lease_LeaseKeepAliveServer) error {
	client := p.router.DefaultClient()
	ctx := stream.Context()

	// Open a keepalive stream to the backend.
	backendStream, err := pb.NewLeaseClient(client.ActiveConnection()).LeaseKeepAlive(ctx)
	if err != nil {
		return err
	}

	errCh := make(chan error, 2)

	// Client → Backend
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			if err := backendStream.Send(req); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Backend → Client
	go func() {
		for {
			resp, err := backendStream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			if err := stream.Send(resp); err != nil {
				errCh <- err
				return
			}
		}
	}()

	err = <-errCh
	if err == io.EOF {
		return nil
	}
	return err
}
