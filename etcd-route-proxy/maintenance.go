package etcdrouteproxy

import (
	"context"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maintenanceProxy implements the etcd Maintenance gRPC service.
// All operations are forwarded to the default backend.
//
// KAS uses:
// - Status: for storage metrics (db size) via etcd3ProberMonitor.Monitor()
// - Get on /registry/health: for health probes (goes through KV, not Maintenance)
//
// Defrag, Snapshot, Hash, HashKV, and Alarm are not used by KAS but are
// implemented as pass-through for completeness (e.g., etcd-defrag controller).
type maintenanceProxy struct {
	router *Router
	logger *zap.Logger
	pb.UnimplementedMaintenanceServer
}

func newMaintenanceProxy(router *Router, logger *zap.Logger) pb.MaintenanceServer {
	return &maintenanceProxy{
		router: router,
		logger: logger,
	}
}

func (p *maintenanceProxy) Alarm(ctx context.Context, r *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	client := p.router.DefaultClient()
	clientResp, err := client.AlarmList(ctx)
	if err != nil {
		return nil, err
	}
	return (*pb.AlarmResponse)(clientResp), nil
}

func (p *maintenanceProxy) Status(ctx context.Context, r *pb.StatusRequest) (*pb.StatusResponse, error) {
	client := p.router.DefaultClient()
	endpoints := client.Endpoints()
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available for status check")
	}
	clientResp, err := client.Status(ctx, endpoints[0])
	if err != nil {
		return nil, err
	}
	return (*pb.StatusResponse)(clientResp), nil
}

func (p *maintenanceProxy) Defragment(ctx context.Context, r *pb.DefragmentRequest) (*pb.DefragmentResponse, error) {
	client := p.router.DefaultClient()
	endpoints := client.Endpoints()
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available for defragment")
	}
	clientResp, err := client.Defragment(ctx, endpoints[0])
	if err != nil {
		return nil, err
	}
	return (*pb.DefragmentResponse)(clientResp), nil
}

func (p *maintenanceProxy) Hash(ctx context.Context, r *pb.HashRequest) (*pb.HashResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Hash is not supported by etcd-route-proxy")
}

func (p *maintenanceProxy) HashKV(ctx context.Context, r *pb.HashKVRequest) (*pb.HashKVResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "HashKV is not supported by etcd-route-proxy")
}

func (p *maintenanceProxy) Snapshot(r *pb.SnapshotRequest, stream pb.Maintenance_SnapshotServer) error {
	return status.Errorf(codes.Unimplemented, "Snapshot is not supported by etcd-route-proxy")
}
