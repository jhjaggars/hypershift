package etcdrouteproxy

import (
	"io"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// watchProxy implements the etcd Watch gRPC service, routing watch requests
// to the appropriate backend based on the watched key prefix.
//
// KAS opens exactly one watch per resource type via the Cacher's Reflector,
// so each Watch stream maps to a single resource prefix and routes to a single backend.
// The proxy does not need to coalesce or fan-in watches across backends.
type watchProxy struct {
	router *Router
	logger *zap.Logger
	pb.UnimplementedWatchServer
}

func newWatchProxy(router *Router, logger *zap.Logger) pb.WatchServer {
	return &watchProxy{
		router: router,
		logger: logger,
	}
}

func (p *watchProxy) Watch(serverStream pb.Watch_WatchServer) error {
	// Wait for the first request to determine routing.
	// Once routed, the entire stream stays on that backend.
	firstReq, err := serverStream.Recv()
	if err != nil {
		return err
	}

	client := p.routeWatchRequest(firstReq)
	ctx := serverStream.Context()

	// Open a watch stream to the backend.
	backendStream, err := pb.NewWatchClient(client.ActiveConnection()).Watch(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to open backend watch stream: %v", err)
	}

	// Forward the first request.
	if err := backendStream.Send(firstReq); err != nil {
		return err
	}

	// Bidirectional forwarding.
	errCh := make(chan error, 2)

	// Client → Backend
	go func() {
		for {
			req, err := serverStream.Recv()
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
			if err := serverStream.Send(resp); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Wait for either direction to finish.
	err = <-errCh
	if err == io.EOF {
		return nil
	}
	return err
}

// routeWatchRequest determines which backend to route a watch to based on the
// first WatchRequest's key.
func (p *watchProxy) routeWatchRequest(req *pb.WatchRequest) *clientv3.Client {
	switch uv := req.RequestUnion.(type) {
	case *pb.WatchRequest_CreateRequest:
		if uv.CreateRequest != nil && len(uv.CreateRequest.Key) > 0 {
			return p.router.Route(uv.CreateRequest.Key)
		}
	case *pb.WatchRequest_ProgressRequest:
		// Progress requests don't have keys; route to default.
	}
	return p.router.DefaultClient()
}
