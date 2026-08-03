package etcdrouteproxy

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// kvProxy implements the etcd KV gRPC service, routing requests to the
// appropriate backend based on key prefix.
type kvProxy struct {
	router *Router
	logger *zap.Logger
	pb.UnimplementedKVServer
}

func newKVProxy(router *Router, logger *zap.Logger) pb.KVServer {
	return &kvProxy{
		router: router,
		logger: logger,
	}
}

func (p *kvProxy) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	client := p.router.Route(r.Key)
	resp, err := client.KV.Do(ctx, RangeRequestToOp(r))
	if err != nil {
		return nil, err
	}
	return (*pb.RangeResponse)(resp.Get()), nil
}

func (p *kvProxy) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	client := p.router.Route(r.Key)
	resp, err := client.KV.Do(ctx, PutRequestToOp(r))
	if err != nil {
		return nil, err
	}
	return (*pb.PutResponse)(resp.Put()), nil
}

func (p *kvProxy) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	client := p.router.Route(r.Key)
	resp, err := client.KV.Do(ctx, DelRequestToOp(r))
	if err != nil {
		return nil, err
	}
	return (*pb.DeleteRangeResponse)(resp.Del()), nil
}

func (p *kvProxy) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	// Determine the backend from the transaction's keys.
	// KAS only generates single-key Txns (OptimisticPut/OptimisticDelete),
	// so all keys in Compare/Success/Failure should route to the same backend.
	client := p.routeTxn(r)
	txnResp, err := client.KV.Txn(ctx).
		If(cmpsFromProto(r.Compare)...).
		Then(opsFromProto(r.Success)...).
		Else(opsFromProto(r.Failure)...).
		Commit()
	if err != nil {
		return nil, err
	}
	return (*pb.TxnResponse)(txnResp), nil
}

func (p *kvProxy) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	// Compact must be sent to all backends.
	var lastResp *pb.CompactionResponse
	for _, client := range p.router.AllClients() {
		var opts []clientv3.CompactOption
		if r.Physical {
			opts = append(opts, clientv3.WithCompactPhysical())
		}
		clientResp, err := client.Compact(ctx, r.Revision, opts...)
		if err != nil {
			return nil, err
		}
		lastResp = (*pb.CompactionResponse)(clientResp)
	}
	return lastResp, nil
}

// routeTxn extracts a key from the Txn to determine which backend to use.
// It checks Compare keys first, then Success ops, then Failure ops.
func (p *kvProxy) routeTxn(r *pb.TxnRequest) *clientv3.Client {
	// Try Compare keys first.
	for _, cmp := range r.Compare {
		if len(cmp.Key) > 0 {
			return p.router.Route(cmp.Key)
		}
	}
	// Try Success ops.
	if key := firstKeyFromOps(r.Success); key != nil {
		return p.router.Route(key)
	}
	// Try Failure ops.
	if key := firstKeyFromOps(r.Failure); key != nil {
		return p.router.Route(key)
	}
	// Fallback to default (shouldn't happen for KAS Txns).
	p.logger.Warn("txn with no identifiable key, routing to default")
	return p.router.DefaultClient()
}

// firstKeyFromOps extracts the first key from a list of request operations.
func firstKeyFromOps(ops []*pb.RequestOp) []byte {
	for _, op := range ops {
		switch tv := op.Request.(type) {
		case *pb.RequestOp_RequestRange:
			if tv.RequestRange != nil && len(tv.RequestRange.Key) > 0 {
				return tv.RequestRange.Key
			}
		case *pb.RequestOp_RequestPut:
			if tv.RequestPut != nil && len(tv.RequestPut.Key) > 0 {
				return tv.RequestPut.Key
			}
		case *pb.RequestOp_RequestDeleteRange:
			if tv.RequestDeleteRange != nil && len(tv.RequestDeleteRange.Key) > 0 {
				return tv.RequestDeleteRange.Key
			}
		case *pb.RequestOp_RequestTxn:
			if tv.RequestTxn != nil {
				for _, cmp := range tv.RequestTxn.Compare {
					if len(cmp.Key) > 0 {
						return cmp.Key
					}
				}
			}
		}
	}
	return nil
}

// --- Proto-to-Op conversion helpers ---
// These mirror the helpers from the etcd grpc-proxy.

func RangeRequestToOp(r *pb.RangeRequest) clientv3.Op {
	var opts []clientv3.OpOption
	if len(r.RangeEnd) != 0 {
		opts = append(opts, clientv3.WithRange(string(r.RangeEnd)))
	}
	opts = append(opts, clientv3.WithRev(r.Revision))
	opts = append(opts, clientv3.WithLimit(r.Limit))
	opts = append(opts, clientv3.WithSort(
		clientv3.SortTarget(r.SortTarget),
		clientv3.SortOrder(r.SortOrder)),
	)
	opts = append(opts, clientv3.WithMaxCreateRev(r.MaxCreateRevision))
	opts = append(opts, clientv3.WithMinCreateRev(r.MinCreateRevision))
	opts = append(opts, clientv3.WithMaxModRev(r.MaxModRevision))
	opts = append(opts, clientv3.WithMinModRev(r.MinModRevision))
	if r.CountOnly {
		opts = append(opts, clientv3.WithCountOnly())
	}
	if r.KeysOnly {
		opts = append(opts, clientv3.WithKeysOnly())
	}
	if r.Serializable {
		opts = append(opts, clientv3.WithSerializable())
	}
	return clientv3.OpGet(string(r.Key), opts...)
}

func PutRequestToOp(r *pb.PutRequest) clientv3.Op {
	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithLease(clientv3.LeaseID(r.Lease)))
	if r.IgnoreValue {
		opts = append(opts, clientv3.WithIgnoreValue())
	}
	if r.IgnoreLease {
		opts = append(opts, clientv3.WithIgnoreLease())
	}
	if r.PrevKv {
		opts = append(opts, clientv3.WithPrevKV())
	}
	return clientv3.OpPut(string(r.Key), string(r.Value), opts...)
}

func DelRequestToOp(r *pb.DeleteRangeRequest) clientv3.Op {
	var opts []clientv3.OpOption
	if len(r.RangeEnd) != 0 {
		opts = append(opts, clientv3.WithRange(string(r.RangeEnd)))
	}
	if r.PrevKv {
		opts = append(opts, clientv3.WithPrevKV())
	}
	return clientv3.OpDelete(string(r.Key), opts...)
}

// cmpsFromProto converts protobuf Compare messages to clientv3.Cmp values.
// In etcd client v3.6, Cmp is a type alias for pb.Compare, so we cast directly.
func cmpsFromProto(cmps []*pb.Compare) []clientv3.Cmp {
	result := make([]clientv3.Cmp, len(cmps))
	for i, c := range cmps {
		result[i] = clientv3.Cmp(*c)
	}
	return result
}

// opsFromProto converts protobuf RequestOp messages to clientv3.Op values.
func opsFromProto(ops []*pb.RequestOp) []clientv3.Op {
	result := make([]clientv3.Op, len(ops))
	for i, op := range ops {
		result[i] = requestOpToOp(op)
	}
	return result
}

func requestOpToOp(union *pb.RequestOp) clientv3.Op {
	switch tv := union.Request.(type) {
	case *pb.RequestOp_RequestRange:
		if tv.RequestRange != nil {
			return RangeRequestToOp(tv.RequestRange)
		}
	case *pb.RequestOp_RequestPut:
		if tv.RequestPut != nil {
			return PutRequestToOp(tv.RequestPut)
		}
	case *pb.RequestOp_RequestDeleteRange:
		if tv.RequestDeleteRange != nil {
			return DelRequestToOp(tv.RequestDeleteRange)
		}
	case *pb.RequestOp_RequestTxn:
		if tv.RequestTxn != nil {
			cmps := make([]clientv3.Cmp, len(tv.RequestTxn.Compare))
			thenOps := make([]clientv3.Op, len(tv.RequestTxn.Success))
			elseOps := make([]clientv3.Op, len(tv.RequestTxn.Failure))
			for j := range tv.RequestTxn.Compare {
				cmps[j] = clientv3.FromCompare(*tv.RequestTxn.Compare[j])
			}
			for j := range tv.RequestTxn.Success {
				thenOps[j] = requestOpToOp(tv.RequestTxn.Success[j])
			}
			for j := range tv.RequestTxn.Failure {
				elseOps[j] = requestOpToOp(tv.RequestTxn.Failure[j])
			}
			return clientv3.OpTxn(cmps, thenOps, elseOps)
		}
	}
	panic("unknown request op type")
}
