package server

import (
	"context"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// KVServer implements etcdserverpb.KVServer backed by Spanner.
type KVServer struct {
	etcdserverpb.UnimplementedKVServer
	store *store.Store
	log   *zap.Logger
}

func newKVServer(s *store.Store, log *zap.Logger) *KVServer {
	return &KVServer{store: s, log: log}
}

// Range handles Get and List requests from Kubernetes.
func (k *KVServer) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	key := string(r.Key)
	end := string(r.RangeEnd)

	// Single key GET.
	if end == "" {
		curRev, kv, err := k.store.Get(ctx, key, r.Revision)
		if err != nil {
			return nil, toGRPCErr(err)
		}
		resp := &etcdserverpb.RangeResponse{
			Header: header(curRev),
		}
		if kv != nil && !kv.Deleted {
			resp.Kvs = []*mvccpb.KeyValue{toProtoKV(kv)}
			resp.Count = 1
		}
		return resp, nil
	}

	// Prefix / range GET.
	prefix := rangeToPrefix(key, end)
	startKey := key

	if r.CountOnly {
		curRev, count, err := k.store.Count(ctx, prefix, startKey, r.Revision)
		if err != nil {
			return nil, toGRPCErr(err)
		}
		return &etcdserverpb.RangeResponse{
			Header: header(curRev),
			Count:  count,
		}, nil
	}

	curRev, _, kvs, err := k.store.List(ctx, prefix, startKey, r.Limit, r.Revision)
	if err != nil {
		return nil, toGRPCErr(err)
	}

	resp := &etcdserverpb.RangeResponse{
		Header: header(curRev),
		Count:  int64(len(kvs)),
	}
	if !r.KeysOnly {
		for _, kv := range kvs {
			resp.Kvs = append(resp.Kvs, toProtoKV(kv))
		}
	} else {
		for _, kv := range kvs {
			resp.Kvs = append(resp.Kvs, &mvccpb.KeyValue{Key: []byte(kv.Key)})
		}
	}
	return resp, nil
}

// Put handles create/update requests. Kubernetes always uses Txn, but etcdctl
// can use Put directly.
func (k *KVServer) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	key := string(r.Key)
	rev, err := k.store.Create(ctx, key, r.Value, r.Lease)
	if err == store.ErrKeyExists {
		curRev, kv, gerr := k.store.Get(ctx, key, 0)
		if gerr != nil {
			return nil, toGRPCErr(gerr)
		}
		var prevModRev int64
		if kv != nil {
			prevModRev = kv.Rev
		}
		rev, kv, _, err = k.store.Update(ctx, key, r.Value, prevModRev, r.Lease)
		if err != nil {
			return nil, toGRPCErr(err)
		}
		resp := &etcdserverpb.PutResponse{Header: header(rev)}
		if r.PrevKv && kv != nil {
			resp.PrevKv = toProtoKV(kv)
		}
		_ = curRev
		return resp, nil
	}
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return &etcdserverpb.PutResponse{Header: header(rev)}, nil
}

// DeleteRange handles bare delete-range requests (e.g. from etcdctl del).
// Kubernetes always deletes via Txn, but etcdctl and other clients use this.
func (k *KVServer) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	key := string(r.Key)
	end := string(r.RangeEnd)

	// Single key delete.
	if end == "" {
		rev, prev, ok, err := k.store.Delete(ctx, key, 0)
		if err != nil {
			return nil, toGRPCErr(err)
		}
		resp := &etcdserverpb.DeleteRangeResponse{
			Header:  header(rev),
			Deleted: boolToInt(ok),
		}
		if r.PrevKv && prev != nil {
			resp.PrevKvs = []*mvccpb.KeyValue{toProtoKV(prev)}
		}
		return resp, nil
	}

	// Range / prefix delete — iterate and delete each key.
	prefix := rangeToPrefix(key, end)
	_, _, kvs, err := k.store.List(ctx, prefix, key, 0, 0)
	if err != nil {
		return nil, toGRPCErr(err)
	}

	var deleted int64
	var prevKvs []*mvccpb.KeyValue
	var lastRev int64

	for _, kv := range kvs {
		rev, prev, ok, err := k.store.Delete(ctx, kv.Key, 0)
		if err != nil {
			continue
		}
		if ok {
			deleted++
			lastRev = rev
			if r.PrevKv && prev != nil {
				prevKvs = append(prevKvs, toProtoKV(prev))
			}
		}
	}

	if lastRev == 0 {
		lastRev, _ = k.store.CurrentRevision(ctx)
	}

	return &etcdserverpb.DeleteRangeResponse{
		Header:  header(lastRev),
		Deleted: deleted,
		PrevKvs: prevKvs,
	}, nil
}

// Txn processes an etcd transaction — the primary operation used by Kubernetes.
// All compare reads and op writes execute inside a single Spanner RWT — atomic.
func (k *KVServer) Txn(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	compares := make([]store.TxnCompare, 0, len(r.Compare))
	for _, c := range r.Compare {
		c := c // capture
		compares = append(compares, store.TxnCompare{
			Key: string(c.Key),
			Evaluate: func(kv *store.KV) bool {
				switch c.Target {
				case etcdserverpb.Compare_VERSION:
					var version int64
					if kv != nil {
						version = kv.Rev - kv.CreateRevision + 1
					}
					return compareInt(version, c.GetVersion(), c.Result)
				case etcdserverpb.Compare_CREATE:
					var createRev int64
					if kv != nil {
						createRev = kv.CreateRevision
					}
					return compareInt(createRev, c.GetCreateRevision(), c.Result)
				case etcdserverpb.Compare_MOD:
					var modRev int64
					if kv != nil {
						modRev = kv.Rev
					}
					return compareInt(modRev, c.GetModRevision(), c.Result)
				case etcdserverpb.Compare_VALUE:
					var val []byte
					if kv != nil {
						val = kv.Value
					}
					return compareBytes(val, c.GetValue(), c.Result)
				}
				return false
			},
		})
	}

	toStoreOps := func(ops []*etcdserverpb.RequestOp) []store.TxnOp {
		result := make([]store.TxnOp, 0, len(ops))
		for _, op := range ops {
			switch v := op.Request.(type) {
			case *etcdserverpb.RequestOp_RequestRange:
				result = append(result, store.TxnOp{Type: store.TxnOpGet, Key: string(v.RequestRange.Key)})
			case *etcdserverpb.RequestOp_RequestPut:
				result = append(result, store.TxnOp{
					Type:    store.TxnOpPut,
					Key:     string(v.RequestPut.Key),
					Value:   v.RequestPut.Value,
					LeaseID: v.RequestPut.Lease,
				})
			case *etcdserverpb.RequestOp_RequestDeleteRange:
				result = append(result, store.TxnOp{Type: store.TxnOpDelete, Key: string(v.RequestDeleteRange.Key)})
			}
		}
		return result
	}

	succeeded, results, commitRev, err := k.store.AtomicTxn(ctx, compares,
		toStoreOps(r.Success), toStoreOps(r.Failure))
	if err != nil {
		return nil, toGRPCErr(err)
	}

	// Build gRPC responses from TxnResults.
	ops := r.Success
	if !succeeded {
		ops = r.Failure
	}
	var responses []*etcdserverpb.ResponseOp
	for i, op := range ops {
		var res store.TxnResult
		if i < len(results) {
			res = results[i]
		}
		switch v := op.Request.(type) {
		case *etcdserverpb.RequestOp_RequestRange:
			rresp := &etcdserverpb.RangeResponse{Header: header(commitRev)}
			if res.KV != nil {
				rresp.Kvs = []*mvccpb.KeyValue{toProtoKV(res.KV)}
				rresp.Count = 1
			}
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: rresp},
			})
		case *etcdserverpb.RequestOp_RequestPut:
			presp := &etcdserverpb.PutResponse{Header: header(commitRev)}
			if v.RequestPut.PrevKv && res.KV != nil {
				presp.PrevKv = toProtoKV(res.KV)
			}
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: presp},
			})
		case *etcdserverpb.RequestOp_RequestDeleteRange:
			dresp := &etcdserverpb.DeleteRangeResponse{
				Header:  header(commitRev),
				Deleted: boolToInt(res.Ok),
			}
			if v.RequestDeleteRange.PrevKv && res.KV != nil {
				dresp.PrevKvs = []*mvccpb.KeyValue{toProtoKV(res.KV)}
			}
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: dresp},
			})
		}
	}

	if commitRev == 0 {
		commitRev, _ = k.store.CurrentRevision(ctx)
	}

	return &etcdserverpb.TxnResponse{
		Header:    header(commitRev),
		Succeeded: succeeded,
		Responses: responses,
	}, nil
}

// Compact records the compaction revision.
func (k *KVServer) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	rev, err := k.store.Compact(ctx, r.Revision)
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return &etcdserverpb.CompactionResponse{Header: header(rev)}, nil
}

// ─── Txn helpers ─────────────────────────────────────────────────────────────

func (k *KVServer) evaluateCompare(ctx context.Context, compares []*etcdserverpb.Compare) (bool, error) {
	for _, c := range compares {
		key := string(c.Key)
		_, kv, err := k.store.Get(ctx, key, 0)
		if err != nil {
			return false, err
		}

		var result bool
		switch c.Target {
		case etcdserverpb.Compare_VERSION:
			var version int64
			if kv != nil {
				version = kv.Rev - kv.CreateRevision + 1
			}
			result = compareInt(version, c.GetVersion(), c.Result)
		case etcdserverpb.Compare_CREATE:
			var createRev int64
			if kv != nil {
				createRev = kv.CreateRevision
			}
			result = compareInt(createRev, c.GetCreateRevision(), c.Result)
		case etcdserverpb.Compare_MOD:
			var modRev int64
			if kv != nil {
				modRev = kv.Rev
			}
			result = compareInt(modRev, c.GetModRevision(), c.Result)
		case etcdserverpb.Compare_VALUE:
			var val []byte
			if kv != nil {
				val = kv.Value
			}
			result = compareBytes(val, c.GetValue(), c.Result)
		default:
			return false, status.Errorf(codes.InvalidArgument, "unsupported compare target: %v", c.Target)
		}
		if !result {
			return false, nil
		}
	}
	return true, nil
}

func (k *KVServer) executeOps(ctx context.Context, ops []*etcdserverpb.RequestOp) ([]*etcdserverpb.ResponseOp, int64, error) {
	var responses []*etcdserverpb.ResponseOp
	var lastRev int64

	for _, op := range ops {
		switch v := op.Request.(type) {
		case *etcdserverpb.RequestOp_RequestRange:
			resp, err := k.Range(ctx, v.RequestRange)
			if err != nil {
				return nil, 0, err
			}
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: resp},
			})
			lastRev = resp.Header.Revision

		case *etcdserverpb.RequestOp_RequestPut:
			resp, err := k.Put(ctx, v.RequestPut)
			if err != nil {
				return nil, 0, err
			}
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: resp},
			})
			lastRev = resp.Header.Revision

		case *etcdserverpb.RequestOp_RequestDeleteRange:
			r := v.RequestDeleteRange
			rev, prev, ok, err := k.store.Delete(ctx, string(r.Key), 0)
			if err != nil {
				return nil, 0, err
			}
			dresp := &etcdserverpb.DeleteRangeResponse{
				Header:  header(rev),
				Deleted: boolToInt(ok),
			}
			if r.PrevKv && prev != nil {
				dresp.PrevKvs = []*mvccpb.KeyValue{toProtoKV(prev)}
			}
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: dresp},
			})
			lastRev = rev
		}
	}

	if lastRev == 0 {
		var err error
		lastRev, err = k.store.CurrentRevision(ctx)
		if err != nil {
			return nil, 0, err
		}
	}
	return responses, lastRev, nil
}

// ─── proto helpers ────────────────────────────────────────────────────────────

func header(rev int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		ClusterId: 1,
		MemberId:  1,
		Revision:  rev,
		RaftTerm:  1,
	}
}

func toProtoKV(kv *store.KV) *mvccpb.KeyValue {
	if kv == nil {
		return nil
	}
	return &mvccpb.KeyValue{
		Key:            []byte(kv.Key),
		Value:          kv.Value,
		ModRevision:    kv.Rev,
		CreateRevision: kv.CreateRevision,
		Version:        kv.Rev - kv.CreateRevision + 1,
		Lease:          kv.LeaseID,
	}
}

// rangeToPrefix converts an etcd key range [key, end) to a LIKE prefix.
// For prefix queries, end = key with last byte incremented.
func rangeToPrefix(key, end string) string {
	if end == "\x00" {
		return ""
	}
	if len(end) > 0 && len(key) > 0 {
		// Check if this is a simple prefix scan.
		if strings.HasPrefix(end, key[:len(key)-1]) {
			return key[:len(key)-1]
		}
	}
	return key
}

func compareInt(actual, expected int64, op etcdserverpb.Compare_CompareResult) bool {
	switch op {
	case etcdserverpb.Compare_EQUAL:
		return actual == expected
	case etcdserverpb.Compare_NOT_EQUAL:
		return actual != expected
	case etcdserverpb.Compare_GREATER:
		return actual > expected
	case etcdserverpb.Compare_LESS:
		return actual < expected
	}
	return false
}

func compareBytes(actual, expected []byte, op etcdserverpb.Compare_CompareResult) bool {
	switch op {
	case etcdserverpb.Compare_EQUAL:
		return string(actual) == string(expected)
	case etcdserverpb.Compare_NOT_EQUAL:
		return string(actual) != string(expected)
	}
	return false
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func toGRPCErr(err error) error {
	switch err {
	case store.ErrKeyExists:
		return status.Error(codes.AlreadyExists, "key already exists")
	case store.ErrKeyNotFound:
		return status.Error(codes.NotFound, "key not found")
	case store.ErrRevisionMismatch:
		return status.Error(codes.FailedPrecondition, "revision mismatch")
	case store.ErrCompacted:
		return status.Error(codes.OutOfRange, "requested revision has been compacted")
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
