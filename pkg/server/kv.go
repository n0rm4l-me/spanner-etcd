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
//
// Routing strategy for maximum compatibility:
//   - If all ops are simple single-key Put/Delete/Get → AtomicTxn (single Spanner RWT, fully atomic)
//   - If any op has RangeEnd/CountOnly/IgnoreValue/etc → non-atomic fallback via evaluateCompare+executeOps
//     (logs a warning; preserves compatibility with operators and tooling that use complex Txn ops)
func (k *KVServer) Txn(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	// Check if all ops can be handled atomically.
	if !k.txnNeedsNonAtomicFallback(r) {
		return k.txnAtomic(ctx, r)
	}
	// Fallback: non-atomic path for complex ops (range scan/delete, CountOnly, IgnoreValue, etc.)
	k.log.Warn("Txn contains complex ops — using non-atomic fallback for compatibility",
		zap.Int("compare", len(r.Compare)),
		zap.Int("success_ops", len(r.Success)),
		zap.Int("failure_ops", len(r.Failure)),
	)
	return k.txnNonAtomic(ctx, r)
}

// txnNeedsNonAtomicFallback returns true when the Txn contains ops that cannot
// be expressed inside a single Spanner ReadWriteTransaction (range ops, etc.).
func (k *KVServer) txnNeedsNonAtomicFallback(r *etcdserverpb.TxnRequest) bool {
	for _, ops := range [][]*etcdserverpb.RequestOp{r.Success, r.Failure} {
		for _, op := range ops {
			switch v := op.Request.(type) {
			case *etcdserverpb.RequestOp_RequestRange:
				rr := v.RequestRange
				if len(rr.RangeEnd) > 0 || rr.Revision != 0 || rr.CountOnly || rr.KeysOnly || rr.Limit != 0 {
					return true
				}
			case *etcdserverpb.RequestOp_RequestPut:
				if v.RequestPut.IgnoreValue || v.RequestPut.IgnoreLease {
					return true
				}
			case *etcdserverpb.RequestOp_RequestDeleteRange:
				if len(v.RequestDeleteRange.RangeEnd) > 0 {
					return true
				}
			}
		}
	}
	return false
}

// txnAtomic executes compare+ops in a single Spanner ReadWriteTransaction.
func (k *KVServer) txnAtomic(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	compares := make([]store.TxnCompare, 0, len(r.Compare))
	for _, c := range r.Compare {
		c := c
		tc := store.TxnCompare{Key: string(c.Key)}
		switch c.Target {
		case etcdserverpb.Compare_VERSION,
			etcdserverpb.Compare_CREATE,
			etcdserverpb.Compare_MOD,
			etcdserverpb.Compare_VALUE:
			tc.Evaluate = func(kv *store.KV) bool {
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
			}
		default:
			tc.Err = status.Errorf(codes.InvalidArgument, "unsupported compare target: %v", c.Target)
		}
		compares = append(compares, tc)
	}

	toStoreOps := func(ops []*etcdserverpb.RequestOp) ([]store.TxnOp, error) {
		result := make([]store.TxnOp, 0, len(ops))
		for _, op := range ops {
			switch v := op.Request.(type) {
			case *etcdserverpb.RequestOp_RequestRange:
				result = append(result, store.TxnOp{Type: store.TxnOpGet, Key: string(v.RequestRange.Key)})
			case *etcdserverpb.RequestOp_RequestPut:
				pr := v.RequestPut
				result = append(result, store.TxnOp{
					Type:    store.TxnOpPut,
					Key:     string(pr.Key),
					Value:   pr.Value,
					LeaseID: pr.Lease,
				})
			case *etcdserverpb.RequestOp_RequestDeleteRange:
				result = append(result, store.TxnOp{Type: store.TxnOpDelete, Key: string(v.RequestDeleteRange.Key)})
			default:
				return nil, status.Error(codes.InvalidArgument, "unsupported Txn op type")
			}
		}
		return result, nil
	}

	successOps, err := toStoreOps(r.Success)
	if err != nil {
		return nil, err
	}
	failureOps, err := toStoreOps(r.Failure)
	if err != nil {
		return nil, err
	}

	// Detect duplicate keys in each branch.
	for _, ops := range [][]store.TxnOp{successOps, failureOps} {
		seen := make(map[string]struct{}, len(ops))
		for _, op := range ops {
			if op.Type == store.TxnOpPut || op.Type == store.TxnOpDelete {
				if _, dup := seen[op.Key]; dup {
					return nil, status.Errorf(codes.InvalidArgument,
						"duplicate mutation for key %q in single Txn branch", op.Key)
				}
				seen[op.Key] = struct{}{}
			}
		}
	}

	succeeded, results, commitRev, err := k.store.AtomicTxn(ctx, compares, successOps, failureOps)
	if err != nil {
		// Pass through gRPC status errors unchanged (e.g. InvalidArgument, Unimplemented).
		// Only map store domain errors via toGRPCErr.
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, toGRPCErr(err)
	}

	// Ensure commitRev is set before building response headers.
	if commitRev == 0 {
		var cerr error
		commitRev, cerr = k.store.CurrentRevision(ctx)
		if cerr != nil {
			return nil, toGRPCErr(cerr)
		}
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

	return &etcdserverpb.TxnResponse{
		Header:    header(commitRev),
		Succeeded: succeeded,
		Responses: responses,
	}, nil
}

// txnNonAtomic handles Txns with complex ops (range scan/delete, CountOnly, etc.)
// that cannot be executed inside a single Spanner ReadWriteTransaction.
// Compare evaluation and op execution happen as separate operations — there is a
// TOCTOU window between them. This is acceptable for ops that Kubernetes core
// does not use for optimistic locking (e.g. range reads by operators).
func (k *KVServer) txnNonAtomic(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	succeeded, err := k.evaluateCompare(ctx, r.Compare)
	if err != nil {
		// Pass through gRPC status errors unchanged; only map store errors.
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, toGRPCErr(err)
	}

	ops := r.Success
	if !succeeded {
		ops = r.Failure
	}

	responses, rev, err := k.executeOps(ctx, ops)
	if err != nil {
		// Range/Put/DeleteRange already return gRPC status errors — pass through.
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, toGRPCErr(err)
	}

	return &etcdserverpb.TxnResponse{
		Header:    header(rev),
		Succeeded: succeeded,
		Responses: responses,
	}, nil
}

// evaluateCompare evaluates Txn compare conditions via independent store.Get calls.
// Used only by txnNonAtomic — not atomic with the subsequent executeOps.
func (k *KVServer) evaluateCompare(ctx context.Context, compares []*etcdserverpb.Compare) (bool, error) {
	for _, c := range compares {
		_, kv, err := k.store.Get(ctx, string(c.Key), 0)
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

// executeOps executes Txn ops sequentially — supports full range/count/etc ops.
// Used only by txnNonAtomic. Each op is a separate store call (not atomic).
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
			resp, err := k.DeleteRange(ctx, v.RequestDeleteRange)
			if err != nil {
				return nil, 0, err
			}
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: resp},
			})
			lastRev = resp.Header.Revision
		default:
			return nil, 0, status.Error(codes.InvalidArgument, "unsupported Txn op type in non-atomic path")
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

// Compact records the compaction revision.
func (k *KVServer) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	rev, err := k.store.Compact(ctx, r.Revision)
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return &etcdserverpb.CompactionResponse{Header: header(rev)}, nil
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
