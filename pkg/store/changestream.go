// Package store — Change Stream reader for low-latency Watch delivery.
//
// # Spanner Change Streams API
//
// Change Streams are read via the SQL TVF READ_{stream_name}:
//
//	READ_kv_changes(start_timestamp, end_timestamp, partition_token, heartbeat_millis)
//
// The initial call uses NULL as the partition_token. Spanner returns one or more
// ChildPartitionsRecord entries per row containing partition token strings.
// Subsequent calls use those tokens for per-partition streaming reads.
//
// The query must run via client.Single().Query() which creates a single-use
// strong read-only transaction — the only mode Spanner accepts for CS queries.
//
// # ChangeRecord decoding
//
// The TVF column "ChangeRecord" is ARRAY<STRUCT<data_change_record, heartbeat_record,
// child_partitions_record>>. All nested fields are also ARRAY<STRUCT> with many
// columns that vary by Spanner version. We use spanner.GenericColumnValue to decode
// the raw protobuf and extract only the fields we need, avoiding breakage when
// Spanner adds new columns to the struct schema.
package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/paas/spanner-etcd/pkg/metrics"
)

const (
	heartbeatInterval   = 2000 // ms
	cursorFlushInterval = 5 * time.Second
	partitionWorkersBuf = 64
)

// DispatchFunc is called for every batch of events decoded from the stream.
type DispatchFunc func(events []*Event)

// partitionState tracks one active Change Stream partition.
type partitionState struct {
	token           string
	startTimestamp  time.Time
	resumeTimestamp time.Time
}

// ChangeStreamReader reads all partitions of the kv_changes Change Stream.
type ChangeStreamReader struct {
	client    *spanner.Client
	replicaID string
	dispatch  DispatchFunc
	log       *zap.Logger

	mu      sync.Mutex
	active  map[string]*partitionState
	pending chan *partitionState
	stopCh  chan struct{}
	stopped chan struct{}
}

// NewChangeStreamReader creates a reader. Call Start to begin streaming.
func NewChangeStreamReader(
	client *spanner.Client,
	replicaID string,
	dispatch DispatchFunc,
	log *zap.Logger,
) *ChangeStreamReader {
	return &ChangeStreamReader{
		client:    client,
		replicaID: replicaID,
		dispatch:  dispatch,
		log:       log,
		active:    make(map[string]*partitionState),
		pending:   make(chan *partitionState, partitionWorkersBuf),
		stopCh:    make(chan struct{}),
		stopped:   make(chan struct{}),
	}
}

// Start begins reading the Change Stream. Blocks until ctx is cancelled.
func (r *ChangeStreamReader) Start(ctx context.Context) error {
	defer close(r.stopped)

	cursors, err := r.loadCursors(ctx)
	if err != nil {
		r.log.Warn("failed to load CS cursors, starting from now", zap.Error(err))
		cursors = map[string]time.Time{}
	}

	startTime := time.Now().Add(-time.Second)
	for _, t := range cursors {
		if t.Before(startTime) {
			startTime = t
		}
	}

	initialPartitions, err := r.queryInitialPartitions(ctx, startTime)
	if err != nil {
		return fmt.Errorf("query initial partitions: %w", err)
	}
	r.log.Info("change stream initial partitions",
		zap.Int("count", len(initialPartitions)),
		zap.Time("start", startTime),
	)

	for _, p := range initialPartitions {
		if resume, ok := cursors[p.token]; ok {
			p.resumeTimestamp = resume
		} else {
			p.resumeTimestamp = startTime
		}
		r.pending <- p
	}

	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		case <-r.stopCh:
			wg.Wait()
			return nil
		case p := <-r.pending:
			r.mu.Lock()
			if _, exists := r.active[p.token]; exists {
				r.mu.Unlock()
				continue
			}
			r.active[p.token] = p
			r.mu.Unlock()

			wg.Add(1)
			metrics.CSActivePartitions.Inc()
			go func(ps *partitionState) {
				defer wg.Done()
				defer func() {
					r.mu.Lock()
					delete(r.active, ps.token)
					r.mu.Unlock()
					metrics.CSActivePartitions.Dec()
				}()
				r.readPartition(ctx, ps)
			}(p)
		}
	}
}

// Stop signals the reader to shut down.
func (r *ChangeStreamReader) Stop() {
	close(r.stopCh)
	<-r.stopped
}

func (r *ChangeStreamReader) readPartition(ctx context.Context, p *partitionState) {
	log := r.log.With(zap.String("token", shortToken(p.token)))
	log.Debug("starting partition reader", zap.Time("resume", p.resumeTimestamp))
	lastFlush := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		default:
		}

		done, err := r.streamPartition(ctx, p, &lastFlush)
		if done {
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if err != nil {
			metrics.CSPartitionRestarts.Inc()
			log.Warn("partition read error, retrying", zap.Error(err))
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return
			case <-r.stopCh:
				return
			}
		}
	}
}

// streamPartition executes one streaming query for a partition.
// Returns (done=true, nil) when the partition ends normally.
func (r *ChangeStreamReader) streamPartition(ctx context.Context, p *partitionState, lastFlush *time.Time) (bool, error) {
	tokenParam := spanner.NullString{Valid: false}
	if p.token != "" {
		tokenParam = spanner.NullString{StringVal: p.token, Valid: true}
	}

	stmt := spanner.Statement{
		SQL: `SELECT ChangeRecord
		      FROM READ_kv_changes(
		        @start_time, NULL, @partition_token, @heartbeat_millis
		      )`,
		Params: map[string]interface{}{
			"start_time":      p.resumeTimestamp,
			"partition_token": tokenParam,
			"heartbeat_millis": int64(heartbeatInterval),
		},
	}

	iter := r.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("next: %w", err)
		}

		// Decode via GenericColumnValue to handle evolving schema without breakage.
		var gcv spanner.GenericColumnValue
		if err := row.Column(0, &gcv); err != nil {
			r.log.Warn("decode ChangeRecord column failed", zap.Error(err))
			continue
		}

		done, ts, events, children := r.decodeRecord(&gcv)
		if len(events) > 0 {
			r.dispatch(events)
		}
		if ts.After(p.resumeTimestamp) {
			p.resumeTimestamp = ts
		}
		for _, child := range children {
			select {
			case r.pending <- child:
				r.log.Debug("queued child partition", zap.String("token", shortToken(child.token)))
			case <-ctx.Done():
				return true, nil
			}
		}
		if done {
			_ = r.flushCursor(ctx, p)
			return true, nil
		}

		if time.Since(*lastFlush) > cursorFlushInterval {
			if err := r.flushCursor(ctx, p); err != nil {
				r.log.Warn("cursor flush failed", zap.Error(err))
			}
			*lastFlush = time.Now()
		}
	}
}

// decodeRecord decodes a ChangeRecord GenericColumnValue.
// Returns: (partitionDone, maxTimestamp, events, childPartitions).
//
// The ChangeRecord column is ARRAY<STRUCT<data_change_record, heartbeat_record,
// child_partitions_record>>. We navigate the protobuf Value tree manually to
// avoid issues with nested ARRAY<STRUCT> decoding in the Spanner Go client.
func (r *ChangeStreamReader) decodeRecord(gcv *spanner.GenericColumnValue) (
	done bool, maxTS time.Time, events []*Event, children []*partitionState,
) {
	// The outer value should be a list (ARRAY).
	outerList := gcv.Value.GetListValue()
	if outerList == nil {
		return
	}

	for _, outerVal := range outerList.Values {
		// Each element is a STRUCT with 3 fields: data_change_record, heartbeat_record, child_partitions_record.
		structVal := outerVal.GetListValue()
		if structVal == nil || len(structVal.Values) < 3 {
			continue
		}

		// Field 0: data_change_record (ARRAY<STRUCT<...>>)
		if dcrs := structVal.Values[0].GetListValue(); dcrs != nil {
			for _, dcr := range dcrs.Values {
				ev, ts := r.decodeDataChangeRecord(dcr)
				if ev != nil {
					events = append(events, ev...)
				}
				if ts.After(maxTS) {
					maxTS = ts
				}
			}
		}

		// Field 1: heartbeat_record (ARRAY<STRUCT<timestamp>>)
		if hbs := structVal.Values[1].GetListValue(); hbs != nil {
			for _, hb := range hbs.Values {
				if ts := r.decodeHeartbeat(hb); ts.After(maxTS) {
					maxTS = ts
				}
			}
		}

		// Field 2: child_partitions_record (ARRAY<STRUCT<...>>)
		if cprs := structVal.Values[2].GetListValue(); cprs != nil && len(cprs.Values) > 0 {
			for _, cpr := range cprs.Values {
				children = append(children, r.decodeChildPartitions(cpr)...)
			}
			done = true
		}
	}
	return
}

// decodeDataChangeRecord extracts events from one data_change_record struct.
// Schema: commit_timestamp, record_sequence, server_transaction_id,
//         is_last_record_in_transaction_in_partition, table_name,
//         column_types, mods, mod_type, value_capture_type,
//         number_of_records_in_transaction, number_of_partitions_in_transaction,
//         transaction_tag, is_system_transaction
func (r *ChangeStreamReader) decodeDataChangeRecord(v *structpb.Value) ([]*Event, time.Time) {
	fields := v.GetListValue()
	if fields == nil || len(fields.Values) < 8 {
		return nil, time.Time{}
	}

	// Field 0: commit_timestamp (STRING in RFC3339)
	commitTSStr := fields.Values[0].GetStringValue()
	commitTS, err := time.Parse(time.RFC3339Nano, commitTSStr)
	if err != nil {
		r.log.Debug("parse commit_timestamp failed", zap.String("ts", commitTSStr), zap.Error(err))
		return nil, time.Time{}
	}

	// Field 4: table_name
	tableName := fields.Values[4].GetStringValue()
	if tableName != "kv" {
		return nil, commitTS
	}

	// Field 7: mod_type (INSERT | UPDATE | DELETE)
	modType := fields.Values[7].GetStringValue()

	// Field 6: mods (ARRAY<STRUCT<keys JSON, new_values JSON, old_values JSON>>)
	modsArr := fields.Values[6].GetListValue()
	if modsArr == nil {
		return nil, commitTS
	}

	var events []*Event
	for _, modVal := range modsArr.Values {
		modFields := modVal.GetListValue()
		if modFields == nil || len(modFields.Values) < 3 {
			continue
		}

		// keys, new_values, old_values — all JSON strings
		newValStr := modFields.Values[1].GetStringValue()
		oldValStr := modFields.Values[2].GetStringValue()

		newVals := parseJSONMap(newValStr)
		oldVals := parseJSONMap(oldValStr)

		kv := extractKV(newVals, oldVals, commitTS)
		if kv == nil {
			continue
		}

		evType := EventPut
		if modType == "DELETE" || kv.Deleted {
			evType = EventDelete
		}
		events = append(events, &Event{KV: kv, Type: evType})
	}
	return events, commitTS
}

// decodeHeartbeat extracts the timestamp from a heartbeat_record struct.
// Schema: timestamp
func (r *ChangeStreamReader) decodeHeartbeat(v *structpb.Value) time.Time {
	fields := v.GetListValue()
	if fields == nil || len(fields.Values) < 1 {
		return time.Time{}
	}
	ts, _ := time.Parse(time.RFC3339Nano, fields.Values[0].GetStringValue())
	return ts
}

// decodeChildPartitions extracts partition states from a child_partitions_record struct.
// Schema: start_timestamp, record_sequence, child_partitions ARRAY<STRUCT<token, parent_partition_tokens>>
func (r *ChangeStreamReader) decodeChildPartitions(v *structpb.Value) []*partitionState {
	fields := v.GetListValue()
	if fields == nil || len(fields.Values) < 3 {
		return nil
	}

	startTS, _ := time.Parse(time.RFC3339Nano, fields.Values[0].GetStringValue())

	childrenArr := fields.Values[2].GetListValue()
	if childrenArr == nil {
		return nil
	}

	var result []*partitionState
	for _, childVal := range childrenArr.Values {
		childFields := childVal.GetListValue()
		if childFields == nil || len(childFields.Values) < 1 {
			continue
		}
		token := childFields.Values[0].GetStringValue()
		if token == "" {
			continue
		}
		result = append(result, &partitionState{
			token:           token,
			startTimestamp:  startTS,
			resumeTimestamp: startTS,
		})
	}
	return result
}

// queryInitialPartitions runs the NULL-token discovery query.
func (r *ChangeStreamReader) queryInitialPartitions(ctx context.Context, startTime time.Time) ([]*partitionState, error) {
	stmt := spanner.Statement{
		SQL: `SELECT ChangeRecord
		      FROM READ_kv_changes(
		        @start_time, NULL, NULL, @heartbeat_millis
		      )`,
		Params: map[string]interface{}{
			"start_time":      startTime,
			"heartbeat_millis": int64(1000),
		},
	}

	iter := r.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var partitions []*partitionState

	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("initial partition query: %w", err)
		}

		var gcv spanner.GenericColumnValue
		if err := row.Column(0, &gcv); err != nil {
			r.log.Warn("decode initial partition record failed", zap.Error(err))
			continue
		}

		done, _, _, children := r.decodeRecord(&gcv)
		partitions = append(partitions, children...)
		if done {
			break
		}
	}

	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions returned — Change Streams may not be available")
	}
	return partitions, nil
}

// ─── cursor persistence ───────────────────────────────────────────────────────

func (r *ChangeStreamReader) loadCursors(ctx context.Context) (map[string]time.Time, error) {
	stmt := spanner.Statement{
		SQL:    `SELECT partition_token, resume_timestamp FROM kv_cs_cursors WHERE replica_id = @rid`,
		Params: map[string]interface{}{"rid": r.replicaID},
	}
	iter := r.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	cursors := make(map[string]time.Time)
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		var token string
		var ts time.Time
		if err := row.Columns(&token, &ts); err != nil {
			return nil, err
		}
		cursors[token] = ts
	}
	return cursors, nil
}

func (r *ChangeStreamReader) flushCursor(ctx context.Context, p *partitionState) error {
	_, err := r.client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("kv_cs_cursors",
			[]string{"replica_id", "partition_token", "resume_timestamp", "updated_at"},
			[]interface{}{r.replicaID, p.token, p.resumeTimestamp, spanner.CommitTimestamp},
		),
	})
	return err
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func shortToken(t string) string {
	if len(t) > 12 {
		return t[:12] + "…"
	}
	return t
}

// parseJSONMap parses a Spanner JSON value string into a map.
// Spanner returns JSON columns as a JSON string in Change Stream records.
func parseJSONMap(s string) map[string]interface{} {
	if s == "" || s == "null" {
		return nil
	}
	var m map[string]interface{}
	// Use structpb to parse the JSON safely.
	sv := &structpb.Struct{}
	if err := sv.UnmarshalJSON([]byte(s)); err != nil {
		return nil
	}
	m = make(map[string]interface{}, len(sv.Fields))
	for k, v := range sv.Fields {
		m[k] = v.AsInterface()
	}
	return m
}

// extractKV builds a KV from new/old value maps.
func extractKV(newV, oldV map[string]interface{}, commitTS time.Time) *KV {
	src := newV
	if src == nil {
		src = oldV
	}
	if src == nil {
		return nil
	}

	kv := &KV{}
	kv.Key = strField(src, "key")
	if kv.Key == "" {
		return nil
	}
	kv.Rev = i64Field(src, "rev")
	kv.Value = bytesField(src, "value")
	kv.Deleted = boolField(src, "deleted")
	kv.Created = boolField(src, "created")
	kv.CreateRevision = i64Field(src, "create_revision")
	kv.PrevRevision = i64Field(src, "prev_revision")
	kv.LeaseID = i64Field(src, "lease_id")
	kv.OldValue = bytesField(oldV, "value")
	return kv
}

func strField(m map[string]interface{}, k string) string {
	if v, ok := m[k]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func i64Field(m map[string]interface{}, k string) int64 {
	if m == nil {
		return 0
	}
	if v, ok := m[k]; ok {
		switch t := v.(type) {
		case string:
			var n int64
			fmt.Sscan(t, &n)
			return n
		case float64:
			return int64(t)
		}
	}
	return 0
}

func bytesField(m map[string]interface{}, k string) []byte {
	if m == nil {
		return nil
	}
	if v, ok := m[k]; ok {
		if s, ok := v.(string); ok && s != "" {
			b, _ := base64.StdEncoding.DecodeString(s)
			return b
		}
	}
	return nil
}

func boolField(m map[string]interface{}, k string) bool {
	if m == nil {
		return false
	}
	if v, ok := m[k]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

// ensure sppb is used (for GenericColumnValue type info in future).
var _ = sppb.TypeCode_ARRAY
