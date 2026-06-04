// Package store — Change Stream reader for low-latency Watch delivery.
//
// # How Spanner Change Streams work
//
// A Change Stream is a logical CDC log attached to one or more tables. Spanner
// shards it into partitions (one per Spanner split). Each partition is read via
// a streaming SQL query that blocks until new records arrive, delivering them
// with ~10–50 ms latency.
//
// Partitions are dynamic: as data volume grows Spanner splits partitions, and
// as it shrinks it merges them. The reader must track these lifecycle events:
//
//	Initial query → list of initial partition tokens
//	Per-partition streaming read → DataChangeRecord | HeartbeatRecord | ChildPartitionsRecord
//	ChildPartitionsRecord → stop current partition, start reading child partitions
//
// # Resumability
//
// Each partition is identified by a token and has an associated commit timestamp.
// We persist (replica_id, partition_token, resume_timestamp) in kv_cs_cursors so
// that after a restart the reader can resume from where it left off instead of
// re-delivering old events.
//
// # Fan-out to Watch subscribers
//
// The ChangeStreamReader converts DataChangeRecords into store.Event values and
// calls the provided dispatchFn. In watch.go, dispatchFn routes each event to
// all matching subscriber channels. Because every spanner-etcd replica runs its
// own reader against the same stream, scaling out does not increase Spanner load
// linearly — each replica independently streams the same partitions.
package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"

	"github.com/paas/spanner-etcd/pkg/metrics"
)

const (
	// heartbeatInterval is passed to the Change Stream query so that Spanner
	// sends a heartbeat record even when there are no data changes.
	// This lets us detect that a partition is alive and advance its resume cursor.
	heartbeatInterval = 2000 // milliseconds

	// cursorFlushInterval controls how often we persist resume cursors to
	// kv_cs_cursors. Flushing on every record would add write overhead; once
	// per flush interval is a good trade-off between durability and performance.
	cursorFlushInterval = 5 * time.Second

	// partitionWorkersBuf is the size of the channel used to hand off newly
	// discovered child partitions to the partition manager goroutine.
	partitionWorkersBuf = 64
)

// DispatchFunc is called for every DataChangeRecord decoded from the stream.
// Implementations must be non-blocking or handle their own buffering.
type DispatchFunc func(events []*Event)

// csChangeRecord mirrors the structure Spanner returns for each row in the
// Change Stream result set. We unmarshal the JSON column manually because the
// Spanner Go client returns Change Stream records as a JSON string column named
// "ChangeRecord".
type csChangeRecord struct {
	DataChangeRecords     []csDataChangeRecord     `json:"data_change_records"`
	HeartbeatRecords      []csHeartbeatRecord      `json:"heartbeat_records"`
	ChildPartitionsRecords []csChildPartitionsRecord `json:"child_partitions_records"`
}

type csDataChangeRecord struct {
	CommitTimestamp    time.Time      `json:"commit_timestamp"`
	RecordSequence     string         `json:"record_sequence"`
	TableName          string         `json:"table_name"`
	ModType            string         `json:"mod_type"` // INSERT | UPDATE | DELETE
	ColumnTypes        []csColumnType `json:"column_types"`
	Mods               []csMod        `json:"mods"`
	IsLastRecordInTxn  bool           `json:"is_last_record_in_transaction_in_partition"`
	NumberOfRecordsInTxn int64        `json:"number_of_records_in_transaction"`
	NumberOfPartitionsInTxn int64     `json:"number_of_partitions_in_transaction"`
	TransactionTag     string         `json:"transaction_tag"`
}

type csColumnType struct {
	Name            string `json:"name"`
	Type            csType `json:"type"`
	IsPrimaryKey    bool   `json:"is_primary_key"`
	OrdinalPosition int64  `json:"ordinal_position"`
}

type csType struct {
	Code string `json:"code"`
}

type csMod struct {
	Keys      map[string]interface{} `json:"keys"`
	NewValues map[string]interface{} `json:"new_values"`
	OldValues map[string]interface{} `json:"old_values"`
}

type csHeartbeatRecord struct {
	Timestamp time.Time `json:"timestamp"`
}

type csChildPartitionsRecord struct {
	StartTimestamp  time.Time        `json:"start_timestamp"`
	RecordSequence  string           `json:"record_sequence"`
	ChildPartitions []csChildPartition `json:"child_partitions"`
}

type csChildPartition struct {
	Token              string   `json:"token"`
	ParentPartitionTokens []string `json:"parent_partition_tokens"`
}

// partitionState tracks a single Change Stream partition.
type partitionState struct {
	token           string
	startTimestamp  time.Time
	resumeTimestamp time.Time // last confirmed read position
}

// ChangeStreamReader reads all partitions of the kv_changes Change Stream
// and delivers events to the provided DispatchFunc. It is safe for concurrent
// use and handles partition splits transparently.
type ChangeStreamReader struct {
	client     *spanner.Client
	replicaID  string
	dispatch   DispatchFunc
	log        *zap.Logger

	mu         sync.Mutex
	active     map[string]*partitionState // token → state
	pending    chan *partitionState       // newly discovered partitions
	stopCh     chan struct{}
	stopped    chan struct{}
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

// Start begins reading the Change Stream. It loads persisted cursors, queries
// for initial partitions, and launches a goroutine per partition. Blocks until
// ctx is cancelled.
func (r *ChangeStreamReader) Start(ctx context.Context) error {
	defer close(r.stopped)

	// Load persisted cursors so we can resume partitions.
	cursors, err := r.loadCursors(ctx)
	if err != nil {
		r.log.Warn("failed to load CS cursors, starting from now", zap.Error(err))
		cursors = map[string]time.Time{}
	}

	// Query the Change Stream for its initial partition list.
	startTime := time.Now().Add(-time.Second) // slight overlap to avoid missing events at startup
	if len(cursors) > 0 {
		// If we have cursors pick the earliest resume point so no partition is missed.
		for _, t := range cursors {
			if t.Before(startTime) {
				startTime = t
			}
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

	// Seed pending channel with initial partitions.
	for _, p := range initialPartitions {
		if resume, ok := cursors[p.token]; ok {
			p.resumeTimestamp = resume
		} else {
			p.resumeTimestamp = startTime
		}
		r.pending <- p
	}

	// Partition manager: spawns a goroutine per partition.
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
				continue // already reading this partition
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

// readPartition streams one partition until it receives a ChildPartitionsRecord
// (meaning this partition has been split/merged and we should read its children)
// or ctx is cancelled.
func (r *ChangeStreamReader) readPartition(ctx context.Context, p *partitionState) {
	log := r.log.With(zap.String("partition", p.token[:min(8, len(p.token))]+"…"))
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

		err := r.streamPartition(ctx, p, &lastFlush)
		if err == nil {
			// Normal termination: ChildPartitionsRecord received, partition is done.
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		// Transient error — backoff and retry.
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

// streamPartition executes the streaming SQL query for one partition and
// processes records until the partition ends or ctx is cancelled.
// Returns nil when the partition is exhausted (ChildPartitionsRecord received).
func (r *ChangeStreamReader) streamPartition(ctx context.Context, p *partitionState, lastFlush *time.Time) error {
	stmt := spanner.Statement{
		SQL: `SELECT ChangeRecord FROM READ_kv_changes(
			start_timestamp  => @start,
			end_timestamp    => NULL,
			partition_token  => @token,
			heartbeat_millis => @hb
		)`,
		Params: map[string]interface{}{
			"start": p.resumeTimestamp,
			"token": p.token,
			"hb":    heartbeatInterval,
		},
	}

	iter := r.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("next record: %w", err)
		}

		// The Change Stream result has a single JSON column named "ChangeRecord".
		var jsonStr string
		if err := row.Column(0, &jsonStr); err != nil {
			return fmt.Errorf("decode column: %w", err)
		}

		var cr csChangeRecord
		if err := json.Unmarshal([]byte(jsonStr), &cr); err != nil {
			return fmt.Errorf("unmarshal change record: %w", err)
		}

		// Process DataChangeRecords — these are actual kv mutations.
		for i := range cr.DataChangeRecords {
			events := r.convertDataRecord(&cr.DataChangeRecords[i])
			if len(events) > 0 {
				r.dispatch(events)
				// Advance cursor to the commit timestamp of the last record.
				if ts := cr.DataChangeRecords[i].CommitTimestamp; ts.After(p.resumeTimestamp) {
					p.resumeTimestamp = ts
				}
			}
		}

		// Process HeartbeatRecords — advance cursor even when no data changed.
		for i := range cr.HeartbeatRecords {
			if ts := cr.HeartbeatRecords[i].Timestamp; ts.After(p.resumeTimestamp) {
				p.resumeTimestamp = ts
			}
		}

		// Flush cursors periodically.
		if time.Since(*lastFlush) > cursorFlushInterval {
			if err := r.flushCursor(ctx, p); err != nil {
				r.log.Warn("cursor flush failed", zap.Error(err))
			}
			*lastFlush = time.Now()
		}

		// Process ChildPartitionsRecords — partition is splitting/merging.
		// Queue child partitions and return so this goroutine exits cleanly.
		for _, cpRecord := range cr.ChildPartitionsRecords {
			for _, child := range cpRecord.ChildPartitions {
				childState := &partitionState{
					token:           child.Token,
					startTimestamp:  cpRecord.StartTimestamp,
					resumeTimestamp: cpRecord.StartTimestamp,
				}
				r.log.Debug("child partition discovered",
					zap.String("token", child.Token[:min(8, len(child.Token))]+"…"),
				)
				select {
				case r.pending <- childState:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			// Final flush before exiting this partition.
			_ = r.flushCursor(ctx, p)
			return nil // partition is done
		}
	}
}

// convertDataRecord maps a Spanner DataChangeRecord for the kv table to
// one or more store.Event values. Returns nil if the record is for an
// internal/fill row that Watch subscribers don't care about.
func (r *ChangeStreamReader) convertDataRecord(dr *csDataChangeRecord) []*Event {
	if dr.TableName != "kv" {
		return nil
	}

	var events []*Event
	for _, mod := range dr.Mods {
		kv := r.modToKV(mod.NewValues, mod.OldValues, dr.CommitTimestamp)
		if kv == nil {
			continue
		}
		evType := EventPut
		if kv.Deleted {
			evType = EventDelete
		}
		events = append(events, &Event{KV: kv, Type: evType})
	}
	return events
}

// modToKV extracts a KV from the new/old value maps in a Change Stream mod.
// The column names match the kv table schema.
func (r *ChangeStreamReader) modToKV(newVals, oldVals map[string]interface{}, ts time.Time) *KV {
	kv := &KV{}

	// Helper to read a string value from the map.
	str := func(m map[string]interface{}, col string) string {
		if v, ok := m[col]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
		return ""
	}
	// Helper to read an int64 from the map (Spanner JSON encodes INT64 as string).
	i64 := func(m map[string]interface{}, col string) int64 {
		if v, ok := m[col]; ok {
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
	// Helper to read bytes (base64-encoded in Spanner JSON).
	byt := func(m map[string]interface{}, col string) []byte {
		if v, ok := m[col]; ok {
			if s, ok := v.(string); ok && s != "" {
				// Spanner encodes BYTES as base64 in JSON change records.
				b, _ := base64.StdEncoding.DecodeString(s)
				return b
			}
		}
		return nil
	}
	bool_ := func(m map[string]interface{}, col string) bool {
		if v, ok := m[col]; ok {
			if b, ok := v.(bool); ok {
				return b
			}
		}
		return false
	}

	src := newVals
	if src == nil {
		src = oldVals
	}
	if src == nil {
		return nil
	}

	kv.Key = str(src, "key")
	if kv.Key == "" {
		return nil
	}
	kv.Rev = i64(src, "rev")
	kv.Value = byt(src, "value")
	kv.Deleted = bool_(src, "deleted")
	kv.Created = bool_(src, "created")
	kv.CreateRevision = i64(src, "create_revision")
	kv.PrevRevision = i64(src, "prev_revision")
	kv.LeaseID = i64(src, "lease_id")
	kv.OldValue = byt(oldVals, "value")

	return kv
}

// queryInitialPartitions calls the Change Stream API to get the starting
// partition list for the given timestamp.
func (r *ChangeStreamReader) queryInitialPartitions(ctx context.Context, startTime time.Time) ([]*partitionState, error) {
	stmt := spanner.Statement{
		SQL: `SELECT PartitionToken, StartTimestamp, EndTimestamp
		      FROM READ_kv_changes_PARTITION(
		        start_timestamp => @start,
		        end_timestamp   => NULL,
		        heartbeat_millis => 0
		      )`,
		Params: map[string]interface{}{"start": startTime},
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
			return nil, err
		}
		var token string
		var start time.Time
		if err := row.Columns(&token, &start, new(interface{})); err != nil {
			return nil, err
		}
		partitions = append(partitions, &partitionState{
			token:           token,
			startTimestamp:  start,
			resumeTimestamp: startTime,
		})
	}

	// If the Change Stream returns no partitions at all (e.g. on the emulator
	// which has limited CS support), fall back to a sentinel empty-token that
	// triggers the poll-based fallback in the Watcher.
	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions returned — Change Streams may not be supported by this Spanner backend")
	}
	return partitions, nil
}

// loadCursors reads persisted resume positions from kv_cs_cursors.
func (r *ChangeStreamReader) loadCursors(ctx context.Context) (map[string]time.Time, error) {
	stmt := spanner.Statement{
		SQL: `SELECT partition_token, resume_timestamp
		      FROM kv_cs_cursors
		      WHERE replica_id = @rid`,
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

// flushCursor persists the current resume position for a partition.
func (r *ChangeStreamReader) flushCursor(ctx context.Context, p *partitionState) error {
	_, err := r.client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("kv_cs_cursors",
			[]string{"replica_id", "partition_token", "resume_timestamp", "updated_at"},
			[]interface{}{r.replicaID, p.token, p.resumeTimestamp, spanner.CommitTimestamp},
		),
	})
	return err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
