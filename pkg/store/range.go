package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// etcd ranges are exact: the keys k with Start <= k < End in byte order, where a
// prefix P is the range [P, PrefixEnd(P)). Keys are STRING columns, which Spanner
// compares by UTF-8 bytes, so the same order applies here.
//
// An earlier version approximated ranges with LIKE 'prefix%'. That returned (and,
// for DeleteRange, deleted) keys outside the range, such as "/a0" and "/ab/x" for
// the prefix "/a/", and treated '_' and '%' in keys as wildcards.

// SortTarget is the column a range read is ordered by.
type SortTarget int

const (
	SortByKey SortTarget = iota
	SortByCreate
	SortByMod
	SortByValue
)

// RangeQuery describes a range read.
type RangeQuery struct {
	Start string // inclusive
	End   string // exclusive; "" means no upper bound
	// A single key is the range [k, k+"\x00").

	Limit    int64 // 0 = no limit
	Revision int64 // 0 = latest
	SortBy   SortTarget
	Desc     bool
}

// RangeResult is the outcome of a range read.
type RangeResult struct {
	CurrentRev int64
	CompactRev int64
	Kvs        []*KV
	// More is true if the range holds keys beyond Limit.
	More bool
}

// PrefixEnd returns the smallest string greater than every string that has the
// given prefix, so that [prefix, PrefixEnd(prefix)) is exactly the keys with that
// prefix. It returns "" (no upper bound) for an empty prefix or one that no
// string can exceed.
//
// The last code point is incremented, not the last byte: for valid UTF-8 the two
// orders agree, and the result stays valid UTF-8, which a STRING parameter
// requires. (etcd's own helper increments the last byte, which can produce invalid
// UTF-8 for a prefix ending in 0x7F.)
func PrefixEnd(prefix string) string {
	r := []rune(prefix)
	for i := len(r) - 1; i >= 0; i-- {
		next := r[i] + 1
		if next == 0xD800 { // skip the surrogate block, which is not valid UTF-8
			next = 0xE000
		}
		if next <= utf8.MaxRune {
			return string(r[:i]) + string(next)
		}
		// r[i] is the largest code point: drop it and carry into the previous one.
	}
	return ""
}

// InRange reports whether key lies in [start, end); end == "" means unbounded.
func InRange(key, start, end string) bool {
	return key >= start && (end == "" || key < end)
}

// emptyRange reports whether [start, end) cannot contain any key.
func emptyRange(start, end string) bool {
	return end != "" && end <= start
}

// rangeCond returns a SQL condition selecting column col within [start, end) and
// adds its parameters. A single key is matched with equality.
//
// A bound ending in a NUL byte is rewritten so that no NUL reaches Spanner:
// "key >= s+NUL" is exactly "key > s" and "key < e+NUL" is exactly "key <= e",
// because no string sorts strictly between s and s+NUL. Paginating clients (the
// Kubernetes API server among them) continue after the last key k by asking for
// the range starting at k+NUL, so this is the common case, not an oddity.
func rangeCond(col, start, end string, params map[string]interface{}) string {
	if end == start+"\x00" {
		params["range_start"] = start
		return col + " = @range_start"
	}

	startOp, endOp := ">=", "<"
	if strings.HasSuffix(start, "\x00") {
		start, startOp = start[:len(start)-1], ">"
	}
	params["range_start"] = start
	cond := col + " " + startOp + " @range_start"
	if end != "" {
		if strings.HasSuffix(end, "\x00") {
			end, endOp = end[:len(end)-1], "<="
		}
		params["range_end"] = end
		cond += " AND " + col + " " + endOp + " @range_end"
	}
	return cond
}

func orderClause(by SortTarget, desc bool) string {
	dir := "ASC"
	if desc {
		dir = "DESC"
	}
	switch by {
	case SortByCreate:
		return fmt.Sprintf("kv.create_revision %s, kv.key ASC", dir)
	case SortByMod:
		return fmt.Sprintf("kv.rev %s, kv.key ASC", dir)
	case SortByValue:
		return fmt.Sprintf("kv.value %s, kv.key ASC", dir)
	default:
		return fmt.Sprintf("kv.key %s", dir)
	}
}

// Range returns the live keys in the range as of q.Revision, ordered and limited
// as requested, and whether more keys exist beyond the limit.
func (s *Store) Range(ctx context.Context, q RangeQuery) (*RangeResult, error) {
	currentRev, err := s.CurrentRevision(ctx)
	if err != nil {
		return nil, err
	}
	compactRev, err := s.compactRevision(ctx)
	if err != nil {
		return nil, err
	}
	res := &RangeResult{CurrentRev: currentRev, CompactRev: compactRev}
	if compactRev > 1 && q.Revision > 0 && q.Revision <= compactRev {
		return res, ErrCompacted
	}
	if emptyRange(q.Start, q.End) {
		return res, nil
	}

	params := map[string]interface{}{"cap": revToTS(revCap(q.Revision, currentRev))}
	cond := rangeCond("key", q.Start, q.End, params)

	// Ask for one row past the limit: its presence is what More means.
	sql := `SELECT kv.rev, kv.key, kv.value, kv.old_value, kv.lease_id,
	               kv.deleted, kv.created, kv.create_revision, kv.prev_revision
	        FROM kv
	        INNER JOIN (
	          SELECT key, MAX(rev) AS max_rev
	          FROM kv
	          WHERE ` + cond + ` AND rev <= @cap
	          GROUP BY key
	        ) AS latest ON kv.key = latest.key AND kv.rev = latest.max_rev
	        WHERE kv.deleted = false
	        ORDER BY ` + orderClause(q.SortBy, q.Desc)
	if q.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", q.Limit+1)
	}

	iter := s.client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("range [%q,%q): %w", q.Start, q.End, err)
		}
		kv, err := scanKV(row)
		if err != nil {
			return nil, err
		}
		res.Kvs = append(res.Kvs, kv)
	}
	if q.Limit > 0 && int64(len(res.Kvs)) > q.Limit {
		res.More = true
		res.Kvs = res.Kvs[:q.Limit]
	}
	return res, nil
}

// CountRange returns (currentRev, number of live keys in [start, end) as of revision).
func (s *Store) CountRange(ctx context.Context, start, end string, revision int64) (int64, int64, error) {
	currentRev, err := s.CurrentRevision(ctx)
	if err != nil {
		return 0, 0, err
	}
	if revision > 0 {
		compactRev, cerr := s.compactRevision(ctx)
		if cerr != nil {
			return 0, 0, fmt.Errorf("get compact revision: %w", cerr)
		}
		if compactRev > 1 && revision <= compactRev {
			return currentRev, 0, ErrCompacted
		}
	}
	if emptyRange(start, end) {
		return currentRev, 0, nil
	}

	params := map[string]interface{}{"cap": revToTS(revCap(revision, currentRev))}
	cond := rangeCond("key", start, end, params)
	stmt := spanner.Statement{
		SQL: `SELECT COUNT(*) FROM (
		        SELECT kv.key FROM kv
		        INNER JOIN (
		          SELECT key AS k2, MAX(rev) AS max_rev
		          FROM kv WHERE ` + cond + ` AND rev <= @cap
		          GROUP BY key
		        ) AS latest ON kv.key = latest.k2 AND kv.rev = latest.max_rev
		        WHERE kv.deleted = false
		      )`,
		Params: params,
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		return 0, 0, fmt.Errorf("count [%q,%q): %w", start, end, err)
	}
	var count int64
	if err := row.Column(0, &count); err != nil {
		return 0, 0, err
	}
	return currentRev, count, nil
}

// AfterRange returns all events with rev > afterRev for keys in [start, end).
func (s *Store) AfterRange(ctx context.Context, start, end string, afterRev, limit int64) (int64, []*Event, error) {
	currentRev, err := s.CurrentRevision(ctx)
	if err != nil {
		return 0, nil, err
	}
	// afterRev=0 means "from beginning" (used by the poll loop, not for client
	// replay); only explicit client-supplied revisions are checked for compaction.
	if afterRev > 0 {
		compactRev, cerr := s.compactRevision(ctx)
		if cerr != nil {
			return 0, nil, fmt.Errorf("get compact revision: %w", cerr)
		}
		// afterRev is exclusive (returns rev > afterRev). etcd compaction is
		// inclusive: reads at compactRev itself are disallowed. So we error when
		// afterRev < compactRev (meaning the first possible event is at or below
		// the compact horizon).
		if afterRev < compactRev {
			return currentRev, nil, ErrCompacted
		}
	}
	if emptyRange(start, end) {
		return currentRev, nil, nil
	}

	params := map[string]interface{}{"after_rev": revToTS(afterRev)}
	cond := rangeCond("key", start, end, params)
	stmt := spanner.Statement{
		SQL: `SELECT rev, key, value, old_value, lease_id, deleted, created, create_revision, prev_revision
		      FROM kv
		      WHERE ` + cond + `
		        AND rev > @after_rev
		      ORDER BY rev ASC` + limitClause(limit),
		Params: params,
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var events []*Event
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return 0, nil, fmt.Errorf("after [%q,%q): %w", start, end, err)
		}
		kv, err := scanKV(row)
		if err != nil {
			return 0, nil, err
		}
		evType := EventPut
		if kv.Deleted {
			evType = EventDelete
		}
		events = append(events, &Event{KV: kv, Type: evType})
	}
	return currentRev, events, nil
}
