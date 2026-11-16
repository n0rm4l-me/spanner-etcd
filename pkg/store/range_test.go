package store_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// PrefixEnd does not need Spanner, so these always run.
func TestPrefixEnd(t *testing.T) {
	for _, tc := range []struct{ prefix, want string }{
		{"a/", "a0"},
		{"/svc/a/", "/svc/a0"},
		{"foo", "fop"},
		{"", ""},               // no upper bound
		{"é", "ê"},             // e-acute -> e-circumflex, still valid UTF-8
		{"a\u007f", "a\u0080"}, // a byte-wise increment would give invalid UTF-8
		{"a\U0010FFFF", "b"},   // largest code point: carry into the previous one
		{"\U0010FFFF", ""},     // nothing is greater: unbounded
		{"퟿", ""},             // the surrogate block is not valid UTF-8: skip it
	} {
		if got := store.PrefixEnd(tc.prefix); got != tc.want {
			t.Errorf("PrefixEnd(%q) = %q, want %q", tc.prefix, got, tc.want)
		}
	}
}

// For any prefix, the range [prefix, PrefixEnd(prefix)) holds exactly the keys
// that start with it.
func TestPrefixEnd_RangeIsExactlyThePrefix(t *testing.T) {
	prefixes := []string{"/a/", "foo", "/u_/", "/p%/", "é", "\U0001F600/"}
	suffixes := []string{"", "x", "/deep/er", "ÿ", "\U0010FFFF", "\x00", "\u007f"}
	for _, p := range prefixes {
		end := store.PrefixEnd(p)
		for _, s := range suffixes {
			if !store.InRange(p+s, p, end) {
				t.Errorf("%q should be inside the range of prefix %q (end %q)", p+s, p, end)
			}
		}
		// Neighbours that merely look similar must be outside.
		for _, k := range []string{end, p[:len(p)-1], "~~~" + p} {
			if k != "" && store.InRange(k, p, end) && !hasPrefix(k, p) {
				t.Errorf("%q must be outside the range of prefix %q", k, p)
			}
		}
	}
	if !store.InRange("anything", "a", "") || store.InRange("A", "a", "") {
		t.Error("an empty end must mean unbounded, and the start still applies")
	}
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

func TestRange_ExactAndNonASCII(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	keys := []string{"/é/1", "/é/2", "/ê/3", "/\U0001F600/4", "/\U0001F601/5", "/e/6"}
	for _, k := range keys {
		if _, _, err := s.Put(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("put %q: %v", k, err)
		}
	}

	res, err := s.Range(ctx, store.RangeQuery{Start: "/é/", End: store.PrefixEnd("/é/")})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if got := keysOf(res.Kvs); !reflect.DeepEqual(got, []string{"/é/1", "/é/2"}) {
		t.Fatalf("prefix /é/: got %q", got)
	}

	res, err = s.Range(ctx, store.RangeQuery{Start: "/\U0001F600/", End: store.PrefixEnd("/\U0001F600/")})
	if err != nil || !reflect.DeepEqual(keysOf(res.Kvs), []string{"/\U0001F600/4"}) {
		t.Fatalf("emoji prefix: got %q (err=%v)", keysOf(res.Kvs), err)
	}

	// Empty and inverted ranges hold nothing and must not error.
	for _, q := range []store.RangeQuery{{Start: "/e/", End: "/e/"}, {Start: "/z", End: "/a"}} {
		res, err := s.Range(ctx, q)
		if err != nil || len(res.Kvs) != 0 || res.More {
			t.Errorf("range [%q,%q): want empty, got %d keys More=%v err=%v", q.Start, q.End, len(res.Kvs), res.More, err)
		}
	}
}

func TestRange_AtRevisionSeesDeletedKeys(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _, _ := s.Put(ctx, "/hr/a", []byte("1"), 0)
	rev2, _, _ := s.Put(ctx, "/hr/b", []byte("2"), 0)
	if _, _, ok, err := s.Delete(ctx, "/hr/a", 0); err != nil || !ok {
		t.Fatalf("delete: ok=%v err=%v", ok, err)
	}

	now, _ := s.Range(ctx, store.RangeQuery{Start: "/hr/", End: store.PrefixEnd("/hr/")})
	if got := keysOf(now.Kvs); !reflect.DeepEqual(got, []string{"/hr/b"}) {
		t.Fatalf("latest: got %q", got)
	}
	then, err := s.Range(ctx, store.RangeQuery{Start: "/hr/", End: store.PrefixEnd("/hr/"), Revision: rev2})
	if err != nil || !reflect.DeepEqual(keysOf(then.Kvs), []string{"/hr/a", "/hr/b"}) {
		t.Fatalf("at rev2: got %q (err=%v)", keysOf(then.Kvs), err)
	}
	only, _ := s.Range(ctx, store.RangeQuery{Start: "/hr/", End: store.PrefixEnd("/hr/"), Revision: rev1})
	if !reflect.DeepEqual(keysOf(only.Kvs), []string{"/hr/a"}) {
		t.Fatalf("at rev1: got %q", keysOf(only.Kvs))
	}
}

// WatchRange for a single key [k, k+"\x00") must see that key and nothing else.
func TestWatchRange_SingleKeyAndRanges(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	single := s.WatchRange(ctx, "/wr/k", "/wr/k\x00", 0)
	ranged := s.WatchRange(ctx, "/wr/a", "/wr/c", 0) // [a, c)

	for _, k := range []string{"/wr/k0", "/wr/k", "/wr/a", "/wr/b", "/wr/c", "/wr/j"} {
		if _, _, err := s.Put(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	drain := func(ch <-chan []*store.Event) []string {
		var got []string
		quiet := time.After(2500 * time.Millisecond)
		for {
			select {
			case evs := <-ch:
				for _, ev := range evs {
					got = append(got, ev.KV.Key)
				}
			case <-quiet:
				return got
			}
		}
	}
	if got := drain(single); !reflect.DeepEqual(got, []string{"/wr/k"}) {
		t.Errorf("single-key watch: got %q, want only /wr/k", got)
	}
	if got := drain(ranged); !reflect.DeepEqual(got, []string{"/wr/a", "/wr/b"}) {
		t.Errorf("[/wr/a, /wr/c) watch: got %q, want /wr/a and /wr/b", got)
	}
}

func keysOf(kvs []*store.KV) []string {
	out := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, kv.Key)
	}
	return out
}

// Continuing a paginated read asks for the range starting at lastKey+NUL. That is
// "keys after lastKey", and it has to work without the NUL being stored or
// compared as a character.
func TestRange_StartWithTrailingNulMeansAfterThatKey(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, k := range []string{"/n/a", "/n/a0", "/n/b", "/n/c"} {
		if _, _, err := s.Put(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	end := store.PrefixEnd("/n/")

	res, err := s.Range(ctx, store.RangeQuery{Start: "/n/a\x00", End: end})
	if err != nil || !reflect.DeepEqual(keysOf(res.Kvs), []string{"/n/a0", "/n/b", "/n/c"}) {
		t.Fatalf("after /n/a: got %q (err=%v)", keysOf(res.Kvs), err)
	}

	// An end of k+NUL includes k itself.
	res, err = s.Range(ctx, store.RangeQuery{Start: "/n/", End: "/n/b\x00"})
	if err != nil || !reflect.DeepEqual(keysOf(res.Kvs), []string{"/n/a", "/n/a0", "/n/b"}) {
		t.Fatalf("up to and including /n/b: got %q (err=%v)", keysOf(res.Kvs), err)
	}

	// The "all keys" idiom, key "\x00" with range_end "\x00".
	res, err = s.Range(ctx, store.RangeQuery{Start: "\x00", End: ""})
	if err != nil || len(res.Kvs) < 4 {
		t.Fatalf("from NUL: want every key, got %q (err=%v)", keysOf(res.Kvs), err)
	}
}
