package server_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// etcd ranges are exact: [key, range_end) in byte order, where a prefix is the
// range [prefix, prefix with its last byte incremented). Anything looser returns
// or, worse, deletes keys the client never asked for.

func keysOf(kvs []*mvccpb.KeyValue) []string {
	out := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, string(kv.Key))
	}
	return out
}

func putAll(t *testing.T, cli *clientv3.Client, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if _, err := cli.Put(context.Background(), k, "v"); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
}

func TestRange_PrefixExcludesSiblings(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()
	// "/svc/a/" must not match "/svc/ab/" (sibling), "/svc/a0" (sorts right
	// after the prefix range) or, with LIKE, anything matching a wildcard.
	putAll(t, cli, "/svc/a/x", "/svc/a/y", "/svc/ab/z", "/svc/a0", "/svc/b/q")

	resp, err := cli.Get(ctx, "/svc/a/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got, want := keysOf(resp.Kvs), []string{"/svc/a/x", "/svc/a/y"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("prefix /svc/a/: want %v, got %v", want, got)
	}

	cnt, err := cli.Get(ctx, "/svc/a/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil || cnt.Count != 2 {
		t.Fatalf("count-only: want 2, got %d (err=%v)", cnt.Count, err)
	}
}

// '_' and '%' are ordinary bytes in etcd keys, not wildcards.
func TestRange_WildcardCharactersAreLiteral(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()
	putAll(t, cli, "/u_/1", "/uX/2", "/p%/1", "/pq/2")

	for prefix, want := range map[string][]string{
		"/u_/": {"/u_/1"},
		"/p%/": {"/p%/1"},
	} {
		resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			t.Fatalf("get %s: %v", prefix, err)
		}
		if got := keysOf(resp.Kvs); !reflect.DeepEqual(got, want) {
			t.Errorf("prefix %q: want %v, got %v", prefix, want, got)
		}
	}
}

// A range that is not a prefix must stop at range_end (exclusive).
func TestRange_ExplicitRangeHonoursEnd(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()
	putAll(t, cli, "/rg/a", "/rg/b", "/rg/c", "/rg/d")

	resp, err := cli.Get(ctx, "/rg/b", clientv3.WithRange("/rg/d"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got, want := keysOf(resp.Kvs), []string{"/rg/b", "/rg/c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("[b,d): want %v, got %v", want, got)
	}

	resp, err = cli.Get(ctx, "/rg/b", clientv3.WithFromKey())
	if err != nil {
		t.Fatalf("from-key get: %v", err)
	}
	got := keysOf(resp.Kvs)
	if len(got) < 3 || got[0] != "/rg/b" || got[1] != "/rg/c" || got[2] != "/rg/d" {
		t.Fatalf("from /rg/b: want b,c,d first, got %v", got)
	}
}

// With a limit, More must say whether keys remain, so that clients (the
// Kubernetes API server among them) know to ask for the next page.
func TestRange_LimitSetsMoreAndPages(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()
	putAll(t, cli, "/pg/1", "/pg/2", "/pg/3", "/pg/4", "/pg/5")

	first, err := cli.Get(ctx, "/pg/", clientv3.WithPrefix(), clientv3.WithLimit(2))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(first.Kvs) != 2 || !first.More {
		t.Fatalf("page 1: want 2 keys and More=true, got %d keys More=%v", len(first.Kvs), first.More)
	}

	// Walk all pages the way a client does: continue after the last key.
	var all []string
	end := clientv3.GetPrefixRangeEnd("/pg/")
	next := "/pg/"
	for i := 0; i < 10; i++ {
		resp, err := cli.Get(ctx, next, clientv3.WithRange(end), clientv3.WithLimit(2))
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		all = append(all, keysOf(resp.Kvs)...)
		if !resp.More {
			break
		}
		next = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
	if want := []string{"/pg/1", "/pg/2", "/pg/3", "/pg/4", "/pg/5"}; !reflect.DeepEqual(all, want) {
		t.Fatalf("paged read: want %v, got %v", want, all)
	}

	// Exactly as many keys as the limit: nothing more.
	exact, err := cli.Get(ctx, "/pg/", clientv3.WithPrefix(), clientv3.WithLimit(5))
	if err != nil || len(exact.Kvs) != 5 || exact.More {
		t.Fatalf("limit == size: want 5 keys More=false, got %d More=%v (err=%v)", len(exact.Kvs), exact.More, err)
	}
}

// WithLastKey / WithFirstKey are how clients ask for the edge of a range.
func TestRange_SortAndLastKey(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()
	putAll(t, cli, "/so/b", "/so/a", "/so/c")

	last, err := cli.Get(ctx, "/so/", clientv3.WithLastKey()...)
	if err != nil || len(last.Kvs) != 1 || string(last.Kvs[0].Key) != "/so/c" {
		t.Fatalf("WithLastKey: want /so/c, got %v (err=%v)", keysOf(last.Kvs), err)
	}
	first, err := cli.Get(ctx, "/so/", clientv3.WithFirstKey()...)
	if err != nil || len(first.Kvs) != 1 || string(first.Kvs[0].Key) != "/so/a" {
		t.Fatalf("WithFirstKey: want /so/a, got %v (err=%v)", keysOf(first.Kvs), err)
	}

	desc, err := cli.Get(ctx, "/so/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got, want := keysOf(desc.Kvs), []string{"/so/c", "/so/b", "/so/a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("key descending: want %v, got %v", want, got)
	}

	// Most recently modified first.
	if _, err := cli.Put(ctx, "/so/a", "again"); err != nil {
		t.Fatalf("put: %v", err)
	}
	byMod, err := cli.Get(ctx, "/so/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend))
	if err != nil || len(byMod.Kvs) != 3 || string(byMod.Kvs[0].Key) != "/so/a" {
		t.Fatalf("mod revision descending: want /so/a first, got %v (err=%v)", keysOf(byMod.Kvs), err)
	}
}

// A prefix delete must not take siblings with it.
func TestDeleteRange_PrefixDoesNotDeleteSiblings(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()
	putAll(t, cli, "/del/a/1", "/del/a/2", "/del/ab/3", "/del/a0")

	d, err := cli.Delete(ctx, "/del/a/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if d.Deleted != 2 {
		t.Fatalf("want 2 deleted, got %d", d.Deleted)
	}
	resp, _ := cli.Get(ctx, "/del/", clientv3.WithPrefix())
	if got, want := keysOf(resp.Kvs), []string{"/del/a0", "/del/ab/3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("survivors: want %v, got %v", want, got)
	}
}

// A watch on a prefix must not deliver events for sibling prefixes.
func TestWatch_PrefixExcludesSiblings(t *testing.T) {
	cli := testServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wch := cli.Watch(ctx, "/wp/a/", clientv3.WithPrefix())
	time.Sleep(500 * time.Millisecond) // let the watch register

	putAll(t, cli, "/wp/ab/x", "/wp/a0", "/wp/a/y")

	var got []string
	deadline := time.After(6 * time.Second)
collect:
	for {
		select {
		case resp := <-wch:
			for _, ev := range resp.Events {
				got = append(got, string(ev.Kv.Key))
			}
			if len(got) >= 1 {
				// Give a wrongly matched sibling event time to show up too.
				time.Sleep(2 * time.Second)
				for {
					select {
					case resp := <-wch:
						for _, ev := range resp.Events {
							got = append(got, string(ev.Kv.Key))
						}
					default:
						break collect
					}
				}
			}
		case <-deadline:
			break collect
		}
	}
	if want := []string{"/wp/a/y"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("watch on /wp/a/: want only %v, got %v", want, got)
	}
}

// Txn range reads must be exact too.
func TestTxn_RangeReadIsExact(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()
	putAll(t, cli, "/tr/a/1", "/tr/ab/2")

	resp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision("/tr/guard"), "=", 0)).
		Then(clientv3.OpGet("/tr/a/", clientv3.WithPrefix())).
		Commit()
	if err != nil || !resp.Succeeded {
		t.Fatalf("txn: succeeded=%v err=%v", resp != nil && resp.Succeeded, err)
	}
	got := keysOf(resp.Responses[0].GetResponseRange().Kvs)
	if want := []string{"/tr/a/1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("txn range read: want %v, got %v", want, got)
	}
}
