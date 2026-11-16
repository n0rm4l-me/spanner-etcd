package main

import (
	"bytes"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"
)

const db = "projects/p/instances/i/databases/d"

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func parse(t *testing.T, e map[string]string, args ...string) (appConfig, error) {
	t.Helper()
	return parseConfig(args, env(e), &bytes.Buffer{})
}

func mustParse(t *testing.T, e map[string]string, args ...string) appConfig {
	t.Helper()
	cfg, err := parse(t, e, args...)
	if err != nil {
		t.Fatalf("parseConfig(%q): %v", args, err)
	}
	return cfg
}

func TestConfig_DefaultsAndEnv(t *testing.T) {
	cfg := mustParse(t, nil, "--spanner-database="+db)
	if cfg.listenAddr != "0.0.0.0:2379" || cfg.metricsAddr != "0.0.0.0:2381" || cfg.logLevel != "info" {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	if cfg.autoCompactInterval != 0 || cfg.authTokenTTL != 0 {
		t.Errorf("durations must default to 0 (unset): %+v", cfg)
	}

	cfg = mustParse(t, map[string]string{
		"SPANNER_DATABASE": db, "LISTEN_ADDR": "1.2.3.4:5", "LOG_LEVEL": "debug",
		"ETCD_AUTH_USERS": "u:p", "PEER_URLS": "http://a:1,http://b:2",
	})
	if cfg.spannerDatabase != db || cfg.listenAddr != "1.2.3.4:5" || cfg.logLevel != "debug" ||
		cfg.authUsers != "u:p" || len(cfg.peerURLs) != 2 {
		t.Errorf("environment not applied: %+v", cfg)
	}

	// A flag beats the environment, and both "--f=v" and "--f v" work.
	cfg = mustParse(t, map[string]string{"LISTEN_ADDR": "1.2.3.4:5"}, "--spanner-database", db, "--listen-address=9.9.9.9:9")
	if cfg.spannerDatabase != db || cfg.listenAddr != "9.9.9.9:9" {
		t.Errorf("flags must override the environment: %+v", cfg)
	}
}

// A typo used to be ignored silently, for instance leaving the server running
// without TLS. Every unknown flag must now stop startup.
func TestConfig_UnknownFlagIsAnError(t *testing.T) {
	for _, args := range [][]string{
		{"--spanner-database=" + db, "--tls-sert=/x"},
		{"--spanner-database=" + db, "--no-such-flag"},
		{"--spanner-database=" + db, "-tls-cert-file=/x"},
	} {
		if _, err := parse(t, nil, args...); err == nil {
			t.Errorf("%q: want an error for the unknown flag", args)
		}
	}
	_, err := parse(t, nil, "--spanner-database="+db, "--tls-sert=/x")
	if err == nil || !strings.Contains(err.Error(), "tls-sert") {
		t.Errorf("the error must name the flag, got %v", err)
	}
	if _, err := parse(t, nil, "--spanner-database="+db, "stray"); err == nil {
		t.Error("a positional argument must be rejected")
	}
}

func TestConfig_DatabaseIsRequired(t *testing.T) {
	if _, err := parse(t, nil); err == nil || !strings.Contains(err.Error(), "spanner-database") {
		t.Errorf("want an error naming --spanner-database, got %v", err)
	}
}

func TestConfig_Durations(t *testing.T) {
	for _, off := range []string{"off", "disable", "0", "0s"} {
		cfg := mustParse(t, nil, "--spanner-database="+db, "--auto-compact-interval="+off)
		if cfg.autoCompactInterval != -1 {
			t.Errorf("--auto-compact-interval=%s: want -1 (disabled), got %v", off, cfg.autoCompactInterval)
		}
	}
	cfg := mustParse(t, nil, "--spanner-database="+db, "--auto-compact-interval=10m", "--auto-compact-age=1h", "--auth-token-ttl=24h")
	if cfg.autoCompactInterval != 10*time.Minute || cfg.autoCompactAge != time.Hour || cfg.authTokenTTL != 24*time.Hour {
		t.Errorf("durations not applied: %+v", cfg)
	}

	// An invalid value used to be swallowed (the token TTL silently stayed at its default).
	for _, bad := range []string{"--auto-compact-interval=abc", "--auto-compact-age=abc", "--auto-compact-age=-1m", "--auth-token-ttl=abc", "--auth-token-ttl=-5m"} {
		if _, err := parse(t, nil, "--spanner-database="+db, bad); err == nil {
			t.Errorf("%s: want an error", bad)
		}
	}
}

func TestConfig_TLSFlagsMustBeConsistent(t *testing.T) {
	if _, err := parse(t, nil, "--spanner-database="+db, "--tls-cert=/c"); err == nil {
		t.Error("--tls-cert without --tls-key must fail: it would run without TLS")
	}
	if _, err := parse(t, nil, "--spanner-database="+db, "--tls-key=/k"); err == nil {
		t.Error("--tls-key without --tls-cert must fail")
	}
	if _, err := parse(t, nil, "--spanner-database="+db, "--tls-ca=/ca"); err == nil {
		t.Error("--tls-ca (mTLS) without a server certificate must fail")
	}
	cfg := mustParse(t, nil, "--spanner-database="+db, "--tls-cert=/c", "--tls-key=/k", "--tls-ca=/ca")
	if cfg.tlsCert != "/c" || cfg.tlsKey != "/k" || cfg.tlsCA != "/ca" {
		t.Errorf("TLS paths not applied: %+v", cfg)
	}
	// The same through the environment, which the Helm chart does not use but docs describe.
	if _, err := parse(t, map[string]string{"TLS_CERT": "/c"}, "--spanner-database="+db); err == nil {
		t.Error("TLS_CERT without TLS_KEY must fail too")
	}
}

func TestConfig_ReadLocationAndMetrics(t *testing.T) {
	cfg := mustParse(t, nil, "--spanner-database="+db, "--spanner-read-location= us-east1 ", "--spanner-native-metrics", "--peer-urls=http://a:1,http://b:2")
	if cfg.spannerReadLocation != "us-east1" || !cfg.spannerNativeMetrics || len(cfg.peerURLs) != 2 {
		t.Errorf("flags not applied: %+v", cfg)
	}
	if _, err := parse(t, nil, "--spanner-database="+db, "--spanner-read-location="); err == nil {
		t.Error("an empty --spanner-read-location must fail")
	}
}

func TestConfig_HelpListsFlags(t *testing.T) {
	var out bytes.Buffer
	_, err := parseConfig([]string{"--help"}, env(nil), &out)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("--help: want flag.ErrHelp, got %v", err)
	}
	for _, name := range []string{"--spanner-database", "--tls-cert", "--auto-compact-interval", "--spanner-read-location"} {
		if !strings.Contains(out.String(), name) {
			t.Errorf("usage should mention %s:\n%s", name, out.String())
		}
	}
}
