package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"
)

type appConfig struct {
	listenAddr           string
	metricsAddr          string
	spannerDatabase      string
	spannerChannels      int
	spannerMinSessions   uint64
	spannerMaxSessions   uint64
	spannerNativeMetrics bool
	spannerReadLocation  string // e.g. "us-east1" for directed reads
	tlsCert              string
	tlsKey               string
	tlsCA                string
	authUsers            string        // "user1:pass1,user2:pass2", from env ETCD_AUTH_USERS
	authTokenTTL         time.Duration // 0 = DefaultTokenTTL (5m)
	peerURLs             []string
	logLevel             string
	autoCompactInterval  time.Duration // 0 = unset (use default 5m); any negative = disabled
	autoCompactAge       time.Duration // 0 = DefaultAutoCompactAge (5m)
}

// parseConfig reads the configuration from args (os.Args[1:]) and the
// environment, flags taking precedence over environment variables.
//
// Anything it does not understand is an error. The previous hand-written parser
// ignored unknown flags and unparsable values, so a typo such as --tls-sert=...
// started the server without TLS and a bad --auth-token-ttl silently kept the
// default. Usage and flag errors are written to out.
func parseConfig(args []string, getenv func(string) string, out io.Writer) (appConfig, error) {
	env := func(key, def string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return def
	}

	cfg := appConfig{
		spannerChannels:    4,
		spannerMinSessions: 10,
		spannerMaxSessions: 100,
		peerURLs:           splitCSV(env("PEER_URLS", "")),
	}

	fs := flag.NewFlagSet("spanner-etcd", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.Usage = func() {
		fmt.Fprintln(out, "Usage: spanner-etcd --spanner-database=projects/P/instances/I/databases/D [flags]")
		fmt.Fprintln(out)
		fs.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(out, "  --%s\n        %s\n", f.Name, f.Usage)
		})
	}

	fs.StringVar(&cfg.spannerDatabase, "spanner-database", env("SPANNER_DATABASE", ""),
		"Spanner database path, projects/P/instances/I/databases/D (required; env SPANNER_DATABASE)")
	fs.StringVar(&cfg.listenAddr, "listen-address", env("LISTEN_ADDR", "0.0.0.0:2379"),
		"gRPC listen address (env LISTEN_ADDR)")
	fs.StringVar(&cfg.metricsAddr, "metrics-addr", env("METRICS_ADDR", "0.0.0.0:2381"),
		"HTTP address for /metrics and /healthz (env METRICS_ADDR)")
	fs.StringVar(&cfg.tlsCert, "tls-cert", env("TLS_CERT", ""), "server TLS certificate file (env TLS_CERT)")
	fs.StringVar(&cfg.tlsKey, "tls-key", env("TLS_KEY", ""), "server TLS private key file (env TLS_KEY)")
	fs.StringVar(&cfg.tlsCA, "tls-ca", env("TLS_CA", ""), "CA file; enables mutual TLS (env TLS_CA)")
	fs.StringVar(&cfg.authUsers, "auth-users", env("ETCD_AUTH_USERS", ""),
		"users as user1:pass1,user2:pass2; empty disables auth (env ETCD_AUTH_USERS)")
	fs.StringVar(&cfg.logLevel, "log-level", env("LOG_LEVEL", "info"), "debug, info, warn or error (env LOG_LEVEL)")
	fs.BoolVar(&cfg.spannerNativeMetrics, "spanner-native-metrics", false,
		"enable Spanner client-side metrics (needs roles/monitoring.metricWriter)")

	fs.Func("spanner-read-location", "GCP region for directed reads, for example us-east1", func(v string) error {
		v = strings.TrimSpace(v)
		if v == "" {
			return errors.New("requires a non-empty GCP region (for example us-east1)")
		}
		cfg.spannerReadLocation = v
		return nil
	})
	fs.Func("peer-urls", "comma-separated peer URLs reported by MemberList (env PEER_URLS)", func(v string) error {
		cfg.peerURLs = splitCSV(v)
		return nil
	})
	fs.Func("auth-token-ttl", "auth token lifetime, for example 24h (default 5m)", func(v string) error {
		d, err := time.ParseDuration(v)
		if err != nil {
			return err
		}
		if d < 0 {
			return errors.New("must not be negative")
		}
		cfg.authTokenTTL = d
		return nil
	})
	fs.Func("auto-compact-interval", "background compaction interval, for example 10m; 0, off or disable turns it off (default 5m)",
		func(v string) error {
			if v == "0" || v == "off" || v == "disable" {
				cfg.autoCompactInterval = -1
				return nil
			}
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			if d == 0 {
				d = -1 // "0s" disables as well
			}
			cfg.autoCompactInterval = d
			return nil
		})
	fs.Func("auto-compact-age", "how much history compaction keeps for Watch replay, for example 1h (default 5m)",
		func(v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			if d < 0 {
				return errors.New("must not be negative")
			}
			cfg.autoCompactAge = d
			return nil
		})

	if err := fs.Parse(args); err != nil {
		return appConfig{}, err
	}
	if fs.NArg() > 0 {
		return appConfig{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if err := cfg.validate(); err != nil {
		return appConfig{}, err
	}
	return cfg, nil
}

// validate rejects combinations that would otherwise degrade silently.
func (c appConfig) validate() error {
	if c.spannerDatabase == "" {
		return errors.New("--spanner-database is required (for example projects/P/instances/I/databases/D)")
	}
	if (c.tlsCert == "") != (c.tlsKey == "") {
		return errors.New("--tls-cert and --tls-key must be set together; with only one the server would run without TLS")
	}
	if c.tlsCA != "" && c.tlsCert == "" {
		return errors.New("--tls-ca (mutual TLS) needs --tls-cert and --tls-key")
	}
	return nil
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}
