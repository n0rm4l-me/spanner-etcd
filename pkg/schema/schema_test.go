package schema

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsConcurrentSchemaChange(t *testing.T) {
	concurrent := status.Error(codes.FailedPrecondition,
		"Schema change operation rejected because a concurrent schema change operation or read-write transaction is already in progress.")
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"concurrent schema change", concurrent, true},
		{"the same, wrapped as Ensure reports it", fmt.Errorf("update DDL: %w", concurrent), true},
		{"aborted", status.Error(codes.Aborted, "transaction was aborted"), true},
		{"other failed precondition", status.Error(codes.FailedPrecondition, "database is not ready"), false},
		{"permission denied", status.Error(codes.PermissionDenied, "no"), false},
		{"not found", status.Error(codes.NotFound, "database not found"), false},
		{"plain error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		if got := isConcurrentSchemaChange(c.err); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
