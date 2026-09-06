// 设计依据：docs/数据模型设计.md、docs/多租户与节点部署设计.md §3「租户隔离」

// Package store is the MySQL access layer for control-plane configuration and
// session data.
//
// Two rules hold throughout:
//
//   - Every query that touches tenant-scoped data filters on tenant_id, and
//     the filter comes first so it matches the leading index column.
//   - Every method takes a context.Context and passes it to the driver, so a
//     cancelled request stops waiting on the database instead of holding a
//     connection until it times out.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// Sentinel errors callers match with errors.Is.
var (
	// ErrNotFound means no row matched. Callers must not treat this as an
	// empty result and continue: an unknown channel binding or an unknown
	// tenant has to stop the request.
	ErrNotFound = errors.New("store: not found")

	// ErrDuplicate means a unique key rejected the write. For inbound_events
	// this is the expected outcome of a redelivered message, not a failure.
	ErrDuplicate = errors.New("store: duplicate key")
)

// mysqlDuplicateEntry is ER_DUP_ENTRY, returned when a unique key rejects an
// insert. Detecting it by code rather than by string keeps the check working
// across server locales.
const mysqlDuplicateEntry = 1062

// Store holds the database handle.
type Store struct {
	db *sql.DB

	// policies caches per-tenant audit policies, which are read on every
	// audit write. Nil when built by NewWithDB, in which case reads go
	// straight to the database.
	policies *auditPolicyCache
}

// Open connects to MySQL and verifies the connection.
func Open(ctx context.Context, cfg config.MySQLConfig) (*Store, error) {
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return &Store{
		db:       db,
		policies: &auditPolicyCache{entries: make(map[string]auditPolicyEntry)},
	}, nil
}

// NewWithDB wraps an existing handle, for tests.
func NewWithDB(db *sql.DB) *Store { return &Store{db: db} }

// DB exposes the handle for callers that need their own statements.
func (s *Store) DB() *sql.DB { return s.db }

// Ping checks liveness, for health endpoints.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Close releases the pool.
func (s *Store) Close() error { return s.db.Close() }

// isDuplicate reports whether err is a unique-key violation.
func isDuplicate(err error) bool {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number == mysqlDuplicateEntry
	}
	return false
}

// decodeJSON unmarshals a nullable JSON column into out. A NULL or empty
// column leaves out untouched rather than erroring, because every JSON column
// in the schema is optional.
func decodeJSON(raw []byte, out any) error {
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// encodeJSON marshals a value for a nullable JSON column, returning nil for
// empty input so the column stores NULL rather than the literal "null".
func encodeJSON(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch t := v.(type) {
	case map[string]any:
		if len(t) == 0 {
			return nil, nil
		}
	case []string:
		if len(t) == 0 {
			return nil, nil
		}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// nullString converts an empty string to NULL, keeping optional columns clean.
func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullTime converts a zero time to NULL.
func nullTime(t *time.Time) any {
	if t == nil || t.IsZero() {
		return nil
	}
	return *t
}
