package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver
	_ "modernc.org/sqlite"             // registers the "sqlite" driver

	"github.com/MahyarNemati/status-page/internal/check"
)

type dialect struct {
	name     string
	driver   string
	autoID   string
	greatest string
	numbered bool
}

var (
	sqliteDialect   = dialect{name: "sqlite", driver: "sqlite", autoID: "INTEGER PRIMARY KEY", greatest: "MAX"}
	postgresDialect = dialect{name: "postgres", driver: "pgx", autoID: "BIGSERIAL PRIMARY KEY", greatest: "GREATEST", numbered: true}
)

// rebind converts ? placeholders to $n for Postgres.
func (d dialect) rebind(q string) string {
	if !d.numbered {
		return q
	}
	var b strings.Builder
	n := 0
	for _, r := range q {
		if r == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// SQL implements Store on database/sql.
type SQL struct {
	db *sql.DB
	d  dialect
}

var _ Store = (*SQL)(nil)

// Open connects to the backend named by driver ("sqlite" or "postgres") and
// applies migrations.
func Open(ctx context.Context, driver, dsn string) (*SQL, error) {
	var d dialect
	switch driver {
	case "sqlite":
		d = sqliteDialect
		dsn = sqliteDSN(dsn)
	case "postgres":
		d = postgresDialect
	default:
		return nil, fmt.Errorf("unsupported storage driver %q", driver)
	}
	db, err := sql.Open(d.driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", driver, err)
	}
	if d.name == "sqlite" {
		// SQLite allows one writer; a single connection avoids SQLITE_BUSY
		// and keeps :memory: databases coherent.
		db.SetMaxOpenConns(1)
	} else {
		db.SetMaxOpenConns(10)
		db.SetConnMaxIdleTime(5 * time.Minute)
	}
	s := &SQL{db: db, d: d}
	if err := s.Ping(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func sqliteDSN(dsn string) string {
	if strings.Contains(dsn, "_pragma=") {
		return dsn
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	pragmas := "_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	if !strings.Contains(dsn, ":memory:") && !strings.Contains(dsn, "mode=memory") {
		pragmas += "&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	}
	return dsn + sep + pragmas
}

var migrations = []string{
	`CREATE TABLE IF NOT EXISTS check_results (
		id {{autoid}},
		service_id TEXT NOT NULL,
		checked_at BIGINT NOT NULL,
		status SMALLINT NOT NULL,
		latency_us BIGINT NOT NULL,
		message TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE INDEX IF NOT EXISTS check_results_service_time ON check_results (service_id, checked_at)`,
	`CREATE INDEX IF NOT EXISTS check_results_time ON check_results (checked_at)`,
	`CREATE TABLE IF NOT EXISTS rollups (
		service_id TEXT NOT NULL,
		resolution TEXT NOT NULL,
		bucket BIGINT NOT NULL,
		total BIGINT NOT NULL,
		up BIGINT NOT NULL,
		degraded BIGINT NOT NULL,
		latency_sum_us BIGINT NOT NULL,
		latency_max_us BIGINT NOT NULL,
		PRIMARY KEY (service_id, resolution, bucket)
	)`,
	`CREATE TABLE IF NOT EXISTS incidents (
		id {{autoid}},
		service_id TEXT NOT NULL,
		title TEXT NOT NULL,
		severity TEXT NOT NULL,
		state TEXT NOT NULL,
		auto SMALLINT NOT NULL,
		started_at BIGINT NOT NULL,
		resolved_at BIGINT
	)`,
	`CREATE INDEX IF NOT EXISTS incidents_service_started ON incidents (service_id, started_at)`,
	`CREATE INDEX IF NOT EXISTS incidents_state ON incidents (state)`,
	`CREATE TABLE IF NOT EXISTS incident_notes (
		id {{autoid}},
		incident_id BIGINT NOT NULL REFERENCES incidents (id) ON DELETE CASCADE,
		author TEXT NOT NULL,
		body TEXT NOT NULL,
		created_at BIGINT NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS incident_notes_incident ON incident_notes (incident_id, created_at)`,
}

func (s *SQL) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	var version int
	err := s.db.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&version)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := s.db.ExecContext(ctx, `INSERT INTO schema_version (version) VALUES (0)`); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	case err != nil:
		return fmt.Errorf("migrate: read version: %w", err)
	}
	for i := version; i < len(migrations); i++ {
		stmt := strings.ReplaceAll(migrations[i], "{{autoid}}", s.d.autoID)
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
		if _, err := tx.ExecContext(ctx, s.d.rebind(`UPDATE schema_version SET version = ?`), i+1); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
	}
	return nil
}

func (s *SQL) exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, s.d.rebind(q), args...)
}

func (s *SQL) query(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, s.d.rebind(q), args...)
}

func (s *SQL) queryRow(ctx context.Context, q string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, s.d.rebind(q), args...)
}

// SaveResult stores the raw result and folds it into hourly and daily rollups
// in one transaction.
func (s *SQL) SaveResult(ctx context.Context, r Result) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("save result: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	latency := r.Latency.Microseconds()
	if _, err := tx.ExecContext(ctx, s.d.rebind(
		`INSERT INTO check_results (service_id, checked_at, status, latency_us, message) VALUES (?, ?, ?, ?, ?)`),
		r.ServiceID, r.At.UnixMilli(), int(r.Status), latency, truncate(r.Message, 512)); err != nil {
		return fmt.Errorf("save result: %w", err)
	}

	up, degraded := 0, 0
	switch r.Status {
	case check.StatusUp:
		up = 1
	case check.StatusDegraded:
		degraded = 1
	}
	upsert := s.d.rebind(fmt.Sprintf(`INSERT INTO rollups
		(service_id, resolution, bucket, total, up, degraded, latency_sum_us, latency_max_us)
		VALUES (?, ?, ?, 1, ?, ?, ?, ?)
		ON CONFLICT (service_id, resolution, bucket) DO UPDATE SET
			total = rollups.total + 1,
			up = rollups.up + excluded.up,
			degraded = rollups.degraded + excluded.degraded,
			latency_sum_us = rollups.latency_sum_us + excluded.latency_sum_us,
			latency_max_us = %s(rollups.latency_max_us, excluded.latency_max_us)`, s.d.greatest))
	for _, res := range []Resolution{Hourly, Daily} {
		if _, err := tx.ExecContext(ctx, upsert, r.ServiceID, string(res), res.Truncate(r.At).Unix(),
			up, degraded, latency, latency); err != nil {
			return fmt.Errorf("save rollup: %w", err)
		}
	}
	return tx.Commit()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func scanResult(sc interface{ Scan(...any) error }) (Result, error) {
	var (
		r       Result
		at, lat int64
		status  int
	)
	if err := sc.Scan(&r.ServiceID, &at, &status, &lat, &r.Message); err != nil {
		return Result{}, err
	}
	r.At = time.UnixMilli(at).UTC()
	r.Status = check.Status(status)
	r.Latency = time.Duration(lat) * time.Microsecond
	return r, nil
}

// LatestResults returns the most recent result for every service.
func (s *SQL) LatestResults(ctx context.Context) (map[string]Result, error) {
	rows, err := s.query(ctx, `SELECT r.service_id, r.checked_at, r.status, r.latency_us, r.message
		FROM check_results r
		JOIN (SELECT service_id, MAX(id) AS id FROM check_results GROUP BY service_id) latest
		ON latest.id = r.id`)
	if err != nil {
		return nil, fmt.Errorf("latest results: %w", err)
	}
	defer rows.Close()
	out := map[string]Result{}
	for rows.Next() {
		r, err := scanResult(rows)
		if err != nil {
			return nil, err
		}
		out[r.ServiceID] = r
	}
	return out, rows.Err()
}

// RecentResults returns up to limit results, newest first.
func (s *SQL) RecentResults(ctx context.Context, serviceID string, limit int) ([]Result, error) {
	rows, err := s.query(ctx, `SELECT service_id, checked_at, status, latency_us, message
		FROM check_results WHERE service_id = ? ORDER BY checked_at DESC, id DESC LIMIT ?`, serviceID, limit)
	if err != nil {
		return nil, fmt.Errorf("recent results: %w", err)
	}
	defer rows.Close()
	var out []Result
	for rows.Next() {
		r, err := scanResult(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RawCounts aggregates raw results in [from, to).
func (s *SQL) RawCounts(ctx context.Context, serviceID string, from, to time.Time) (Counts, error) {
	var c Counts
	err := s.queryRow(ctx, `SELECT COUNT(*),
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0)
		FROM check_results WHERE service_id = ? AND checked_at >= ? AND checked_at < ?`,
		int(check.StatusUp), int(check.StatusDegraded), serviceID, from.UnixMilli(), to.UnixMilli(),
	).Scan(&c.Total, &c.Up, &c.Degraded)
	if err != nil {
		return Counts{}, fmt.Errorf("raw counts: %w", err)
	}
	return c, nil
}

// Rollups returns buckets whose start lies in [from, to), oldest first.
func (s *SQL) Rollups(ctx context.Context, serviceID string, res Resolution, from, to time.Time) ([]Bucket, error) {
	rows, err := s.query(ctx, `SELECT bucket, total, up, degraded, latency_sum_us, latency_max_us
		FROM rollups WHERE service_id = ? AND resolution = ? AND bucket >= ? AND bucket < ?
		ORDER BY bucket`, serviceID, string(res), from.Unix(), to.Unix())
	if err != nil {
		return nil, fmt.Errorf("rollups: %w", err)
	}
	defer rows.Close()
	var out []Bucket
	for rows.Next() {
		var (
			b              Bucket
			start, sum, mx int64
		)
		if err := rows.Scan(&start, &b.Total, &b.Up, &b.Degraded, &sum, &mx); err != nil {
			return nil, err
		}
		b.Start = time.Unix(start, 0).UTC()
		if b.Total > 0 {
			b.AvgLatency = time.Duration(sum/b.Total) * time.Microsecond
		}
		b.MaxLatency = time.Duration(mx) * time.Microsecond
		out = append(out, b)
	}
	return out, rows.Err()
}

const incidentCols = `id, service_id, title, severity, state, auto, started_at, resolved_at`

func scanIncident(sc interface{ Scan(...any) error }) (Incident, error) {
	var (
		in       Incident
		state    string
		auto     int
		started  int64
		resolved sql.NullInt64
	)
	if err := sc.Scan(&in.ID, &in.ServiceID, &in.Title, &in.Severity, &state, &auto, &started, &resolved); err != nil {
		return Incident{}, err
	}
	in.State = IncidentState(state)
	in.Auto = auto == 1
	in.StartedAt = time.UnixMilli(started).UTC()
	if resolved.Valid {
		t := time.UnixMilli(resolved.Int64).UTC()
		in.ResolvedAt = &t
	}
	return in, nil
}

// CreateIncident inserts an open incident.
func (s *SQL) CreateIncident(ctx context.Context, in Incident) (Incident, error) {
	in.State = IncidentOpen
	in.ResolvedAt = nil
	auto := 0
	if in.Auto {
		auto = 1
	}
	err := s.queryRow(ctx, `INSERT INTO incidents (service_id, title, severity, state, auto, started_at)
		VALUES (?, ?, ?, ?, ?, ?) RETURNING id`,
		in.ServiceID, in.Title, in.Severity, string(in.State), auto, in.StartedAt.UnixMilli()).Scan(&in.ID)
	if err != nil {
		return Incident{}, fmt.Errorf("create incident: %w", err)
	}
	in.StartedAt = time.UnixMilli(in.StartedAt.UnixMilli()).UTC()
	return in, nil
}

// ResolveIncident marks an open incident resolved. Resolving an already
// resolved incident is a no-op that returns the stored record.
func (s *SQL) ResolveIncident(ctx context.Context, id int64, at time.Time) (Incident, error) {
	if _, err := s.exec(ctx, `UPDATE incidents SET state = ?, resolved_at = ? WHERE id = ? AND state = ?`,
		string(IncidentResolved), at.UnixMilli(), id, string(IncidentOpen)); err != nil {
		return Incident{}, fmt.Errorf("resolve incident: %w", err)
	}
	return s.GetIncident(ctx, id)
}

// GetIncident returns an incident with its notes.
func (s *SQL) GetIncident(ctx context.Context, id int64) (Incident, error) {
	in, err := scanIncident(s.queryRow(ctx, `SELECT `+incidentCols+` FROM incidents WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Incident{}, ErrNotFound
	}
	if err != nil {
		return Incident{}, fmt.Errorf("get incident: %w", err)
	}
	rows, err := s.query(ctx, `SELECT id, incident_id, author, body, created_at FROM incident_notes
		WHERE incident_id = ? ORDER BY created_at, id`, id)
	if err != nil {
		return Incident{}, fmt.Errorf("incident notes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			n  Note
			at int64
		)
		if err := rows.Scan(&n.ID, &n.IncidentID, &n.Author, &n.Body, &at); err != nil {
			return Incident{}, err
		}
		n.CreatedAt = time.UnixMilli(at).UTC()
		in.Notes = append(in.Notes, n)
	}
	return in, rows.Err()
}

// ListIncidents returns incidents newest first, without notes.
func (s *SQL) ListIncidents(ctx context.Context, f IncidentFilter) ([]Incident, error) {
	q := `SELECT ` + incidentCols + ` FROM incidents WHERE 1 = 1`
	var args []any
	if f.ServiceID != "" {
		q += ` AND service_id = ?`
		args = append(args, f.ServiceID)
	}
	if f.State != "" {
		q += ` AND state = ?`
		args = append(args, string(f.State))
	}
	if !f.Since.IsZero() {
		q += ` AND (started_at >= ? OR resolved_at IS NULL OR resolved_at >= ?)`
		args = append(args, f.Since.UnixMilli(), f.Since.UnixMilli())
	}
	q += ` ORDER BY started_at DESC, id DESC`
	if f.Limit > 0 {
		q += ` LIMIT ?`
		args = append(args, f.Limit)
	}
	rows, err := s.query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list incidents: %w", err)
	}
	defer rows.Close()
	var out []Incident
	for rows.Next() {
		in, err := scanIncident(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

// AddNote appends a note to an existing incident.
func (s *SQL) AddNote(ctx context.Context, n Note) (Note, error) {
	var exists int
	if err := s.queryRow(ctx, `SELECT COUNT(*) FROM incidents WHERE id = ?`, n.IncidentID).Scan(&exists); err != nil {
		return Note{}, fmt.Errorf("add note: %w", err)
	}
	if exists == 0 {
		return Note{}, ErrNotFound
	}
	err := s.queryRow(ctx, `INSERT INTO incident_notes (incident_id, author, body, created_at)
		VALUES (?, ?, ?, ?) RETURNING id`, n.IncidentID, n.Author, n.Body, n.CreatedAt.UnixMilli()).Scan(&n.ID)
	if err != nil {
		return Note{}, fmt.Errorf("add note: %w", err)
	}
	n.CreatedAt = time.UnixMilli(n.CreatedAt.UnixMilli()).UTC()
	return n, nil
}

// Prune deletes data older than each resolution's retention.
func (s *SQL) Prune(ctx context.Context, now time.Time, r Retention) (PruneStats, error) {
	var st PruneStats
	res, err := s.exec(ctx, `DELETE FROM check_results WHERE checked_at < ?`, now.Add(-r.Raw).UnixMilli())
	if err != nil {
		return st, fmt.Errorf("prune raw: %w", err)
	}
	st.Raw, _ = res.RowsAffected()
	for _, p := range []struct {
		res Resolution
		ret time.Duration
		n   *int64
	}{{Hourly, r.Hourly, &st.Hourly}, {Daily, r.Daily, &st.Daily}} {
		res, err := s.exec(ctx, `DELETE FROM rollups WHERE resolution = ? AND bucket < ?`,
			string(p.res), now.Add(-p.ret).Unix())
		if err != nil {
			return st, fmt.Errorf("prune %s: %w", p.res, err)
		}
		*p.n, _ = res.RowsAffected()
	}
	return st, nil
}

// Ping verifies connectivity.
func (s *SQL) Ping(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping %s: %w", s.d.name, err)
	}
	return nil
}

// Close releases the connection pool.
func (s *SQL) Close() error { return s.db.Close() }
