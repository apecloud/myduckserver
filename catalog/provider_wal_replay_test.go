package catalog

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type walReplaySnapshot struct {
	currentCatalog   string
	memoryUserTables int
	columns          []string
	ordinaryRows     []string
	generatedRows    []string
	vectorRows       []string
	positions        []string
}

func TestPersistentCatalogWALReplay(t *testing.T) {
	dataDir := t.TempDir()
	provider, err := NewDBProvider("", dataDir, "myduck")
	require.NoError(t, err)
	seedWALReplayState(t, provider)

	before := readWALReplaySnapshot(t, provider.Storage())
	assertWALReplaySnapshot(t, before)
	t.Logf("pre-restart snapshot: %#v", before)
	require.NoError(t, provider.Close())
	assertWALExists(t, dataDir)

	reopened, err := NewDBProvider("", dataDir, "myduck")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})
	after := readWALReplaySnapshot(t, reopened.Storage())
	require.Equal(t, before, after)
	t.Logf("post-restart snapshot: %#v", after)
	assertPersistentCatalogOnConcurrentConnections(t, reopened, 8)
}

func TestPersistentCatalogWALReplayAcrossProcess(t *testing.T) {
	dataDir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPersistentCatalogWALReplayProcessHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "MYDUCK_WAL_REPLAY_HELPER_DIR="+dataDir)
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "WAL writer subprocess failed:\n%s", output)
	assertWALExists(t, dataDir)

	provider, err := NewDBProvider("", dataDir, "myduck")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, provider.Close())
	})
	snapshot := readWALReplaySnapshot(t, provider.Storage())
	assertWALReplaySnapshot(t, snapshot)
	t.Logf("cross-process replay snapshot: %#v", snapshot)
}

func TestPersistentCatalogWALReplayProcessHelper(t *testing.T) {
	dataDir := os.Getenv("MYDUCK_WAL_REPLAY_HELPER_DIR")
	if dataDir == "" {
		t.Skip("subprocess helper")
	}
	provider, err := NewDBProvider("", dataDir, "myduck")
	require.NoError(t, err)
	seedWALReplayState(t, provider)
	runtime.KeepAlive(provider)
	os.Exit(0)
}

func seedWALReplayState(t *testing.T, provider *DatabaseProvider) {
	t.Helper()
	_, err := provider.Storage().ExecContext(context.Background(), `
		CREATE SCHEMA wal_replay;
		CREATE TABLE wal_replay.ordinary_without_comment (id INTEGER, payload VARCHAR);
		CREATE TABLE wal_replay.ordinary_with_comment (id INTEGER, payload VARCHAR);
		CREATE TABLE wal_replay.generated_without_comment (
			source BIGINT,
			generated_value DOUBLE GENERATED ALWAYS AS (mysql_rand(source))
		);
		CREATE TABLE wal_replay.generated_with_comment (
			source BIGINT,
			generated_value DOUBLE GENERATED ALWAYS AS (mysql_rand(source))
		);
		CREATE TABLE wal_replay.vector_storage_with_comment (
			source VARCHAR,
			generated_value BLOB GENERATED ALWAYS AS (encode(source))
		);
		CHECKPOINT;
		PRAGMA disable_checkpoint_on_shutdown;
		COMMENT ON COLUMN wal_replay.ordinary_with_comment.payload IS 'ordinary metadata';
		COMMENT ON COLUMN wal_replay.generated_with_comment.source IS 'generated metadata';
		COMMENT ON COLUMN wal_replay.vector_storage_with_comment.generated_value IS 'vector metadata';
		INSERT INTO wal_replay.ordinary_without_comment VALUES (1, 'plain');
		INSERT INTO wal_replay.ordinary_with_comment VALUES (2, 'commented');
		INSERT INTO wal_replay.generated_without_comment (source) VALUES (7);
		INSERT INTO wal_replay.generated_with_comment (source) VALUES (11);
		INSERT INTO wal_replay.vector_storage_with_comment (source) VALUES ('[1.25,-2.5]');
		INSERT INTO __sys__.binlog_position (channel, position) VALUES
			('task53-file-pos', 'MySQL56/binlog.000123:456'),
			('task53-gtid', 'MySQL56/24bc7850-2c16-11e6-a073-0242ac110002:1-42')
	`)
	require.NoError(t, err)
}

func assertWALExists(t *testing.T, dataDir string) {
	t.Helper()
	walPath := filepath.Join(dataDir, "myduck.db.wal")
	walInfo, err := os.Stat(walPath)
	require.NoError(t, err, "shutdown with checkpoint disabled must preserve the WAL")
	require.Positive(t, walInfo.Size())
}

func assertWALReplaySnapshot(t *testing.T, snapshot walReplaySnapshot) {
	t.Helper()
	require.Equal(t, "myduck", snapshot.currentCatalog)
	require.Zero(t, snapshot.memoryUserTables)
	require.Equal(t, []string{
		"generated_with_comment|source|BIGINT|true||generated metadata",
		`generated_with_comment|generated_value|DOUBLE|true|CAST(mysql_rand("source") AS DOUBLE)|`,
		"generated_without_comment|source|BIGINT|true||",
		`generated_without_comment|generated_value|DOUBLE|true|CAST(mysql_rand("source") AS DOUBLE)|`,
		"ordinary_with_comment|id|INTEGER|true||",
		"ordinary_with_comment|payload|VARCHAR|true||ordinary metadata",
		"ordinary_without_comment|id|INTEGER|true||",
		"ordinary_without_comment|payload|VARCHAR|true||",
		"vector_storage_with_comment|source|VARCHAR|true||",
		`vector_storage_with_comment|generated_value|BLOB|true|CAST(encode("source") AS BLOB)|vector metadata`,
	}, snapshot.columns)
	require.Equal(t, []string{
		"ordinary_with_comment|2|commented",
		"ordinary_without_comment|1|plain",
	}, snapshot.ordinaryRows)
	require.Equal(t, []string{
		"generated_with_comment|11|0.09147746500107735",
		"generated_without_comment|7|0.9188921592527635",
	}, snapshot.generatedRows)
	require.Equal(t, []string{"[1.25,-2.5]|5B312E32352C2D322E355D"}, snapshot.vectorRows)
	require.Equal(t, []string{
		"task53-file-pos|MySQL56/binlog.000123:456",
		"task53-gtid|MySQL56/24bc7850-2c16-11e6-a073-0242ac110002:1-42",
	}, snapshot.positions)
}

func TestPersistentCatalogProvidersAreIsolated(t *testing.T) {
	type isolatedProvider struct {
		provider *DatabaseProvider
		value    string
	}

	providers := make([]isolatedProvider, 0, 2)
	for _, value := range []string{"left", "right"} {
		provider, err := NewDBProvider("", t.TempDir(), "myduck")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, provider.Close())
		})
		_, err = provider.Storage().Exec(`CREATE TABLE public.provider_isolation (value VARCHAR); INSERT INTO public.provider_isolation VALUES (?)`, value)
		require.NoError(t, err)
		providers = append(providers, isolatedProvider{provider: provider, value: value})
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(providers))
	for _, item := range providers {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			var catalogName, value string
			var memoryTables int
			err := item.provider.Storage().QueryRow(`
				SELECT current_catalog,
				       (SELECT value FROM public.provider_isolation),
				       (SELECT count(*) FROM duckdb_tables() WHERE database_name = 'memory' AND NOT internal)
			`).Scan(&catalogName, &value, &memoryTables)
			if err != nil {
				errs <- err
				return
			}
			if catalogName != "myduck" || value != item.value || memoryTables != 0 {
				errs <- fmt.Errorf("catalog=%q value=%q memory tables=%d", catalogName, value, memoryTables)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func assertPersistentCatalogOnConcurrentConnections(t *testing.T, provider *DatabaseProvider, count int) {
	t.Helper()
	ctx := context.Background()
	conns := make([]*stdsql.Conn, 0, count)
	for range count {
		conn, err := provider.Storage().Conn(ctx)
		require.NoError(t, err)
		conns = append(conns, conn)
	}
	defer func() {
		for _, conn := range conns {
			require.NoError(t, conn.Close())
		}
	}()

	var wg sync.WaitGroup
	errs := make(chan error, count)
	for _, conn := range conns {
		conn := conn
		wg.Add(1)
		go func() {
			defer wg.Done()
			var currentCatalog, payload string
			if err := conn.QueryRowContext(ctx, `
				SELECT current_catalog,
				       (SELECT payload FROM wal_replay.ordinary_with_comment WHERE id = 2)
			`).Scan(&currentCatalog, &payload); err != nil {
				errs <- err
				return
			}
			if currentCatalog != "myduck" || payload != "commented" {
				errs <- fmt.Errorf("catalog=%q payload=%q", currentCatalog, payload)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func readWALReplaySnapshot(t *testing.T, db *stdsql.DB) walReplaySnapshot {
	t.Helper()
	ctx := context.Background()
	var snapshot walReplaySnapshot
	require.NoError(t, db.QueryRowContext(ctx, "SELECT current_catalog").Scan(&snapshot.currentCatalog))
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'memory' AND NOT internal").Scan(&snapshot.memoryUserTables))

	snapshot.columns = scanStrings(t, db, `
		SELECT concat_ws('|', table_name, column_name, data_type, is_nullable::VARCHAR,
		                      replace(replace(coalesce(column_default, ''), 'AS "DOUBLE"', 'AS DOUBLE'),
		                                      'AS "BLOB"', 'AS BLOB'),
		                      coalesce(comment, ''))
		FROM duckdb_columns()
		WHERE database_name = 'myduck' AND schema_name = 'wal_replay'
		ORDER BY table_name, column_index
	`)
	snapshot.ordinaryRows = scanStrings(t, db, `
		SELECT concat_ws('|', table_name, id::VARCHAR, payload)
		FROM (
			SELECT 'ordinary_without_comment' AS table_name, * FROM wal_replay.ordinary_without_comment
			UNION ALL
			SELECT 'ordinary_with_comment' AS table_name, * FROM wal_replay.ordinary_with_comment
		)
		ORDER BY table_name, id
	`)
	snapshot.generatedRows = scanStrings(t, db, `
		SELECT concat_ws('|', table_name, source::VARCHAR, generated_value::VARCHAR)
		FROM (
			SELECT 'generated_without_comment' AS table_name, * FROM wal_replay.generated_without_comment
			UNION ALL
			SELECT 'generated_with_comment' AS table_name, * FROM wal_replay.generated_with_comment
		)
		ORDER BY table_name, source
	`)
	snapshot.vectorRows = scanStrings(t, db, `
		SELECT concat_ws('|', source, hex(generated_value))
		FROM wal_replay.vector_storage_with_comment
		ORDER BY source
	`)
	snapshot.positions = scanStrings(t, db, `
		SELECT concat_ws('|', channel, position)
		FROM __sys__.binlog_position
		WHERE channel IN ('task53-file-pos', 'task53-gtid')
		ORDER BY channel
	`)
	return snapshot
}

func scanStrings(t *testing.T, db *stdsql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	require.NoError(t, err)
	defer rows.Close()

	var values []string
	for rows.Next() {
		var value string
		require.NoError(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.NoError(t, rows.Err())
	return values
}
