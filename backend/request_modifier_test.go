package backend

import (
	"strings"
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/require"
)

func TestSkipUnsupportedRoutineDDL(t *testing.T) {
	cases := []struct {
		name  string
		input string
		skip  bool
	}{
		{
			name:  "versioned drop function",
			input: "/*!50003 DROP FUNCTION IF EXISTS `toDate` */",
			skip:  true,
		},
		{
			name:  "create function",
			input: "CREATE DEFINER=`root`@`%` FUNCTION `toDate`() RETURNS date DETERMINISTIC RETURN CURDATE()",
			skip:  true,
		},
		{
			name:  "drop procedure",
			input: "DROP PROCEDURE IF EXISTS `do_work`",
			skip:  true,
		},
		{
			name:  "create trigger",
			input: "CREATE TRIGGER foo AFTER UPDATE ON t FOR EACH ROW BEGIN SET NEW.i = 1; END",
			skip:  true,
		},
		{
			name:  "create event",
			input: "CREATE EVENT foo ON SCHEDULE EVERY 1 YEAR DO CREATE TABLE bar (i INT)",
			skip:  true,
		},
		{
			name:  "create table not skipped",
			input: "CREATE TABLE t (function INT, event INT)",
			skip:  false,
		},
		{
			name:  "create view not skipped",
			input: "CREATE VIEW v AS SELECT 1",
			skip:  false,
		},
		{
			name:  "drop table named function",
			input: "DROP TABLE IF EXISTS function",
			skip:  false,
		},
		{
			name:  "plain select",
			input: "SELECT function FROM t",
			skip:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := skipUnsupportedRoutineDDL(tc.input, nil)
			if tc.skip {
				require.Equal(t, skipUnsupportedDDL, got)
			} else {
				require.Equal(t, tc.input, got)
			}
		})
	}
}

func TestUnwrapCreateViewParens(t *testing.T) {
	in := `CREATE OR REPLACE SQL SECURITY INVOKER VIEW log_report_count AS (
    SELECT
        report_code,
        report_name,
        report_count
    FROM report_logs
    WHERE report_count > 0
);`
	got := unwrapCreateViewParens(in, nil)
	require.NotContains(t, got, "AS (\n")
	require.Contains(t, got, "VIEW log_report_count AS SELECT")
	require.Contains(t, got, "WHERE report_count > 0")
	require.True(t, strings.HasSuffix(strings.TrimSpace(got), ");") || strings.Contains(got, "0\n);") || strings.HasSuffix(strings.TrimSpace(got), ";"))
}

func TestUnwrapCreateViewParensKeepsPlainView(t *testing.T) {
	in := "CREATE VIEW v AS SELECT 1"
	require.Equal(t, in, unwrapCreateViewParens(in, nil))
}

func TestUnwrapCreateViewParensWithColumnList(t *testing.T) {
	in := "CREATE VIEW v (a, b) AS (SELECT 1, 2)"
	got := unwrapCreateViewParens(in, nil)
	require.Equal(t, "CREATE VIEW v (a, b) AS SELECT 1, 2", got)
}

func TestShouldIgnoreFailedViewDuringSnapshot(t *testing.T) {
	view := "CREATE OR REPLACE SQL SECURITY INVOKER VIEW log_report_count AS SELECT 1"
	require.True(t, isCreateViewStmt(view))
	require.True(t, shouldIgnoreFailedView(view, true))
	require.False(t, shouldIgnoreFailedView(view, false))
	require.False(t, shouldIgnoreFailedView("CREATE TABLE t (i INT)", true))
	require.False(t, shouldIgnoreFailedView("SELECT 1", true))
}

func TestApplyRequestModifiersIssue329(t *testing.T) {
	fn, _ := applyRequestModifiers("/*!50003 DROP FUNCTION IF EXISTS `toDate` */", defaultRequestModifiers)
	require.Equal(t, skipUnsupportedDDL, fn)

	view, _ := applyRequestModifiers(
		"CREATE OR REPLACE SQL SECURITY INVOKER VIEW log_report_count AS (\n    SELECT 1 WHERE 1 > 0\n)",
		defaultRequestModifiers,
	)
	require.Contains(t, view, "VIEW log_report_count AS SELECT 1 WHERE 1 > 0")
	require.NotContains(t, view, "AS (")
}

func TestExposeReplicaFilePosition(t *testing.T) {
	query, modifiers := applyRequestModifiers(" SHOW REPLICA STATUS; ", defaultRequestModifiers)
	require.Equal(t, " SHOW REPLICA STATUS; ", query)
	require.Len(t, modifiers, 1)

	result := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Source_Log_File", Type: querypb.Type_VARCHAR},
			{Name: "Read_Source_Log_Pos", Type: querypb.Type_VARCHAR},
			{Name: "Executed_Gtid_Set", Type: querypb.Type_VARCHAR},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("INVALID"),
			sqltypes.NewVarChar("0"),
			sqltypes.NewVarChar("mysql-bin.000042:987654"),
		}},
	}

	modified := modifiers[0](result)
	require.Equal(t, "mysql-bin.000042", modified.Rows[0][0].ToString())
	require.Equal(t, "987654", modified.Rows[0][1].ToString())
}

func TestExposeReplicaFilePositionLeavesGTIDStatus(t *testing.T) {
	_, modifiers := applyRequestModifiers("SHOW REPLICA STATUS", defaultRequestModifiers)
	require.Len(t, modifiers, 1)

	gtid := "24bc7850-2c16-11e6-a073-0242ac110002:1-42"
	result := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Source_Log_File", Type: querypb.Type_VARCHAR},
			{Name: "Read_Source_Log_Pos", Type: querypb.Type_VARCHAR},
			{Name: "Executed_Gtid_Set", Type: querypb.Type_VARCHAR},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("INVALID"),
			sqltypes.NewVarChar("0"),
			sqltypes.NewVarChar(gtid),
		}},
	}

	modified := modifiers[0](result)
	require.Equal(t, "INVALID", modified.Rows[0][0].ToString())
	require.Equal(t, "0", modified.Rows[0][1].ToString())
}

func TestIsWriteQueryText(t *testing.T) {
	cases := []struct {
		name  string
		query string
		write bool
	}{
		{name: "select", query: "SELECT 1", write: false},
		{name: "create table", query: "CREATE OR REPLACE TABLE t (id INT)", write: true},
		{name: "versioned drop routine", query: "/*!50003 DROP FUNCTION IF EXISTS f */", write: true},
		{name: "line comment", query: "-- dump\nINSERT INTO t VALUES (1)", write: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.write, IsWriteQueryText(tc.query))
		})
	}
}
