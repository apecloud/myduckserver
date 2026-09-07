package pgserver

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestConnectionHandlerTransactionStatusTransitions(t *testing.T) {
	h := &ConnectionHandler{}

	// A zero-value handler is equivalent to a newly opened connection.
	require.Equal(t, ReadyForQueryTransactionIndicator_Idle, h.readyForQueryStatus())

	h.recordTransactionResult(&tree.BeginTransaction{}, nil)
	require.Equal(t, ReadyForQueryTransactionIndicator_TransactionBlock, h.readyForQueryStatus())

	// Successful work inside the block preserves T.
	h.recordTransactionResult(nil, nil)
	require.Equal(t, ReadyForQueryTransactionIndicator_TransactionBlock, h.readyForQueryStatus())

	// The first statement error moves an explicit block to E, and later
	// non-control results cannot clear it.
	h.recordTransactionResult(nil, errors.New("statement failed"))
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
	h.recordTransactionResult(nil, nil)
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())

	// Cleanup errors after a completed rollback must still advertise idle.
	h.recordTransactionResult(&tree.RollbackTransaction{}, errors.New("cleanup failed"))
	require.Equal(t, ReadyForQueryTransactionIndicator_Idle, h.readyForQueryStatus())

	h.recordTransactionResult(&tree.BeginTransaction{}, nil)
	require.Equal(t, ReadyForQueryTransactionIndicator_TransactionBlock, h.readyForQueryStatus())
	h.recordTransactionResult(&tree.CommitTransaction{}, errors.New("commit failed"))
	require.Equal(t, ReadyForQueryTransactionIndicator_Idle, h.readyForQueryStatus())

	// A nested BEGIN is a warning/no-op, so PostgreSQL keeps the original block
	// active rather than entering the failed E state.
	h.txStatus = ReadyForQueryTransactionIndicator_TransactionBlock
	h.recordTransactionResult(&tree.BeginTransaction{}, nil)
	require.Equal(t, ReadyForQueryTransactionIndicator_TransactionBlock, h.readyForQueryStatus())
}

func TestConnectionHandlerEndOfMessagesUsesTransactionStatus(t *testing.T) {
	var output bytes.Buffer
	h := &ConnectionHandler{
		backend:  pgproto3.NewBackend(bytes.NewReader(nil), &output),
		txStatus: ReadyForQueryTransactionIndicator_TransactionBlock,
	}

	h.endOfMessages(errors.New("statement failed"))

	frontend := pgproto3.NewFrontend(bytes.NewReader(output.Bytes()), io.Discard)
	message, err := frontend.Receive()
	require.NoError(t, err)
	pgErr, ok := message.(*pgproto3.ErrorResponse)
	require.True(t, ok)
	require.Equal(t, "XX000", pgErr.Code)

	message, err = frontend.Receive()
	require.NoError(t, err)
	ready, ok := message.(*pgproto3.ReadyForQuery)
	require.True(t, ok)
	require.Equal(t, byte(ReadyForQueryTransactionIndicator_FailedTransactionBlock), ready.TxStatus)
}

func TestConnectionHandlerRejectsStatementsInFailedTransaction(t *testing.T) {
	h := &ConnectionHandler{txStatus: ReadyForQueryTransactionIndicator_FailedTransactionBlock}

	for _, statement := range []ConvertedStatement{
		{AST: &tree.CommitTransaction{}, String: "COMMIT"},
		{AST: &tree.RollbackTransaction{}, String: "ROLLBACK"},
		{String: ""},
	} {
		require.NoError(t, h.rejectStatementIfTransactionFailed(statement))
	}

	for _, statement := range []ConvertedStatement{
		{AST: &tree.BeginTransaction{}, String: "BEGIN"},
		{AST: &tree.Select{}, String: "SELECT 1"},
		{String: "not valid transaction syntax"},
	} {
		err := h.rejectStatementIfTransactionFailed(statement)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "25P02", pgErr.Code)
		require.Equal(t, postgresFailedTransactionMessage, pgErr.Message)
	}
}

func TestConnectionHandlerFailedQueryGateModelsControlsInSimpleBatch(t *testing.T) {
	h := &ConnectionHandler{txStatus: ReadyForQueryTransactionIndicator_FailedTransactionBlock}

	// A recovery control permits later statements in the same simple-query
	// message to be evaluated from the newly idle state.
	require.NoError(t, h.rejectFailedQueryStatements([]ConvertedStatement{
		{AST: &tree.RollbackTransaction{}, String: "ROLLBACK"},
		{AST: &tree.Select{}, String: "SELECT 1"},
	}))

	// Without a recovery control, the first ordinary statement is rejected.
	var pgErr *pgconn.PgError
	err := h.rejectFailedQueryStatements([]ConvertedStatement{
		{AST: &tree.Select{}, String: "SELECT 1"},
		{AST: &tree.RollbackTransaction{}, String: "ROLLBACK"},
	})
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "25P02", pgErr.Code)
}

func TestConnectionHandlerFailedParseUsesTransactionStateBeforeSQLGates(t *testing.T) {
	h := &ConnectionHandler{txStatus: ReadyForQueryTransactionIndicator_FailedTransactionBlock}

	// Parse is a protocol message, but its SQL still must be rejected while the
	// transaction is failed. No engine/session is needed to exercise this gate.
	err := h.handleParse(&pgproto3.Parse{Query: "SELECT 1", Name: "failed"})
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "25P02", pgErr.Code)
}

func TestConnectionHandlerSemicolonShortcutPreservesFailedState(t *testing.T) {
	var output bytes.Buffer
	h := &ConnectionHandler{
		backend:  pgproto3.NewBackend(bytes.NewReader(nil), &output),
		txStatus: ReadyForQueryTransactionIndicator_FailedTransactionBlock,
	}

	end, err := h.handleQuery(&pgproto3.Query{String: ";"})
	require.NoError(t, err)
	require.True(t, end)
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
}

func TestConnectionHandlerCopyDataWithoutCopyStateReturnsError(t *testing.T) {
	h := &ConnectionHandler{}
	require.NotPanics(t, func() {
		_, _, err := h.handleCopyData(&pgproto3.CopyData{Data: []byte("x")})
		require.Error(t, err)
	})
}

func TestConnectionHandlerSerializesFailedTransactionSQLState(t *testing.T) {
	var output bytes.Buffer
	h := &ConnectionHandler{
		backend:  pgproto3.NewBackend(bytes.NewReader(nil), &output),
		txStatus: ReadyForQueryTransactionIndicator_FailedTransactionBlock,
	}

	h.endOfMessages(postgresFailedTransactionError())
	frontend := pgproto3.NewFrontend(bytes.NewReader(output.Bytes()), io.Discard)
	message, err := frontend.Receive()
	require.NoError(t, err)
	pgErr, ok := message.(*pgproto3.ErrorResponse)
	require.True(t, ok)
	require.Equal(t, "25P02", pgErr.Code)
	require.Equal(t, postgresFailedTransactionMessage, pgErr.Message)

	message, err = frontend.Receive()
	require.NoError(t, err)
	ready, ok := message.(*pgproto3.ReadyForQuery)
	require.True(t, ok)
	require.Equal(t, byte(ReadyForQueryTransactionIndicator_FailedTransactionBlock), ready.TxStatus)
}

func TestConnectionHandlerCommitInFailedTransactionReportsRollback(t *testing.T) {
	h := &ConnectionHandler{txStatus: ReadyForQueryTransactionIndicator_FailedTransactionBlock}
	statement := ConvertedStatement{AST: &tree.CommitTransaction{}, Tag: "COMMIT"}
	require.Equal(t, "ROLLBACK", h.protocolCommandTag(statement))
	h.txStatus = ReadyForQueryTransactionIndicator_TransactionBlock
	require.Equal(t, "COMMIT", h.protocolCommandTag(statement))
}

func TestConnectionHandlerDescribePreparedPortalWithoutRawStatement(t *testing.T) {
	var output bytes.Buffer
	h := &ConnectionHandler{
		backend: pgproto3.NewBackend(bytes.NewReader(nil), &output),
		portals: map[string]PortalData{
			"portal": {
				Prepared:  true,
				Statement: ConvertedStatement{Tag: "SELECT"},
				Fields:    []pgproto3.FieldDescription{{Name: []byte("value"), DataTypeOID: 25}},
			},
		},
	}

	require.NoError(t, h.handleDescribe(&pgproto3.Describe{ObjectType: 'P', Name: "portal"}))
	frontend := pgproto3.NewFrontend(bytes.NewReader(output.Bytes()), io.Discard)
	message, err := frontend.Receive()
	require.NoError(t, err)
	description, ok := message.(*pgproto3.RowDescription)
	require.True(t, ok)
	require.Len(t, description.Fields, 1)
	require.Equal(t, []byte("value"), description.Fields[0].Name)
}
