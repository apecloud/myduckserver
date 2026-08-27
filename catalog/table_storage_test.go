package catalog

import (
	"errors"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestNormalizeTableStorageOptionsDefaultsAndSelectors(t *testing.T) {
	tests := []struct {
		name    string
		options []TableStorageOption
		want    TableStorageSelection
	}{
		{
			name: "default",
			want: DefaultTableStorageSelection(),
		},
		{
			name: "postgres object",
			options: []TableStorageOption{{
				Name:  TableStorageOptionName,
				Value: "'object'",
			}},
			want: TableStorageSelection{Kind: TableStorageObject, Explicit: true, Source: "storage-option"},
		},
		{
			name: "postgres local",
			options: []TableStorageOption{{
				Name:  TableStorageOptionName,
				Value: "local",
			}},
			want: TableStorageSelection{Kind: TableStorageLocal, Explicit: true, Source: "storage-option"},
		},
		{
			name:    "mysql object",
			options: []TableStorageOption{{Name: "ENGINE", Value: "DUCKLAKE"}},
			want:    TableStorageSelection{Kind: TableStorageObject, Explicit: true, Source: "mysql-engine"},
		},
		{
			name:    "mysql explicit local",
			options: []TableStorageOption{{Name: "engine", Value: "LOCAL"}},
			want:    TableStorageSelection{Kind: TableStorageLocal, Explicit: true, Source: "mysql-engine"},
		},
		{
			name:    "ordinary mysql engine remains local",
			options: []TableStorageOption{{Name: "engine", Value: "InnoDB"}},
			want:    DefaultTableStorageSelection(),
		},
		{
			name:    "ordinary options remain local",
			options: []TableStorageOption{{Name: "comment", Value: "kept by the normal planner"}},
			want:    DefaultTableStorageSelection(),
		},
		{
			name:    "ordinary key option remains local",
			options: []TableStorageOption{{Name: "key_block_size", Value: "8"}},
			want:    DefaultTableStorageSelection(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeTableStorageOptions(tt.options)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestResolveTableStorageProtocolMaps(t *testing.T) {
	mysql, err := ResolveMySQLTableStorage(map[string]interface{}{"ENGINE": "DUCKLAKE"})
	require.NoError(t, err)
	require.Equal(t, TableStorageSelection{Kind: TableStorageObject, Explicit: true, Source: "mysql-engine"}, mysql)

	postgres, err := ResolvePostgresStorageParams(map[string]string{TableStorageOptionName: "'object'"})
	require.NoError(t, err)
	require.Equal(t, TableStorageSelection{Kind: TableStorageObject, Explicit: true, Source: "storage-option"}, postgres)

	local, err := ResolvePostgresStorageParams(nil)
	require.NoError(t, err)
	require.Equal(t, DefaultTableStorageSelection(), local)
}

func TestNormalizeTableStorageOptionsRejectsInvalidSelectors(t *testing.T) {
	tests := []struct {
		name    string
		options []TableStorageOption
		want    error
	}{
		{
			name: "duplicate postgres selector",
			options: []TableStorageOption{
				{Name: TableStorageOptionName, Value: "object"},
				{Name: TableStorageOptionName, Value: "object"},
			},
			want: ErrTableStorageDuplicate,
		},
		{
			name: "duplicate mysql selector",
			options: []TableStorageOption{
				{Name: "engine", Value: "DUCKLAKE"},
				{Name: "engine", Value: "DUCKLAKE"},
			},
			want: ErrTableStorageDuplicate,
		},
		{
			name: "same value through both selectors",
			options: []TableStorageOption{
				{Name: TableStorageOptionName, Value: "object"},
				{Name: "engine", Value: "DUCKLAKE"},
			},
			want: ErrTableStorageDuplicate,
		},
		{
			name: "same value through both selectors reversed",
			options: []TableStorageOption{
				{Name: "engine", Value: "DUCKLAKE"},
				{Name: TableStorageOptionName, Value: "object"},
			},
			want: ErrTableStorageDuplicate,
		},
		{
			name: "conflicting selectors",
			options: []TableStorageOption{
				{Name: TableStorageOptionName, Value: "object"},
				{Name: "engine", Value: "LOCAL"},
			},
			want: ErrTableStorageConflict,
		},
		{
			name:    "unknown postgres value",
			options: []TableStorageOption{{Name: TableStorageOptionName, Value: "remote"}},
			want:    ErrInvalidTableStorage,
		},
		{
			name:    "unknown selector alias",
			options: []TableStorageOption{{Name: "storage_kind", Value: "object"}},
			want:    ErrInvalidTableStorage,
		},
		{
			name:    "unknown mysql engine remains local",
			options: []TableStorageOption{{Name: "engine", Value: "remote"}},
			want:    nil,
		},
		{
			name:    "credential option",
			options: []TableStorageOption{{Name: "myduck_access_key_id", Value: "not persisted"}},
			want:    ErrTableStorageCredentialSQL,
		},
		{
			name:    "path option",
			options: []TableStorageOption{{Name: "ducklake_bucket", Value: "test-bucket"}},
			want:    ErrTableStoragePathSQL,
		},
		{
			name:    "nonliteral value",
			options: []TableStorageOption{{Name: TableStorageOptionName, Value: 42}},
			want:    ErrInvalidTableStorage,
		},
		{
			name:    "injected value",
			options: []TableStorageOption{{Name: TableStorageOptionName, Value: "object;DROP TABLE t"}},
			want:    ErrInvalidTableStorage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeTableStorageOptions(tt.options)
			if tt.want == nil {
				require.NoError(t, err)
				require.Equal(t, DefaultTableStorageSelection(), got)
				return
			}
			require.Error(t, err)
			require.Truef(t, errors.Is(err, tt.want), "error %q does not wrap %v", err, tt.want)
		})
	}
}

func TestTableStorageSelectionContext(t *testing.T) {
	ctx := sql.NewEmptyContext()
	object := TableStorageSelection{Kind: TableStorageObject, Explicit: true, Source: "test"}
	require.NoError(t, SetTableStorageSelection(ctx, object))

	got, ok := TableStorageSelectionFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, object, got)

	local := TableStorageSelection{Kind: TableStorageLocal, Explicit: true, Source: "copy"}
	copyCtx, err := WithTableStorageSelection(ctx, local)
	require.NoError(t, err)
	copyGot, copyOK := TableStorageSelectionFromContext(copyCtx)
	require.True(t, copyOK)
	require.Equal(t, local, copyGot)

	originalGot, originalOK := TableStorageSelectionFromContext(ctx)
	require.True(t, originalOK)
	require.Equal(t, object, originalGot)

	_, ok = TableStorageSelectionFromContext(sql.NewEmptyContext())
	require.False(t, ok)
	require.Error(t, SetTableStorageSelection(nil, object))
	require.Error(t, SetTableStorageSelection(ctx, TableStorageSelection{}))
	_, err = WithTableStorageSelection(nil, object)
	require.Error(t, err)
}

func TestTableStorageSelectionEffectiveKind(t *testing.T) {
	require.Equal(t, TableStorageLocal, (TableStorageSelection{}).EffectiveKind())
	require.Equal(t, TableStorageObject, (TableStorageSelection{Kind: TableStorageObject}).EffectiveKind())
}
