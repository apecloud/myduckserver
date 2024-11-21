// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgserver

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

// ValidateCopyFrom returns an error if the CopyFrom node is invalid.
func ValidateCopyFrom(cf *tree.CopyFrom, ctx *sql.Context) (sql.InsertableTable, error) {
	table, err := GetSqlTableFromContext(ctx, cf.Table.Schema(), cf.Table.Table())
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, fmt.Errorf(`relation "%s" does not exist`, cf.Table.Table())
	}
	if it, ok := table.(sql.InsertableTable); !ok {
		return nil, fmt.Errorf(`table "%s" is read-only`, cf.Table.Table())
	} else {
		return it, nil
	}
}

// ValidateCopyTo returns an error if the CopyTo node is invalid, for example if it contains columns that
// are not in the table schema.
func ValidateCopyTo(ct *tree.CopyTo, ctx *sql.Context) (sql.Table, error) {
	if ct.Table.Table() == "" {
		if ct.Statement == nil {
			return nil, fmt.Errorf("no table specified")
		}
		return nil, nil
	}
	table, err := GetSqlTableFromContext(ctx, ct.Table.Schema(), ct.Table.Table())
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, fmt.Errorf(`relation "%s" does not exist`, ct.Table.Table())
	}
	return table, nil
}
