// Copyright 2024-2025 ApeCloud, Ltd.
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

package backend

import (
	"context"
	"fmt"
	"github.com/apecloud/myduckserver/catalog"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
)

type MyHandler struct {
	*server.Handler
	provider *catalog.DatabaseProvider
}

func (h *MyHandler) ConnectionClosed(c *mysql.Conn) {
	h.provider.Pool().CloseConn(c.ConnectionID)
	h.Handler.ConnectionClosed(c)
}

func (h *MyHandler) ComInitDB(c *mysql.Conn, schemaName string) error {
	_, err := h.provider.Pool().GetConnForSchema(context.Background(), c.ConnectionID, schemaName)
	if err != nil {
		return err
	}
	return h.Handler.ComInitDB(c, schemaName)
}

func wrapResultCallback(callback mysql.ResultSpoolFn, modifiers ...ResultModifier) mysql.ResultSpoolFn {
	return func(res *sqltypes.Result, more bool) error {
		// Apply all modifiers in sequence
		result := res
		for _, modifier := range modifiers {
			result = modifier(result)
		}
		return callback(result, more)
	}
}

func (h *MyHandler) ComMultiQuery(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	callback mysql.ResultSpoolFn,
) (string, error) {
	var modifiers []ResultModifier
	query, modifiers = applyRequestModifiers(query, defaultRequestModifiers)

	return h.Handler.ComMultiQuery(ctx, c, query, wrapResultCallback(callback, modifiers...))
}

// Naive query rewriting. This is just a temporary solution
// and should be replaced with a more robust implementation.
func (h *MyHandler) ComQuery(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	callback mysql.ResultSpoolFn,
) error {
	var modifiers []ResultModifier
	query, modifiers = applyRequestModifiers(query, defaultRequestModifiers)

	return h.Handler.ComQuery(ctx, c, query, wrapResultCallback(callback, modifiers...))
}

func WrapHandler(provider *catalog.DatabaseProvider) server.HandlerWrapper {
	return func(h mysql.Handler) (mysql.Handler, error) {
		handler, ok := h.(*server.Handler)
		if !ok {
			return nil, fmt.Errorf("expected *server.Handler, got %T", h)
		}

		return &MyHandler{
			Handler:  handler,
			provider: provider,
		}, nil
	}
}
