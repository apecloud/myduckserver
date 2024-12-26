// Copyright 2024 ApeCloud, Inc.

// Copyright 2020-2021 Dolthub, Inc.
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

package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/vitess/go/mysql"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
)

const TestNumPartitions = 5

type IndexDriverInitializer func([]sql.Database) sql.IndexDriver

type DuckHarness struct {
	name               string
	parallelism        int
	numTablePartitions int
	//	readonly                  bool
	provider                  sql.DatabaseProvider
	pool                      *catalog.ConnectionPool
	indexDriverInitializer    IndexDriverInitializer
	driver                    sql.IndexDriver
	nativeIndexSupport        bool
	skippedQueries            map[string]struct{}
	skippedSetupScripts       [][]setup.SetupScript
	session                   sql.Session
	retainSession             bool
	setupData                 []setup.SetupScript
	externalProcedureRegistry sql.ExternalStoredProcedureRegistry
	server                    bool
	mu                        *sync.Mutex
}

var _ enginetest.Harness = (*DuckHarness)(nil)

var _ enginetest.IndexDriverHarness = (*DuckHarness)(nil)
var _ enginetest.IndexHarness = (*DuckHarness)(nil)
var _ enginetest.ForeignKeyHarness = (*DuckHarness)(nil)
var _ enginetest.KeylessTableHarness = (*DuckHarness)(nil)
var _ enginetest.ClientHarness = (*DuckHarness)(nil)
var _ enginetest.ServerHarness = (*DuckHarness)(nil)
var _ sql.ExternalStoredProcedureProvider = (*DuckHarness)(nil)
var _ enginetest.SkippingHarness = (*DuckHarness)(nil)

func NewDuckHarness(name string, parallelism int, numTablePartitions int, useNativeIndexes bool, driverInitializer IndexDriverInitializer) *DuckHarness {
	externalProcedureRegistry := sql.NewExternalStoredProcedureRegistry()
	for _, esp := range memory.ExternalStoredProcedures {
		externalProcedureRegistry.Register(esp)
	}

	var useServer bool
	if _, ok := os.LookupEnv("SERVER_ENGINE_TEST"); ok {
		useServer = true
	}

	return &DuckHarness{
		name:                      name,
		numTablePartitions:        numTablePartitions,
		indexDriverInitializer:    driverInitializer,
		parallelism:               parallelism,
		nativeIndexSupport:        useNativeIndexes,
		skippedQueries:            make(map[string]struct{}),
		externalProcedureRegistry: externalProcedureRegistry,
		mu:                        &sync.Mutex{},
		server:                    useServer,
	}
}

func NewDefaultDuckHarness() *DuckHarness {
	return NewDuckHarness("default", 1, TestNumPartitions, true, nil).SetupScriptsToSkip(
		setup.Fk_tblData, // Skip foreign key setup (not supported)
	)
}

// func NewReadOnlyDuckHarness() *DuckHarness {
// 	h := NewDefaultDuckHarness()
// 	h.readonly = true
// 	return h
// }

func (m *DuckHarness) SessionBuilder() server.SessionBuilder {
	return func(ctx context.Context, c *mysql.Conn, addr string) (sql.Session, error) {
		host := ""
		user := ""
		mysqlConnectionUser, ok := c.UserData.(sql.MysqlConnectionUser)
		if ok {
			host = mysqlConnectionUser.Host
			user = mysqlConnectionUser.User
		}
		client := sql.Client{Address: host, User: user, Capabilities: c.Capabilities}
		baseSession := sql.NewBaseSessionWithClientServer(addr, client, c.ConnectionID)
		memSession := memory.NewSession(baseSession, m.getProvider())
		return backend.NewSession(memSession, m.getProvider().(*catalog.DatabaseProvider)), nil
	}
}

// ExternalStoredProcedure implements the sql.ExternalStoredProcedureProvider interface
func (m *DuckHarness) ExternalStoredProcedure(_ *sql.Context, name string, numOfParams int) (*sql.ExternalStoredProcedureDetails, error) {
	return m.externalProcedureRegistry.LookupByNameAndParamCount(name, numOfParams)
}

// ExternalStoredProcedures implements the sql.ExternalStoredProcedureProvider interface
func (m *DuckHarness) ExternalStoredProcedures(_ *sql.Context, name string) ([]sql.ExternalStoredProcedureDetails, error) {
	return m.externalProcedureRegistry.LookupByName(name)
}

func (m *DuckHarness) InitializeIndexDriver(dbs []sql.Database) {
	if m.indexDriverInitializer != nil {
		m.driver = m.indexDriverInitializer(dbs)
	}
}

func (m *DuckHarness) NewSession() *sql.Context {
	m.session = m.newSession()
	return m.NewContext()
}

// see querySignature for the detail
func (m *DuckHarness) SkipQueryTest(query string) bool {
	_, ok := m.skippedQueries[querySignature(query)]
	return ok
}

// see querySignature for the detail
func (m *DuckHarness) QueriesToSkip(queries ...string) *DuckHarness {
	for _, query := range queries {
		m.skippedQueries[querySignature(query)] = struct{}{}
	}
	return m
}

// querySignature returns a normalized signature of the query
// Examples:
// - "SELECT 1 % true" -> "select_1_%_true"
// - "select_1_%_true" -> "select_1_%_true"
// The signature is identical to the test name in the test case,
// allowing either the query or the test case name to be used
// for skipping specific queries.
func querySignature(query string) string {
	return rewrite(strings.ToLower(query))
}

// copy from testing/match.go
// rewrite rewrites a subname to having only printable characters and no white
// space.
func rewrite(s string) string {
	b := []byte{}
	for _, r := range s {
		switch {
		case isSpace(r):
			b = append(b, '_')
		case !strconv.IsPrint(r):
			s := strconv.QuoteRune(r)
			b = append(b, s[1:len(s)-1]...)
		default:
			b = append(b, string(r)...)
		}
	}
	return string(b)
}

func isSpace(r rune) bool {
	if r < 0x2000 {
		switch r {
		// Note: not the same as Unicode Z class.
		case '\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0, 0x1680:
			return true
		}
	} else {
		if r <= 0x200a {
			return true
		}
		switch r {
		case 0x2028, 0x2029, 0x202f, 0x205f, 0x3000:
			return true
		}
	}
	return false
}

func (m *DuckHarness) SetupScriptsToSkip(setupScripts ...[]setup.SetupScript) *DuckHarness {
	m.skippedSetupScripts = append(m.skippedSetupScripts, setupScripts...)
	return m
}

func (m *DuckHarness) UseServer() {
	m.server = true
}

func (m *DuckHarness) IsUsingServer() bool {
	return m.server
}

type SkippingDuckHarness struct {
	DuckHarness
}

var _ enginetest.SkippingHarness = (*SkippingDuckHarness)(nil)

func NewSkippingDuckHarness() *SkippingDuckHarness {
	return &SkippingDuckHarness{
		DuckHarness: *NewDefaultDuckHarness(),
	}
}

func (s SkippingDuckHarness) SkipQueryTest(query string) bool {
	return true
}

func (m *DuckHarness) Setup(setupData ...[]setup.SetupScript) {

	m.setupData = nil
	for i := range setupData {
		skip := false
		for _, skipped := range m.skippedSetupScripts {
			if sameSetupScript(setupData[i], skipped) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}

		m.setupData = append(m.setupData, setupData[i]...)
	}
}

func sameSetupScript(a, b []setup.SetupScript) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

func (m *DuckHarness) NewEngine(t *testing.T) (enginetest.QueryEngine, error) {
	if !m.retainSession {
		m.session = nil
		m.provider = nil
	}
	return NewEngine(t, m, m.getProvider(), m.setupData, memory.NewStatsProv(), m.server)
}

type DuckTestEngine struct {
	enginetest.QueryEngine
	pool *catalog.ConnectionPool
}

func (e *DuckTestEngine) Close() error {
	return errors.Join(e.QueryEngine.Close(), e.pool.Close())
}

// copy from go-mysql-server/enginetest/initialization.go
// NewEngine creates an engine and sets it up for testing using harness, provider, and setup data given.
func NewEngine(t *testing.T, harness enginetest.Harness, dbProvider sql.DatabaseProvider, setupData []setup.SetupScript, statsProvider sql.StatsProvider, server bool) (enginetest.QueryEngine, error) {
	// Create the connection pool first, as it is needed by `NewEngineWithProvider`
	provider := dbProvider.(*catalog.DatabaseProvider)
	harness.(*DuckHarness).pool = provider.Pool()

	e := enginetest.NewEngineWithProvider(t, harness, dbProvider)
	e.Analyzer.Catalog.StatsProvider = statsProvider

	builder := backend.NewDuckBuilder(e.Analyzer.ExecBuilder, provider)
	e.Analyzer.ExecBuilder = builder

	ctx := enginetest.NewContext(harness)

	var supportsIndexes bool
	if ih, ok := harness.(enginetest.IndexHarness); ok && ih.SupportsNativeIndexCreation() {
		supportsIndexes = true
	}

	// TODO: remove ths, make it explicit everywhere
	if len(setupData) == 0 {
		setupData = setup.MydbData
	}
	e, err := enginetest.RunSetupScripts(ctx, e, setupData, supportsIndexes)
	if err != nil {
		return nil, err
	}

	var qe enginetest.QueryEngine = e
	if server {
		qe, err = enginetest.NewServerQueryEngine(t, e, harness.(*DuckHarness).SessionBuilder())
		if err != nil {
			return nil, err
		}
	}
	return &DuckTestEngine{qe, provider.Pool()}, nil
}

func (m *DuckHarness) SupportsNativeIndexCreation() bool {
	return m.nativeIndexSupport
}

func (m *DuckHarness) SupportsForeignKeys() bool {
	return true
}

func (m *DuckHarness) SupportsKeylessTables() bool {
	return true
}

func (m *DuckHarness) Parallelism() int {
	return m.parallelism
}

func (m *DuckHarness) NewContext() *sql.Context {
	if m.session == nil {
		m.session = m.newSession()
	}

	return sql.NewContext(
		context.Background(),
		sql.WithSession(m.session),
	)
}

func (m *DuckHarness) newSession() sql.Session {
	baseSession := enginetest.NewBaseSession()
	session := memory.NewSession(baseSession, m.getProvider())
	if m.driver != nil {
		session.GetIndexRegistry().RegisterIndexDriver(m.driver)
	}
	return backend.NewSession(session, m.getProvider().(*catalog.DatabaseProvider))
}

func (m *DuckHarness) NewContextWithClient(client sql.Client) *sql.Context {
	baseSession := sql.NewBaseSessionWithClientServer("address", client, 1)

	return sql.NewContext(
		context.Background(),
		sql.WithSession(memory.NewSession(baseSession, m.getProvider())),
	)
}

func (m *DuckHarness) IndexDriver(dbs []sql.Database) sql.IndexDriver {
	if m.indexDriverInitializer != nil {
		return m.indexDriverInitializer(dbs)
	}
	return nil
}

func (m *DuckHarness) WithProvider(provider sql.DatabaseProvider) *DuckHarness {
	ret := *m
	ret.provider = provider
	return &ret
}

func (m *DuckHarness) getProvider() sql.DatabaseProvider {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.provider == nil {
		m.provider = m.NewDatabaseProvider().(*catalog.DatabaseProvider)
	}

	return m.provider
}

func (m *DuckHarness) NewDatabaseProvider() sql.MutableDatabaseProvider {
	return catalog.NewInMemoryDBProvider()
}

func (m *DuckHarness) Provider() *catalog.DatabaseProvider {
	return m.getProvider().(*catalog.DatabaseProvider)
}

func (m *DuckHarness) ValidateEngine(ctx *sql.Context, e *sqle.Engine) error {
	return sanityCheckEngine(ctx, e)
}

func sanityCheckEngine(ctx *sql.Context, e *sqle.Engine) (err error) {
	for _, db := range e.Analyzer.Catalog.AllDatabases(ctx) {
		if err = sanityCheckDatabase(ctx, db); err != nil {
			return err
		}
	}
	return
}

func sanityCheckDatabase(ctx *sql.Context, db sql.Database) error {
	names, err := db.GetTableNames(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		t, ok, err := db.GetTableInsensitive(ctx, name)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("expected to find table %s", name)
		}
		if t.Name() != name {
			return fmt.Errorf("unexpected table name (%s !=  %s)", name, t.Name())
		}
	}
	return nil
}

// Find the test statements under ScriptTest by name.
func (m *DuckHarness) GetScriptQueries(tests []queries.ScriptTest, namesToSkip []string) []string {

	skipMap := make(map[string]bool)
	for _, name := range namesToSkip {
		skipMap[querySignature(name)] = true
	}

	var results []string
	for _, test := range tests {
		testName := querySignature(test.Name)
		if skipMap[testName] {
			for _, setupscript := range test.SetUpScript {
				results = append(results, querySignature(setupscript))
			}
			for _, assertion := range test.Assertions {
				results = append(results, querySignature(assertion.Query))
			}
		}
	}
	return results
}
