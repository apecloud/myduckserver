// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transpiler

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"

	"gopkg.in/src-d/go-errors.v1"
)

const (
	cmdExit = "CMD:EXIT"
	cmdRun  = "CMD:RUN"

	resultOK  = "OK:"
	resultErr = "ERROR:"
)

var (
	errPythonProcessUnhealthy = errors.NewKind("sqlglot python process is unhealthy: %s")
)

type translateService struct {
	mu       *sync.Mutex
	pyCmd    *exec.Cmd
	pyStdin  io.Writer
	pyStdout io.Reader
	pyStderr *bytes.Buffer
}

var (
	translationSvcOnce sync.Once
	translationSvc     *translateService
)

func newTranslateService() (*translateService, error) {
	pythonPath, err := getPythonPath()
	if err != nil {
		return nil, fmt.Errorf("failed to get Python path: %v", err)
	}

	pythonScript := fmt.Sprintf(`
import sys
import sqlglot
from sqlglot import exp

CMD_EXIT = %q
CMD_RUN = %q
RESULT_OK = %q
RESULT_ERR = %q

def read_bytes(n: int):
    bytes = b''
    while n > 0:
        reads = sys.stdin.buffer.read(n)
        if len(reads) == 0:
            # The stdin has been closed, indicating that the parent process has exited.
            # We should exit the child process to prevent orphan Python processes.
            raise EOFError("EOF")
        bytes += reads
        n -= len(reads)
    return bytes

def read_string():
    data = read_bytes(4)
    length = int.from_bytes(data, byteorder='big')
    data = read_bytes(length)
    return data.decode('utf-8')

def write_string(s: str):
    data = s.encode('utf-8')
    # write the length of the string first
    sys.stdout.buffer.write(len(data).to_bytes(4, byteorder='big'))
    sys.stdout.buffer.write(data)
    sys.stdout.flush()

def _table_alias_or_name(t):
    if t is None:
        return ""
    if isinstance(t, exp.Table):
        if t.alias:
            return str(t.alias).lower()
        return (t.name or "").lower()
    if isinstance(t, exp.Alias) and t.alias:
        return str(t.alias).lower()
    if isinstance(t, exp.Subquery) and t.alias:
        return str(t.alias).lower()
    return ""

def _plain_source(src):
    if src is None:
        return src
    copied = src.copy()
    if isinstance(copied, exp.Table) and copied.args.get("joins"):
        copied.set("joins", None)
    return copied

def _collect_from_src(src, tables, ons):
    if src is None:
        return
    if isinstance(src, exp.Table):
        tables.append(_plain_source(src))
        for j in src.args.get("joins") or []:
            _collect_from_src(j.this, tables, ons)
            if j.args.get("on") is not None:
                ons.append(j.args.get("on"))
        return
    tables.append(src)

def _and_where(where, extra):
    if extra is None:
        return where
    if where is None:
        return exp.Where(this=extra)
    return exp.Where(this=exp.And(this=where.this, expression=extra))

def _match_target(src, tgt_name):
    if not tgt_name:
        return False
    if _table_alias_or_name(src) == tgt_name:
        return True
    return isinstance(src, exp.Table) and (src.name or "").lower() == tgt_name

def _with_kwargs(node):
    if node.args.get("with_") is not None:
        return {"with_": node.args.get("with_")}
    return {}

def _rewrite_mysql_delete(node):
    if not isinstance(node, exp.Delete):
        return None
    # DuckDB DELETE has no ORDER BY / LIMIT. Keep the same rows via rowid.
    if node.args.get("order") is not None or node.args.get("limit") is not None:
        tbl = node.this
        sub = exp.select(exp.column("rowid")).from_(tbl.copy())
        if node.args.get("where") is not None:
            sub = sub.where(node.args.get("where").this)
        if node.args.get("order") is not None:
            sub.set("order", node.args.get("order"))
        if node.args.get("limit") is not None:
            sub.set("limit", node.args.get("limit"))
        kwargs = {
            "this": tbl,
            "where": exp.Where(this=exp.In(this=exp.column("rowid"), expressions=[exp.paren(sub)])),
        }
        kwargs.update(_with_kwargs(node))
        return exp.Delete(**kwargs)
    targets = list(node.args.get("tables") or [])
    using = node.args.get("using")
    # MySQL: DELETE t FROM t JOIN other. DuckDB: DELETE FROM t USING other.
    if targets:
        if len(targets) != 1:
            return None
        tgt_name = _table_alias_or_name(targets[0])
        srcs, ons = [], []
        _collect_from_src(node.this, srcs, ons)
        match = None
        others = []
        for s in srcs:
            if match is None and _match_target(s, tgt_name):
                match = s
                continue
            others.append(s)
        if match is None:
            match = targets[0]
        where = node.args.get("where")
        for on in ons:
            where = _and_where(where, on)
        kwargs = {"this": match}
        if others:
            kwargs["using"] = others
        if where is not None:
            kwargs["where"] = where
        kwargs.update(_with_kwargs(node))
        return exp.Delete(**kwargs)
    # MySQL: DELETE FROM t USING t JOIN other. Drop t from USING.
    if isinstance(using, list) and using:
        srcs, ons = [], []
        for u in using:
            _collect_from_src(u, srcs, ons)
        tgt = node.this
        tgt_name = _table_alias_or_name(tgt)
        others = [s for s in srcs if not _match_target(s, tgt_name)]
        if others == srcs and not ons:
            return None
        where = node.args.get("where")
        for on in ons:
            where = _and_where(where, on)
        kwargs = {"this": tgt}
        if others:
            kwargs["using"] = others
        if where is not None:
            kwargs["where"] = where
        kwargs.update(_with_kwargs(node))
        return exp.Delete(**kwargs)
    return None

def _join_is_inner_or_cross(j):
    kind = (j.args.get("kind") or "").upper()
    side = (j.args.get("side") or "").upper()
    if side in ("LEFT", "RIGHT", "FULL"):
        return False
    return kind in ("", "INNER", "CROSS")

def _update_set_tables(exprs):
    tables = []
    seen = set()
    for e in exprs or []:
        if isinstance(e, exp.EQ) and isinstance(e.this, exp.Column) and e.this.table:
            name = str(e.this.table).lower()
            if name not in seen:
                seen.add(name)
                tables.append(name)
    return tables

def _strip_target_table(exprs, tgt_name):
    out = []
    for e in exprs or []:
        if isinstance(e, exp.EQ) and isinstance(e.this, exp.Column):
            col = e.this.copy()
            if (col.table or "").lower() == tgt_name:
                col.set("table", None)
            out.append(exp.EQ(this=col, expression=e.expression))
        else:
            out.append(e)
    return out

def _expr_tables(e):
    names = set()
    if e is None:
        return names
    for c in e.find_all(exp.Column):
        if c.table:
            names.add(str(c.table).lower())
    return names

def _target_rowid(match, tgt_name):
    alias = _table_alias_or_name(match) or tgt_name
    if alias:
        return exp.column("rowid", table=alias)
    return exp.column("rowid")

def _rewrite_mysql_update(node):
    if not isinstance(node, exp.Update):
        return None
    srcs, ons = [], []
    _collect_from_src(node.this, srcs, ons)
    has_join = len(srcs) > 1
    # DuckDB UPDATE has no ORDER BY / LIMIT. Keep the same rows via rowid.
    if (node.args.get("order") is not None or node.args.get("limit") is not None) and not has_join:
        tbl = _plain_source(node.this)
        sub = exp.select(exp.column("rowid")).from_(tbl.copy())
        if node.args.get("where") is not None:
            sub = sub.where(node.args.get("where").this)
        if node.args.get("order") is not None:
            sub.set("order", node.args.get("order"))
        if node.args.get("limit") is not None:
            sub.set("limit", node.args.get("limit"))
        kwargs = {
            "this": tbl,
            "expressions": list(node.expressions or []),
            "where": exp.Where(this=exp.In(this=exp.column("rowid"), expressions=[exp.paren(sub)])),
        }
        kwargs.update(_with_kwargs(node))
        return exp.Update(**kwargs)
    if not has_join:
        return None
    if isinstance(node.this, exp.Table):
        for j in node.this.args.get("joins") or []:
            if not _join_is_inner_or_cross(j):
                return None
    set_tables = _update_set_tables(node.expressions)
    if len(set_tables) != 1:
        return None
    tgt_name = set_tables[0]
    match = None
    for s in srcs:
        if _match_target(s, tgt_name):
            match = s
            break
    if match is None:
        return None
    other_refs = set()
    for e in node.expressions or []:
        if isinstance(e, exp.EQ):
            other_refs |= _expr_tables(e.expression)
    other_refs.discard(tgt_name)
    rid = _target_rowid(match, tgt_name)
    if not other_refs:
        # MySQL updates each target row once. rowid IN avoids 1:N double-count
        # and DuckDB unique-index errors on UPDATE FROM.
        sub = exp.select(rid).from_(node.this.copy()).distinct()
        if node.args.get("where") is not None:
            sub = sub.where(node.args.get("where").this)
        kwargs = {
            "this": _plain_source(match),
            "expressions": _strip_target_table(node.expressions, tgt_name),
            "where": exp.Where(this=exp.In(this=exp.column("rowid"), expressions=[exp.paren(sub)])),
        }
        kwargs.update(_with_kwargs(node))
        return exp.Update(**kwargs)
    # SET uses other tables: pick one joined row per target.
    select_exprs = [exp.alias_(rid, "__rid")]
    new_sets = []
    for i, e in enumerate(node.expressions or []):
        if not isinstance(e, exp.EQ):
            return None
        alias = "__s" + str(i)
        select_exprs.append(exp.alias_(e.expression, alias))
        col = e.this.copy()
        col.set("table", None)
        new_sets.append(exp.EQ(this=col, expression=exp.column(alias, table="src")))
    sub = exp.Select(expressions=select_exprs)
    sub.set("from_", exp.From(this=node.this.copy()))
    if node.args.get("where") is not None:
        sub = sub.where(node.args.get("where").this)
    sub.set(
        "qualify",
        exp.Qualify(
            this=exp.EQ(
                this=exp.Window(this=exp.RowNumber(), partition_by=[rid], over="OVER"),
                expression=exp.Literal.number(1),
            )
        ),
    )
    kwargs = {
        "this": _plain_source(match),
        "expressions": new_sets,
        "from_": exp.From(this=exp.Subquery(this=sub, alias="src")),
        "where": exp.Where(
            this=exp.EQ(this=exp.column("rowid"), expression=exp.column("__rid", table="src"))
        ),
    }
    kwargs.update(_with_kwargs(node))
    return exp.Update(**kwargs)

def rewrite_mysql_for_duckdb(node):
    # DuckDB has no DUAL; replace with a one-row subquery.
    if isinstance(node, exp.Table) and not node.args.get("db") and (node.name or "").lower() == "dual":
        return exp.Subquery(
            this=exp.Select(expressions=[exp.Literal.number(1)]),
            alias="dual",
        )
    # MySQL WITH t AS (SELECT * FROM t) uses the base table inside. DuckDB treats it as circular.
    if isinstance(node, (exp.Select, exp.Union, exp.Except, exp.Intersect)) and node.args.get("with_"):
        w = node.args.get("with_")
        if not w.args.get("recursive"):
            renames = {}
            new_ctes = []
            changed = False
            for cte in list(w.expressions or []):
                alias = cte.args.get("alias")
                cname = ""
                if isinstance(alias, exp.TableAlias) and alias.this:
                    cname = str(alias.this).lower()
                shadows = False
                if cname and cte.this is not None:
                    for t in cte.this.find_all(exp.Table):
                        if not t.args.get("db") and (t.name or "").lower() == cname:
                            shadows = True
                            break
                if shadows:
                    new_name = cname + "__mds"
                    renames[cname] = new_name
                    cte = cte.copy()
                    cte.set("alias", exp.TableAlias(this=exp.to_identifier(new_name)))
                    changed = True
                new_ctes.append(cte)
            if changed:
                def _rename_outer(n):
                    if isinstance(n, exp.Table) and not n.args.get("db"):
                        nm = (n.name or "").lower()
                        if nm in renames:
                            copied = n.copy()
                            copied.set("this", exp.to_identifier(renames[nm]))
                            return copied
                    return n
                updated = node.copy()
                new_w = w.copy()
                new_w.set("expressions", new_ctes)
                updated.set("with_", new_w)
                if isinstance(updated, exp.Select):
                    if updated.args.get("from_"):
                        updated.set("from_", updated.args.get("from_").transform(_rename_outer))
                    if updated.args.get("joins"):
                        updated.set("joins", [j.transform(_rename_outer) for j in updated.args.get("joins")])
                else:
                    if updated.this is not None:
                        updated.set("this", updated.this.transform(_rename_outer))
                    if updated.args.get("expression") is not None:
                        updated.set("expression", updated.args.get("expression").transform(_rename_outer))
                return updated
    # MySQL VALUES ROW(1, '1') -> DuckDB VALUES (1, '1') AS t(column_0, column_1).
    if isinstance(node, exp.Values):
        new_exprs = []
        changed = False
        for e in node.expressions or []:
            inner = e.expressions[0] if isinstance(e, exp.Tuple) and len(e.expressions or []) == 1 else e
            if isinstance(inner, exp.Anonymous) and str(inner.this).lower() == "row":
                new_exprs.append(exp.Tuple(expressions=list(inner.expressions or [])))
                changed = True
            else:
                new_exprs.append(e)
        if changed:
            alias = node.args.get("alias")
            ncols = len(new_exprs[0].expressions) if new_exprs else 0
            if alias is not None and ncols:
                alias = exp.TableAlias(
                    this=alias.this if isinstance(alias, exp.TableAlias) else alias,
                    columns=[exp.to_identifier("column_" + str(i)) for i in range(ncols)],
                )
            updated = node.copy()
            updated.set("expressions", new_exprs)
            if alias is not None:
                updated.set("alias", alias)
            return updated
    rewritten_delete = _rewrite_mysql_delete(node)
    if rewritten_delete is not None:
        if rewritten_delete.args.get("with_") is not None:
            rewritten_delete.set("with_", rewritten_delete.args.get("with_").transform(rewrite_mysql_for_duckdb))
        if rewritten_delete.args.get("where") is not None:
            rewritten_delete.set("where", rewritten_delete.args.get("where").transform(rewrite_mysql_for_duckdb))
        if isinstance(rewritten_delete.args.get("using"), list):
            rewritten_delete.set("using", [u.transform(rewrite_mysql_for_duckdb) for u in rewritten_delete.args.get("using")])
        if rewritten_delete.this is not None:
            rewritten_delete.set("this", rewritten_delete.this.transform(rewrite_mysql_for_duckdb))
        return rewritten_delete
    rewritten_update = _rewrite_mysql_update(node)
    if rewritten_update is not None:
        if rewritten_update.args.get("with_") is not None:
            rewritten_update.set("with_", rewritten_update.args.get("with_").transform(rewrite_mysql_for_duckdb))
        if rewritten_update.args.get("where") is not None:
            rewritten_update.set("where", rewritten_update.args.get("where").transform(rewrite_mysql_for_duckdb))
        if rewritten_update.args.get("from_") is not None:
            rewritten_update.set("from_", rewritten_update.args.get("from_").transform(rewrite_mysql_for_duckdb))
        if rewritten_update.this is not None:
            rewritten_update.set("this", rewritten_update.this.transform(rewrite_mysql_for_duckdb))
        new_exprs = []
        for e in rewritten_update.expressions or []:
            new_exprs.append(e.transform(rewrite_mysql_for_duckdb) if e is not None else e)
        rewritten_update.set("expressions", new_exprs)
        return rewritten_update
    # Nested MySQL XOR is left as the XOR keyword, which DuckDB rejects.
    # Expand to the equivalent boolean form before generating DuckDB SQL.
    if isinstance(node, exp.Xor):
        left, right = node.this, node.expression
        return exp.or_(
            exp.and_(exp.paren(left), exp.Not(this=exp.paren(right))),
            exp.and_(exp.Not(this=exp.paren(left)), exp.paren(right)),
        )
    # DuckDB has no AUTO_INCREMENT; map it to IDENTITY.
    if isinstance(node, exp.ColumnDef):
        constraints = list(node.args.get("constraints") or [])
        kept = []
        has_autoinc = False
        for c in constraints:
            if isinstance(getattr(c, "kind", None), exp.AutoIncrementColumnConstraint):
                has_autoinc = True
                continue
            kept.append(c)
        if has_autoinc:
            kept.append(exp.ColumnConstraint(
                kind=exp.GeneratedAsIdentityColumnConstraint(this=False)
            ))
            return exp.ColumnDef(
                this=node.this,
                kind=node.args.get("kind"),
                constraints=kept,
            )
    # MySQL CHAR_LENGTH / CHARACTER_LENGTH -> DuckDB LENGTH (character count).
    # Without this, some paths still emit CHAR_LENGTH, which DuckDB does not have.
    if isinstance(node, exp.Length) and not node.args.get("binary"):
        return exp.Anonymous(this="length", expressions=[node.this])
    # MySQL FORMAT(x, d[, locale]) is number-to-string with grouping.
    # DuckDB FORMAT is {fmt}; map the scale and swap separators for common EU locales.
    if isinstance(node, exp.NumberToStr):
        value = node.this
        digits = node.args.get("format")
        culture = node.args.get("culture")
        if isinstance(digits, exp.Literal) and digits.is_int:
            fmt = "{:,." + str(int(digits.this)) + "f}"
            formatted = exp.Anonymous(
                this="format",
                expressions=[exp.Literal.string(fmt), value],
            )
        else:
            formatted = exp.Anonymous(
                this="format",
                expressions=[
                    exp.Anonymous(
                        this="concat",
                        expressions=[
                            exp.Literal.string("{:,."),
                            exp.Cast(this=digits, to=exp.DataType.build("TEXT")),
                            exp.Literal.string("f}"),
                        ],
                    ),
                    value,
                ],
            )
        if isinstance(culture, exp.Literal) and culture.is_string:
            loc = str(culture.this).lower().replace("-", "_")
            prefix = loc.split("_", 1)[0]
            if prefix in ("da", "de", "nl", "es", "it", "pt"):
                formatted = exp.Anonymous(
                    this="replace",
                    expressions=[
                        exp.Anonymous(
                            this="replace",
                            expressions=[
                                exp.Anonymous(
                                    this="replace",
                                    expressions=[
                                        formatted,
                                        exp.Literal.string(","),
                                        exp.Literal.string("\x01"),
                                    ],
                                ),
                                exp.Literal.string("."),
                                exp.Literal.string(","),
                            ],
                        ),
                        exp.Literal.string("\x01"),
                        exp.Literal.string("."),
                    ],
                )
        return formatted
    # MySQL CRC32 is IEEE of the string bytes. DuckDB has no CRC32; use __sys__.mysql_crc32.
    if isinstance(node, exp.Anonymous) and str(node.this).upper() == "CRC32" and node.expressions:
        return exp.Anonymous(
            this="__sys__.mysql_crc32",
            expressions=[exp.Cast(this=node.expressions[0], to=exp.DataType.build("TEXT"))],
        )
    # MySQL FIND_IN_SET(str, list) -> 1-based index or 0. DuckDB has no native function.
    if isinstance(node, exp.Anonymous) and str(node.this).lower() == "find_in_set" and len(node.expressions) == 2:
        needle, hay = node.expressions
        return exp.Anonymous(
            this="coalesce",
            expressions=[
                exp.Anonymous(
                    this="list_position",
                    expressions=[
                        exp.Anonymous(this="string_split", expressions=[hay, exp.Literal.string(",")]),
                        needle,
                    ],
                ),
                exp.Literal.number(0),
            ],
        )
    # MySQL SUBSTRING_INDEX(str, delim, n): first/last n delimited parts.
    if isinstance(node, exp.SubstringIndex):
        src = node.args.get("this")
        delim = node.args.get("delimiter")
        count = node.args.get("count")
        parts = exp.Anonymous(this="string_split", expressions=[src, delim])
        parts_len = exp.Anonymous(this="len", expressions=[parts])
        pos = exp.Anonymous(
            this="array_to_string",
            expressions=[
                exp.Anonymous(
                    this="list_slice",
                    expressions=[parts, exp.Literal.number(1), count],
                ),
                delim,
            ],
        )
        neg = exp.Anonymous(
            this="array_to_string",
            expressions=[
                exp.Anonymous(
                    this="list_slice",
                    expressions=[
                        parts,
                        exp.Add(this=parts_len, expression=exp.Add(this=count, expression=exp.Literal.number(1))),
                        parts_len,
                    ],
                ),
                delim,
            ],
        )
        return exp.Case(
            ifs=[
                exp.If(this=exp.EQ(this=count, expression=exp.Literal.number(0)), true=exp.Literal.string("")),
                exp.If(this=exp.GT(this=count, expression=exp.Literal.number(0)), true=pos),
            ],
            default=neg,
        )
    # MySQL HOUR/MINUTE/SECOND accept datetime strings; DuckDB needs a timestamp.
    if isinstance(node, (exp.Hour, exp.Minute, exp.Second)):
        inner = node.this
        if not isinstance(inner, exp.Cast):
            inner = exp.Cast(this=inner, to=exp.DataType.build("TIMESTAMP"))
        return node.__class__(this=inner)
    # MySQL ELT(n, a, b, ...) is 1-based; out of range is NULL.
    if isinstance(node, exp.Elt):
        idx = node.this
        items = list(node.expressions or [])
        return exp.Anonymous(
            this="list_extract",
            expressions=[exp.Array(expressions=items), idx],
        )
    # MySQL FIELD(str, a, b, ...) is 1-based index or 0.
    if isinstance(node, exp.Anonymous) and str(node.this).lower() == "field" and len(node.expressions) >= 2:
        needle = node.expressions[0]
        items = list(node.expressions[1:])
        return exp.Anonymous(
            this="coalesce",
            expressions=[
                exp.Anonymous(
                    this="list_position",
                    expressions=[exp.Array(expressions=items), needle],
                ),
                exp.Literal.number(0),
            ],
        )
    # MySQL ADDDATE/SUBDATE -> date +/- INTERVAL. Bare numbers are days.
    if isinstance(node, exp.Anonymous) and str(node.this).upper() in ("ADDDATE", "DATE_ADD", "SUBDATE", "DATE_SUB") and len(node.expressions) == 2:
        base, delta = node.expressions
        if not isinstance(delta, exp.Interval):
            delta = exp.Interval(this=delta, unit=exp.Var(this="DAY"))
        if str(node.this).upper() in ("ADDDATE", "DATE_ADD"):
            return exp.Add(this=base, expression=delta)
        return exp.Sub(this=base, expression=delta)
    # MySQL CONV(n, from, to). Cover the common 10 -> 2/8/16 cases with {fmt}.
    if isinstance(node, exp.Anonymous) and str(node.this).upper() == "CONV" and len(node.expressions) == 3:
        value, frm, to = node.expressions
        if isinstance(frm, exp.Literal) and frm.is_int and int(frm.this) == 10 and isinstance(to, exp.Literal) and to.is_int:
            spec = {2: "{:b}", 8: "{:o}", 16: "{:X}"}.get(int(to.this))
            if spec:
                return exp.Anonymous(
                    this="format",
                    expressions=[exp.Literal.string(spec), value],
                )
    # MySQL BIT_LENGTH is 8 * byte length, not character length.
    if isinstance(node, exp.BitLength):
        return exp.Mul(
            this=exp.Anonymous(
                this="octet_length",
                expressions=[exp.Cast(this=node.this, to=exp.DataType.build("TEXT"))],
            ),
            expression=exp.Literal.number(8),
        )
    # MySQL OCTET_LENGTH is bytes. DuckDB STRLEN counts characters.
    if isinstance(node, exp.Anonymous) and str(node.this).upper() == "OCTET_LENGTH" and node.expressions:
        return exp.Anonymous(this="octet_length", expressions=[node.expressions[0]])
    # MySQL UNHEX -> DuckDB from_hex.
    if isinstance(node, exp.Unhex):
        return exp.Anonymous(this="from_hex", expressions=[node.this])
    def _as_text(e):
        if isinstance(e, exp.Cast):
            return e
        return exp.Cast(this=e, to=exp.DataType.build("TEXT"))
    # MySQL MD5/SHA* hash strings; integers must be cast first.
    if isinstance(node, exp.MD5):
        return exp.MD5(this=_as_text(node.this))
    if isinstance(node, exp.SHA):
        return exp.Anonymous(this="sha1", expressions=[_as_text(node.this)])
    if isinstance(node, exp.SHA2):
        length = node.args.get("length")
        bits = int(length.this) if isinstance(length, exp.Literal) and length.is_int else 256
        fn = {224: "sha224", 256: "sha256", 384: "sha384", 512: "sha512"}.get(bits, "sha256")
        return exp.Anonymous(this=fn, expressions=[_as_text(node.this)])
    # MySQL ASCII is the first byte, not the Unicode code point.
    if isinstance(node, exp.Ascii):
        return exp.Anonymous(
            this="get_byte",
            expressions=[
                exp.Cast(this=node.this, to=exp.DataType.build("BLOB")),
                exp.Literal.number(0),
            ],
        )
    # MySQL CHAR(n, ...) concatenates code points. DuckDB CHR takes one argument.
    if isinstance(node, exp.Chr):
        args = list(node.expressions or [])
        if not args:
            return node
        chrs = [exp.Anonymous(this="chr", expressions=[a]) for a in args]
        if len(chrs) == 1:
            return chrs[0]
        return exp.Anonymous(this="concat", expressions=chrs)
    # MySQL JSON_PRETTY -> DuckDB json_pretty on JSON values.
    if isinstance(node, exp.Anonymous) and str(node.this).upper() == "JSON_PRETTY" and node.expressions:
        return exp.Anonymous(
            this="json_pretty",
            expressions=[exp.Cast(this=node.expressions[0], to=exp.DataType.build("JSON"))],
        )
    # MySQL JSON_KEYS returns NULL on bad JSON. DuckDB json_keys errors.
    if isinstance(node, exp.JSONKeys):
        inner = node.this.transform(rewrite_mysql_for_duckdb) if node.this is not None else node.this
        return exp.Anonymous(
            this="json_keys",
            expressions=[exp.TryCast(this=inner, to=exp.DataType.build("JSON"))],
        )
    # MySQL accepts YYYYMMDD date strings; DuckDB DATE needs YYYY-MM-DD.
    def _compact_date_literal(e):
        if isinstance(e, exp.Literal) and e.is_string:
            s = str(e.this)
            if len(s) == 8 and s.isdigit():
                return exp.Literal.string(s[0:4] + "-" + s[4:6] + "-" + s[6:8])
        return e
    if isinstance(node, exp.DayOfYear):
        inner = node.this
        if isinstance(inner, exp.TsOrDsToDate):
            updated = inner.copy()
            updated.set("this", _compact_date_literal(inner.this))
            return node.__class__(this=updated)
        return node.__class__(this=_compact_date_literal(inner))
    # MySQL JSON_MERGE is object merge; DuckDB json_merge_patch is the closest builtin.
    if isinstance(node, exp.Anonymous) and str(node.this).upper() == "JSON_MERGE" and len(node.expressions) == 2:
        a, b = node.expressions
        return exp.Anonymous(
            this="json_merge_patch",
            expressions=[
                exp.Cast(this=a, to=exp.DataType.build("JSON")),
                exp.Cast(this=b, to=exp.DataType.build("JSON")),
            ],
        )
    # MySQL JSON_MERGE_PRESERVE concatenates arrays; objects still use merge_patch.
    if isinstance(node, exp.Anonymous) and str(node.this).upper() == "JSON_MERGE_PRESERVE" and len(node.expressions) == 2:
        a, b = node.expressions
        aj = exp.Cast(this=a, to=exp.DataType.build("JSON"))
        bj = exp.Cast(this=b, to=exp.DataType.build("JSON"))
        both_arrays = exp.and_(
            exp.EQ(this=exp.Anonymous(this="json_type", expressions=[aj]), expression=exp.Literal.string("ARRAY")),
            exp.EQ(this=exp.Anonymous(this="json_type", expressions=[bj]), expression=exp.Literal.string("ARRAY")),
        )
        concat = exp.Anonymous(
            this="to_json",
            expressions=[
                exp.Anonymous(
                    this="list_concat",
                    expressions=[
                        exp.Anonymous(this="json_extract", expressions=[aj, exp.Literal.string("$")]),
                        exp.Anonymous(this="json_extract", expressions=[bj, exp.Literal.string("$")]),
                    ],
                )
            ],
        )
        return exp.If(
            this=both_arrays,
            true=concat,
            false=exp.Anonymous(this="json_merge_patch", expressions=[aj, bj]),
        )
    # MySQL TIME_FORMAT -> DuckDB strftime. %%h is 01-12 like DuckDB %%I.
    if isinstance(node, exp.Anonymous) and str(node.this).upper() == "TIME_FORMAT" and len(node.expressions) == 2:
        t, fmt = node.expressions
        if isinstance(fmt, exp.Literal) and fmt.is_string:
            fmt = exp.Literal.string(str(fmt.this).replace("%%h", "%%I"))
        return exp.Anonymous(
            this="strftime",
            expressions=[
                exp.Cast(this=t, to=exp.DataType.build("TIMESTAMP")),
                fmt,
            ],
        )
    # MySQL JSON_OVERLAPS: documents share a value. json_contains either way is the closest builtin.
    if isinstance(node, exp.Anonymous) and str(node.this).upper() == "JSON_OVERLAPS" and len(node.expressions) == 2:
        a, b = node.expressions
        aj = exp.Cast(this=a, to=exp.DataType.build("JSON"))
        bj = exp.Cast(this=b, to=exp.DataType.build("JSON"))
        return exp.or_(
            exp.Anonymous(this="json_contains", expressions=[aj, bj]),
            exp.Anonymous(this="json_contains", expressions=[bj, aj]),
        )
    # MySQL DATETIME('...') is a timestamp constructor. DuckDB has no DATETIME().
    if isinstance(node, exp.Datetime):
        return exp.Cast(this=node.this, to=exp.DataType.build("TIMESTAMP"))
    # MySQL ASIN/ACOS return NULL outside [-1, 1]. DuckDB errors.
    if isinstance(node, (exp.Asin, exp.Acos)):
        x = node.this.transform(rewrite_mysql_for_duckdb) if node.this is not None else node.this
        return exp.If(
            this=exp.GT(this=exp.Abs(this=x), expression=exp.Literal.number(1)),
            true=exp.Null(),
            false=node.__class__(this=x),
        )
    # MySQL DAYNAME accepts many types; integers are YYYYMMDD. DuckDB only DATE/TIMESTAMP.
    if isinstance(node, exp.Dayname):
        x = node.this.transform(rewrite_mysql_for_duckdb) if node.this is not None else node.this
        as_ts = exp.TryCast(this=x, to=exp.DataType.build("TIMESTAMP"))
        as_ymd = exp.Anonymous(
            this="try_strptime",
            expressions=[
                exp.Anonymous(
                    this="lpad",
                    expressions=[
                        exp.TryCast(this=x, to=exp.DataType.build("TEXT")),
                        exp.Literal.number(8),
                        exp.Literal.string("0"),
                    ],
                ),
                exp.Literal.string("%%Y%%m%%d"),
            ],
        )
        return exp.Anonymous(
            this="dayname",
            expressions=[exp.Anonymous(this="coalesce", expressions=[as_ts, as_ymd])],
        )
    # MySQL CASE allows mixed string/number results. DuckDB requires one type.
    if isinstance(node, exp.Case):
        ifs = list(node.args.get("ifs") or [])
        new_ifs = []
        for iff in ifs:
            cond = iff.args.get("this")
            true = iff.args.get("true")
            if cond is not None:
                cond = cond.transform(rewrite_mysql_for_duckdb)
            if true is not None:
                true = true.transform(rewrite_mysql_for_duckdb)
            new_ifs.append(exp.If(this=cond, true=true))
        default = node.args.get("default")
        if default is not None:
            default = default.transform(rewrite_mysql_for_duckdb)
        this = node.this.transform(rewrite_mysql_for_duckdb) if node.this is not None else None
        branches = [iff.args.get("true") for iff in new_ifs]
        if default is not None:
            branches.append(default)
        def _is_str(e):
            return isinstance(e, exp.Literal) and e.is_string
        has_str = any(_is_str(b) for b in branches if b is not None)
        has_other = any(b is not None and not _is_str(b) for b in branches)
        if has_str and has_other:
            def _as_text(e):
                if e is None:
                    return e
                return exp.Cast(this=e, to=exp.DataType.build("TEXT"))
            new_ifs = [
                exp.If(this=iff.args.get("this"), true=_as_text(iff.args.get("true")))
                for iff in new_ifs
            ]
            if default is not None:
                default = _as_text(default)
        args = {"ifs": new_ifs}
        if this is not None:
            args["this"] = this
        if default is not None:
            args["default"] = default
        return exp.Case(**args)
    # MySQL treats a string predicate as a number: invalid strings are 0, not an error.
    def _mysql_string_as_number(e):
        return exp.Anonymous(
            this="coalesce",
            expressions=[
                exp.TryCast(this=e, to=exp.DataType.build("DOUBLE")),
                exp.Literal.number(0),
            ],
        )
    if isinstance(node, exp.Where) and isinstance(node.this, exp.Literal) and node.this.is_string:
        return exp.Where(
            this=exp.NEQ(this=_mysql_string_as_number(node.this), expression=exp.Literal.number(0))
        )
    if isinstance(node, exp.Not) and isinstance(node.this, exp.Literal) and node.this.is_string:
        return exp.EQ(this=_mysql_string_as_number(node.this), expression=exp.Literal.number(0))
    # Bare column predicates: WHERE v / NOT v / v AND v.
    if isinstance(node, exp.Where) and isinstance(node.this, exp.Column):
        return exp.Where(
            this=exp.NEQ(this=_mysql_string_as_number(node.this), expression=exp.Literal.number(0))
        )
    if isinstance(node, exp.Not) and isinstance(node.this, exp.Column):
        return exp.EQ(this=_mysql_string_as_number(node.this), expression=exp.Literal.number(0))
    # MySQL compares strings to numbers by coercing the string (invalid -> 0).
    # Do not rewrite id = 1; only inequalities may coerce a column.
    if isinstance(node, (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.EQ, exp.NEQ)):
        left, right = node.this, node.expression
        def _is_num_lit(e):
            return isinstance(e, exp.Literal) and e.is_number
        def _is_str_lit(e):
            return isinstance(e, exp.Literal) and e.is_string
        def _needs_num(e):
            if _is_str_lit(e):
                return True
            if isinstance(e, exp.Column) and isinstance(node, (exp.GT, exp.GTE, exp.LT, exp.LTE)):
                return True
            return False
        if _needs_num(left) and _is_num_lit(right):
            return node.__class__(this=_mysql_string_as_number(left), expression=right)
        if _is_num_lit(left) and _needs_num(right):
            return node.__class__(this=left, expression=_mysql_string_as_number(right))
        # MySQL DATE vs '2019-12-31T00:00:01' keeps the time. DuckDB casts the
        # string to DATE and drops it, so equality is wrongly true.
        def _is_iso_dt(e):
            if isinstance(e, exp.Literal) and e.is_string:
                s = str(e.this)
                return "T" in s and ":" in s
            return False
        if _is_iso_dt(right):
            return node.__class__(
                this=left,
                expression=exp.Cast(this=right, to=exp.DataType.build("TIMESTAMP")),
            )
        if _is_iso_dt(left):
            return node.__class__(
                this=exp.Cast(this=left, to=exp.DataType.build("TIMESTAMP")),
                expression=right,
            )
    # MySQL BETWEEN / IN coerce mixed strings, numbers, and booleans to numbers.
    def _unwrap_paren(e):
        while isinstance(e, exp.Paren):
            e = e.this
        return e
    def _bool_to_int(e):
        if isinstance(e, exp.Boolean):
            return exp.Literal.number(1 if e.this else 0)
        return e
    def _lit_str_to_num(e):
        e = _unwrap_paren(e)
        if isinstance(e, exp.Literal) and e.is_string:
            return _mysql_string_as_number(e)
        return _bool_to_int(e)
    if isinstance(node, exp.Between):
        this = node.this.transform(rewrite_mysql_for_duckdb) if node.this is not None else node.this
        low = node.args.get("low")
        high = node.args.get("high")
        if low is not None:
            low = low.transform(rewrite_mysql_for_duckdb)
        if high is not None:
            high = high.transform(rewrite_mysql_for_duckdb)
        cores = (_unwrap_paren(this), _unwrap_paren(low), _unwrap_paren(high))
        has_str = any(isinstance(e, exp.Literal) and e.is_string for e in cores)
        has_num = any(
            e is not None and (
                (isinstance(e, exp.Literal) and e.is_number)
                or isinstance(e, (exp.Cast, exp.Boolean))
            )
            for e in cores
        )
        if has_str and has_num:
            this, low, high = _lit_str_to_num(this), _lit_str_to_num(low), _lit_str_to_num(high)
            return exp.Cast(
                this=exp.Between(this=this, low=low, high=high),
                to=exp.DataType.build("INT"),
            )
        return exp.Between(this=this, low=low, high=high)
    if isinstance(node, exp.In):
        query = node.args.get("query")
        if query is not None or (not node.expressions and node.args.get("unbound")):
            this = node.this.transform(rewrite_mysql_for_duckdb) if node.this is not None else node.this
            if query is not None:
                query = query.transform(rewrite_mysql_for_duckdb)
            # DuckDB IN expects one column. (a, b) IN (SELECT x, y) -> EXISTS.
            if query is not None and isinstance(this, exp.Tuple):
                sel = query.this if isinstance(query, exp.Subquery) else query
                if isinstance(sel, exp.Select):
                    new_sel = sel.copy()
                    aliases = []
                    new_exprs = []
                    for i, e in enumerate(new_sel.expressions or []):
                        name = e.alias if isinstance(e, exp.Alias) and e.alias else ("_c" + str(i))
                        aliases.append(name)
                        if isinstance(e, exp.Alias) and e.alias:
                            new_exprs.append(e)
                        else:
                            new_exprs.append(exp.alias_(e, name))
                    new_sel.set("expressions", new_exprs)
                    eqs = []
                    for lhs, name in zip(this.expressions or [], aliases):
                        eqs.append(exp.EQ(this=lhs, expression=exp.column(name, table="q")))
                    cond = eqs[0]
                    for e in eqs[1:]:
                        cond = exp.And(this=cond, expression=e)
                    return exp.Exists(
                        this=exp.Select(
                            expressions=[exp.Literal.number(1)],
                            from_=exp.From(this=exp.Subquery(this=new_sel, alias="q")),
                            where=exp.Where(this=cond),
                        )
                    )
            kwargs = {"this": this}
            if query is not None:
                kwargs["query"] = query
            return exp.In(**kwargs)
        this = node.this.transform(rewrite_mysql_for_duckdb) if node.this is not None else node.this
        exprs = [
            e.transform(rewrite_mysql_for_duckdb) if e is not None else e
            for e in list(node.expressions or [])
        ]
        parts = [this] + exprs
        has_bool = any(isinstance(e, exp.Boolean) for e in parts)
        has_str = any(isinstance(e, exp.Literal) and e.is_string for e in exprs)
        has_num = any(isinstance(e, exp.Literal) and e.is_number for e in exprs)
        if has_bool and has_str:
            this = _lit_str_to_num(this)
            exprs = [_lit_str_to_num(e) for e in exprs]
            return exp.Cast(
                this=exp.In(this=this, expressions=exprs),
                to=exp.DataType.build("INT"),
            )
        # s IN (1, 'first_row'): compare each item with MySQL's pairwise coercion.
        if has_str and has_num:
            parts_eq = []
            for e in exprs:
                if isinstance(e, exp.Literal) and e.is_number:
                    parts_eq.append(exp.EQ(this=_mysql_string_as_number(this), expression=e))
                else:
                    parts_eq.append(exp.EQ(this=this, expression=e))
            cond = parts_eq[0]
            for p in parts_eq[1:]:
                cond = exp.Or(this=cond, expression=p)
            return cond
        return exp.In(this=this, expressions=exprs)
    def _mysql_truthy(e):
        # MySQL AND/OR coerce strings and numbers: invalid strings are 0.
        if isinstance(e, exp.Column) or (isinstance(e, exp.Literal) and (e.is_string or e.is_number)):
            return exp.NEQ(this=_mysql_string_as_number(e), expression=exp.Literal.number(0))
        return None
    if isinstance(node, (exp.And, exp.Or)):
        # Transform is pre-order; rewrite nested OR/AND before coercing this node.
        left = node.this.transform(rewrite_mysql_for_duckdb) if node.this is not None else node.this
        right = node.expression.transform(rewrite_mysql_for_duckdb) if node.expression is not None else node.expression
        changed = False
        new_left = _mysql_truthy(left)
        if new_left is not None:
            left = new_left
            changed = True
        new_right = _mysql_truthy(right)
        if new_right is not None:
            right = new_right
            changed = True
        cond = node.__class__(this=left, expression=right)
        if changed:
            # MySQL AND/OR yield 0/1/NULL, used as LIKE patterns and BETWEEN bounds.
            return exp.Cast(this=cond, to=exp.DataType.build("INT"))
        return cond
    # LIKE 0 must match the string '0', not a boolean.
    # Transform is pre-order; rewrite the pattern first so string OR becomes 0/1.
    if isinstance(node, exp.Like):
        this = node.this.transform(rewrite_mysql_for_duckdb) if node.this is not None else node.this
        pat = node.args.get("expression")
        if pat is not None:
            pat = pat.transform(rewrite_mysql_for_duckdb)
        core = pat.this if isinstance(pat, exp.Paren) else pat
        if not (isinstance(core, exp.Literal) and core.is_string):
            pat = exp.Cast(this=pat, to=exp.DataType.build("TEXT"))
        return exp.Like(this=this, expression=pat)
    # MySQL TRUE/FALSE are 1/0 in bitwise operators. DuckDB rejects BOOLEAN | INTEGER.
    def _bool_to_int(e):
        if isinstance(e, exp.Boolean):
            return exp.Literal.number(1 if e.this else 0)
        return e
    if isinstance(node, (exp.BitwiseOr, exp.BitwiseAnd, exp.BitwiseXor)):
        left, right = _bool_to_int(node.this), _bool_to_int(node.expression)
        if left is not node.this or right is not node.expression:
            return node.__class__(this=left, expression=right)
    # MySQL DATE_FORMAT %%D is 1st/2nd/3rd/4th. DuckDB %%D is not that.
    if isinstance(node, exp.TimeToStr):
        fmt = node.args.get("format")
        ts = node.this
        if isinstance(fmt, exp.Literal) and fmt.is_string and str(fmt.this) == "%%D":
            d = exp.Anonymous(this="day", expressions=[ts])
            suffix = exp.Case(
                this=exp.Anonymous(this="mod", expressions=[d, exp.Literal.number(10)]),
                ifs=[
                    exp.If(this=exp.Literal.number(1), true=exp.Literal.string("st")),
                    exp.If(this=exp.Literal.number(2), true=exp.Literal.string("nd")),
                    exp.If(this=exp.Literal.number(3), true=exp.Literal.string("rd")),
                ],
                default=exp.Literal.string("th"),
            )
            teen = exp.In(this=d, expressions=[exp.Literal.number(11), exp.Literal.number(12), exp.Literal.number(13)])
            suf = exp.If(this=teen, true=exp.Literal.string("th"), false=suffix)
            return exp.Anonymous(
                this="concat",
                expressions=[exp.Cast(this=d, to=exp.DataType.build("TEXT")), suf],
            )
    # MySQL CAST(negative AS UNSIGNED) wraps in 2^64. DuckDB UBIGINT rejects negatives.
    if isinstance(node, exp.Cast):
        to = node.args.get("to")
        inner = node.this
        if to is not None and not isinstance(inner, (exp.If, exp.Case)):
            to_sql = to.sql(dialect="duckdb").upper()
            # MySQL CAST(x AS DECIMAL) is integer-scale. DuckDB DECIMAL is (18,3).
            if to_sql in ("DECIMAL", "NUMERIC"):
                return exp.Cast(this=inner, to=exp.DataType.build("DECIMAL(38, 0)"))
            if to_sql.startswith("U") and "INT" in to_sql:
                wrapped = exp.If(
                    this=exp.LT(this=inner, expression=exp.Literal.number(0)),
                    true=exp.Add(this=inner, expression=exp.Literal.number(18446744073709551616)),
                    false=inner,
                )
                return exp.Cast(this=wrapped, to=to)
    # MySQL allows ORDER BY a non-selected column on a lone aggregate.
    # The result is one row, so drop the ORDER BY for DuckDB.
    if isinstance(node, exp.Select) and node.args.get("order") is not None and node.args.get("group") is None:
        exprs = list(node.expressions or [])
        def _is_agg_or_lit(e):
            if e is None or isinstance(e, exp.Star):
                return False
            if isinstance(e, exp.Literal):
                return True
            return any(isinstance(x, exp.AggFunc) for x in e.walk())
        if exprs and all(_is_agg_or_lit(e) for e in exprs):
            updated = node.copy()
            updated.set("order", None)
            return updated
    # MySQL allows SELECT pk, SUM(c) without GROUP BY when ONLY_FULL_GROUP_BY is off.
    # DuckDB requires the extra columns to be aggregated; ANY_VALUE matches that mode.
    if isinstance(node, exp.Select) and node.args.get("group") is None:
        exprs = list(node.expressions or [])
        def _has_agg(e):
            return any(isinstance(x, exp.AggFunc) for x in e.walk())
        def _already_any_value(e):
            inner = e.this if isinstance(e, exp.Alias) else e
            return isinstance(inner, exp.Anonymous) and str(inner.this).lower() == "any_value"
        def _has_column(e):
            return any(isinstance(x, exp.Column) for x in e.walk())
        if exprs and any(_has_agg(e) for e in exprs) and any(not _has_agg(e) for e in exprs):
            new_exprs = []
            for e in exprs:
                if _has_agg(e) or isinstance(e, exp.Star) or _already_any_value(e) or not _has_column(e):
                    new_exprs.append(e)
                    continue
                alias = e.alias if isinstance(e, exp.Alias) else None
                inner = e.this if isinstance(e, exp.Alias) else e
                wrapped = exp.Anonymous(this="any_value", expressions=[inner])
                if alias:
                    wrapped = exp.alias_(wrapped, alias)
                new_exprs.append(wrapped)
            updated = node.copy()
            updated.set("expressions", new_exprs)
            return updated
    # MySQL also allows SELECT s, i GROUP BY i. Wrap non-grouped, non-agg columns.
    if isinstance(node, exp.Select) and node.args.get("group") is not None:
        exprs = list(node.expressions or [])
        group_exprs = list(node.args.get("group").expressions or [])
        group_keys = set()
        for g in group_exprs:
            group_keys.add(g.sql(dialect="duckdb").lower())
            if isinstance(g, exp.Literal) and g.is_int:
                group_keys.add(str(int(g.this)))
        def _has_agg(e):
            return any(isinstance(x, exp.AggFunc) for x in e.walk())
        def _already_any_value(e):
            inner = e.this if isinstance(e, exp.Alias) else e
            return isinstance(inner, exp.Anonymous) and str(inner.this).lower() == "any_value"
        def _in_group(e, idx):
            if str(idx) in group_keys:
                return True
            ident = e.sql(dialect="duckdb").lower()
            if ident in group_keys:
                return True
            inner = e.this if isinstance(e, exp.Alias) else e
            return inner.sql(dialect="duckdb").lower() in group_keys
        new_exprs = []
        changed = False
        def _has_column(e):
            return any(isinstance(x, exp.Column) for x in e.walk())
        for idx, e in enumerate(exprs, 1):
            if _has_agg(e) or isinstance(e, exp.Star) or _already_any_value(e) or _in_group(e, idx) or not _has_column(e):
                new_exprs.append(e)
                continue
            alias = e.alias if isinstance(e, exp.Alias) else None
            inner = e.this if isinstance(e, exp.Alias) else e
            wrapped = exp.Anonymous(this="any_value", expressions=[inner])
            if alias:
                wrapped = exp.alias_(wrapped, alias)
            new_exprs.append(wrapped)
            changed = True
        if changed:
            updated = node.copy()
            updated.set("expressions", new_exprs)
            return updated
    # MySQL HAVING can use SELECT aliases. Substitute the aliased expression.
    if isinstance(node, exp.Select) and node.args.get("having") is not None:
        aliases = {}
        for e in node.expressions or []:
            if isinstance(e, exp.Alias) and e.alias:
                aliases[str(e.alias).lower()] = e.this
        if aliases:
            def _replace_having_alias(n):
                if isinstance(n, exp.Column) and not n.table and n.name and n.name.lower() in aliases:
                    return aliases[n.name.lower()]
                return n
            having = node.args.get("having")
            new_having = having.transform(_replace_having_alias)
            if new_having is not having:
                updated = node.copy()
                updated.set("having", new_having)
                return updated
    return node

def transpile_mysql_to_duckdb(sql: str) -> str:
    import re
    # MySQL allows t.AND / t.OR / t.SELECT as identifiers. Quote so sqlglot parses them.
    bq = chr(96)
    sql = re.sub(r'\.(AND|OR|SELECT)\b', lambda m: '.' + bq + m.group(1) + bq, sql, flags=re.I)
    try:
        trees = sqlglot.parse(sql, read="mysql")
    except Exception:
        # SQLGlot's MySQL parser rejects DELETE ... LIMIT n OFFSET m.
        import re
        m = re.match(
            r"(?is)^(.*?delete\s+from\s+)([A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)*)(\s+where\s+.*?)?(\s+order\s+by\s+.+)$",
            sql.strip().rstrip(";").strip(),
        )
        if not m:
            raise
        prefix, table, where, tail = m.group(1), m.group(2), m.group(3) or "", m.group(4)
        sql = prefix + table + " WHERE rowid IN (SELECT rowid FROM " + table + where + tail + ")"
        trees = sqlglot.parse(sql, read="mysql")
    if not trees or trees[0] is None:
        return ""
    # Keep the historical contract: only the first statement is returned.
    # VALUES(col) is NULL in SELECT, but INSERT ON DUPLICATE KEY must keep it.
    in_insert = [0]
    def rewrite_with_insert_context(node):
        if isinstance(node, exp.Insert):
            in_insert[0] += 1
        if isinstance(node, exp.Anonymous) and str(node.this).upper() == "VALUES":
            if in_insert[0]:
                return node
            return exp.Null()
        return rewrite_mysql_for_duckdb(node)
    return trees[0].transform(rewrite_with_insert_context).sql(dialect="duckdb")

while True:
    inp = read_string()
    if inp == CMD_EXIT:
        break
    if inp.startswith(CMD_RUN):
        sql = inp[len(CMD_RUN):]
        try:
            result = transpile_mysql_to_duckdb(sql)
            write_string(RESULT_OK + result)
        except Exception as e:
            write_string(RESULT_ERR + str(e))
`, cmdExit, cmdRun, resultOK, resultErr)

	pyCmd := exec.Command(pythonPath, "-u", "-c", pythonScript)

	pyStdin, err := pyCmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdin pipe: %v", err)
	}

	pyStdout, err := pyCmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %v", err)
	}

	var stderrBuf bytes.Buffer
	pyCmd.Stderr = &stderrBuf

	err = pyCmd.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start Python process: %v", err)
	}

	svc := &translateService{
		mu:       &sync.Mutex{},
		pyCmd:    pyCmd,
		pyStdin:  pyStdin,
		pyStdout: bufio.NewReader(pyStdout),
		pyStderr: &stderrBuf,
	}

	// Test the translation service with a simple query
	testSQL := "SELECT 1"
	translatedSQL, err := svc.translate(testSQL)
	if err != nil {
		svc.cleanup()
		return nil, fmt.Errorf("failed to test translation service: %v", err)
	}
	if translatedSQL != "SELECT 1" {
		svc.cleanup()
		return nil, fmt.Errorf("unexpected translation result: %s", translatedSQL)
	}

	return svc, nil
}

func (svc *translateService) translate(sql string) (string, error) {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	translatedSQL, err := translateInternalImpl(svc.pyStdin, svc.pyStdout, sql)
	if err != nil {
		if errors.Is(err, errPythonProcessUnhealthy) {
			panic(fmt.Errorf("%v\ncmd:\n%s\nstderr:\n%s", err, svc.pyCmd.String(), svc.pyStderr.String()))
		}
		return "", err
	}
	return translatedSQL, nil
}

func translateInternalImpl(pyStdin io.Writer, pyStdout io.Reader, sql string) (string, error) {
	err := sendString(pyStdin, cmdRun+sql)
	if err != nil {
		return "", errPythonProcessUnhealthy.New(err)
	}

	result, err := recvString(pyStdout)
	if err != nil {
		return "", errPythonProcessUnhealthy.New(err)
	}

	result = strings.TrimSpace(result)

	if strings.HasPrefix(result, resultErr) {
		return "", fmt.Errorf(result[len(resultErr):])
	} else if strings.HasPrefix(result, resultOK) {
		return strings.TrimSpace(result[len(resultOK):]), nil
	} else {
		return "", fmt.Errorf("unexpected result: %s", result)
	}
}

// the schema is 4 bytes length + data
func sendString(writer io.Writer, str string) error {
	data := []byte(str)
	length := len(data)
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	_, err := writer.Write(lengthBytes)
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	return err
}

func recvString(reader io.Reader) (string, error) {
	lengthBytes := make([]byte, 4)
	_, err := io.ReadFull(reader, lengthBytes)
	if err != nil {
		return "", err
	}
	length := binary.BigEndian.Uint32(lengthBytes)
	data := make([]byte, length)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (svc *translateService) cleanup() {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	sendString(svc.pyStdin, cmdExit)
	svc.pyCmd.Wait()
}

func TranslateWithSQLGlot(sql string) (string, error) {
	translationSvcOnce.Do(func() {
		svc, err := newTranslateService()
		if err != nil {
			panic(fmt.Errorf("failed to initialize translation service: %v", err))
		}
		translationSvc = svc
	})

	return translationSvc.translate(sql)
}

func getPythonPath() (string, error) {
	// Try to find python3 in the system PATH
	pythonPath, err := exec.LookPath("python3")
	if err == nil {
		return pythonPath, nil
	}

	// If python3 is not found, try to find python
	pythonPath, err = exec.LookPath("python")
	if err == nil {
		return pythonPath, nil
	}

	// If neither python3 nor python is found, return an error
	return "", fmt.Errorf("neither python3 nor python was found in PATH")
}
