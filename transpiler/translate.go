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

def rewrite_mysql_for_duckdb(node):
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
            if to_sql.startswith("U") and "INT" in to_sql:
                wrapped = exp.If(
                    this=exp.LT(this=inner, expression=exp.Literal.number(0)),
                    true=exp.Add(this=inner, expression=exp.Literal.number(18446744073709551616)),
                    false=inner,
                )
                return exp.Cast(this=wrapped, to=to)
    # MySQL allows SELECT pk, SUM(c) without GROUP BY when ONLY_FULL_GROUP_BY is off.
    # DuckDB requires the extra columns to be aggregated; ANY_VALUE matches that mode.
    if isinstance(node, exp.Select) and node.args.get("group") is None:
        exprs = list(node.expressions or [])
        def _has_agg(e):
            return any(isinstance(x, exp.AggFunc) for x in e.walk())
        def _already_any_value(e):
            inner = e.this if isinstance(e, exp.Alias) else e
            return isinstance(inner, exp.Anonymous) and str(inner.this).lower() == "any_value"
        if exprs and any(_has_agg(e) for e in exprs) and any(not _has_agg(e) for e in exprs):
            new_exprs = []
            for e in exprs:
                if _has_agg(e) or isinstance(e, exp.Star) or _already_any_value(e):
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
        for idx, e in enumerate(exprs, 1):
            if _has_agg(e) or isinstance(e, exp.Star) or _already_any_value(e) or _in_group(e, idx):
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
    trees = sqlglot.parse(sql, read="mysql")
    if not trees or trees[0] is None:
        return ""
    # Keep the historical contract: only the first statement is returned.
    return trees[0].transform(rewrite_mysql_for_duckdb).sql(dialect="duckdb")

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
