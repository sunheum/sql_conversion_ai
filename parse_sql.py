import os
import re
import json
import uuid
import hashlib
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import ParseError


# -----------------------------
# Config
# -----------------------------
DEFAULT_MAX_CHARS = 1000
DEFAULT_DIALECT = "oracle"

# (A) MyBatis 바인딩 / 치환
RE_MYBATIS_BIND = re.compile(r"#\{[^}]*\}")   # #{...}
RE_MYBATIS_DOLLAR = re.compile(r"\$\{[^}]*\}")  # ${...}  (필요하면 활성화)

# (B) 기존 #VAR# 토큰
RE_HASH_VAR = re.compile(r"#([A-Za-z0-9_]+)#")

# (C) MyBatis XML 동적 태그들: <if>, </if>, <foreach ...>, </foreach>, ...
#     - SQL의 <> 비교와 충돌하지 않도록 "알려진 태그명"만 매칭
MYBATIS_TAGS = [
    "if", "choose", "when", "otherwise",
    "where", "set", "trim", "foreach",
    "bind", "include", "sql", "selectKey",
    # mapper 상단에서 나올 수 있는 것들(있어도 안전)
    "select", "insert", "update", "delete",
]
RE_MYBATIS_XML_TAG = re.compile(
    rf"</?\s*(?:{'|'.join(MYBATIS_TAGS)})\b[^>]*?>",
    re.IGNORECASE
)

# (D) CDATA도 만약 섞여있으면 파싱 깨질 수 있음
RE_CDATA_OPEN = re.compile(r"<!\[CDATA\[", re.IGNORECASE)
RE_CDATA_CLOSE = re.compile(r"\]\]>")

# (E) 유니코드 연산자 (>=, <=, <> 로 치환 + 주석 마커로 원복 가능하게)
UNICODE_OPS = {
    "≥": ">=",
    "≤": "<=",
    "≠": "<>",
}


# -----------------------------
# Data structures
# -----------------------------
@dataclass
class SQLPart:
    statement_index: int
    original_sql: str              # masked
    main_sql: str                  # masked
    ctes: Dict[str, str]           # masked
    meta: Dict[str, object]
    placeholder_map: Dict[str, str]  # replacement -> original


# -----------------------------
# Placeholder mask / unmask
# -----------------------------
def mask_placeholders(sql: str) -> Tuple[str, Dict[str, str]]:
    """
    아래 항목을 모두 '파싱 안전' 형태로 치환:
      1) MyBatis XML 동적 태그(<if> 등) -> /*__MBTAG__...__*/ (SQL 주석)
      2) CDATA -> /*__CDATA__...__*/ (SQL 주석)
      3) 유니코드 연산자(≥/≤/≠) -> >=/*__UOP__*/ (ASCII 연산자 + 주석)
      4) MyBatis #{...}, ${...}, #VAR# -> '__PH__...__' (문자열 리터럴)

    반환 mapping은 replacement -> original 로 저장되어 unmask로 원복 가능.
    """
    run_id = uuid.uuid4().hex
    mapping: Dict[str, str] = {}
    counter = 0

    def new_comment_marker(prefix: str) -> str:
        nonlocal counter
        marker = f"/*__{prefix}__{run_id}__{counter}__*/"
        counter += 1
        return marker

    def new_string_marker() -> str:
        nonlocal counter
        marker = f"'__PH__{run_id}__{counter}__'"
        counter += 1
        return marker

    masked = sql

    # 1) MyBatis XML 태그: 주석으로 치환 (문자열리터럴로 넣으면 SQL 구문을 깨기 쉬움)
    def repl_xml(m: re.Match) -> str:
        original = m.group(0)
        marker = new_comment_marker("MBTAG")
        mapping[marker] = original
        return marker

    masked = RE_MYBATIS_XML_TAG.sub(repl_xml, masked)

    # 2) CDATA 처리
    def repl_cdata_open(m: re.Match) -> str:
        original = m.group(0)
        marker = new_comment_marker("CDATA_OPEN")
        mapping[marker] = original
        return marker

    def repl_cdata_close(m: re.Match) -> str:
        original = m.group(0)
        marker = new_comment_marker("CDATA_CLOSE")
        mapping[marker] = original
        return marker

    masked = RE_CDATA_OPEN.sub(repl_cdata_open, masked)
    masked = RE_CDATA_CLOSE.sub(repl_cdata_close, masked)

    # 3) 유니코드 연산자: ASCII 연산자 + 주석 마커로 바꿔서 파서가 인식하게 만들고 원복도 가능하게
    #    예: ≥  -> >=/*__UOP__...__*/   (unmask 시 이 전체 문자열을 ≥로 되돌림)
    for uop, ascii_op in UNICODE_OPS.items():
        if uop in masked:
            # 각 등장마다 다른 marker를 부여해야 정확히 원복됨
            parts = masked.split(uop)
            if len(parts) > 1:
                rebuilt = [parts[0]]
                for _ in range(len(parts) - 1):
                    marker = new_comment_marker("UOP")
                    replacement = f"{ascii_op}{marker}"
                    mapping[replacement] = uop
                    rebuilt.append(replacement)
                    rebuilt.append(parts[len(rebuilt)//2] if False else "")  # placeholder, not used
                # 위 방식은 리스트 관리가 지저분하므로 안전하게 재구성
                # -> 아래에서 더 깔끔하게 재구성
                pass

    # 깔끔한 방식으로 재구성 (유니코드 연산자별로 정규식 치환)
    def replace_unicode_op(text: str, uop: str, ascii_op: str) -> str:
        # uop를 하나씩 치환하면서 mapping을 쌓는다
        while uop in text:
            marker = new_comment_marker("UOP")
            replacement = f"{ascii_op}{marker}"
            mapping[replacement] = uop
            text = text.replace(uop, replacement, 1)
        return text

    for uop, ascii_op in UNICODE_OPS.items():
        masked = replace_unicode_op(masked, uop, ascii_op)

    # 4) MyBatis #{...} / ${...} / #VAR#: 문자열 리터럴로 치환
    #    - 파싱 목적이므로 값이 뭔지 중요하지 않다(원복은 mapping으로)
    def repl_stringish(m: re.Match) -> str:
        original = m.group(0)
        marker = new_string_marker()
        mapping[marker] = original
        return marker

    # #{...} 먼저
    masked = RE_MYBATIS_BIND.sub(repl_stringish, masked)

    # ${...} 도 쓰면 활성화 (원하면 아래 주석 해제)
    masked = RE_MYBATIS_DOLLAR.sub(repl_stringish, masked)

    # #VAR#
    masked = RE_HASH_VAR.sub(repl_stringish, masked)

    return masked, mapping


def unmask_placeholders(sql: str, mapping: Dict[str, str]) -> str:
    # 긴 replacement부터 치환(부분 겹침 방지)
    for replacement in sorted(mapping.keys(), key=len, reverse=True):
        sql = sql.replace(replacement, mapping[replacement])
    return sql


# -----------------------------
# Helpers
# -----------------------------
def normalize_sql(s: str) -> str:
    return s.strip().rstrip(";").strip()


def parse_statements(sql_text: str, dialect: str) -> List[exp.Expression]:
    return sqlglot.parse(sql_text, read=dialect)


def split_sql_text_statements(sql_text: str) -> List[str]:
    """
    세미콜론 기준 statement 분리.
    문자열 리터럴/식별자("...", `...`) 내부의 세미콜론은 무시한다.
    """
    statements: List[str] = []
    buf: List[str] = []
    in_single = False
    in_double = False
    in_backtick = False

    i = 0
    while i < len(sql_text):
        ch = sql_text[i]

        if ch == "'" and not in_double and not in_backtick:
            # Oracle style escaped quote('') 처리
            if in_single and i + 1 < len(sql_text) and sql_text[i + 1] == "'":
                buf.append(ch)
                buf.append(sql_text[i + 1])
                i += 2
                continue
            in_single = not in_single
            buf.append(ch)
            i += 1
            continue

        if ch == '"' and not in_single and not in_backtick:
            in_double = not in_double
            buf.append(ch)
            i += 1
            continue

        if ch == "`" and not in_single and not in_double:
            in_backtick = not in_backtick
            buf.append(ch)
            i += 1
            continue

        if ch == ";" and not in_single and not in_double and not in_backtick:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def _balance_parentheses(sql_text: str) -> str:
    """단순 괄호 밸런싱(문자열 리터럴 밖 기준)."""
    in_single = False
    opens = 0
    closes = 0
    i = 0
    while i < len(sql_text):
        ch = sql_text[i]
        if ch == "'":
            if in_single and i + 1 < len(sql_text) and sql_text[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
        elif not in_single:
            if ch == "(":
                opens += 1
            elif ch == ")":
                closes += 1
        i += 1

    if opens > closes:
        return sql_text + (")" * (opens - closes))
    return sql_text


def _is_known_unsupported_oracle_block(sql_text: str) -> bool:
    head = sql_text.lstrip().upper()
    return head.startswith(("DECLARE", "BEGIN"))


def parse_statement_with_recovery(sql_text: str, dialect: str) -> Tuple[Optional[exp.Expression], Optional[str]]:
    """
    sqlglot 파싱 실패 시 일반적으로 실패하는 Oracle 패턴을 완화해 재시도한다.
    최종 실패하면 (None, reason)을 반환한다.
    """
    if _is_known_unsupported_oracle_block(sql_text):
        return None, "unsupported_plsql_block"

    candidates = [sql_text]
    balanced = _balance_parentheses(sql_text)
    if balanced != sql_text:
        candidates.append(balanced)

    for cand in candidates:
        try:
            return sqlglot.parse_one(cand, read=dialect), None
        except ParseError as e:
            last_err = str(e)
        except Exception as e:  # pragma: no cover - 방어적 처리
            last_err = str(e)

    return None, last_err


def to_sql(node: exp.Expression, dialect: str) -> str:
    return node.sql(dialect=dialect)


def _get_query_expr(stmt: exp.Expression) -> Optional[exp.Expression]:
    if isinstance(stmt, exp.Insert):
        return stmt.args.get("expression")
    if isinstance(stmt, (exp.Select, exp.Union, exp.With)):
        return stmt
    return None


def traverse_with_depth(node: exp.Expression, depth: int = 0, parent: Optional[exp.Expression] = None):
    yield node, depth, parent
    for child in node.args.values():
        if child is None:
            continue
        if isinstance(child, exp.Expression):
            yield from traverse_with_depth(child, depth + 1, node)
        elif isinstance(child, list):
            for item in child:
                if isinstance(item, exp.Expression):
                    yield from traverse_with_depth(item, depth + 1, node)


def _parent_is_from_or_join(parent: Optional[exp.Expression]) -> bool:
    return isinstance(parent, (exp.From, exp.Join))


def _safe_cte_name(alias_name: Optional[str], inner_sql: str) -> str:
    h = hashlib.sha1(inner_sql.encode("utf-8")).hexdigest()[:10]
    u = uuid.uuid4().hex[:8]
    suffix = re.sub(r"[^A-Za-z0-9_]", "_", alias_name or "part")
    return f"cte__{h}__{u}__{suffix}"


def _make_identifier(name: str) -> exp.Identifier:
    return exp.Identifier(this=name)


def _make_table(name: str, alias: Optional[str] = None) -> exp.Expression:
    t = exp.Table(this=_make_identifier(name))
    if alias:
        t = t.as_(alias)
    return t


def _extract_alias_from_tablealias(table_alias: Optional[exp.Expression]) -> Optional[str]:
    if isinstance(table_alias, exp.TableAlias):
        ident = table_alias.args.get("this")
        if isinstance(ident, exp.Identifier):
            return ident.this
    return None


def _derived_info(node: exp.Expression) -> Optional[Tuple[exp.Expression, Optional[str]]]:
    if isinstance(node, exp.Subquery) and isinstance(node.args.get("this"), exp.Expression):
        inner = node.args["this"]
        alias_name = _extract_alias_from_tablealias(node.args.get("alias"))
        return inner, alias_name
    return None


def _replace_child_in_parent(parent: exp.Expression, old: exp.Expression, new: exp.Expression) -> bool:
    for k, v in parent.args.items():
        if v is old:
            parent.set(k, new)
            return True
        if isinstance(v, list):
            for i, item in enumerate(v):
                if item is old:
                    v[i] = new
                    return True
    return False


# -----------------------------
# Split core (IN-PLACE replacement)
# -----------------------------
def split_long_statement_by_from_join_derived_tables(
    stmt: exp.Expression,
    statement_index: int,
    placeholder_map: Dict[str, str],
    dialect: str,
    max_chars: int,
    min_depth_to_extract: int = 0,
) -> SQLPart:
    original_sql_masked = normalize_sql(to_sql(stmt, dialect=dialect)) + ";"

    if len(normalize_sql(original_sql_masked)) < max_chars:
        return SQLPart(
            statement_index=statement_index,
            original_sql=original_sql_masked,
            main_sql=original_sql_masked,
            ctes={},
            meta={"split": False, "reason": "below_threshold", "extracted": []},
            placeholder_map=placeholder_map,
        )

    stmt2 = stmt.copy()
    query_expr = _get_query_expr(stmt2)
    if query_expr is None:
        return SQLPart(
            statement_index=statement_index,
            original_sql=original_sql_masked,
            main_sql=original_sql_masked,
            ctes={},
            meta={"split": False, "reason": "unsupported_statement_type", "extracted": []},
            placeholder_map=placeholder_map,
        )

    targets: List[Tuple[exp.Expression, exp.Expression, int, exp.Expression, Optional[str]]] = []
    for node, depth, parent in traverse_with_depth(query_expr):
        if depth < min_depth_to_extract:
            continue
        if parent is None or not _parent_is_from_or_join(parent):
            continue

        info = _derived_info(node)
        if info is None:
            continue
        inner_q, alias_name = info
        targets.append((node, parent, depth, inner_q, alias_name))

    if not targets:
        return SQLPart(
            statement_index=statement_index,
            original_sql=original_sql_masked,
            main_sql=original_sql_masked,
            ctes={},
            meta={"split": False, "reason": "no_targets", "extracted": []},
            placeholder_map=placeholder_map,
        )

    targets.sort(key=lambda t: t[2], reverse=True)

    ctes: Dict[str, str] = {}
    extracted_info = []
    used = set()

    for node, parent, depth, inner_q, alias_name in targets:
        inner_sql = normalize_sql(inner_q.sql(dialect=dialect))
        cte_name = _safe_cte_name(alias_name, inner_sql)
        while cte_name.lower() in used:
            cte_name = _safe_cte_name(alias_name, inner_sql)
        used.add(cte_name.lower())

        ctes[cte_name] = inner_sql + ";"

        replacement = _make_table(cte_name, alias=alias_name)
        replaced = _replace_child_in_parent(parent, node, replacement)

        extracted_info.append(
            {
                "cte_name": cte_name,
                "alias_name": alias_name,
                "depth": depth,
                "parent_type": type(parent).__name__,
                "replaced": replaced,
            }
        )

    main_sql_masked = normalize_sql(to_sql(stmt2, dialect=dialect)) + ";"

    return SQLPart(
        statement_index=statement_index,
        original_sql=original_sql_masked,
        main_sql=main_sql_masked,
        ctes=ctes,
        meta={"split": True, "reason": "extracted_from_join_derived_tables", "extracted": extracted_info},
        placeholder_map=placeholder_map,
    )


# -----------------------------
# Output
# -----------------------------
def write_parts(parts: List[SQLPart], out_dir: str, base_name: str, output_masked: bool = False):
    os.makedirs(out_dir, exist_ok=True)
    manifest = []

    for p in parts:
        idx = f"{p.statement_index:03d}"
        mp = p.placeholder_map

        def maybe_unmask(s: str) -> str:
            return s if output_masked else unmask_placeholders(s, mp)

        main_file = f"{base_name}__{idx}__main.sql"
        with open(os.path.join(out_dir, main_file), "w", encoding="utf-8") as f:
            f.write(maybe_unmask(p.main_sql).strip() + "\n")

        cte_files = {}
        for cte_name, cte_sql in p.ctes.items():
            fn = f"{base_name}__{idx}__{cte_name}.sql"
            with open(os.path.join(out_dir, fn), "w", encoding="utf-8") as f:
                f.write(maybe_unmask(cte_sql).strip() + "\n")
            cte_files[cte_name] = fn

        manifest.append(
            {
                "statement_index": p.statement_index,
                "main_file": main_file,
                "cte_files": cte_files,
                "meta": p.meta,
                "output_masked": output_masked,
            }
        )

    manifest_path = os.path.join(out_dir, f"{base_name}__manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


def split_sql_file(
    input_path: str,
    out_dir: str,
    dialect: str = DEFAULT_DIALECT,
    max_chars: int = DEFAULT_MAX_CHARS,
    min_depth_to_extract: int = 0,
    output_masked: bool = False,
):
    with open(input_path, "r", encoding="utf-8") as f:
        original_text = f.read()

    masked_text, mp = mask_placeholders(original_text)
    base = os.path.splitext(os.path.basename(input_path))[0]
    parts: List[SQLPart] = []

    raw_statements = split_sql_text_statements(masked_text)
    for i, raw_stmt in enumerate(raw_statements, start=1):
        parsed_stmt, err = parse_statement_with_recovery(raw_stmt, dialect=dialect)
        if parsed_stmt is None:
            fallback_sql = normalize_sql(raw_stmt) + ";"
            parts.append(
                SQLPart(
                    statement_index=i,
                    original_sql=fallback_sql,
                    main_sql=fallback_sql,
                    ctes={},
                    meta={"split": False, "reason": "parse_error", "parse_error": err, "extracted": []},
                    placeholder_map=mp,
                )
            )
            continue

        parts.append(
            split_long_statement_by_from_join_derived_tables(
                stmt=parsed_stmt,
                statement_index=i,
                placeholder_map=mp,
                dialect=dialect,
                max_chars=max_chars,
                min_depth_to_extract=min_depth_to_extract,
            )
        )

    write_parts(parts, out_dir=out_dir, base_name=base, output_masked=output_masked)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("out_dir")
    parser.add_argument("--dialect", default=DEFAULT_DIALECT)
    parser.add_argument("--max_chars", type=int, default=DEFAULT_MAX_CHARS)
    parser.add_argument("--min_depth", type=int, default=2)
    parser.add_argument("--output_masked", action="store_true")
    args = parser.parse_args()

    split_sql_file(
        args.input,
        args.out_dir,
        dialect=args.dialect,
        max_chars=args.max_chars,
        min_depth_to_extract=args.min_depth,
        output_masked=args.output_masked,
    )
