import os
import re
import json
import uuid
import hashlib
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import sqlglot
from sqlglot import expressions as exp


# -----------------------------
# Config
# -----------------------------
DEFAULT_MAX_CHARS = 1000
DEFAULT_DIALECT = "oracle"

PH_PATTERNS = [
    re.compile(r"#([A-Za-z0-9_]+)#"),
]


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
def mask_placeholders(sql: str, patterns: List[re.Pattern]) -> Tuple[str, Dict[str, str]]:
    run_id = uuid.uuid4().hex
    mapping: Dict[str, str] = {}
    counter = 0

    def make_replacement(i: int) -> str:
        return f"'__PH__{run_id}__{i}__'"

    masked = sql
    for pat in patterns:
        def repl(m):
            nonlocal counter
            original = m.group(0)
            replacement = make_replacement(counter)
            mapping[replacement] = original
            counter += 1
            return replacement
        masked = pat.sub(repl, masked)

    return masked, mapping


def unmask_placeholders(sql: str, mapping: Dict[str, str]) -> str:
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


def to_sql(node: exp.Expression, dialect: str) -> str:
    return node.sql(dialect=dialect)


def _get_query_expr(stmt: exp.Expression) -> Optional[exp.Expression]:
    # INSERT ... SELECT 지원
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
        # FROM cte_name AS Z  (Oracle에서 AS 생략될 수 있음)
        t = t.as_(alias)
    return t


def _extract_alias_from_tablealias(table_alias: Optional[exp.Expression]) -> Optional[str]:
    # Subquery.alias is TableAlias(this=Identifier("Z")) 형태가 흔함
    if isinstance(table_alias, exp.TableAlias):
        ident = table_alias.args.get("this")
        if isinstance(ident, exp.Identifier):
            return ident.this
    return None


def _derived_info(node: exp.Expression) -> Optional[Tuple[exp.Expression, Optional[str]]]:
    """
    FROM/JOIN의 derived table 서브쿼리 감지.
    대표적으로:
      - Subquery(this=<Select/Union/...>, alias=TableAlias(Z))
    """
    if isinstance(node, exp.Subquery) and isinstance(node.args.get("this"), exp.Expression):
        inner = node.args["this"]
        alias_name = _extract_alias_from_tablealias(node.args.get("alias"))
        return inner, alias_name
    return None


def _replace_child_in_parent(parent: exp.Expression, old: exp.Expression, new: exp.Expression) -> bool:
    """
    transform() 대신 부모의 args를 직접 수정해서 교체
    """
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
    # original masked SQL (참고용)
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

    # copy는 하되, 이후 교체는 in-place로 수행
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

    # 타깃 수집: (node, parent, depth, inner_q, alias_name)
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

    # 깊은 것부터 (중첩 대비)
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

        # 저장
        ctes[cte_name] = inner_sql + ";"

        # main 치환: FROM cte_name AS Z  (alias 유지)
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
    placeholder_patterns: Optional[List[re.Pattern]] = None,
):
    if placeholder_patterns is None:
        placeholder_patterns = PH_PATTERNS

    with open(input_path, "r", encoding="utf-8") as f:
        original_text = f.read()

    masked_text, mp = mask_placeholders(original_text, placeholder_patterns)
    stmts = parse_statements(masked_text, dialect=dialect)

    base = os.path.splitext(os.path.basename(input_path))[0]
    parts: List[SQLPart] = []
    for i, stmt in enumerate(stmts, start=1):
        parts.append(
            split_long_statement_by_from_join_derived_tables(
                stmt=stmt,
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