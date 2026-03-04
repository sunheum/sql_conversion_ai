import os
import json
import argparse
from typing import Dict
import re

def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()

def _write_text(path: str, s: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(s.strip() + "\n")

def _norm(s: str) -> str:
    return s.strip().rstrip(";").strip()

JOIN_KWS = r"""
FROM|
JOIN|
LEFT\s+JOIN|
LEFT\s+OUTER\s+JOIN|
RIGHT\s+JOIN|
RIGHT\s+OUTER\s+JOIN|
FULL\s+JOIN|
FULL\s+OUTER\s+JOIN|
INNER\s+JOIN|
CROSS\s+JOIN
"""

def merge_regex_only(main_sql: str, cte_sql_map: Dict[str, str]) -> str:
    out = main_sql

    for cte_name in sorted(cte_sql_map.keys(), key=len, reverse=True):
        body = _norm(cte_sql_map[cte_name])

        pattern = re.compile(
            rf"""
            \b(?P<kw>{JOIN_KWS})\s+
            (?P<table>(?:[A-Za-z0-9_]+\.)*{re.escape(cte_name)})
            (?:\s+(?:AS\s+)?(?P<alias>[A-Za-z0-9_]+))?
            """,
            re.IGNORECASE | re.VERBOSE,
        )

        def repl(m):
            kw = m.group("kw")
            alias = m.group("alias")
            if alias:
                return f"{kw} (\n{body}\n) {alias}"
            return f"{kw} (\n{body}\n)"

        out = pattern.sub(repl, out)

    return _norm(out) + ";"

def load_parts(parts_dir: str, manifest_path: str, use_transformed: bool, transformed_suffix: str):
    manifest = json.loads(_read_text(manifest_path))
    merged_statements = []

    def pick(path: str) -> str:
        if use_transformed:
            cand = path + transformed_suffix
            if os.path.exists(cand):
                return cand
        return path

    for entry in manifest:
        main_path = pick(os.path.join(parts_dir, entry["main_file"]))
        main_sql = _read_text(main_path)

        cte_sql_map = {}
        for cte_name, cte_file in (entry.get("cte_files") or {}).items():
            cte_path = pick(os.path.join(parts_dir, cte_file))
            cte_sql_map[cte_name] = _read_text(cte_path)

        merged = merge_regex_only(main_sql, cte_sql_map)
        merged_statements.append(merged)

    return "\n\n".join(merged_statements).strip() + "\n"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--parts_dir", required=True, help="분할 결과 폴더")
    p.add_argument("--manifest", required=True, help="manifest json 경로")
    p.add_argument("--out", required=True, help="합친 SQL 출력 경로")
    p.add_argument("--use_transformed", action="store_true", help="*.transformed.sql 우선 사용")
    p.add_argument("--transformed_suffix", default=".transformed.sql", help="변환 파일 suffix")
    args = p.parse_args()

    final_sql = load_parts(
        parts_dir=args.parts_dir,
        manifest_path=args.manifest,
        use_transformed=args.use_transformed,
        transformed_suffix=args.transformed_suffix,
    )
    _write_text(args.out, final_sql)

if __name__ == "__main__":
    main()