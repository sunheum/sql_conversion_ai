import argparse
import re
from pathlib import Path
import xml.etree.ElementTree as ET

DEFAULT_XML_DIR = Path("./data/oracle/")
DEFAULT_OUTPUT_DIR = Path("./data/oracle/_exported_sql/")
TARGET_TAGS = {"select", "insert", "update", "delete", "sql", "query", "statement"}


def normalize_sql(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return ""
    return cleaned[:-1].strip() if cleaned.endswith(";") else cleaned


def collect_sql_from_node(node: ET.Element) -> list[str]:
    statements: list[str] = []
    for child in node.iter():
        tag = child.tag.split("}")[-1].lower() if isinstance(child.tag, str) else ""
        if tag not in TARGET_TAGS:
            continue
        text = "".join(child.itertext())
        sql = normalize_sql(text)
        if sql:
            statements.append(sql)
    return statements


def export_xml_to_sql(xml_dir: Path = DEFAULT_XML_DIR, output_dir: Path = DEFAULT_OUTPUT_DIR) -> tuple[int, int]:
    output_dir.mkdir(parents=True, exist_ok=True)

    xml_files = sorted(path for path in xml_dir.glob("*.xml") if path.is_file())
    exported_files = 0
    exported_sql_count = 0

    for xml_file in xml_files:
        try:
            tree = ET.parse(xml_file)
        except ET.ParseError:
            continue

        statements = collect_sql_from_node(tree.getroot())
        if not statements:
            continue

        out_file = output_dir / f"{xml_file.stem}.sql"
        with out_file.open("w", encoding="utf-8") as file:
            file.write(";\n\n".join(statements) + ";\n")

        exported_files += 1
        exported_sql_count += len(statements)

    return exported_files, exported_sql_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract SQL from XML files and export as .sql files")
    parser.add_argument("--xml-dir", default=str(DEFAULT_XML_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    files, count = export_xml_to_sql(Path(args.xml_dir), Path(args.output_dir))
    print(f"Exported {count} SQL statements from {files} XML files.")


if __name__ == "__main__":
    main()
