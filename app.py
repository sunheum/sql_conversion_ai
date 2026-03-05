import io
import json
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
import requests
import sqlglot
import streamlit as st
from dotenv import load_dotenv

from parse_sql import split_sql_file
from xml_to_sql import export_from_xml_file


REQUIRED_COLUMNS = ["sql_src", "sql_length", "sql_modified"]
SUPPORTED_SQL_KEYWORDS = {
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "WITH",
}
NON_QUERY_KEYWORDS = {
    "CREATE",
    "DECLARE",
    "BEGIN",
    "ALTER",
    "DROP",
    "GRANT",
    "REVOKE",
}

ORACLE_XML_DIR = Path("./data/oracle/")
EXPORTED_SQL_DIR = Path("./data/oracle/_exported_sql/")
PARTS_OUT_DIR = Path("./out_parts/")


def build_payload(user_input: str) -> dict:
    return {"question": user_input}


def build_verify_payload(oracle_sql: str, pg_sql: str) -> dict:
    return {"oracle_sql": oracle_sql, "pg_sql": pg_sql}


def clean_response_text(text: str) -> str:
    if not text:
        return text
    cleaned = text.lstrip("\ufeff").lstrip()
    cleaned = re.sub(r"^(?:\\n)+", "", cleaned)
    cleaned = cleaned.lstrip("\n")
    cleaned = re.sub(r"(?i)^\s*assistant[:\s]*", "", cleaned)
    cleaned = re.sub(r"^[^A-Za-z0-9_]+", "", cleaned)
    return cleaned


def get_response_text(response: requests.Response) -> str:
    content_type = response.headers.get("Content-Type", "")
    if "application/json" in content_type:
        try:
            payload = response.json()
        except json.JSONDecodeError:
            return clean_response_text(response.text)
        if isinstance(payload, dict) and "response" in payload:
            text = str(payload["response"])
            return clean_response_text(text)
        return json.dumps(payload, ensure_ascii=False)
    return clean_response_text(response.text)


def fetch_response_text(api_url: str, payload: dict, max_retries: int = 2) -> str:
    attempts = 0
    last_response_text = ""
    while attempts <= max_retries:
        response = requests.post(api_url, json=payload)
        response.raise_for_status()
        last_response_text = get_response_text(response)
        if last_response_text.strip():
            return last_response_text
        attempts += 1
    return last_response_text


def get_db_settings() -> tuple[str | None, int | None]:
    host = os.getenv("POSTGRES_HOST")
    port_value = os.getenv("POSTGRES_PORT")
    if not host or not port_value:
        return host, None
    try:
        port = int(port_value)
    except ValueError:
        return host, None
    return host, port


def validate_dataframe(dataframe: pd.DataFrame) -> list[str]:
    missing = [column for column in REQUIRED_COLUMNS if column not in dataframe.columns]
    return missing


def insert_source_rows(cursor: Any, rows: list[tuple[Any, Any, Any]]) -> None:
    cursor.executemany(
        """
        INSERT INTO scai_iv.ais_sql_obj_dtl (sql_src, sql_length, sql_modified)
        VALUES (%s, %s, %s)
        """,
        rows,
    )


def fetch_source_rows(connection: psycopg2.extensions.connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT id, sql_src, sql_length, sql_modified
        FROM scai_iv.ais_sql_obj_dtl
        """,
        connection,
    )


def fetch_sql_text_rows(connection: psycopg2.extensions.connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT id, COALESCE(sql_modified, sql_src) AS sql_text
        FROM scai_iv.ais_sql_obj_dtl
        ORDER BY id
        """,
        connection,
    )


def fetch_verify_rows(connection: psycopg2.extensions.connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT d.id, d.sql_modified, r.new_sql_src
        FROM scai_iv.ais_sql_obj_dtl AS d
        JOIN scai_iv.ais_chg_rslt AS r ON d.id = CAST(r.src_obj_id AS INTEGER);
        """,
        connection,
    )


def upsert_result_row(cursor: Any, row: dict[str, Any]) -> None:
    cursor.execute(
        """
        UPDATE scai_iv.ais_chg_rslt
        SET
            "변경수행차수" = %s,
            "변경수행일시" = CURRENT_TIMESTAMP,
            "new_sql_src" = %s
        WHERE "src_obj_id" = %s
        """,
        (
            1,
            row["response"],
            str(row["src_obj_id"]),
        ),
    )

    if cursor.rowcount == 0:
        cursor.execute(
            """
            INSERT INTO scai_iv.ais_chg_rslt ("변경수행차수", "변경수행일시", "new_sql_src", "src_obj_id")
            VALUES (%s, CURRENT_TIMESTAMP, %s, %s)
            """,
            (
                1,
                row["response"],
                str(row["src_obj_id"]),
            ),
        )


def build_template_excel_bytes() -> bytes:
    template_df = pd.DataFrame(columns=REQUIRED_COLUMNS)
    buffer = io.BytesIO()
    template_df.to_excel(buffer, index=False)
    return buffer.getvalue()


def classify_sql_text(sql_text: str) -> tuple[bool, str]:

    _MYBATIS_ROUTINE_CALL_RE = re.compile(
        r"""
        \{                              # opening '{'
        [^{}]*?                         # anything (non-greedy), not crossing other braces
        (?:=\s*)?                       # optional "= " (function style)
        call\s+                         # "call" keyword
        [A-Za-z_][\w$]*                 # identifier start
        (?:\.[A-Za-z_][\w$]*)*          # optional dotted qualifiers
        \s*\(                           # opening parenthesis of call
        """,
        re.IGNORECASE | re.VERBOSE | re.DOTALL,
    )
    
    if _MYBATIS_ROUTINE_CALL_RE.search(sql_text):
        return False, "routine_call:mybatis"

    return True, ""


def list_sql_files(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(path for path in directory.glob("*.sql") if path.is_file())


def export_xml_directory_to_sql(src_dir: Path, out_dir: Path) -> tuple[int, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    xml_files = sorted(src_dir.glob("*.xml"))

    exported_count = 0
    for xml_path in xml_files:
        exported_count += export_from_xml_file(xml_path, out_dir)

    return len(xml_files), exported_count


def run_preprocessing(db_name: str, db_user: str, db_password: str, db_host: str | None, db_port: int | None) -> None:
    st.subheader("1. XML to SQL")
    st.caption(f"XML 폴더: `{ORACLE_XML_DIR}` / SQL 출력 폴더: `{EXPORTED_SQL_DIR}`")

    if st.button("변환 실행"):
        try:
            exported_files, exported_count = export_xml_directory_to_sql(ORACLE_XML_DIR, EXPORTED_SQL_DIR)
        except Exception as exc:  # noqa: BLE001
            st.error(f"xml_to_sql.py 실행이 실패했습니다: {exc}")
        else:
            if exported_files == 0:
                st.warning(f"{ORACLE_XML_DIR} 경로에 XML 파일이 없습니다.")
            else:
                st.success(
                    f"실행 완료: XML {exported_files}개, SQL {exported_count}건을 `{EXPORTED_SQL_DIR}`에 저장했습니다."
                )

    st.subheader("2. SQL 로드")
    load_method = st.radio(
        "SQL 로드 방식을 선택하세요.",
        ["경로에서 .sql 파일 로드", "DB에서 로드"],
        horizontal=True,
        key="preprocess_load_method",
    )

    loaded_records: list[dict[str, str]] = []

    if load_method == "경로에서 .sql 파일 로드":
        st.caption(f"대상 경로: `{EXPORTED_SQL_DIR}`")
        if st.button(".sql 파일 읽기"):
            sql_files = list_sql_files(EXPORTED_SQL_DIR)
            if not sql_files:
                st.warning("읽을 .sql 파일이 없습니다.")
            else:
                for path in sql_files:
                    loaded_records.append({"name": path.name, "sql_text": path.read_text(encoding="utf-8")})
    else:
        if st.button("DB SQL 로드"):
            if not db_name or not db_user or not db_password:
                st.error("DB명/사용자/비밀번호를 입력하세요.")
            elif not db_host or not db_port:
                st.error(".env의 POSTGRES_HOST/POSTGRES_PORT 설정을 확인하세요.")
            else:
                try:
                    connection = psycopg2.connect(
                        host=db_host,
                        port=db_port,
                        dbname=db_name,
                        user=db_user,
                        password=db_password,
                    )
                    sql_df = fetch_sql_text_rows(connection)
                except psycopg2.Error as exc:
                    st.error(f"DB 로드 실패: {exc}")
                finally:
                    if "connection" in locals():
                        connection.close()
                if "sql_df" in locals() and not sql_df.empty:
                    loaded_records = [
                        {"name": f"db_row_{row.id}.sql", "sql_text": str(row.sql_text)} for row in sql_df.itertuples(index=False)
                    ]

    if loaded_records:
        sql_only: list[dict[str, str]] = []
        non_sql: list[dict[str, str]] = []
        for item in loaded_records:
            is_sql, reason = classify_sql_text(item["sql_text"])
            if is_sql:
                sql_only.append(item)
            else:
                non_sql.append({"name": item["name"], "reason": reason})

        st.session_state["preprocess_sql_only"] = sql_only
        st.success(f"로드 완료: 총 {len(loaded_records)}건 / SQL {len(sql_only)}건 / 비SQL {len(non_sql)}건")

        if sql_only:
            st.dataframe(pd.DataFrame(sql_only), use_container_width=True)
        if non_sql:
            st.markdown("**비SQL(제외) 목록**")
            st.dataframe(pd.DataFrame(non_sql), use_container_width=True)
    elif "preprocess_sql_only" in st.session_state:
        st.info(f"현재 로드된 SQL 건수: {len(st.session_state['preprocess_sql_only'])}")
    else:
        st.info("아직 로드된 SQL이 없습니다.")

    st.subheader("3. SQL 분할")
    st.caption("./out_parts/{sql파일명}/ 폴더에 저장합니다.")

    if st.button("SQL 분할 실행"):
        sql_items = st.session_state.get("preprocess_sql_only", [])
        if not sql_items:
            st.error("먼저 SQL 파일을 로드하세요.")
            return

        PARTS_OUT_DIR.mkdir(parents=True, exist_ok=True)
        success_count = 0
        fail_messages: list[str] = []

        for item in sql_items:
            base_name = Path(item["name"]).stem
            target_dir = PARTS_OUT_DIR / base_name
            target_dir.mkdir(parents=True, exist_ok=True)
            temp_input = target_dir / f"{base_name}.sql"
            temp_input.write_text(item["sql_text"], encoding="utf-8")

            try:
                split_sql_file(str(temp_input), str(target_dir), dialect="oracle", max_chars=1000, min_depth_to_extract=2)
                success_count += 1
            except Exception as exc:  # noqa: BLE001
                fail_messages.append(f"{item['name']}: {exc}")

        st.success(f"SQL 분할 완료: {success_count}건")
        if fail_messages:
            st.warning("일부 SQL 분할이 실패했습니다.")
            for message in fail_messages:
                st.write(f"- {message}")


def run_conversion(db_name: str, db_user: str, db_password: str, db_host: str | None, db_port: int | None) -> None:
    st.subheader("변환 SQL 불러오기")
    data_source = st.radio(
        "데이터를 불러올 방법을 선택하세요.",
        ["엑셀 업로드", "DB에서 불러오기"],
        horizontal=True,
    )

    upload_file = None
    if data_source == "엑셀 업로드":
        upload_file = st.file_uploader("엑셀 파일 (.xlsx/.xls)", type=["xlsx", "xls"])
        st.download_button(
            label="엑셀 양식 다운로드",
            data=build_template_excel_bytes(),
            file_name="sql_conversion_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        if st.button("엑셀 데이터 불러오기"):
            if not upload_file:
                st.error("엑셀 파일을 업로드하세요.")
            else:
                try:
                    dataframe = pd.read_excel(upload_file)
                except Exception as exc:  # noqa: BLE001
                    st.error(f"엑셀 파일을 읽을 수 없습니다: {exc}")
                else:
                    missing_columns = validate_dataframe(dataframe)
                    if missing_columns:
                        st.error(f"필수 컬럼이 없습니다: {', '.join(missing_columns)}")
                    else:
                        dataframe = dataframe[REQUIRED_COLUMNS].copy()
                        st.session_state["loaded_df"] = dataframe
                        st.session_state["excel_df"] = dataframe
    else:
        if st.button("DB 데이터 불러오기"):
            if not db_name or not db_user or not db_password:
                st.error("DB 접속 정보(ID/PW/DB 이름)를 입력하세요.")
            elif not db_host or not db_port:
                st.error("DB Host/Port 설정이 올바르지 않습니다.")
            else:
                try:
                    connection = psycopg2.connect(
                        host=db_host,
                        port=db_port,
                        dbname=db_name,
                        user=db_user,
                        password=db_password,
                    )
                except psycopg2.Error as exc:
                    st.error(f"DB 연결 실패: {exc}")
                else:
                    try:
                        dataframe = fetch_source_rows(connection)
                    except Exception as exc:  # noqa: BLE001
                        st.error(f"DB 데이터 조회 실패: {exc}")
                    else:
                        st.session_state["loaded_df"] = dataframe
                        st.session_state.pop("excel_df", None)
                    finally:
                        connection.close()

    loaded_df = st.session_state.get("loaded_df")
    if isinstance(loaded_df, pd.DataFrame):
        st.markdown("**불러온 데이터**")
        st.dataframe(loaded_df, use_container_width=True)
    else:
        st.info("불러온 데이터가 없습니다.")

    st.subheader("DB 저장하기")
    if st.button("엑셀 데이터 DB 저장"):
        excel_df = st.session_state.get("excel_df")
        if not isinstance(excel_df, pd.DataFrame):
            st.error("먼저 엑셀 데이터를 불러오세요.")
        elif not db_name or not db_user or not db_password:
            st.error("DB 접속 정보(ID/PW/DB 이름)를 입력하세요.")
        elif not db_host or not db_port:
            st.error("DB Host/Port 설정이 올바르지 않습니다.")
        else:
            with st.spinner("엑셀 데이터를 DB에 저장 중..."):
                try:
                    connection = psycopg2.connect(
                        host=db_host,
                        port=db_port,
                        dbname=db_name,
                        user=db_user,
                        password=db_password,
                    )
                    connection.autocommit = True
                except psycopg2.Error as exc:
                    st.error(f"DB 연결 실패: {exc}")
                else:
                    try:
                        with connection.cursor() as cursor:
                            source_rows = [
                                (
                                    row.sql_src,
                                    row.sql_length,
                                    row.sql_modified,
                                )
                                for row in excel_df.itertuples(index=False)
                            ]
                            insert_source_rows(cursor, source_rows)
                        st.success("엑셀 데이터가 DB에 저장되었습니다.")
                    except psycopg2.Error as exc:
                        st.error(f"DB 저장 실패: {exc}")
                    finally:
                        connection.close()

    st.subheader("SQL 변환")
    api_url = st.text_input("API URL", placeholder="http://localhost:8000/generate")
    if st.button("SQL 변환 API 호출하기", type="primary"):
        if not api_url:
            st.error("API URL을 입력하세요.")
            return
        if not isinstance(loaded_df, pd.DataFrame):
            st.error("먼저 데이터를 불러오세요.")
            return
        if not db_name or not db_user or not db_password:
            st.error("DB 접속 정보(ID/PW/DB 이름)를 입력하세요.")
            return
        if not db_host or not db_port:
            st.error("DB Host/Port 설정이 올바르지 않습니다.")
            return

        result_rows: list[dict[str, Any]] = []
        errors: list[str] = []

        with st.spinner("API 호출 중..."):
            try:
                connection = psycopg2.connect(
                    host=db_host,
                    port=db_port,
                    dbname=db_name,
                    user=db_user,
                    password=db_password,
                )
                connection.autocommit = True
            except psycopg2.Error as exc:
                st.error(f"DB 연결 실패: {exc}")
                return

            try:
                with connection.cursor() as cursor:
                    total_rows = len(loaded_df.index)
                    progress_bar = st.progress(0, text="API 호출을 준비 중입니다.")
                    status_text = st.empty()

                    for index, row in enumerate(loaded_df.itertuples(index=False), start=1):
                        status_text.info(f"API 호출 중... ({index}/{total_rows})")
                        question = str(row.sql_modified)
                        payload = build_payload(question)
                        try:
                            response_text = fetch_response_text(api_url, payload)
                        except requests.RequestException as exc:
                            errors.append(f"API 호출 실패 (row {index}): {exc}")
                            continue
                        result_row = {
                            "src_obj_id": getattr(row, "id", None),
                            "question": question,
                            "response": response_text,
                        }
                        upsert_result_row(cursor, result_row)
                        result_rows.append(result_row)
                        progress_bar.progress(index / total_rows)

                    progress_bar.progress(1.0, text="API 호출이 완료되었습니다.")
                    status_text.empty()
            finally:
                connection.close()

        if errors:
            st.warning("일부 요청이 실패했습니다.")
            for error in errors:
                st.write(f"- {error}")

        if result_rows:
            st.subheader("저장된 결과")
            st.dataframe(pd.DataFrame(result_rows), use_container_width=True)
        else:
            st.info("저장된 결과가 없습니다.")


def run_verification(db_name: str, db_user: str, db_password: str, db_host: str | None, db_port: int | None) -> None:
    st.subheader("SQL 검증")
    verify_api_url = st.text_input("검증 API URL", placeholder="http://localhost:8000/verify")
    if st.button("SQL 검증 API 호출하기", type="primary"):
        if not verify_api_url:
            st.error("검증 API URL을 입력하세요.")
            return
        if not db_name or not db_user or not db_password:
            st.error("DB 접속 정보(ID/PW/DB 이름)를 입력하세요.")
            return
        if not db_host or not db_port:
            st.error("DB Host/Port 설정이 올바르지 않습니다.")
            return

        verify_rows: list[dict[str, Any]] = []
        errors: list[str] = []

        with st.spinner("검증 API 호출 중..."):
            try:
                connection = psycopg2.connect(
                    host=db_host,
                    port=db_port,
                    dbname=db_name,
                    user=db_user,
                    password=db_password,
                )
                verify_df = fetch_verify_rows(connection)
            except psycopg2.Error as exc:
                st.error(f"DB 연결/조회 실패: {exc}")
                return
            except Exception as exc:  # noqa: BLE001
                st.error(f"검증 대상 조회 실패: {exc}")
                return
            finally:
                if "connection" in locals():
                    connection.close()

            if verify_df.empty:
                st.info("검증 대상 데이터가 없습니다.")
                return

            total_rows = len(verify_df.index)
            progress_bar = st.progress(0, text="검증 API 호출을 준비 중입니다.")
            status_text = st.empty()

            for index, row in enumerate(verify_df.itertuples(index=False), start=1):
                status_text.info(f"검증 API 호출 중... ({index}/{total_rows})")
                payload = build_verify_payload(
                    oracle_sql=str(row.sql_modified),
                    pg_sql=str(row.new_sql_src),
                )
                try:
                    response_text = fetch_response_text(verify_api_url, payload)
                except requests.RequestException as exc:
                    errors.append(f"검증 API 호출 실패 (row {index}, id {row.id}): {exc}")
                    continue

                verify_rows.append(
                    {
                        "id": row.id,
                        "oracle_sql": row.sql_modified,
                        "pg_sql": row.new_sql_src,
                        "verify_result": response_text,
                    }
                )
                progress_bar.progress(index / total_rows)

            progress_bar.progress(1.0, text="검증 API 호출이 완료되었습니다.")
            status_text.empty()

        if errors:
            st.warning("일부 검증 요청이 실패했습니다.")
            for error in errors:
                st.write(f"- {error}")

        if verify_rows:
            st.dataframe(pd.DataFrame(verify_rows), use_container_width=True)
        else:
            st.info("검증 결과가 없습니다.")


def main() -> None:
    load_dotenv()
    st.set_page_config(page_title="SQL Conversion AI", page_icon="📝", layout="centered")
    st.title("📝 SQL Conversion AI")
    st.write("전처리, 변환, 검증 메뉴를 통해 Oracle SQL 변환 작업을 수행합니다.")

    st.subheader("공통 DB 접속 정보")
    db_name = st.text_input("DB 이름", placeholder="scai")
    db_user = st.text_input("DB 사용자", placeholder="dataware")
    db_password = st.text_input("DB 비밀번호", type="password", placeholder="••••••••")
    db_host, db_port = get_db_settings()
    if db_host and db_port:
        st.caption("DB Host/Port는 .env에서 불러옵니다.")
    else:
        st.warning(".env에서 DB Host/Port를 불러오지 못했습니다. POSTGRES_HOST/POSTGRES_PORT를 확인하세요.")

    tab_preprocess, tab_convert, tab_verify = st.tabs(["전처리", "변환", "검증"])

    with tab_preprocess:
        run_preprocessing(db_name, db_user, db_password, db_host, db_port)

    with tab_convert:
        run_conversion(db_name, db_user, db_password, db_host, db_port)

    with tab_verify:
        run_verification(db_name, db_user, db_password, db_host, db_port)


if __name__ == "__main__":
    main()
