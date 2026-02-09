import io
import json
import os
import re
from typing import Any

import pandas as pd
import psycopg2
import requests
import streamlit as st
from dotenv import load_dotenv



REQUIRED_COLUMNS = ["sql_src", "sql_length", "sql_modified"]


def build_payload(
    user_input: str,
    max_new_tokens: int,
    temperature: float,
    top_p: float,
    top_k: int,
    repetition_penalty: float,
    do_sample: bool,
) -> dict:
    return {
        "question": user_input,
        "max_new_tokens": max_new_tokens,
        "temperature": temperature,
        "top_p": top_p,
        "top_k": top_k,
        "repetition_penalty": repetition_penalty,
        "do_sample": do_sample,
    }




def clean_response_text(text: str) -> str:
    if not text:
        return text
    original = text
    cleaned = text.lstrip("\ufeff").lstrip()
    cleaned = re.sub(r"^(?:\\n)+", "", cleaned)
    cleaned = cleaned.lstrip("\n")
    cleaned = re.sub(r"(?i)^\s*assistant[:\s]*", "", cleaned)
    cleaned = re.sub(r"^```(?:\w+)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()
    if cleaned:
        return cleaned
    return original.strip()


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


def insert_result_row(cursor: Any, row: dict[str, Any]) -> None:
    cursor.execute(
        """
        INSERT INTO scai_iv.ais_chg_rslt ("변경수행차수", "변경수행일시", "new_sql_src", "src_obj_id")
        VALUES (%s, CURRENT_TIMESTAMP, %s, %s)
        """,
        (
            1,
            row["response"],
            row["src_obj_id"],
        ),
    )


def build_template_excel_bytes() -> bytes:
    template_df = pd.DataFrame(columns=REQUIRED_COLUMNS)
    buffer = io.BytesIO()
    template_df.to_excel(buffer, index=False)
    return buffer.getvalue()


def main() -> None:
    load_dotenv()
    st.set_page_config(page_title="API 호출 데모", page_icon="📝", layout="centered")
    st.title("📝 SQL Conversion AI")
    st.write("Oracle SQL을 PostgreSQL로 변환하여 DB에 저장합니다.")

    api_url = st.text_input("API URL", placeholder="http://localhost:8000/generate")

    st.subheader("DB 접속 정보")
    db_name = st.text_input("DB 이름", placeholder="scai")
    db_user = st.text_input("DB 사용자", placeholder="dataware")
    db_password = st.text_input("DB 비밀번호", type="password", placeholder="••••••••")
    db_host, db_port = get_db_settings()
    if db_host and db_port:
        st.caption(f"DB Host/Port는 .env에서 불러옵니다.")
    else:
        st.warning(".env에서 DB Host/Port를 불러오지 못했습니다. POSTGRES_HOST/POSTGRES_PORT를 확인하세요.")

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

    col1, col2 = st.columns(2)
    with col1:
        max_new_tokens = st.number_input("max_new_tokens", min_value=1, max_value=2048, value=1024, step=1)
        temperature = st.slider("temperature", min_value=0.0, max_value=2.0, value=0.1, step=0.05)
        top_p = st.slider("top_p", min_value=0.0, max_value=1.0, value=0.8, step=0.05)
    with col2:
        top_k = st.number_input("top_k", min_value=1, max_value=200, value=20, step=1)
        repetition_penalty = st.slider("repetition_penalty", min_value=1.0, max_value=2.0, value=1.05, step=0.01)
        do_sample = st.checkbox("do_sample", value=True)

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
                        payload = build_payload(
                            user_input=question,
                            max_new_tokens=max_new_tokens,
                            temperature=temperature,
                            top_p=top_p,
                            top_k=top_k,
                            repetition_penalty=repetition_penalty,
                            do_sample=do_sample,
                        )
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
                        insert_result_row(cursor, result_row)
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


if __name__ == "__main__":
    main()
