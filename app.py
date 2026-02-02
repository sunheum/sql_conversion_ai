import json

import requests
import streamlit as st


def build_payload(user_input: str) -> dict:
    return {"question": user_input}


def main() -> None:
    st.set_page_config(page_title="API 호출 데모", page_icon="📝", layout="centered")
    st.title("📝 SQL Conversion AI")
    st.write("ORACLE SQL을 입력하고 PostgreSQL을 반환합니다.")

    api_url = st.text_input("API URL", placeholder="https://api.example.com/generate")
    user_input = st.text_area(
        "입력값 (question)",
        placeholder="SELECT DECODE('A','A','1','2') FROM DUAL",
        height=160,
    )
    st.caption("입력한 ORACLE SQL은 /generate API의 question 필드로 전송됩니다.")
    timeout_seconds = st.number_input("타임아웃(초)", min_value=1, max_value=120, value=10, step=1)

    if st.button("API 호출", type="primary"):
        if not api_url:
            st.error("API URL을 입력하세요.")
            return

        payload = build_payload(user_input)

        with st.spinner("요청 중..."):
            try:
                response = requests.post(api_url, json=payload, timeout=timeout_seconds)
            except requests.RequestException as exc:
                st.error(f"요청 실패: {exc}")
                return

        st.subheader("응답 요약")
        st.write(f"상태 코드: {response.status_code}")

        st.subheader("응답 본문")
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                st.json(response.json())
            except json.JSONDecodeError:
                st.text(response.text)
        else:
            st.text(response.text)


if __name__ == "__main__":
    main()
