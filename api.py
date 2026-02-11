from __future__ import annotations

from functools import lru_cache

from fastapi import FastAPI
from pydantic import BaseModel, Field

from prompt import (
    prompt_trans_system,
    prompt_trans_user,
    prompt_varify_system,
    prompt_varify_user,
)
from qwencoder import GenerationConfig, QwenSqlEncoder


class GenerateRequest(BaseModel):
    question: str = Field(..., description="입력 SQL 혹은 변환 질문")
    system_prompt: str | None = Field(None, description="시스템 프롬프트(미입력 시 기본 프롬프트 사용)")
    user_prompt: str | None = Field(None, description="유저 프롬프트(미입력 시 question 기반 기본 프롬프트 사용)")
    max_new_tokens: int = Field(1024, ge=1, le=2048)
    temperature: float = Field(0.1, ge=0.0, le=2.0)
    top_p: float = Field(0.8, ge=0.0, le=1.0)
    do_sample: bool = True


class GenerateResponse(BaseModel):
    response: str


class VerifyRequest(BaseModel):
    oracle_sql: str = Field(..., description="원본 Oracle SQL")
    pg_sql: str = Field(..., description="변환된 PostgreSQL SQL")
    max_new_tokens: int = Field(1024, ge=1, le=2048)
    temperature: float = Field(0.1, ge=0.0, le=2.0)
    top_p: float = Field(0.8, ge=0.0, le=1.0)
    do_sample: bool = True


app = FastAPI(title="Qwen SQL Encoder API")


@lru_cache(maxsize=1)
def get_encoder() -> QwenSqlEncoder:
    model_name = "hf_models/XGenerationLab__XiYanSQL-QwenCoder-32B-2504"
    return QwenSqlEncoder(model_name)


@app.post("/generate", response_model=GenerateResponse)
def generate_sql(payload: GenerateRequest) -> GenerateResponse:
    encoder = get_encoder()
    system_prompt = payload.system_prompt or prompt_trans_system()
    user_prompt = payload.user_prompt or prompt_trans_user(question=payload.question)
    config = GenerationConfig(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        max_new_tokens=payload.max_new_tokens,
        temperature=payload.temperature,
        top_p=payload.top_p,
        do_sample=payload.do_sample,
    )
    output = encoder.generate(payload.question, config=config)
    return GenerateResponse(response=output)


@app.post("/verify", response_model=GenerateResponse)
def verify_sql(payload: VerifyRequest) -> GenerateResponse:
    encoder = get_encoder()
    system_prompt = prompt_varify_system()
    user_prompt = prompt_varify_user(oracle_sql=payload.oracle_sql, pg_sql=payload.pg_sql)
    config = GenerationConfig(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        max_new_tokens=payload.max_new_tokens,
        temperature=payload.temperature,
        top_p=payload.top_p,
        do_sample=payload.do_sample,
    )
    output = encoder.generate(payload.oracle_sql, config=config)
    return GenerateResponse(response=output)
