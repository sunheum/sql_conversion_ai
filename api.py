from __future__ import annotations

from functools import lru_cache

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from models import DEFAULT_MODEL_LABEL, MODEL_CHOICES, get_model_path
from qwencoder import GenerationConfig, QwenSqlEncoder


class GenerateRequest(BaseModel):
    question: str = Field(..., description="입력 SQL 혹은 변환 질문")
    model_label: str = Field(DEFAULT_MODEL_LABEL, description="사용할 모델 이름")
    max_new_tokens: int = Field(1024, ge=1, le=2048)
    temperature: float = Field(0.1, ge=0.0, le=2.0)
    top_p: float = Field(0.8, ge=0.0, le=1.0)
    top_k: int = Field(20, ge=1, le=200)
    repetition_penalty: float = Field(1.05, ge=1.0, le=2.0)
    do_sample: bool = True


class GenerateResponse(BaseModel):
    response: str


app = FastAPI(title="Qwen SQL Encoder API")


@lru_cache(maxsize=1)
def get_encoder(model_name: str) -> QwenSqlEncoder:
    return QwenSqlEncoder(model_name)


@app.post("/generate", response_model=GenerateResponse)
def generate_sql(payload: GenerateRequest) -> GenerateResponse:
    if payload.model_label not in MODEL_CHOICES:
        model_names = ", ".join(MODEL_CHOICES.keys())
        raise HTTPException(status_code=400, detail=f"지원하지 않는 모델입니다. 선택 가능 모델: {model_names}")

    encoder = get_encoder(get_model_path(payload.model_label))
    config = GenerationConfig(
        max_new_tokens=payload.max_new_tokens,
        temperature=payload.temperature,
        top_p=payload.top_p,
        top_k=payload.top_k,
        repetition_penalty=payload.repetition_penalty,
        do_sample=payload.do_sample,
    )
    output = encoder.generate(payload.question, config=config)
    return GenerateResponse(response=output)
