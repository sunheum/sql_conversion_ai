from __future__ import annotations

from functools import lru_cache

from fastapi import FastAPI
from pydantic import BaseModel, Field

from qwencoder import GenerationConfig, QwenSqlEncoder


class GenerateRequest(BaseModel):
    question: str = Field(..., description="입력 SQL 혹은 변환 질문")
    max_new_tokens: int = Field(1024, ge=1, le=2048)
    temperature: float = Field(0.1, ge=0.0, le=2.0)
    top_p: float = Field(0.8, ge=0.0, le=1.0)
    do_sample: bool = True


class GenerateResponse(BaseModel):
    response: str


app = FastAPI(title="Qwen SQL Encoder API")


@lru_cache(maxsize=1)
def get_encoder() -> QwenSqlEncoder:
    model_name = "hf_models/XGenerationLab__XiYanSQL-QwenCoder-32B-2504"
    return QwenSqlEncoder(model_name)


@app.post("/generate", response_model=GenerateResponse)
def generate_sql(payload: GenerateRequest) -> GenerateResponse:
    encoder = get_encoder()
    config = GenerationConfig(
        max_new_tokens=payload.max_new_tokens,
        temperature=payload.temperature,
        top_p=payload.top_p,
        do_sample=payload.do_sample,
    )
    output = encoder.generate(payload.question, config=config)
    return GenerateResponse(response=output)
