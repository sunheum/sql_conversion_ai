from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

from prompt import prompt_system, prompt_user


@dataclass(frozen=True)
class GenerationConfig:
    max_new_tokens: int = 1024
    temperature: float = 0.1
    top_p: float = 0.8
    do_sample: bool = True


class QwenSqlEncoder:
    def __init__(self, model_name: str) -> None:
        self.model_name = model_name
        self.model = AutoModelForCausalLM.from_pretrained(
            model_name,
            dtype=torch.bfloat16,
            device_map="auto",
        )
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)

    def generate(self, question: str, config: GenerationConfig | None = None) -> str:
        if config is None:
            config = GenerationConfig()

        system_prompt = prompt_system()
        user_prompt = prompt_user(question=question)
        message = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        text = self.tokenizer.apply_chat_template(
            message,
            tokenize=False,
            add_generation_prompt=False,
        )
        model_inputs = self.tokenizer([text], return_tensors="pt").to(self.model.device)

        generated_ids = self.model.generate(
            **model_inputs,
            pad_token_id=self.tokenizer.pad_token_id,
            eos_token_id=self.tokenizer.eos_token_id,
            max_new_tokens=config.max_new_tokens,
            temperature=config.temperature,
            top_p=config.top_p,
            do_sample=config.do_sample,
        )
        generated_ids = [
            output_ids[len(input_ids):]
            for input_ids, output_ids in zip(model_inputs.input_ids, generated_ids)
        ]
        return self.tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate SQL using QwenSqlEncoder")
    parser.add_argument(
        "question",
        nargs="?",
        help="입력 SQL 혹은 변환 질문 (미지정 시 stdin에서 읽음)",
    )
    parser.add_argument(
        "--model-name",
        default="hf_models/XGenerationLab__XiYanSQL-QwenCoder-32B-2504",
        help="사용할 모델 경로 또는 이름",
    )
    args = parser.parse_args()

    question = args.question
    if question is None:
        question = sys.stdin.read().strip()

    if not question:
        raise SystemExit("질문이 비어 있습니다. 인자로 전달하거나 stdin으로 입력하세요.")

    encoder = QwenSqlEncoder(args.model_name)
    response = encoder.generate(question)
    print(response)


if __name__ == "__main__":
    main()
