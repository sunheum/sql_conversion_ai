from __future__ import annotations

MODEL_CHOICES: dict[str, str] = {
    "XiYanSQL-QwenCoder-32B-2504": "hf_models/XGenerationLab__XiYanSQL-QwenCoder-32B-2504",
    "Qwen3-Coder-30B-A3B-Instruct": "hf_models/Qwen__Qwen3-Coder-30B-A3B-Instruct",
}

DEFAULT_MODEL_LABEL = "XiYanSQL-QwenCoder-32B-2504"


def get_model_path(label: str) -> str:
    return MODEL_CHOICES[label]
