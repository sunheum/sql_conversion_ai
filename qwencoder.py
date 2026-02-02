import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from prompt import prompt_system, prompt_user

model_name = "hf_models/XGenerationLab__XiYanSQL-QwenCoder-32B-2504"
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    dtype=torch.bfloat16,
    device_map="auto"
)

tokenizer = AutoTokenizer.from_pretrained(model_name)

question = """
SELECT DECODE('A','A','1','2') FROM DUAL
"""

prompt_system = prompt_system()
prompt_user = prompt_user(question=question)
message = [{'role': 'system', 'content': prompt_system},
            {'role': 'user', 'content': prompt_user}]

text = tokenizer.apply_chat_template(
    message,
    tokenize=False,
    add_generation_prompt=False
)
model_inputs = tokenizer([text], return_tensors="pt").to(model.device)

generated_ids = model.generate(
    **model_inputs,
    pad_token_id=tokenizer.pad_token_id,
    eos_token_id=tokenizer.eos_token_id,
    max_new_tokens=1024,
    temperature=0.1,
    top_p=0.8,
    do_sample=True,
)
generated_ids = [
    output_ids[len(input_ids):] for input_ids, output_ids in zip(model_inputs.input_ids, generated_ids)
]
response = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]
print(response)
