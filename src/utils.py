import os
from typing import Dict, List

DEF_MODEL_NAME        = "meta-llama/Llama-3.3-70B-Instruct-Turbo-Free"

# ───────────────────────────  BACKEND LOADER  ──────────────────────────
def get_generator(backend: str, model_name: str = DEF_MODEL_NAME):
    """
    Returns a single function `generate(messages, max_tokens, temperature, stop)` that
    hides all the backend‑specific machinery.
    """
    backend = backend.lower()
    if backend == "hf":
        # --- local HF model (4‑bit, bitsandbytes) --------------------------------
        import torch
        from transformers import (
            AutoModelForCausalLM,
            AutoTokenizer,
            BitsAndBytesConfig,
        )

        bnb_cfg = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_use_double_quant=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.float16,
        )

        print("⏳  Loading tokenizer & model locally …")
        tokenizer = AutoTokenizer.from_pretrained(
            model_name, use_fast=True, token=os.getenv("HF_TOKEN")
        )
        model = AutoModelForCausalLM.from_pretrained(
            model_name,
            quantization_config=bnb_cfg,
            device_map="auto",
            torch_dtype=torch.float16,
            trust_remote_code=True,
            token=os.getenv("HF_TOKEN"),
        )
        model.eval()

        def _generate(messages: List[Dict[str, str]], max_tokens, temperature, stop):
            prompt_text = tokenizer.apply_chat_template(
                messages, add_generation_prompt=True
            )
            inp = tokenizer(prompt_text, return_tensors="pt").to(model.device)
            out_ids = model.generate(
                **inp,
                max_new_tokens=max_tokens,
                temperature=temperature,
                pad_token_id=tokenizer.eos_token_id,
            )
            completion = tokenizer.decode(
                out_ids[0][inp.input_ids.shape[-1] :]
            ).strip()
            # obey custom delimiter if present
            if stop:
                completion = completion.split(stop[0])[0]
            return completion.strip()

        return _generate

    # ------------------ Together AI hosted backend -------------------------------
    elif backend == "together":
        from together import Together
        TOGETHER_API_KEY="f882d63f818dc302682900cbbacf4d7027445f21f682cba9a62d5e7597e6cf03"
        client = Together(api_key=TOGETHER_API_KEY)

        def _generate(messages: List[Dict[str, str]], max_tokens, temperature, stop):
            resp = client.chat.completions.create(
                model=model_name,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                stop=stop,
            )
            return resp.choices[0].message.content.strip()

        return _generate

    else:
        raise ValueError(f"Unknown backend '{backend}'. Choose 'hf' or 'together'.")