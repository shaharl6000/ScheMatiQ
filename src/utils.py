import os
import sys
from pathlib import Path
from typing import Dict, List, Callable

# Add QBSD directory to path to import llm_backends
sys.path.append(str(Path(__file__).parent.parent / "QBSD"))
from llm_backends import TogetherLLM, HuggingFaceLLM

DEF_MODEL_NAME = "meta-llama/Llama-3.3-70B-Instruct-Turbo-Free"

# ───────────────────────────  BACKEND LOADER  ──────────────────────────
def get_generator(backend: str, model_name: str = DEF_MODEL_NAME) -> Callable:
    """
    Returns a single function `generate(messages, max_tokens, temperature, stop)` that
    uses the standardized llm_backends.py interface.
    """
    backend = backend.lower()
    
    if backend == "hf":
        # Use HuggingFaceLLM from llm_backends.py
        llm = HuggingFaceLLM(
            model=model_name,
            max_tokens=1024,  # default, will be overridden by caller
            temperature=0.3,  # default, will be overridden by caller
        )
        
        def _generate(messages: List[Dict[str, str]], max_tokens, temperature, stop):
            # HuggingFaceLLM doesn't support stop parameter in generate method
            # But we can post-process the output if needed
            result = llm.generate(
                messages, 
                max_tokens=max_tokens, 
                temperature=temperature
            )
            # Apply stop processing if provided
            if stop:
                for stop_seq in stop:
                    if stop_seq in result:
                        result = result.split(stop_seq)[0]
            return result.strip()
        
        return _generate

    elif backend == "together":
        # Use TogetherLLM from llm_backends.py
        llm = TogetherLLM(
            model=model_name,
            max_tokens=1024,  # default, will be overridden by caller
            temperature=0.3,  # default, will be overridden by caller
        )
        
        def _generate(messages: List[Dict[str, str]], max_tokens, temperature, stop):
            # TogetherLLM supports stop parameter but through different interface
            # We'll post-process if needed
            result = llm.generate(
                messages,
                max_tokens=max_tokens,
                temperature=temperature
            )
            # Apply stop processing if provided
            if stop:
                for stop_seq in stop:
                    if stop_seq in result:
                        result = result.split(stop_seq)[0]
            return result.strip()
        
        return _generate

    else:
        raise ValueError(f"Unknown backend '{backend}'. Choose 'hf' or 'together'.")