from typing import List, Dict, Sequence, Tuple


class EmbeddingRetriever:
    """
    A minimal passage retriever.  Plug in FAISS, Chroma, Milvus, etc.
    """
    def __init__(self, **backend_kwargs):
        self.backend_kwargs = backend_kwargs

    def query(self, docs: Sequence[str], question: str, k: int = 3) -> List[str]:
        """
        Return up to `k` passages per doc that best match `question`.
        """
        raise NotImplementedError("Replace with actual dense/keyword retriever.")