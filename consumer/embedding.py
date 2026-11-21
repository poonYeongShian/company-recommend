"""Text embeddings from a small open-source multilingual model that runs on a laptop CPU.

About 30% of the feed's descriptions are in Malay and the rest in English. An English-only model
(bge-small-en-v1.5) barely separates "coffee shop" from "hotel" across languages, so we use a multilingual one.
"""

from typing import Final, Protocol

import numpy as np

MODEL_NAME: Final = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
DIMENSIONS: Final = 384


class Embedder(Protocol):
    def embed(self, texts: list[str]) -> list[np.ndarray]: ...


class FastEmbedder:
    """Loads the model on first use and remembers results: the feed has few distinct descriptions."""

    def __init__(self, model_name: str = MODEL_NAME) -> None:
        self._model_name = model_name
        self._model = None
        self._cache: dict[str, np.ndarray] = {}

    def warm_up(self) -> None:
        if self._model is None:
            from fastembed import TextEmbedding

            self._model = TextEmbedding(self._model_name)

    def embed(self, texts: list[str]) -> list[np.ndarray]:
        missing = list(dict.fromkeys(text for text in texts if text not in self._cache))
        if missing:
            self.warm_up()
            assert self._model is not None
            for text, vector in zip(missing, self._model.embed(missing), strict=True):
                self._cache[text] = vector.astype(np.float32)
        return [self._cache[text] for text in texts]
