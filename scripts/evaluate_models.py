"""Compare embedding models by how often a company's nearest neighbours share its registry industry.

    just evaluate        # needs the companies table filled by the consumer

The registry's `industry` is a free label, so precision@k = "of the k most similar companies, how many are in the
same industry" is a cheap proxy for recommendation quality. Caveats are in DESIGN.md.

Companies sharing one description get identical embeddings and would make the score look better than it is, so each
distinct description is evaluated once, labelled with its most common industry.
"""

import re
from collections import Counter, defaultdict

import numpy as np
import psycopg
from fastembed import TextEmbedding

from common import config

MODELS = ("BAAI/bge-small-en-v1.5", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
K = 5
# A crude marker for Malay descriptions (there is no language field in the feed); good enough to split the report.
MALAY = re.compile(r"\b(dan|untuk|kerja|perkhidmatan|pengeluaran|jualan|kedai|pembinaan|pembuatan|restoran|pengangkutan|runcit|borong|ladang|hotel butik)\b", re.I)


def load_descriptions() -> dict[str, str]:
    """Distinct description -> its most common industry (companies with no industry are skipped)."""
    with psycopg.connect(config.database_url()) as connection:
        rows = connection.execute("SELECT description, industry FROM companies WHERE description IS NOT NULL AND industry IS NOT NULL").fetchall()
    industries: dict[str, Counter[str]] = defaultdict(Counter)
    for description, industry in rows:
        industries[description][industry] += 1
    return {description: counts.most_common(1)[0][0] for description, counts in industries.items()}


def precision_at_k(vectors: np.ndarray, labels: list[str], k: int) -> np.ndarray:
    normalised = vectors / np.linalg.norm(vectors, axis=1, keepdims=True)
    similarity = normalised @ normalised.T
    np.fill_diagonal(similarity, -np.inf)
    neighbours = np.argsort(-similarity, axis=1)[:, :k]
    label_array = np.array(labels)
    return (label_array[neighbours] == label_array[:, None]).mean(axis=1)


def main() -> None:
    labelled = load_descriptions()
    texts = sorted(labelled)
    labels = [labelled[text] for text in texts]
    is_malay = np.array([bool(MALAY.search(text)) for text in texts])
    print(f"{len(texts)} distinct descriptions with an industry ({is_malay.sum()} look Malay), k={K}")
    print(f"random baseline: {sum((c / len(labels)) ** 2 for c in Counter(labels).values()):.2f}\n")
    print(f"{'model':<62}{'all':>6}{'english':>9}{'malay':>7}")
    for model in MODELS:
        vectors = np.array(list(TextEmbedding(model).embed(texts)))
        precision = precision_at_k(vectors, labels, K)
        print(f"{model:<62}{precision.mean():>6.2f}{precision[~is_malay].mean():>9.2f}{precision[is_malay].mean():>7.2f}")


if __name__ == "__main__":
    main()
