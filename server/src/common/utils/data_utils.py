from typing import List

import numpy as np


def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    if not vec_a or not vec_b or None in vec_a or None in vec_b:
        return 0.0

    a = np.asarray(vec_a, dtype=float)
    b = np.asarray(vec_b, dtype=float)

    if a.shape != b.shape:
        return 0.0

    denominator = np.linalg.norm(a) * np.linalg.norm(b)
    if denominator == 0:
        return 0.0

    return float(np.dot(a, b) / denominator)
