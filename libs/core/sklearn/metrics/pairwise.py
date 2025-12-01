"""Pairwise metrics shim implementing linear_kernel."""

from __future__ import annotations

from typing import Iterable, List, Mapping


def _dot(lhs: Mapping[str, float], rhs: Mapping[str, float]) -> float:
    if not lhs or not rhs:
        return 0.0
    # iterate over smaller mapping for efficiency
    if len(lhs) > len(rhs):
        lhs, rhs = rhs, lhs
    total = 0.0
    for key, value in lhs.items():
        other = rhs.get(key)
        if other is not None:
            total += value * other
    return total


def linear_kernel(matrix_a, matrix_b) -> List[List[float]]:
    """Compute linear kernel (dot product) between sparse dict vectors."""

    vectors_a: Iterable[Mapping[str, float]] = getattr(matrix_a, "vectors", []) or []
    vectors_b: Iterable[Mapping[str, float]] = getattr(matrix_b, "vectors", []) or []
    vectors_b = list(vectors_b)
    if not vectors_b:
        return [[0.0] * 0 for _ in vectors_a]
    if not vectors_a:
        return [[0.0 for _ in vectors_b]]

    result: List[List[float]] = []
    for vec_a in vectors_a:
        row = [_dot(vec_a, vec_b) for vec_b in vectors_b]
        result.append(row)
    return result


__all__ = ["linear_kernel"]
