from __future__ import annotations

from typing import Dict, Iterable, Tuple


class QuboBuilder:
    def __init__(self) -> None:
        self.linear: Dict[str, float] = {}
        self.quadratic: Dict[Tuple[str, str], float] = {}
        self.offset: float = 0.0

    def add_linear(self, var: str, value: float) -> None:
        self.linear[var] = self.linear.get(var, 0.0) + value

    def add_quadratic(self, left: str, right: str, value: float) -> None:
        if left == right:
            self.add_linear(left, value)
            return
        key = tuple(sorted((left, right)))
        self.quadratic[key] = self.quadratic.get(key, 0.0) + value

    def add_constant(self, value: float) -> None:
        self.offset += value

    def add_square(
        self, terms: Iterable[Tuple[str, float]], target: float, weight: float
    ) -> None:
        expanded = list(terms)
        self.add_constant(weight * target * target)
        for var, coeff in expanded:
            self.add_linear(var, weight * (coeff * coeff - 2.0 * target * coeff))
        for idx, (left, left_coeff) in enumerate(expanded):
            for right, right_coeff in expanded[idx + 1 :]:
                self.add_quadratic(left, right, weight * 2.0 * left_coeff * right_coeff)

    def to_payload(self) -> Dict[str, object]:
        return {
            "linear": dict(sorted(self.linear.items())),
            "quadratic": [
                {"u": left, "v": right, "value": value}
                for (left, right), value in sorted(self.quadratic.items())
            ],
            "offset": self.offset,
        }
