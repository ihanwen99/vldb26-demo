from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple


PAPER_TITLE = "Leveraging Quantum Computing for Database Index Selection"
PAPER_URL = "https://doi.org/10.1145/3665225.3665445"


@dataclass(frozen=True)
class IndexCandidate:
    name: str
    table: str
    storage: int
    utility: float
    clustered: bool = False


@dataclass
class IndexSelectionInstance:
    indices: List[IndexCandidate]
    storage_bound: int
    constraint_weight: float | None = None


class QuboBuilder:
    def __init__(self) -> None:
        self.linear: Dict[str, float] = {}
        self.quadratic: Dict[Tuple[str, str], float] = {}
        self.offset: float = 0.0

    def add_linear(self, var: str, value: float) -> None:
        self.linear[var] = self.linear.get(var, 0.0) + value

    def add_quadratic(self, left: str, right: str, value: float) -> None:
        key = tuple(sorted((left, right)))
        self.quadratic[key] = self.quadratic.get(key, 0.0) + value

    def add_constant(self, value: float) -> None:
        self.offset += value

    def add_square(self, terms: Iterable[Tuple[str, float]], target: float, weight: float) -> None:
        expanded = list(terms)
        self.add_constant(weight * target * target)
        for var, coeff in expanded:
            self.add_linear(var, weight * (coeff * coeff - 2.0 * target * coeff))
        for idx, (left, left_coeff) in enumerate(expanded):
            for right, right_coeff in expanded[idx + 1 :]:
                self.add_quadratic(left, right, weight * 2.0 * left_coeff * right_coeff)

    def export(self) -> Dict[str, object]:
        return {
            "linear": dict(sorted(self.linear.items())),
            "quadratic": [
                {"u": left, "v": right, "value": value}
                for (left, right), value in sorted(self.quadratic.items())
            ],
            "offset": self.offset,
        }


def index_var(index_name: str) -> str:
    return f"index::{index_name}"


def capacity_var(capacity: int) -> str:
    return f"cap::{capacity}"


def binary_storage_fractions(storage_bound: int) -> List[int]:
    fractions: List[int] = []
    remaining = storage_bound
    current = 1
    while remaining > 0:
        fraction = min(current, remaining)
        fractions.append(fraction)
        remaining -= fraction
        current *= 2
    return fractions


def build_index_selection_qubo(instance: IndexSelectionInstance) -> Dict[str, object]:
    builder = QuboBuilder()
    fractions = binary_storage_fractions(instance.storage_bound)

    max_utility = max((index.utility for index in instance.indices), default=1.0)
    max_storage = max((index.storage for index in instance.indices), default=1)
    constraint_weight = instance.constraint_weight or (max_utility + max_storage * instance.storage_bound + 1.0)

    # EU term from the paper.
    for index in instance.indices:
        builder.add_linear(index_var(index.name), -index.utility)

    # EM term from the paper: pairwise penalties for clustered indices on the same table.
    clustered_by_table: Dict[str, List[IndexCandidate]] = {}
    for index in instance.indices:
        if index.clustered:
            clustered_by_table.setdefault(index.table, []).append(index)

    for table, clustered in clustered_by_table.items():
        for left_pos, left in enumerate(clustered):
            for right in clustered[left_pos + 1 :]:
                builder.add_quadratic(index_var(left.name), index_var(right.name), constraint_weight)

    # ES^(1) from the paper:
    # (sum_i s_i g_i - sum_k f_k c_k)^2
    storage_terms: List[Tuple[str, float]] = [
        (index_var(index.name), float(index.storage)) for index in instance.indices
    ] + [(capacity_var(fraction), float(-fraction)) for fraction in fractions]
    builder.add_square(storage_terms, target=0.0, weight=constraint_weight)

    variables: List[Dict[str, object]] = []
    for index in instance.indices:
        variables.append(
            {
                "name": index_var(index.name),
                "kind": "index_selection",
                "index": index.name,
                "table": index.table,
                "storage": index.storage,
                "utility": index.utility,
                "clustered": index.clustered,
                "db_element": {"type": "index_candidate", "name": index.name, "table": index.table},
            }
        )
    for fraction in fractions:
        variables.append(
            {
                "name": capacity_var(fraction),
                "kind": "storage_fraction",
                "fraction": fraction,
                "db_element": {"type": "storage_fraction", "value": fraction},
            }
        )

    return {
        "paper": PAPER_TITLE,
        "paper_url": PAPER_URL,
        "note": (
            "This module implements the paper's utility term, pairwise clustered-index exclusion term, "
            "and the first storage-capacity QUBO using binary storage fractions."
        ),
        "qubo": builder.export(),
        "variables": variables,
        "construction_blocks": [
            {
                "name": "ES",
                "title": "Storage Budget",
                "formula": "(sum s_i g_i - sum f_k c_k)^2",
                "meaning": "Keep the chosen indexes within the storage capacity.",
                "variables": ["index", "cap"],
            },
            {
                "name": "EM",
                "title": "Index Conflict",
                "formula": "1/2 sum_t sum_{i1,i2 in C_t} g_i1 g_i2",
                "meaning": "Block incompatible clustered indexes from being selected together.",
                "variables": ["index"],
            },
            {
                "name": "EU",
                "title": "Utility",
                "formula": "-sum u_i g_i",
                "meaning": "Reward index choices with higher workload benefit.",
                "variables": ["index"],
            },
        ],
        "viz": {
            "tables": [
                {
                    "table": table,
                    "clustered_candidates": [index.name for index in clustered],
                }
                for table, clustered in sorted(clustered_by_table.items())
            ],
            "storage_bound": instance.storage_bound,
            "storage_fractions": fractions,
        },
    }


def build_demo_model(scale: int = 3) -> Dict[str, object]:
    base_tables = ["orders", "lineitem", "customer", "supplier", "part", "nation"]
    num_tables = max(2, min(scale, len(base_tables)))
    indices: List[IndexCandidate] = []
    for idx, table in enumerate(base_tables[:num_tables]):
        indices.append(IndexCandidate(f"idx_{table}_main", table, 2 + (idx % 3), 5.0 + idx, clustered=False))
        indices.append(IndexCandidate(f"idx_{table}_clustered_a", table, 3 + (idx % 2), 6.5 + idx, clustered=True))
        if idx % 2 == 0:
            indices.append(IndexCandidate(f"idx_{table}_clustered_b", table, 2 + (idx % 2), 4.5 + idx, clustered=True))
    storage_bound = max(6, int(sum(index.storage for index in indices) * 0.45))
    instance = IndexSelectionInstance(
        indices=indices,
        storage_bound=storage_bound,
    )
    return build_index_selection_qubo(instance)


if __name__ == "__main__":
    print(json.dumps(build_demo_model(), indent=2))
