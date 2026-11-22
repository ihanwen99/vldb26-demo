from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from .qubo_builder import QuboBuilder


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


def index_var(index_name: str) -> str:
    return f"index::{index_name}"


def storage_slack_var(k: int) -> str:
    return f"cap::k{k}"


def compute_binary_storage_fractions(storage_bound: int) -> List[int]:
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
    fractions = compute_binary_storage_fractions(instance.storage_bound)

    max_utility = max((index.utility for index in instance.indices), default=1.0)
    max_storage = max((index.storage for index in instance.indices), default=1)
    default_constraint_weight = max_utility + max_storage * instance.storage_bound + 1.0
    constraint_weight = (
        default_constraint_weight
        if instance.constraint_weight is None
        else float(instance.constraint_weight)
    )
    if constraint_weight <= 0.0:
        raise ValueError("constraint_weight must be positive")
    if constraint_weight < default_constraint_weight:
        raise ValueError(
            f"constraint_weight is too small: got {constraint_weight}, require at least {default_constraint_weight}"
        )

    for index in instance.indices:
        builder.add_linear(index_var(index.name), -index.utility)

    clustered_by_table: Dict[str, List[IndexCandidate]] = {}
    for index in instance.indices:
        if index.clustered:
            clustered_by_table.setdefault(index.table, []).append(index)

    for table, clustered in clustered_by_table.items():
        for left_pos, left in enumerate(clustered):
            for right in clustered[left_pos + 1 :]:
                builder.add_quadratic(
                    index_var(left.name), index_var(right.name), constraint_weight
                )

    storage_terms: List[Tuple[str, float]] = [
        (index_var(index.name), float(index.storage)) for index in instance.indices
    ] + [
        (storage_slack_var(k), float(-fraction)) for k, fraction in enumerate(fractions)
    ]
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
                "db_element": {
                    "type": "index_candidate",
                    "name": index.name,
                    "table": index.table,
                },
            }
        )
    for k, fraction in enumerate(fractions):
        variables.append(
            {
                "name": storage_slack_var(k),
                "kind": "storage_fraction",
                "fraction": fraction,
                "db_element": {"type": "storage_fraction", "value": fraction},
            }
        )

    return {
        "paper": PAPER_TITLE,
        "paper_url": PAPER_URL,
        "note": (
            "This demo builder uses workload-benefit utility, same-table clustered-index exclusion, "
            "and binary storage-fraction slack variables. It does not model a general incompatibility "
            "catalog or an optimizer-derived workload-cost function."
        ),
        "qubo": builder.to_payload(),
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


def build_demo_qubo(scale: int = 3) -> Dict[str, object]:
    base_tables = ["orders", "lineitem", "customer", "supplier", "part", "nation"]
    num_tables = max(2, min(scale, len(base_tables)))
    indices: List[IndexCandidate] = []
    for idx, table in enumerate(base_tables[:num_tables]):
        indices.append(
            IndexCandidate(
                f"idx_{table}_main", table, 2 + (idx % 3), 5.0 + idx, clustered=False
            )
        )
        indices.append(
            IndexCandidate(
                f"idx_{table}_clustered_a",
                table,
                3 + (idx % 2),
                6.5 + idx,
                clustered=True,
            )
        )
        if idx % 2 == 0:
            indices.append(
                IndexCandidate(
                    f"idx_{table}_clustered_b",
                    table,
                    2 + (idx % 2),
                    4.5 + idx,
                    clustered=True,
                )
            )
    storage_bound = max(6, int(sum(index.storage for index in indices) * 0.45))
    instance = IndexSelectionInstance(
        indices=indices,
        storage_bound=storage_bound,
    )
    return build_index_selection_qubo(instance)
