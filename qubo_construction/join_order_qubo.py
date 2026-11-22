from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from .qubo_builder import QuboBuilder


PAPER_TITLE = "Quantum-Inspired Digital Annealing for Join Ordering"
PAPER_REPO_URL = "https://github.com/lfd/vldb24"
RELATION_NAMES = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")


@dataclass(frozen=True)
class Relation:
    name: str
    log_cardinality: float


@dataclass(frozen=True)
class Predicate:
    name: str
    left_relation: str
    right_relation: str
    log_selectivity: float


@dataclass
class JoinOrderInstance:
    relations: List[Relation]
    predicates: List[Predicate]
    validity_weight: float = 20.0
    predicate_weight: float | None = None
    cost_weight: float = 1.0


def relation_join_var(relation: str, join_index: int) -> str:
    return f"roj::{relation}::j{join_index}"


def predicate_join_var(predicate: str, join_index: int) -> str:
    return f"paj::{predicate}::j{join_index}"


def build_join_order_qubo(instance: JoinOrderInstance) -> Dict[str, object]:
    num_relations = len(instance.relations)
    num_joins = max(0, num_relations - 2)
    if instance.validity_weight <= 0.0:
        raise ValueError("validity_weight must be positive")
    if instance.cost_weight < 0.0:
        raise ValueError("cost_weight must be non-negative")

    effective_validity_weight = instance.validity_weight * num_joins
    effective_predicate_weight = (
        effective_validity_weight
        if instance.predicate_weight is None
        else float(instance.predicate_weight)
    )
    if effective_predicate_weight <= 0.0:
        raise ValueError("predicate_weight must be positive")
    builder = QuboBuilder()

    relation_names = [relation.name for relation in instance.relations]
    relation_by_name = {relation.name: relation for relation in instance.relations}

    for join_index in range(num_joins):
        builder.add_square(
            (
                (relation_join_var(relation_name, join_index), 1.0)
                for relation_name in relation_names
            ),
            target=join_index + 2,
            weight=effective_validity_weight,
        )

    for relation_name in relation_names:
        for join_index in range(1, num_joins):
            previous_var = relation_join_var(relation_name, join_index - 1)
            current_var = relation_join_var(relation_name, join_index)
            builder.add_linear(previous_var, effective_validity_weight)
            builder.add_quadratic(previous_var, current_var, -effective_validity_weight)

    for predicate in instance.predicates:
        for join_index in range(num_joins):
            predicate_var = predicate_join_var(predicate.name, join_index)
            left_var = relation_join_var(predicate.left_relation, join_index)
            right_var = relation_join_var(predicate.right_relation, join_index)
            builder.add_linear(predicate_var, 2.0 * effective_predicate_weight)
            builder.add_quadratic(predicate_var, left_var, -effective_predicate_weight)
            builder.add_quadratic(predicate_var, right_var, -effective_predicate_weight)

    for join_index in range(num_joins):
        cost_terms: List[Tuple[str, float]] = [
            (
                relation_join_var(relation_name, join_index),
                relation_by_name[relation_name].log_cardinality,
            )
            for relation_name in relation_names
        ] + [
            (predicate_join_var(predicate.name, join_index), predicate.log_selectivity)
            for predicate in instance.predicates
        ]
        builder.add_square(cost_terms, target=0.0, weight=instance.cost_weight)

    variables: List[Dict[str, object]] = []
    for join_index in range(num_joins):
        for relation in instance.relations:
            variables.append(
                {
                    "name": relation_join_var(relation.name, join_index),
                    "kind": "relation_operand_for_join",
                    "join_index": join_index,
                    "relation": relation.name,
                    "db_element": {"type": "relation", "name": relation.name},
                }
            )
        for predicate in instance.predicates:
            variables.append(
                {
                    "name": predicate_join_var(predicate.name, join_index),
                    "kind": "predicate_applicable_for_join",
                    "join_index": join_index,
                    "predicate": predicate.name,
                    "db_element": {
                        "type": "predicate",
                        "name": predicate.name,
                        "relations": [
                            predicate.left_relation,
                            predicate.right_relation,
                        ],
                    },
                }
            )

    return {
        "paper": PAPER_TITLE,
        "repo_url": PAPER_REPO_URL,
        "note": (
            "This builder keeps the paper's roj/paj variable families and validity terms, "
            "while using a visualization-oriented logarithmic cost approximation instead of "
            "the full threshold discretisation used for Fujitsu DA deployment."
        ),
        "qubo": builder.to_payload(),
        "variables": variables,
        "construction_blocks": [
            {
                "name": "HVa",
                "title": "Prefix Size",
                "formula": "(b_j - sum_r roj[r,j])^2",
                "meaning": "Keep the right number of relations at each step.",
                "variables": ["roj"],
            },
            {
                "name": "HVb",
                "title": "Carry Forward",
                "formula": "roj[r,j-1] * (1 - roj[r,j])",
                "meaning": "Once a relation appears, keep it in later steps.",
                "variables": ["roj"],
            },
            {
                "name": "Hp",
                "title": "Predicate Ready",
                "formula": "paj[p,j] * (2 - roj[left,j] - roj[right,j])",
                "meaning": "Activate a predicate only when both relations are present.",
                "variables": ["paj", "roj"],
            },
            {
                "name": "Cost",
                "title": "Cost Approximation",
                "formula": "sum_j (sum_r log_cardinality * roj[r,j] + sum_p log_selectivity * paj[p,j])^2",
                "meaning": "Approximate the join cost as a squared (quadratic) term so predicate selection is a genuine tradeoff.",
                "variables": ["roj", "paj"],
            },
        ],
        "viz": {
            "join_prefixes": [
                {
                    "join_index": join_index,
                    "required_relations": join_index + 2,
                    "relation_vars": [
                        relation_join_var(name, join_index) for name in relation_names
                    ],
                    "predicate_vars": [
                        predicate_join_var(predicate.name, join_index)
                        for predicate in instance.predicates
                    ],
                }
                for join_index in range(num_joins)
            ],
            "join_graph": {
                "nodes": [
                    {
                        "id": relation.name,
                        "label": relation.name,
                        "log_cardinality": relation.log_cardinality,
                    }
                    for relation in instance.relations
                ],
                "edges": [
                    {
                        "id": predicate.name,
                        "source": predicate.left_relation,
                        "target": predicate.right_relation,
                        "label": predicate.name,
                        "log_selectivity": predicate.log_selectivity,
                    }
                    for predicate in instance.predicates
                ],
            },
        },
    }


def build_demo_qubo(scale: int = 4) -> Dict[str, object]:
    num_relations = max(3, min(scale, len(RELATION_NAMES)))
    relation_names = RELATION_NAMES[:num_relations]
    relations = [
        Relation(
            name, 2.0 + (idx % 3) * 0.5 + (0.5 if idx == num_relations - 1 else 0.0)
        )
        for idx, name in enumerate(relation_names)
    ]
    predicates = [
        Predicate(
            f"p_{relation_names[idx].lower()}{relation_names[idx + 1].lower()}",
            relation_names[idx],
            relation_names[idx + 1],
            -max(0.05, 1.2 - idx * 0.15),
        )
        for idx in range(num_relations - 1)
    ]
    if num_relations >= 4:
        predicates.append(
            Predicate(
                f"p_{relation_names[0].lower()}{relation_names[2].lower()}",
                relation_names[0],
                relation_names[2],
                -0.45,
            )
        )
    instance = JoinOrderInstance(
        relations=relations,
        predicates=predicates,
    )
    return build_join_order_qubo(instance)
