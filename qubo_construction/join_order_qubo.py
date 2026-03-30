from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple


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
    predicate_weight: float = 8.0
    cost_weight: float = 1.0


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


def roj_var(relation: str, join_index: int) -> str:
    return f"roj::{relation}::j{join_index}"


def paj_var(predicate: str, join_index: int) -> str:
    return f"paj::{predicate}::j{join_index}"


def build_join_order_qubo(instance: JoinOrderInstance) -> Dict[str, object]:
    num_relations = len(instance.relations)
    num_joins = max(0, num_relations - 1)
    builder = QuboBuilder()

    relation_names = [relation.name for relation in instance.relations]
    relation_by_name = {relation.name: relation for relation in instance.relations}

    # Paper-aligned validity term HVa:
    # each join prefix j must contain exactly j+2 relations.
    for join_index in range(num_joins):
        builder.add_square(
            ((roj_var(relation_name, join_index), 1.0) for relation_name in relation_names),
            target=join_index + 2,
            weight=instance.validity_weight,
        )

    # Paper-aligned validity term HVb:
    # once a relation appears in a prefix, it must stay in all later prefixes.
    for relation_name in relation_names:
        for join_index in range(1, num_joins):
            previous_var = roj_var(relation_name, join_index - 1)
            current_var = roj_var(relation_name, join_index)
            builder.add_linear(previous_var, instance.validity_weight)
            builder.add_quadratic(previous_var, current_var, -instance.validity_weight)

    # Paper-aligned predicate applicability term Hp.
    for predicate in instance.predicates:
        for join_index in range(num_joins):
            predicate_var = paj_var(predicate.name, join_index)
            left_var = roj_var(predicate.left_relation, join_index)
            right_var = roj_var(predicate.right_relation, join_index)
            builder.add_linear(predicate_var, 2.0 * instance.predicate_weight)
            builder.add_quadratic(predicate_var, left_var, -instance.predicate_weight)
            builder.add_quadratic(predicate_var, right_var, -instance.predicate_weight)

    # Visualization-friendly cost approximation:
    # sum of logarithmic intermediate cardinalities across join prefixes.
    # This keeps the same roj/paj variable families from the paper while
    # avoiding the hardware-specific threshold discretisation in the first version.
    for join_index in range(num_joins):
        for relation_name in relation_names:
            builder.add_linear(
                roj_var(relation_name, join_index),
                instance.cost_weight * relation_by_name[relation_name].log_cardinality,
            )
        for predicate in instance.predicates:
            builder.add_linear(
                paj_var(predicate.name, join_index),
                instance.cost_weight * predicate.log_selectivity,
            )

    variables: List[Dict[str, object]] = []
    for join_index in range(num_joins):
        for relation in instance.relations:
            variables.append(
                {
                    "name": roj_var(relation.name, join_index),
                    "kind": "relation_operand_for_join",
                    "join_index": join_index,
                    "relation": relation.name,
                    "db_element": {"type": "relation", "name": relation.name},
                }
            )
        for predicate in instance.predicates:
            variables.append(
                {
                    "name": paj_var(predicate.name, join_index),
                    "kind": "predicate_applicable_for_join",
                    "join_index": join_index,
                    "predicate": predicate.name,
                    "db_element": {
                        "type": "predicate",
                        "name": predicate.name,
                        "relations": [predicate.left_relation, predicate.right_relation],
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
        "qubo": builder.export(),
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
                "formula": "sum log_cardinality + sum log_selectivity",
                "meaning": "Approximate the join cost.",
                "variables": ["roj", "paj"],
            },
        ],
        "viz": {
            "join_prefixes": [
                {
                    "join_index": join_index,
                    "required_relations": join_index + 2,
                    "relation_vars": [roj_var(name, join_index) for name in relation_names],
                    "predicate_vars": [paj_var(predicate.name, join_index) for predicate in instance.predicates],
                }
                for join_index in range(num_joins)
            ],
            "join_graph": {
                "nodes": [
                    {"id": relation.name, "label": relation.name, "log_cardinality": relation.log_cardinality}
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


def build_demo_model(scale: int = 4) -> Dict[str, object]:
    num_relations = max(3, min(scale, len(RELATION_NAMES)))
    relation_names = RELATION_NAMES[:num_relations]
    relations = [
        Relation(name, 2.0 + (idx % 3) * 0.5 + (0.5 if idx == num_relations - 1 else 0.0))
        for idx, name in enumerate(relation_names)
    ]
    predicates = [
        Predicate(
            f"p_{relation_names[idx].lower()}{relation_names[idx + 1].lower()}",
            relation_names[idx],
            relation_names[idx + 1],
            -1.2 + idx * 0.25,
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


if __name__ == "__main__":
    print(json.dumps(build_demo_model(), indent=2))
