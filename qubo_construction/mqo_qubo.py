from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple


PAPER_TITLE = "Large-Scale Multiple Query Optimisation with Incremental Quantum(-Inspired) Annealing"
PAPER_REPO_URL = "https://github.com/lfd/sigmod26"
RELATED_BASELINE_REPO_URL = "https://github.com/itrummer/quantumdb"


@dataclass(frozen=True)
class Plan:
    name: str
    query: str
    cost: float


@dataclass(frozen=True)
class Savings:
    left_plan: str
    right_plan: str
    value: float


@dataclass
class MQOInstance:
    plans: List[Plan]
    savings: List[Savings]
    exactly_one_weight: float | None = None


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


def plan_var(plan_name: str) -> str:
    return f"plan::{plan_name}"


def build_mqo_qubo(instance: MQOInstance) -> Dict[str, object]:
    builder = QuboBuilder()
    plans_by_query: Dict[str, List[Plan]] = {}

    for plan in instance.plans:
        plans_by_query.setdefault(plan.query, []).append(plan)
        builder.add_linear(plan_var(plan.name), plan.cost)

    for saving in instance.savings:
        builder.add_quadratic(plan_var(saving.left_plan), plan_var(saving.right_plan), -saving.value)

    default_weight = max((plan.cost for plan in instance.plans), default=1.0) + sum(
        saving.value for saving in instance.savings
    )
    exactly_one_weight = instance.exactly_one_weight or (default_weight + 1.0)

    # Classical MQO QUBO:
    # sum plan costs - sum pairwise savings + lambda * sum_q (sum_{p in P_q} x_p - 1)^2
    for query, query_plans in plans_by_query.items():
        builder.add_square(
            ((plan_var(plan.name), 1.0) for plan in query_plans),
            target=1.0,
            weight=exactly_one_weight,
        )

    variables = [
        {
            "name": plan_var(plan.name),
            "kind": "plan_selection",
            "query": plan.query,
            "plan": plan.name,
            "cost": plan.cost,
            "db_element": {"type": "plan", "query": plan.query, "name": plan.name},
        }
        for plan in instance.plans
    ]

    return {
        "paper": PAPER_TITLE,
        "repo_url": PAPER_REPO_URL,
        "related_repo_url": RELATED_BASELINE_REPO_URL,
        "note": (
            "This module implements the core MQO QUBO used by quantum/quantum-inspired solvers. "
            "The SIGMOD 2025 paper adds incremental partitioning and dynamic search steering on top "
            "of this base plan-selection encoding."
        ),
        "qubo": builder.export(),
        "variables": variables,
        "construction_blocks": [
            {
                "name": "HSel",
                "title": "Plan Selection",
                "formula": "sum_q (sum_{p in P_q} x_p - 1)^2",
                "meaning": "Force each query to keep exactly one execution plan.",
                "variables": ["plan"],
            },
            {
                "name": "HShare",
                "title": "Shared Reuse",
                "formula": "-sum s_ij x_i x_j",
                "meaning": "Reward compatible plans that can share intermediate work across queries.",
                "variables": ["plan"],
            },
            {
                "name": "HCost",
                "title": "Execution Cost",
                "formula": "sum c_i x_i",
                "meaning": "Model the execution cost contributed by the selected plans.",
                "variables": ["plan"],
            },
        ],
        "viz": {
            "query_groups": [
                {
                    "query": query,
                    "plan_vars": [plan_var(plan.name) for plan in query_plans],
                }
                for query, query_plans in sorted(plans_by_query.items())
            ],
            "mqo_graph": {
                "nodes": [
                    {"id": plan.name, "label": plan.name, "query": plan.query, "cost": plan.cost}
                    for plan in instance.plans
                ],
                "edges": [
                    {
                        "source": saving.left_plan,
                        "target": saving.right_plan,
                        "saving": saving.value,
                    }
                    for saving in instance.savings
                ],
            },
        },
    }


def build_demo_model(scale: int = 3) -> Dict[str, object]:
    num_queries = max(2, min(scale, 6))
    plans: List[Plan] = []
    savings: List[Savings] = []
    for query_idx in range(num_queries):
        query_name = f"q{query_idx + 1}"
        for plan_offset in range(2):
            plan_id = query_idx * 2 + plan_offset + 1
            plans.append(Plan(f"p{plan_id}", query_name, 9.0 + ((query_idx + plan_offset) % 4)))
    for query_idx in range(num_queries - 1):
        left_a = f"p{query_idx * 2 + 1}"
        left_b = f"p{query_idx * 2 + 2}"
        right_a = f"p{(query_idx + 1) * 2 + 1}"
        right_b = f"p{(query_idx + 1) * 2 + 2}"
        savings.extend(
            [
                Savings(left_a, right_a, 1.0 + query_idx),
                Savings(left_b, right_b, 3.0 + query_idx),
            ]
        )
        if query_idx + 2 < num_queries:
            far = f"p{(query_idx + 2) * 2 + 1}"
            savings.append(Savings(left_b, far, 2.0 + 0.5 * query_idx))
    instance = MQOInstance(
        plans=plans,
        savings=savings,
    )
    return build_mqo_qubo(instance)


if __name__ == "__main__":
    print(json.dumps(build_demo_model(), indent=2))
