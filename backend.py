from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Tuple

from qubo_construction.index_selection_qubo import build_demo_model as build_index_selection_demo
from qubo_construction.join_order_qubo import build_demo_model as build_join_order_demo
from qubo_construction.mqo_qubo import build_demo_model as build_mqo_demo


ProblemPayload = Dict[str, object]
CACHE_DIR = Path(__file__).resolve().parent / ".cache"
CACHE_VERSION = "v12"
REAL_FUSION_CACHE_VERSION = "rf8"
MERGE_STRATEGIES = ("direct_fusion", "top2_merge", "conditioned_fusion")
MERGE_ORDERS = ("left_deep", "bushy")
PLANNER_MODES = ("default", "cost_based")
PROBLEM_SCALE_BOUNDS = {
    "join_order": (3, 8),
    "mqo": (2, 6),
    "index_selection": (2, 6),
}
PARTITION_BOUNDS = (2, 8)
FUSION_CANDIDATE_LIMIT = 2
FUSION_NUM_READS = 20
_QPU_LOCK = threading.Lock()


PROBLEM_BUILDERS = {
    "join_order": build_join_order_demo,
    "mqo": build_mqo_demo,
    "index_selection": build_index_selection_demo,
}


PROBLEM_LABELS = {
    "join_order": "Join Order",
    "mqo": "Multiple Query Optimization",
    "index_selection": "Index Selection",
}


class LiveFusionDisabledError(RuntimeError):
    """Raised when a cache miss would require a disabled live QPU call."""


def validate_request_parameters(
    problem: str,
    scale: int,
    partitions: int,
    merge_strategy: str,
    merge_order: str,
    planner_mode: str,
) -> None:
    if problem not in PROBLEM_BUILDERS:
        raise KeyError(f"Unknown problem: {problem}")
    if merge_strategy not in MERGE_STRATEGIES:
        raise ValueError(f"Unknown merge strategy: {merge_strategy}")
    if merge_order not in MERGE_ORDERS:
        raise ValueError(f"Unknown fusion tree structure: {merge_order}")
    if planner_mode not in PLANNER_MODES:
        raise ValueError(f"Unknown planner mode: {planner_mode}")

    minimum_scale, maximum_scale = PROBLEM_SCALE_BOUNDS[problem]
    if not minimum_scale <= scale <= maximum_scale:
        raise ValueError(
            f"scale for {problem} must be between {minimum_scale} and {maximum_scale}"
        )
    minimum_partitions, maximum_partitions = PARTITION_BOUNDS
    if not minimum_partitions <= partitions <= maximum_partitions:
        raise ValueError(
            f"partitions must be between {minimum_partitions} and {maximum_partitions}"
        )


def _load_cache(cache_path: Path) -> Dict[str, object] | None:
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    payload["cache_hit"] = True
    return payload


def _write_cache(cache_path: Path, payload: Dict[str, object]) -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    temporary_path: Path | None = None
    try:
        with NamedTemporaryFile(
            "w", encoding="utf-8", dir=CACHE_DIR, delete=False
        ) as handle:
            json.dump(payload, handle)
            handle.flush()
            os.fsync(handle.fileno())
            temporary_path = Path(handle.name)
        os.replace(temporary_path, cache_path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


@dataclass
class Edge:
    left: str
    right: str
    value: float


def build_problem_payload(
    problem: str,
    scale: int,
    partitions: int,
    merge_strategy: str,
    merge_order: str,
    planner_mode: str = "default",
) -> ProblemPayload:
    validate_request_parameters(
        problem, scale, partitions, merge_strategy, merge_order, planner_mode
    )
    cache_key = (
        f"{CACHE_VERSION}_{problem}_s{scale}_p{partitions}_"
        f"{merge_strategy}_{merge_order}_{planner_mode}.json"
    )
    cache_path = CACHE_DIR / cache_key
    cached_payload = _load_cache(cache_path)
    if cached_payload is not None:
        return cached_payload

    base = PROBLEM_BUILDERS[problem](scale)
    graph = qubo_to_graph(base)
    enrich_construction_blocks(problem, base, graph)
    partitioning = decompose_problem(problem, base, graph, max(2, partitions))
    merge_plan = build_merge_plan(graph, partitioning, merge_strategy, merge_order, planner_mode)
    metrics = summarize_metrics(graph, partitioning, merge_plan)

    payload = {
        "cache_hit": False,
        "problem_id": problem,
        "problem_label": PROBLEM_LABELS[problem],
        "scale": scale,
        "requested_partitions": partitions,
        "effective_partitions": len(partitioning["partitions"]),
        "construction": base,
        "graph": graph,
        "partitioning": partitioning,
        "merge_plan": merge_plan,
        "metrics": metrics,
    }
    _write_cache(cache_path, payload)
    return payload


def build_real_fusion_payload(
    problem: str,
    scale: int,
    partitions: int,
    merge_strategy: str,
    merge_order: str,
    planner_mode: str = "default",
    allow_live: bool = False,
) -> Dict[str, object]:
    validate_request_parameters(
        problem, scale, partitions, merge_strategy, merge_order, planner_mode
    )
    cache_key = (
        f"{REAL_FUSION_CACHE_VERSION}_{problem}_s{scale}_p{partitions}_"
        f"{merge_strategy}_{merge_order}_{planner_mode}_"
        f"k{FUSION_CANDIDATE_LIMIT}_r{FUSION_NUM_READS}.json"
    )
    cache_path = CACHE_DIR / cache_key
    cached_payload = _load_cache(cache_path)
    if cached_payload is not None:
        cached_payload["message"] = "Replayed a cached fusion result; no QPU job was submitted for this request."
        return cached_payload
    if not allow_live:
        raise LiveFusionDisabledError(
            "No cached result exists and live QPU execution is disabled. "
            "Set QFUSION_ENABLE_QPU=1 only in a trusted environment."
        )

    with _QPU_LOCK:
        cached_payload = _load_cache(cache_path)
        if cached_payload is not None:
            cached_payload["message"] = "Replayed a cached fusion result; no QPU job was submitted for this request."
            return cached_payload

        base = PROBLEM_BUILDERS[problem](scale)
        graph = qubo_to_graph(base)
        partitioning = decompose_problem(problem, base, graph, max(2, partitions))
        merge_plan = build_merge_plan(graph, partitioning, merge_strategy, merge_order, planner_mode)
        from merge_strategy.fusion_runtime import execute_tree_fusion
        result = execute_tree_fusion(
            qubo=base["qubo"],
            partitions=partitioning["partitions"],
            merge_steps=merge_plan["steps"],
            merge_strategy=merge_strategy,
            merge_order=merge_order,
            k=FUSION_CANDIDATE_LIMIT,
            num_reads=FUSION_NUM_READS,
        )
        payload = {
            "supported": True,
            "cache_hit": False,
            "problem_id": problem,
            "merge_order": merge_order,
            "planner_mode": planner_mode,
            "strategy": result["strategy"],
            "candidate_limit": result["candidate_limit"],
            "num_reads": result["num_reads"],
            "solver_id": result["solver_id"],
            "ocean_versions": result["ocean_versions"],
            "energy": result["energy"],
            "active_positive_coupling_count": result["active_positive_coupling_count"],
            "active_positive_coupling_weight": result["active_positive_coupling_weight"],
            "sample_ms": result["sample_ms"],
            "fusion_ms": result["fusion_ms"],
            "total_runtime_ms": result["total_runtime_ms"],
            "assignment_size": result["assignment_size"],
            "execution_steps": result["execution_steps"],
            "message": "Live tree-guided execution completed on a D-Wave solver.",
        }
        _write_cache(cache_path, payload)
        return payload


def qubo_to_graph(base: Dict[str, object]) -> Dict[str, object]:
    qubo = base["qubo"]
    variables = base["variables"]
    variable_map = {entry["name"]: entry for entry in variables}
    node_weights = qubo["linear"]
    edges = [Edge(item["u"], item["v"], float(item["value"])) for item in qubo["quadratic"]]
    adjacency: Dict[str, List[Dict[str, object]]] = {name: [] for name in node_weights}
    for edge in edges:
        adjacency[edge.left].append({"neighbor": edge.right, "value": edge.value})
        adjacency[edge.right].append({"neighbor": edge.left, "value": edge.value})

    return {
        "nodes": [
            {
                "id": name,
                "linear": float(weight),
                "degree": len(adjacency[name]),
                "meta": variable_map[name],
            }
            for name, weight in sorted(node_weights.items())
        ],
        "edges": [
            {
                "id": edge_key(edge.left, edge.right),
                "source": edge.left,
                "target": edge.right,
                "value": edge.value,
            }
            for edge in edges
        ],
        "offset": qubo["offset"],
        "adjacency": adjacency,
    }


def edge_key(left: str, right: str) -> str:
    return "|".join(sorted((left, right)))


def enrich_construction_blocks(problem: str, base: Dict[str, object], graph: Dict[str, object]) -> None:
    blocks = base.get("construction_blocks", [])
    if not blocks:
        return
    if problem == "join_order":
        enrich_join_order_blocks(blocks, graph)
        return
    if problem == "mqo":
        enrich_mqo_blocks(blocks, graph)
        return
    if problem == "index_selection":
        enrich_index_selection_blocks(blocks, graph)
        return

    node_meta = {node["id"]: node["meta"] for node in graph["nodes"]}
    for block in blocks:
        target_kinds = block_variable_kinds(problem, block.get("variables", []))
        node_ids = sorted(
            [node_id for node_id, meta in node_meta.items() if meta["kind"] in target_kinds]
        )
        edge_ids = sorted(
            [
                edge["id"]
                for edge in graph["edges"]
                if edge["source"] in node_ids or edge["target"] in node_ids
            ]
        )
        block["highlight"] = {"node_ids": node_ids, "edge_ids": edge_ids}


def block_variable_kinds(problem: str, variable_names: List[str]) -> List[str]:
    mapping = {
        "join_order": {
            "roj": "relation_operand_for_join",
            "paj": "predicate_applicable_for_join",
        },
        "mqo": {
            "plan": "plan_selection",
        },
        "index_selection": {
            "index": "index_selection",
            "cap": "storage_fraction",
        },
    }
    return [mapping[problem][name] for name in variable_names if name in mapping.get(problem, {})]


def enrich_join_order_blocks(blocks: List[Dict[str, object]], graph: Dict[str, object]) -> None:
    node_meta = {node["id"]: node["meta"] for node in graph["nodes"]}
    by_name = {block["name"]: block for block in blocks}
    all_roj = sorted([node_id for node_id, meta in node_meta.items() if meta["kind"] == "relation_operand_for_join"])
    all_paj = sorted([node_id for node_id, meta in node_meta.items() if meta["kind"] == "predicate_applicable_for_join"])

    hva_edges = []
    hvb_edges = []
    hp_edges = []
    hp_nodes = set()
    same_prefix_edges = []
    for edge in graph["edges"]:
        left_meta = node_meta[edge["source"]]
        right_meta = node_meta[edge["target"]]
        pair = {left_meta["kind"], right_meta["kind"]}
        if (
            left_meta.get("join_index") == right_meta.get("join_index")
            and left_meta.get("join_index") is not None
        ):
            same_prefix_edges.append(edge["id"])
        if pair == {"relation_operand_for_join"}:
            same_join = left_meta["join_index"] == right_meta["join_index"]
            same_relation = left_meta.get("relation") == right_meta.get("relation")
            if same_join and not same_relation:
                hva_edges.append(edge["id"])
            if same_relation and abs(left_meta["join_index"] - right_meta["join_index"]) == 1:
                hvb_edges.append(edge["id"])
        if pair == {"predicate_applicable_for_join", "relation_operand_for_join"}:
            predicate_meta, relation_meta = (
                (left_meta, right_meta)
                if left_meta["kind"] == "predicate_applicable_for_join"
                else (right_meta, left_meta)
            )
            endpoint_relations = predicate_meta.get("db_element", {}).get("relations", [])
            if (
                predicate_meta["join_index"] == relation_meta["join_index"]
                and relation_meta.get("relation") in endpoint_relations
            ):
                hp_edges.append(edge["id"])
                hp_nodes.update((edge["source"], edge["target"]))

    if "HVa" in by_name:
        by_name["HVa"]["highlight"] = {"node_ids": all_roj, "edge_ids": sorted(hva_edges)}
        by_name["HVa"]["focus"] = {
            "node_pattern": "All relation-prefix nodes roj[r,j]",
            "edge_pattern": "Intra-layer roj-roj couplings inside the same join prefix",
            "overlap_note": "Aggregated roj-roj coefficients can also include HCost contributions.",
        }
    if "HVb" in by_name:
        by_name["HVb"]["highlight"] = {"node_ids": all_roj, "edge_ids": sorted(hvb_edges)}
        by_name["HVb"]["focus"] = {
            "node_pattern": "All relation-prefix nodes roj[r,j]",
            "edge_pattern": "Cross-layer propagation edges for the same relation between adjacent prefixes",
        }
    if "Hp" in by_name:
        by_name["Hp"]["highlight"] = {
            "node_ids": sorted(hp_nodes),
            "edge_ids": sorted(hp_edges),
        }
        by_name["Hp"]["focus"] = {
            "node_pattern": "Predicate nodes paj[p,j] plus their endpoint relation nodes roj[left/right,j]",
            "edge_pattern": "Predicate-to-relation applicability edges within the same prefix",
            "overlap_note": "Aggregated roj-paj coefficients can also include HCost contributions.",
        }
    if "Cost" in by_name:
        by_name["Cost"]["highlight"] = {"node_ids": sorted(set(all_roj + all_paj)), "edge_ids": sorted(same_prefix_edges)}
        by_name["Cost"]["focus"] = {
            "node_pattern": "All relation and predicate variables that contribute to the cost term",
            "edge_pattern": "Within-prefix quadratic couplings from the squared cost approximation",
            "overlap_note": "The aggregated graph does not separate coefficients shared with HVa or Hp.",
        }


def enrich_mqo_blocks(blocks: List[Dict[str, object]], graph: Dict[str, object]) -> None:
    node_meta = {node["id"]: node["meta"] for node in graph["nodes"]}
    all_plans = sorted([node_id for node_id, meta in node_meta.items() if meta["kind"] == "plan_selection"])
    by_name = {block["name"]: block for block in blocks}

    selection_edges = []
    share_edges = []
    for edge in graph["edges"]:
        left_meta = node_meta[edge["source"]]
        right_meta = node_meta[edge["target"]]
        if left_meta["kind"] != "plan_selection" or right_meta["kind"] != "plan_selection":
            continue
        if left_meta["query"] == right_meta["query"]:
            selection_edges.append(edge["id"])
        elif edge["value"] < 0:
            share_edges.append(edge["id"])

    if "HSel" in by_name:
        by_name["HSel"]["highlight"] = {"node_ids": all_plans, "edge_ids": sorted(selection_edges)}
        by_name["HSel"]["focus"] = {
            "node_pattern": "Plan variables x_p grouped by query",
            "edge_pattern": "Within-query exclusion edges that enforce one plan per query",
        }
    if "HShare" in by_name:
        share_nodes = sorted(
            {
                edge_endpoint
                for edge in graph["edges"]
                if edge["id"] in share_edges
                for edge_endpoint in (edge["source"], edge["target"])
            }
        )
        by_name["HShare"]["highlight"] = {"node_ids": share_nodes, "edge_ids": sorted(share_edges)}
        by_name["HShare"]["focus"] = {
            "node_pattern": "Plans that can reuse shared work across different queries",
            "edge_pattern": "Cross-query savings edges between compatible plans",
        }
    if "HCost" in by_name:
        by_name["HCost"]["highlight"] = {"node_ids": all_plans, "edge_ids": []}
        by_name["HCost"]["focus"] = {
            "node_pattern": "All plan variables with their execution costs",
            "edge_pattern": "Linear cost contribution in the current demo model",
        }


def enrich_index_selection_blocks(blocks: List[Dict[str, object]], graph: Dict[str, object]) -> None:
    node_meta = {node["id"]: node["meta"] for node in graph["nodes"]}
    index_nodes = sorted([node_id for node_id, meta in node_meta.items() if meta["kind"] == "index_selection"])
    storage_nodes = sorted([node_id for node_id, meta in node_meta.items() if meta["kind"] == "storage_fraction"])
    by_name = {block["name"]: block for block in blocks}

    storage_edges = []
    conflict_edges = []
    for edge in graph["edges"]:
        storage_edges.append(edge["id"])
        left_meta = node_meta[edge["source"]]
        right_meta = node_meta[edge["target"]]
        kinds = {left_meta["kind"], right_meta["kind"]}
        if kinds == {"index_selection"}:
            same_table = left_meta.get("table") == right_meta.get("table")
            both_clustered = left_meta.get("clustered") and right_meta.get("clustered")
            if same_table and both_clustered:
                conflict_edges.append(edge["id"])

    if "ES" in by_name:
        by_name["ES"]["highlight"] = {
            "node_ids": sorted(set(index_nodes + storage_nodes)),
            "edge_ids": sorted(storage_edges),
        }
        by_name["ES"]["focus"] = {
            "node_pattern": "Index variables plus storage-fraction variables",
            "edge_pattern": "All pairwise terms produced by the squared storage-slack expression",
            "overlap_note": "Clustered-index pairs can also include an EM conflict contribution.",
        }
    if "EM" in by_name:
        conflict_nodes = sorted(
            {
                edge_endpoint
                for edge in graph["edges"]
                if edge["id"] in conflict_edges
                for edge_endpoint in (edge["source"], edge["target"])
            }
        )
        by_name["EM"]["highlight"] = {"node_ids": conflict_nodes, "edge_ids": sorted(conflict_edges)}
        by_name["EM"]["focus"] = {
            "node_pattern": "Conflicting clustered index candidates on the same table",
            "edge_pattern": "Pairwise penalties that block incompatible choices",
            "overlap_note": "These aggregated coefficients also include their ES storage contribution.",
        }
    if "EU" in by_name:
        by_name["EU"]["highlight"] = {"node_ids": index_nodes, "edge_ids": []}
        by_name["EU"]["focus"] = {
            "node_pattern": "All index candidates scored by workload benefit",
            "edge_pattern": "Linear utility contribution in the current demo model",
        }


def semantic_group(problem: str, node: Dict[str, object]) -> str:
    meta = node["meta"]
    if problem == "join_order":
        if meta["kind"] == "relation_operand_for_join":
            return f"join::{meta['join_index']}"
        return f"predicate::{meta['join_index']}"
    if problem == "mqo":
        return f"query::{meta['query']}"
    if problem == "index_selection":
        if meta["kind"] == "index_selection":
            return f"table::{meta['table']}"
        return "storage"
    return meta["kind"]


def decompose_problem(
    problem: str,
    base: Dict[str, object],
    graph: Dict[str, object],
    partitions: int,
) -> Dict[str, object]:
    nodes = graph["nodes"]
    node_by_id = {node["id"]: node for node in nodes}
    grouped: Dict[str, List[str]] = {}
    for node in nodes:
        grouped.setdefault(semantic_group(problem, node), []).append(node["id"])

    effective_partitions = max(1, min(partitions, len(grouped), len(nodes)))
    partition_buckets = [{"id": idx, "nodes": []} for idx in range(effective_partitions)]
    groups = sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
    for group_index, (_, group_nodes) in enumerate(groups):
        target = partition_buckets[group_index % effective_partitions]
        target["nodes"].extend(sorted(group_nodes))

    node_to_partition = {}
    for bucket in partition_buckets:
        for name in bucket["nodes"]:
            node_to_partition[name] = bucket["id"]

    internal_edges = []
    boundary_edges = []
    for edge in graph["edges"]:
        left_partition = node_to_partition[edge["source"]]
        right_partition = node_to_partition[edge["target"]]
        enriched = {
            **edge,
            "left_partition": left_partition,
            "right_partition": right_partition,
            "abs_value": abs(edge["value"]),
        }
        if left_partition == right_partition:
            internal_edges.append(enriched)
        else:
            boundary_edges.append(enriched)

    assignments = initial_assignments(graph)
    partition_metrics = []
    for bucket in partition_buckets:
        partition_nodes = [node_by_id[name] for name in bucket["nodes"]]
        partition_metrics.append(
            {
                "id": bucket["id"],
                "size": len(bucket["nodes"]),
                "nodes": bucket["nodes"],
                "linear_weight_sum": round(sum(abs(node["linear"]) for node in partition_nodes), 3),
                "summary_label": partition_summary_label(problem, partition_nodes),
                "summary_detail": partition_summary_detail(problem, partition_nodes),
            }
        )

    strongest_boundary = sorted(boundary_edges, key=lambda item: item["abs_value"], reverse=True)
    return {
        "partitions": partition_metrics,
        "node_to_partition": node_to_partition,
        "boundary_edges": strongest_boundary,
        "internal_edge_count": len(internal_edges),
        "boundary_edge_count": len(boundary_edges),
        "boundary_weight_sum": round(sum(edge["abs_value"] for edge in boundary_edges), 3),
        "assignments_before_merge": assignments,
        "boundary_focus": strongest_boundary[:20],
        "db_view": build_db_view(base, node_to_partition),
        "boundary_groups": build_boundary_groups(strongest_boundary, graph),
    }


def partition_summary_label(problem: str, partition_nodes: List[Dict[str, object]]) -> str:
    metas = [node["meta"] for node in partition_nodes]
    if not metas:
        return "Empty partition"
    if problem == "join_order":
        join_indices = sorted({meta["join_index"] for meta in metas})
        kinds = {meta["kind"] for meta in metas}
        if len(join_indices) == 1:
            step = join_indices[0] + 1
            if kinds == {"relation_operand_for_join"}:
                return f"Step {step} relations"
            if kinds == {"predicate_applicable_for_join"}:
                return f"Step {step} predicates"
            return f"Step {step} variables"
        return f"Join steps {', '.join(str(idx + 1) for idx in join_indices)}"
    if problem == "mqo":
        queries = sorted({meta["query"] for meta in metas if meta.get("query")})
        if len(queries) == 1:
            return f"Query {queries[0]}"
        return f"{len(queries)} query groups"
    if problem == "index_selection":
        if all(meta["kind"] == "storage_fraction" for meta in metas):
            return "Storage variables"
        tables = sorted({meta["table"] for meta in metas if meta.get("table")})
        if len(tables) == 1:
            return f"{tables[0]} indexes"
        return f"{len(tables)} tables"
    return "QUBO variables"


def partition_summary_detail(problem: str, partition_nodes: List[Dict[str, object]]) -> str:
    metas = [node["meta"] for node in partition_nodes]
    if not metas:
        return ""
    if problem == "join_order":
        names = []
        if all(meta["kind"] == "relation_operand_for_join" for meta in metas):
            names = sorted({meta["relation"] for meta in metas})
        elif all(meta["kind"] == "predicate_applicable_for_join" for meta in metas):
            names = sorted({meta["predicate"] for meta in metas})
        elif len({meta["join_index"] for meta in metas}) == 1:
            relation_names = sorted({meta["relation"] for meta in metas if meta["kind"] == "relation_operand_for_join"})
            predicate_names = sorted({meta["predicate"] for meta in metas if meta["kind"] == "predicate_applicable_for_join"})
            relation_text = ", ".join(relation_names[:4]) + (" ..." if len(relation_names) > 4 else "")
            predicate_text = ", ".join(predicate_names[:4]) + (" ..." if len(predicate_names) > 4 else "")
            return f"Relations: {relation_text} | Predicates: {predicate_text}"
        return ", ".join(names[:5]) + (" ..." if len(names) > 5 else "")
    if problem == "mqo":
        queries = sorted({meta["query"] for meta in metas if meta.get("query")})
        plans = sorted({meta["plan"] for meta in metas if meta.get("plan")})
        if len(queries) == 1:
            return ", ".join(plans[:4]) + (" ..." if len(plans) > 4 else "")
        return ", ".join(queries[:4]) + (" ..." if len(queries) > 4 else "")
    if problem == "index_selection":
        if all(meta["kind"] == "storage_fraction" for meta in metas):
            fractions = sorted({meta["fraction"] for meta in metas if meta.get("fraction") is not None})
            return ", ".join(f"S{fraction}" for fraction in fractions[:5]) + (" ..." if len(fractions) > 5 else "")
        indexes = sorted({meta["index"] for meta in metas if meta.get("index")})
        simplified = [simplify_index_name(name) for name in indexes]
        return ", ".join(simplified[:4]) + (" ..." if len(simplified) > 4 else "")
    return ""


def simplify_index_name(name: str) -> str:
    if "_main" in name:
        return "main"
    if "_clustered_a" in name:
        return "clustered A"
    if "_clustered_b" in name:
        return "clustered B"
    return name


def initial_assignments(graph: Dict[str, object]) -> Dict[str, int]:
    assignments: Dict[str, int] = {}
    for node in graph["nodes"]:
        assignments[node["id"]] = 1 if node["linear"] < 0 else 0
    return assignments


def subproblem_energy(graph: Dict[str, object], assignments: Dict[str, int], scope: Iterable[str]) -> float:
    scope_set = set(scope)
    energy = 0.0
    linear = {node["id"]: node["linear"] for node in graph["nodes"]}
    for var in scope_set:
        energy += linear[var] * assignments[var]
    for edge in graph["edges"]:
        if edge["source"] in scope_set and edge["target"] in scope_set:
            energy += edge["value"] * assignments[edge["source"]] * assignments[edge["target"]]
    return round(energy, 3)


def total_energy(graph: Dict[str, object], assignments: Dict[str, int]) -> float:
    energy = graph["offset"]
    for node in graph["nodes"]:
        energy += node["linear"] * assignments[node["id"]]
    for edge in graph["edges"]:
        energy += edge["value"] * assignments[edge["source"]] * assignments[edge["target"]]
    return round(energy, 3)


def active_positive_couplings(graph: Dict[str, object], assignments: Dict[str, int], edge_scope: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    active_couplings = []
    for edge in edge_scope:
        left_active = assignments[edge["source"]]
        right_active = assignments[edge["target"]]
        contribution = edge["value"] * left_active * right_active
        if contribution > 0:
            active_couplings.append({**edge, "contribution": round(contribution, 3)})
    return sorted(active_couplings, key=lambda item: item["contribution"], reverse=True)


def assignment_delta(graph: Dict[str, object], assignments: Dict[str, int], var: str) -> float:
    current_value = assignments[var]
    next_value = 1 - current_value
    delta = graph["offset"] * 0.0
    linear = next(node["linear"] for node in graph["nodes"] if node["id"] == var)
    delta += linear * (next_value - current_value)
    for edge in graph["adjacency"][var]:
        neighbor = edge["neighbor"]
        delta += edge["value"] * assignments[neighbor] * (next_value - current_value)
    return delta


def optimize_assignments(
    graph: Dict[str, object],
    starting_assignments: Dict[str, int],
    variables: List[str],
    iterations: int,
) -> Dict[str, int]:
    assignments = dict(starting_assignments)
    for _ in range(iterations):
        improved = False
        for var in variables:
            delta = assignment_delta(graph, assignments, var)
            if delta < 0:
                assignments[var] = 1 - assignments[var]
                improved = True
        if not improved:
            break
    return assignments


def merge_tree_left_deep(
    partitions: List[Dict[str, object]],
) -> List[Tuple[Tuple[int, ...], Tuple[int, ...], Tuple[int, ...]]]:
    order = []
    current = (partitions[0]["id"],)
    for partition in partitions[1:]:
        child_b = (partition["id"],)
        nxt = current + child_b
        order.append((current, child_b, nxt))
        current = nxt
    return order


def merge_tree_bushy(
    partitions: List[Dict[str, object]],
) -> List[Tuple[Tuple[int, ...], Tuple[int, ...], Tuple[int, ...]]]:
    frontier = [(partition["id"],) for partition in partitions]
    plan = []
    while len(frontier) > 1:
        merged = []
        for idx in range(0, len(frontier), 2):
            if idx + 1 < len(frontier):
                child_a = frontier[idx]
                child_b = frontier[idx + 1]
                combo = tuple(sorted(child_a + child_b))
                plan.append((child_a, child_b, combo))
                merged.append(combo)
            else:
                merged.append(frontier[idx])
        frontier = merged
    return plan


def boundary_strength_between(
    cluster_a: Iterable[int],
    cluster_b: Iterable[int],
    boundary_edges: List[Dict[str, object]],
) -> float:
    left_ids = set(cluster_a)
    right_ids = set(cluster_b)
    total = 0.0
    for edge in boundary_edges:
        lp = edge["left_partition"]
        rp = edge["right_partition"]
        if (lp in left_ids and rp in right_ids) or (lp in right_ids and rp in left_ids):
            total += float(edge["abs_value"])
    return total


def tree_cost(
    plan: List[Tuple[Tuple[int, ...], Tuple[int, ...], Tuple[int, ...]]],
    boundary_edges: List[Dict[str, object]],
) -> float:
    # Paper cost model: C(T) = sum over merges of |L union R| * cut(L, R).
    total = 0.0
    for child_a, child_b, merged in plan:
        total += len(merged) * boundary_strength_between(child_a, child_b, boundary_edges)
    return total


def merge_tree_dp(
    partitions: List[Dict[str, object]],
    boundary_edges: List[Dict[str, object]],
) -> List[Tuple[Tuple[int, ...], Tuple[int, ...], Tuple[int, ...]]]:
    ids = [partition["id"] for partition in partitions]
    n = len(ids)
    if n > 14:
        raise ValueError(f"DP fusion planner supports at most 14 blocks, got {n}")
    index_of = {block_id: index for index, block_id in enumerate(ids)}

    pair_cut = [[0.0] * n for _ in range(n)]
    for edge in boundary_edges:
        left = index_of[edge["left_partition"]]
        right = index_of[edge["right_partition"]]
        pair_cut[left][right] += float(edge["abs_value"])
        pair_cut[right][left] += float(edge["abs_value"])

    # inside[mask] = edge weight with both endpoints in mask, so that
    # cut(L, S \ L) = inside[S] - inside[L] - inside[S \ L].
    inside = [0.0] * (1 << n)
    for mask in range(1, 1 << n):
        rest = mask & (mask - 1)
        low = (mask & -mask).bit_length() - 1
        added = 0.0
        walk = rest
        while walk:
            added += pair_cut[low][(walk & -walk).bit_length() - 1]
            walk &= walk - 1
        inside[mask] = inside[rest] + added

    # cost[S] = min over proper nonempty L subset S of
    #           cost[L] + cost[S \ L] + |S| * cut(L, S \ L).
    cost = [0.0] * (1 << n)
    split = [0] * (1 << n)
    for mask in range(1, 1 << n):
        if mask & (mask - 1) == 0:
            continue
        size = bin(mask).count("1")
        low_bit = mask & -mask
        best = float("inf")
        sub = (mask - 1) & mask
        while sub:
            # Requiring the lowest set bit in L visits each unordered split once.
            if sub & low_bit:
                other = mask ^ sub
                candidate = cost[sub] + cost[other] + size * (inside[mask] - inside[sub] - inside[other])
                if candidate < best:
                    best = candidate
                    split[mask] = sub
            sub = (sub - 1) & mask
        cost[mask] = best

    plan: List[Tuple[Tuple[int, ...], Tuple[int, ...], Tuple[int, ...]]] = []

    def emit(mask: int) -> Tuple[int, ...]:
        if mask & (mask - 1) == 0:
            return (ids[(mask & -mask).bit_length() - 1],)
        left = emit(split[mask])
        right = emit(mask ^ split[mask])
        merged = tuple(sorted(left + right))
        plan.append((left, right, merged))
        return merged

    emit((1 << n) - 1)
    return plan


def build_merge_plan(
    graph: Dict[str, object],
    partitioning: Dict[str, object],
    merge_strategy: str,
    merge_order: str,
    planner_mode: str = "default",
) -> Dict[str, object]:
    partitions = partitioning["partitions"]
    boundary_edges = partitioning["boundary_edges"]
    starting_assignments = partitioning["assignments_before_merge"]

    if merge_order not in MERGE_ORDERS:
        raise ValueError(f"Unknown merge order: {merge_order}")
    if planner_mode not in PLANNER_MODES:
        raise ValueError(f"Unknown planner mode: {planner_mode}")

    if planner_mode == "cost_based":
        # The DP searches every tree shape, so merge_order does not constrain the result.
        merge_sequence = merge_tree_dp(partitions, boundary_edges)
    else:
        if merge_order == "bushy":
            merge_sequence = merge_tree_bushy(partitions)
        else:
            merge_sequence = merge_tree_left_deep(partitions)

    if merge_strategy not in MERGE_STRATEGIES:
        raise ValueError(f"Unknown merge strategy: {merge_strategy}")

    current_assignments = dict(starting_assignments)
    steps = []
    merged_so_far: set[int] = set()
    fixed_partition_ids: set[int] = set()
    node_linear = {node["id"]: node["linear"] for node in graph["nodes"]}
    for step_index, (child_a, child_b, cluster) in enumerate(merge_sequence, start=1):
        merged_so_far.update(cluster)
        scope = [
            node
            for partition in partitions
            if partition["id"] in cluster
            for node in partition["nodes"]
        ]
        if merge_strategy == "top2_merge":
            important = []
            for edge in boundary_edges:
                if edge["left_partition"] in cluster or edge["right_partition"] in cluster:
                    important.extend([edge["source"], edge["target"]])
            ordered = list(dict.fromkeys(important + scope))
            current_assignments = optimize_assignments(graph, current_assignments, ordered, iterations=5)
        elif merge_strategy == "conditioned_fusion":
            # Conditioned Fusion: fix already-solved upstream partitions, solve only the
            # incoming variables against the conditioned sub-objective. Holding the fixed
            # variables constant while flipping incoming ones makes assignment_delta absorb
            # each fixed neighbor's coupling quadratic(u, v) * value(u) into the incoming
            # variable v's linear coefficient, mirroring fusion_runtime.conditioned_subbqm.
            incoming_partition_ids = set(cluster) - fixed_partition_ids
            incoming_vars = [
                node
                for partition in partitions
                if partition["id"] in incoming_partition_ids
                for node in partition["nodes"]
            ]
            # Deterministic sign-of-linear initialization for the incoming variables, then a
            # deterministic single-bit-flip descent restricted to them (upstream stays fixed).
            for name in incoming_vars:
                current_assignments[name] = 1 if node_linear[name] < 0 else 0
            focus_vars = sorted(incoming_vars, key=lambda name: (abs(node_linear[name]), name))
            current_assignments = optimize_assignments(graph, current_assignments, focus_vars, iterations=5)
            fixed_partition_ids.update(cluster)

        visible_boundary = [
            edge
            for edge in boundary_edges
            if edge["left_partition"] in merged_so_far or edge["right_partition"] in merged_so_far
        ]
        active_couplings = active_positive_couplings(graph, current_assignments, visible_boundary)
        energy = total_energy(graph, current_assignments)
        steps.append(
            {
                "step": step_index,
                "cluster": list(cluster),
                "scope_size": len(scope),
                "boundary_cut": round(boundary_strength_between(child_a, child_b, boundary_edges), 3),
                "energy": energy,
                "active_positive_coupling_count": len(active_couplings),
                "top_active_positive_couplings": active_couplings[:6],
            }
        )

    final_active_couplings = active_positive_couplings(graph, current_assignments, boundary_edges)
    return {
        "strategy": merge_strategy,
        "order": merge_order,
        "planner_mode": planner_mode,
        "tree_cost": round(tree_cost(merge_sequence, boundary_edges), 3),
        "steps": steps,
        "final_assignments": current_assignments,
        "final_energy": total_energy(graph, current_assignments),
        "final_active_positive_coupling_count": len(final_active_couplings),
        "top_final_active_positive_couplings": final_active_couplings[:10],
    }


def build_db_view(base: Dict[str, object], node_to_partition: Dict[str, int]) -> Dict[str, object]:
    items = []
    for variable in base["variables"]:
        db_element = variable["db_element"]
        items.append(
            {
                "label": db_element.get("name", variable["name"]),
                "type": db_element["type"],
                "variable": variable["name"],
                "partition": node_to_partition[variable["name"]],
                "extra": {
                    key: value for key, value in variable.items() if key not in {"name", "db_element"}
                },
            }
        )
    structure_summary = build_structure_summary(base, node_to_partition)
    return {
        "items": items,
        "summary": structure_summary,
        "viz": base.get("viz", {}),
        "note": base.get("note", ""),
    }


def build_structure_summary(base: Dict[str, object], node_to_partition: Dict[str, int]) -> Dict[str, object]:
    variables = base["variables"]
    by_type: Dict[str, List[Dict[str, object]]] = {}
    for variable in variables:
        by_type.setdefault(variable["db_element"]["type"], []).append(variable)

    joins = {}
    queries = {}
    tables = {}
    for variable in variables:
        if variable["kind"] == "relation_operand_for_join":
            joins.setdefault(variable["join_index"], {"relations": [], "predicates": []})
            joins[variable["join_index"]]["relations"].append(
                {
                    "name": variable["relation"],
                    "variable": variable["name"],
                    "partition": node_to_partition[variable["name"]],
                }
            )
        elif variable["kind"] == "predicate_applicable_for_join":
            joins.setdefault(variable["join_index"], {"relations": [], "predicates": []})
            joins[variable["join_index"]]["predicates"].append(
                {
                    "name": variable["predicate"],
                    "variable": variable["name"],
                    "partition": node_to_partition[variable["name"]],
                    "relations": variable["db_element"].get("relations", []),
                }
            )
        elif variable["kind"] == "plan_selection":
            queries.setdefault(variable["query"], []).append(
                {
                    "plan": variable["plan"],
                    "variable": variable["name"],
                    "cost": variable["cost"],
                    "partition": node_to_partition[variable["name"]],
                }
            )
        elif variable["kind"] == "index_selection":
            tables.setdefault(variable["table"], {"indices": [], "storage_vars": []})
            tables[variable["table"]]["indices"].append(
                {
                    "index": variable["index"],
                    "variable": variable["name"],
                    "clustered": variable["clustered"],
                    "storage": variable["storage"],
                    "partition": node_to_partition[variable["name"]],
                }
            )
        elif variable["kind"] == "storage_fraction":
            tables.setdefault("_storage_", {"indices": [], "storage_vars": []})
            tables["_storage_"]["storage_vars"].append(
                {
                    "fraction": variable["fraction"],
                    "variable": variable["name"],
                    "partition": node_to_partition[variable["name"]],
                }
            )

    relations = sorted(
        {
            variable["relation"]
            for variable in variables
            if variable["kind"] == "relation_operand_for_join"
        }
    )
    predicates = sorted(
        {
            variable["predicate"]
            for variable in variables
            if variable["kind"] == "predicate_applicable_for_join"
        }
    )

    return {
        "problem": _infer_problem_from_variables(variables),
        "by_type_counts": {key: len(value) for key, value in sorted(by_type.items())},
        "by_kind_counts": _count_by_key(variables, "kind"),
        "relations": relations,
        "predicates": predicates,
        "join_prefixes": [
            {
                "join_index": join_index,
                "relations": sorted(payload["relations"], key=lambda item: item["name"]),
                "predicates": sorted(payload["predicates"], key=lambda item: item["name"]),
            }
            for join_index, payload in sorted(joins.items())
        ],
        "queries": [
            {"query": query, "plans": sorted(plans, key=lambda item: item["plan"])}
            for query, plans in sorted(queries.items())
        ],
        "tables": [
            {
                "table": table,
                "indices": sorted(payload["indices"], key=lambda item: item["index"]),
                "storage_vars": sorted(payload["storage_vars"], key=lambda item: str(item.get("fraction"))),
            }
            for table, payload in sorted(tables.items())
        ],
    }


def _count_by_key(items: List[Dict[str, object]], key: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        value = item[key]
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _infer_problem_from_variables(variables: List[Dict[str, object]]) -> str:
    kinds = {variable["kind"] for variable in variables}
    if "relation_operand_for_join" in kinds:
        return "join_order"
    if "plan_selection" in kinds:
        return "mqo"
    return "index_selection"


def build_boundary_groups(boundary_edges: List[Dict[str, object]], graph: Dict[str, object]) -> List[Dict[str, object]]:
    meta_by_var = {node["id"]: node["meta"] for node in graph["nodes"]}
    grouped: Dict[Tuple[int, int], Dict[str, object]] = {}
    for edge in boundary_edges:
        pair = tuple(sorted((edge["left_partition"], edge["right_partition"])))
        group = grouped.setdefault(
            pair,
            {
                "pair": pair,
                "edge_count": 0,
                "weight_sum": 0.0,
                "strongest_edge": None,
                "type_counts": {},
            },
        )
        group["edge_count"] += 1
        group["weight_sum"] += edge["abs_value"]
        edge_type = classify_boundary_type(meta_by_var[edge["source"]], meta_by_var[edge["target"]])
        group["type_counts"][edge_type] = group["type_counts"].get(edge_type, 0) + 1
        if group["strongest_edge"] is None or edge["abs_value"] > group["strongest_edge"]["abs_value"]:
            group["strongest_edge"] = {**edge, "type": edge_type}

    return [
        {
            "pair": list(group["pair"]),
            "edge_count": group["edge_count"],
            "weight_sum": round(group["weight_sum"], 3),
            "strongest_edge": group["strongest_edge"],
            "type_counts": group["type_counts"],
        }
        for _, group in sorted(grouped.items(), key=lambda item: item[1]["weight_sum"], reverse=True)
    ]


def classify_boundary_type(left_meta: Dict[str, object], right_meta: Dict[str, object]) -> str:
    left_kind = left_meta["kind"]
    right_kind = right_meta["kind"]
    kinds = tuple(sorted((left_kind, right_kind)))
    if kinds == ("predicate_applicable_for_join", "relation_operand_for_join"):
        return "predicate-to-relation"
    if kinds == ("plan_selection", "plan_selection"):
        return "plan-sharing"
    if kinds == ("index_selection", "storage_fraction"):
        return "index-to-storage"
    if kinds == ("index_selection", "index_selection"):
        return "index-conflict"
    if kinds == ("relation_operand_for_join", "relation_operand_for_join"):
        return "prefix-propagation"
    return "-".join(kinds)


def summarize_metrics(
    graph: Dict[str, object],
    partitioning: Dict[str, object],
    merge_plan: Dict[str, object],
) -> Dict[str, object]:
    before = partitioning["assignments_before_merge"]
    boundary_active_couplings_before = active_positive_couplings(
        graph, before, partitioning["boundary_edges"]
    )
    steps = merge_plan["steps"]
    return {
        "partition_count": len(partitioning["partitions"]),
        "boundary_size": partitioning["boundary_edge_count"],
        "boundary_weight_sum": partitioning["boundary_weight_sum"],
        "active_positive_coupling_count_before": len(boundary_active_couplings_before),
        "active_positive_coupling_count_after": merge_plan["final_active_positive_coupling_count"],
        "energy_before": total_energy(graph, before),
        "energy_after": merge_plan["final_energy"],
        "merge_depth": len(merge_plan["steps"]),
        "energy_series": [step["energy"] for step in merge_plan["steps"]],
        "active_positive_coupling_series": [
            step["active_positive_coupling_count"] for step in merge_plan["steps"]
        ],
        "max_step_cut": max((s["boundary_cut"] for s in steps), default=0),
        "cut_series": [s["boundary_cut"] for s in steps],
    }
