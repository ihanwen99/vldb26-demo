from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List, Tuple

import dimod
from dwave.system import DWaveSampler, EmbeddingComposite


Assignment = Dict[Any, int]
Candidate = Dict[str, Any]


def bqm_from_qubo_payload(qubo: Dict[str, object]) -> dimod.BinaryQuadraticModel:
    linear = {name: float(value) for name, value in qubo["linear"].items()}
    quadratic = {
        (item["u"], item["v"]): float(item["value"])
        for item in qubo["quadratic"]
    }
    return dimod.BinaryQuadraticModel(linear, quadratic, float(qubo.get("offset", 0.0)), dimod.BINARY)


def induced_bqm(full_bqm: dimod.BinaryQuadraticModel, variables: Iterable[Any]) -> dimod.BinaryQuadraticModel:
    scope = set(variables)
    linear = {var: float(full_bqm.linear[var]) for var in scope if var in full_bqm.linear}
    quadratic = {
        (u, v): float(weight)
        for (u, v), weight in full_bqm.quadratic.items()
        if u in scope and v in scope
    }
    return dimod.BinaryQuadraticModel(linear, quadratic, 0.0, full_bqm.vartype)


def conditioned_subbqm(
    full_bqm: dimod.BinaryQuadraticModel,
    fixed: Assignment,
    remaining_variables: Iterable[Any],
) -> dimod.BinaryQuadraticModel:
    scope = set(remaining_variables)
    linear: Dict[Any, float] = {}
    quadratic: Dict[Tuple[Any, Any], float] = {}
    offset = 0.0

    for var in scope:
        linear[var] = float(full_bqm.linear.get(var, 0.0))

    for (u, v), weight in full_bqm.quadratic.items():
        if u in fixed and v in fixed:
            offset += float(weight) * fixed[u] * fixed[v]
        elif u in fixed and v in scope:
            linear[v] = linear.get(v, 0.0) + float(weight) * fixed[u]
        elif v in fixed and u in scope:
            linear[u] = linear.get(u, 0.0) + float(weight) * fixed[v]
        elif u in scope and v in scope:
            quadratic[(u, v)] = float(weight)

    return dimod.BinaryQuadraticModel(linear, quadratic, offset, full_bqm.vartype)


def sample_top_k(
    bqm: dimod.BinaryQuadraticModel,
    sampler: EmbeddingComposite,
    k: int,
    num_reads: int,
) -> Tuple[List[Candidate], float]:
    t0 = time.perf_counter()
    sampleset = sampler.sample(bqm, num_reads=num_reads)
    runtime_ms = (time.perf_counter() - t0) * 1000.0

    candidates: List[Candidate] = []
    seen = set()
    for row in sampleset.data(["sample", "energy"]):
        sample = {var: int(value) for var, value in row.sample.items()}
        key = tuple(sorted(sample.items()))
        if key in seen:
            continue
        seen.add(key)
        candidates.append({"sample": sample, "energy": float(row.energy)})
        if len(candidates) >= k:
            break
    return candidates, runtime_ms


def merge_samples(left: Assignment, right: Assignment) -> Assignment:
    merged = dict(left)
    merged.update(right)
    return merged


def rank_assignments(
    bqm: dimod.BinaryQuadraticModel,
    assignments: Iterable[Assignment],
    k: int,
) -> List[Candidate]:
    ranked: List[Candidate] = []
    seen = set()
    for assignment in assignments:
        key = tuple(sorted(assignment.items()))
        if key in seen:
            continue
        seen.add(key)
        ranked.append({"sample": assignment, "energy": float(bqm.energy(assignment))})
    ranked.sort(key=lambda item: item["energy"])
    return ranked[:k]


def count_active_conflicts(full_bqm: dimod.BinaryQuadraticModel, assignment: Assignment) -> Tuple[int, float]:
    conflict_count = 0
    conflict_weight = 0.0
    for (u, v), weight in full_bqm.quadratic.items():
        if float(weight) <= 0:
            continue
        if int(assignment.get(u, 0)) == 1 and int(assignment.get(v, 0)) == 1:
            conflict_count += 1
            conflict_weight += float(weight)
    return conflict_count, conflict_weight


def cluster_key(ids: Iterable[int]) -> str:
    return "-".join(str(value) for value in sorted(ids))


def resolve_children(cluster_ids: Iterable[int], clusters: Dict[str, Dict[str, object]]) -> Tuple[Dict[str, object], Dict[str, object]]:
    cluster_ids = sorted(cluster_ids)
    candidates = [
        cluster
        for cluster in clusters.values()
        if all(partition_id in cluster_ids for partition_id in cluster["partition_ids"])
    ]
    candidates.sort(key=lambda item: len(item["partition_ids"]), reverse=True)
    for left_index, left in enumerate(candidates):
        for right in candidates[left_index + 1 :]:
            if set(left["partition_ids"]) & set(right["partition_ids"]):
                continue
            union = sorted(left["partition_ids"] + right["partition_ids"])
            if union == cluster_ids:
                return left, right
    raise ValueError(f"Could not resolve merge children for cluster {cluster_ids}")


def execute_tree_fusion(
    qubo: Dict[str, object],
    partitions: List[Dict[str, object]],
    merge_steps: List[Dict[str, object]],
    merge_strategy: str,
    merge_order: str,
    k: int = 2,
    num_reads: int = 20,
) -> Dict[str, Any]:
    full_bqm = bqm_from_qubo_payload(qubo)
    clusters: Dict[str, Dict[str, object]] = {}
    execution_steps: List[Dict[str, Any]] = []
    total_sample_ms = 0.0
    total_fusion_ms = 0.0

    with DWaveSampler() as raw_sampler:
        sampler = EmbeddingComposite(raw_sampler)

        for partition in partitions:
            partition_ids = [partition["id"]]
            scope = list(partition["nodes"])
            sub_bqm = induced_bqm(full_bqm, scope)
            candidates, sample_ms = sample_top_k(sub_bqm, sampler, k, num_reads)
            if not candidates:
                raise RuntimeError(f"D-Wave returned no candidates for partition {partition['id'] + 1}.")
            total_sample_ms += sample_ms
            clusters[cluster_key(partition_ids)] = {
                "partition_ids": partition_ids,
                "variables": scope,
                "candidates": candidates,
            }
            execution_steps.append(
                {
                    "label": f"Sample Partition {partition['id'] + 1}",
                    "type": "sampling",
                    "runtime_ms": round(sample_ms, 2),
                    "scope_size": len(scope),
                }
            )

        for step_index, step in enumerate(merge_steps, start=1):
            cluster_ids = sorted(step["cluster"])
            left_cluster, right_cluster = resolve_children(cluster_ids, clusters)
            cluster_vars = sorted(set(left_cluster["variables"]) | set(right_cluster["variables"]))
            cluster_bqm = induced_bqm(full_bqm, cluster_vars)

            sample_ms = 0.0
            t0 = time.perf_counter()
            if merge_strategy == "direct_fusion":
                merged_assignments = [
                    merge_samples(left_cluster["candidates"][0]["sample"], right_cluster["candidates"][0]["sample"])
                ]
                merged_candidates = rank_assignments(cluster_bqm, merged_assignments, 1)
            elif merge_strategy == "top2_merge":
                merged_assignments = []
                for left_candidate in left_cluster["candidates"][:k]:
                    for right_candidate in right_cluster["candidates"][:k]:
                        merged_assignments.append(
                            merge_samples(left_candidate["sample"], right_candidate["sample"])
                        )
                merged_candidates = rank_assignments(cluster_bqm, merged_assignments, k)
            elif merge_strategy == "conditioned_fusion":
                fixed_left = left_cluster["candidates"][0]["sample"]
                conditioned_right_bqm = conditioned_subbqm(full_bqm, fixed_left, right_cluster["variables"])
                conditioned_candidates, conditioned_ms = sample_top_k(conditioned_right_bqm, sampler, k, num_reads)
                if not conditioned_candidates:
                    raise RuntimeError(f"D-Wave returned no conditioned candidates at merge step {step_index}.")
                sample_ms = conditioned_ms
                total_sample_ms += conditioned_ms
                execution_steps.append(
                    {
                        "label": f"Sample conditioned right side for Step {step_index}",
                        "type": "sampling",
                        "runtime_ms": round(conditioned_ms, 2),
                        "scope_size": len(right_cluster["variables"]),
                    }
                )
                merged_assignments = [
                    merge_samples(fixed_left, candidate["sample"])
                    for candidate in conditioned_candidates
                ]
                merged_candidates = rank_assignments(cluster_bqm, merged_assignments, k)
            else:
                raise ValueError(f"Unknown fusion strategy: {merge_strategy}")
            fusion_ms = (time.perf_counter() - t0) * 1000.0
            total_fusion_ms += fusion_ms

            best_candidate = merged_candidates[0]
            conflict_count, _ = count_active_conflicts(cluster_bqm, best_candidate["sample"])
            execution_steps.append(
                {
                    "label": f"Fuse [{', '.join(f'P{pid + 1}' for pid in left_cluster['partition_ids'])}] with [{', '.join(f'P{pid + 1}' for pid in right_cluster['partition_ids'])}]",
                    "type": "fusion",
                    "runtime_ms": round(sample_ms + fusion_ms, 2),
                    "fusion_ms": round(fusion_ms, 2),
                    "sample_ms": round(sample_ms, 2),
                    "scope_size": len(cluster_vars),
                    "energy": round(best_candidate["energy"], 4),
                    "conflicts": conflict_count,
                }
            )

            clusters[cluster_key(cluster_ids)] = {
                "partition_ids": cluster_ids,
                "variables": cluster_vars,
                "candidates": merged_candidates,
            }

    root_key = cluster_key(partition["id"] for partition in partitions)
    if root_key not in clusters:
        raise RuntimeError("Fusion tree execution did not produce a root cluster.")

    final_candidate = clusters[root_key]["candidates"][0]
    final_assignment = final_candidate["sample"]
    final_energy = float(full_bqm.energy(final_assignment))
    conflict_count, conflict_weight = count_active_conflicts(full_bqm, final_assignment)
    total_runtime_ms = total_sample_ms + total_fusion_ms
    execution_steps.append(
        {
            "label": "Final merged assignment",
            "type": "result",
            "runtime_ms": round(total_runtime_ms, 2),
            "energy": round(final_energy, 4),
            "conflicts": conflict_count,
        }
    )

    return {
        "strategy": merge_strategy,
        "merge_order": merge_order,
        "energy": final_energy,
        "conflict_count": conflict_count,
        "conflict_weight": round(conflict_weight, 4),
        "sample_ms": round(total_sample_ms, 2),
        "fusion_ms": round(total_fusion_ms, 2),
        "total_runtime_ms": round(total_runtime_ms, 2),
        "assignment_size": len(final_assignment),
        "execution_steps": execution_steps,
    }
