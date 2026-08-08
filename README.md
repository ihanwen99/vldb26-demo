# QFusion: A Demonstration of Boundary-Aware Fusion Planning and Execution for Large-Scale QUBO Optimization

QFusion is an interactive demonstration (PVLDB Vol. 19, No. 12, VLDB 2026) that shows how large-scale QUBO optimization problems are constructed, decomposed along their boundaries, and solved by boundary-aware fusion planning and execution on a real quantum annealer. It covers three database optimization problems: Join Order, Multiple Query Optimization, and Index Selection.

## Requirements / Install

Python 3.9+.

```bash
pip install -r requirements.txt
```

`dimod` and `dwave-system` are only needed for Scenario 3 execution; Scenarios 1 and 2 run on the Python standard library alone.

## Run

```bash
python demo_app.py
```

Then open `http://127.0.0.1:8000` in a browser.

The host and port default to `127.0.0.1` and `8000`. Set `VLDB_DEMO_HOST` and `VLDB_DEMO_PORT` to override the defaults.

## Scenarios

### Scenario 1: QUBO Construction

Build a problem-specific QUBO and inspect it as a graph or matrix alongside its database semantics.

### Scenario 2: Decomposition and Boundary

Split the QUBO into partitions and inspect the resulting subproblems and cut boundaries.

### Scenario 3: Fusion Planning and Execution

Configure the fusion, plan it, then run the actual execution:

- Merge Strategy: Direct Fusion, Top-2 Merge, Conditioned Fusion
- Fusion Tree Structure: Linear, Bushy (the default tree before planning)
- Plan: rewrites the default tree with the cost-based fusion planner. The planner scores a tree by summing, over its merges, the boundary coupling between the two merged sides weighted by the number of blocks the merge produces, and finds the lowest-cost tree exactly with a subset dynamic program over the blocks. The resulting tree cost is reported next to the plan.

## D-Wave Setup

Scenario 3 executes on a real D-Wave quantum annealer and needs D-Wave Leap credentials: run `dwave config create` or set the `DWAVE_API_TOKEN` environment variable. Scenarios 1 and 2 work without D-Wave.

## Project Structure

```text
demo_app.py         Main entrance (HTTP server)
demo_backend.py     Payload generation, decomposition, boundary summaries, fusion planning (cost model + subset DP)
merge_strategy/     Quantum-backed fusion runtime
qubo_construction/  Database problem-specific QUBO builders
web/                Frontend UI
```

## Citation

```bibtex
@article{qfusion26,
  author  = {Hanwen Liu and Ibrahim Sabek},
  title   = {{QFusion}: A Demonstration of Boundary-Aware Fusion Planning and Execution for Large-Scale {QUBO} Optimization},
  journal = {Proc. VLDB Endow.},
  volume  = {19},
  number  = {12},
  pages   = {4826--4829},
  year    = {2026},
  doi     = {10.14778/3827998.3828132}
}
```

## License

GPL-3.0 (see `LICENSE`). QFusion derives its QUBO formulations from prior work; the modules under `qubo_construction/` cite their source papers and repositories in their `PAPER_*` constants.
