# QFusion: A Demonstration of Boundary-Aware Fusion Planning and Execution for Large-Scale QUBO Optimization

QFusion is an interactive demo that shows how large-scale QUBO optimization problems are constructed, decomposed along their boundaries, and solved by boundary-aware fusion planning and execution. It covers three database optimization problems: Join Order, Multiple Query Optimization, and Index Selection.

## Problems

- Join Order
- Multiple Query Optimization (MQO)
- Index Selection

## Requirements / Install

Python 3.9+.

```bash
pip install -r requirements.txt
```

`dimod` and `dwave-system` are only needed for Scenario 3; Scenarios 1 and 2 run on the Python standard library alone.

## Run

Start the demo server:

```bash
python demo_app.py
```

Then open `http://127.0.0.1:8000` in a browser.

The host and port default to `127.0.0.1` and `8000`. Set `VLDB_DEMO_HOST` and `VLDB_DEMO_PORT` to override the defaults.

## Scenarios

The UI walks through three scenarios.

### Scenario 1: QUBO Construction

Build a problem-specific QUBO and inspect it as a graph or matrix alongside its database semantics.

### Scenario 2: Decomposition and Boundary

Split the QUBO into partitions and inspect the resulting subproblems and cut boundaries.

### Scenario 3: Fusion Planning and Execution

Choose the fusion configuration, then run the actual execution. Two controls are available:

- Merge Strategy: Direct Fusion, Top-2 Merge, Conditioned Fusion
- Fusion Tree Structure: Linear, Bushy

## D-Wave Setup

Scenario 3 executes on a real D-Wave quantum annealer and needs D-Wave Leap credentials. Use a D-Wave Leap account, then either run `dwave config create` or set the `DWAVE_API_TOKEN` environment variable.

Without D-Wave credentials, Scenario 3 (the actual fusion execution) cannot run. Scenarios 1 and 2 (construction, and decomposition and boundary inspection) work without D-Wave, since they do not touch the actual quantum execution.

## Project Structure

```text
demo_app.py
demo_backend.py
merge_strategy/
qubo_construction/
web/
```

- `demo_app.py`: Main entrance (HTTP server)
- `demo_backend.py`: Payload generation, decomposition, boundary summaries, merge planning
- `merge_strategy/`: Quantum-backed fusion runtime
- `qubo_construction/`: Database problem-specific QUBO builders
- `web/`: Frontend UI

## License

GPL-3.0 (see `LICENSE`). QFusion derives its QUBO formulations from prior work; the modules under `qubo_construction/` cite their source papers and repositories in their `PAPER_*` constants.
