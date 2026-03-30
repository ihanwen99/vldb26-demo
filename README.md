# QFusion: A Demonstration of Boundary-Aware Fusion Planning and Execution for Large-Scale QUBO Optimization

Support three key database optimization problems:
- Join Order
- Multiple Query Optimization
- Index Selection

## Scenarios

User can interact directly with QFusion with three designed scenarios.

### Scenario 1: QUBO Construction

### Scenario 2: Decomposition and Boundary

### Scenario 3: Fusion Planning and Execution

**Supported fusion strategies:**

- Direct Fusion
- Top-2 Merge
- Conditioned Fusion

**Supported merge orders:**

- Left-Deep
- Bushy

## Project Structure

```text
demo_app.py
demo_backend.py
merge_strategy/
qubo_construction/
web/
```

- `demo_app.py`: Main Entrance
- `demo_backend.py`: Payload generation, decomposition, boundary summaries, merge planning
- `merge_strategy/`: Quantum-backed fusion runtime
- `qubo_construction/`: Database problem-specific QUBO builders
- `web/`: Frontend UI

## Run

Start the demo server:

```bash
python demo_app.py
```

Then open:

```text
http://127.0.0.1:8000
```


## Notes

- Scenario 3 requires the D-Wave runtime
