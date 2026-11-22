# QFusion: A Demonstration of Boundary-Aware Fusion Planning and Execution for Large-Scale QUBO Optimization

PVLDB 19(12), 2026. Paper: https://doi.org/10.14778/3827998.3828132

QFusion is a boundary-aware fusion planning and execution framework that plans fusion with a DBMS-inspired, cost-based fusion tree and runs it on an actual quantum annealer, for join order optimization, multiple query optimization, and index selection. This repository contains the QFusion web interface, the fusion planner, and the fusion execution on the D-Wave annealer for the three demonstration scenarios.

## Requirements

- Python 3.10
- A D-Wave Leap account and API token

## Install

```bash
conda create -n qfusion python=3.10
conda activate qfusion
pip install -r requirements.txt
dwave config create
```

## Run

```bash
python server.py
```

This starts the web interface at `http://127.0.0.1:8000`. Scenarios 1 and 2 run locally, and Scenario 3 samples the partitions on the D-Wave annealer and fuses them along the chosen fusion tree.

## Repository layout

```text
vldb26-demo/
├── planner.py           Decomposition and fusion planning
├── fusion_execution.py  Fusion on D-Wave
├── server.py            Web server
├── qubo_construction/   QUBO builders
└── web/                 Web interface
```

## Citation

```bibtex
@article{liu2026qfusion,
  author  = {Hanwen Liu and Ibrahim Sabek},
  title   = {{QFusion}: A Demonstration of Boundary-Aware Fusion Planning and Execution for Large-Scale {QUBO} Optimization},
  journal = {Proceedings of the VLDB Endowment},
  volume  = {19},
  number  = {12},
  pages   = {4826--4829},
  year    = {2026},
  doi     = {10.14778/3827998.3828132}
}
```

## License

GPL-3.0-only. See `LICENSE`.
