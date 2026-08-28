# QFusion: A Demonstration of Boundary-Aware Fusion Planning and Execution for Large-Scale QUBO Optimization

QFusion is the interactive artifact for our VLDB 2026 demonstration. It presents a boundary-aware approach for decomposing large QUBO problems and combining their partial solutions through a fusion tree. The demo covers three database optimization problems: Join Order, Multiple Query Optimization, and Index Selection.

The demonstration is organized into three scenarios:

1. **QUBO Construction and Database Semantics:** build a problem-specific QUBO and inspect its graph or matrix together with the corresponding database elements.
2. **Decomposition and Boundary Inspection:** partition the QUBO using database semantics and examine the couplings that cross partition boundaries.
3. **Fusion Planning and Execution:** construct a fusion tree and compare Direct Fusion, Top-2 Merge, and Conditioned Fusion on a D-Wave quantum annealer.

The repository uses self-contained demo instances and does not require a database, an external dataset, Node.js, or a frontend build step.

## Requirements

- Python 3.9+
- A D-Wave Leap account and API token

## Full Fusion with D-Wave

```bash
git clone https://github.com/ihanwen99/vldb26-demo.git
cd vldb26-demo
python3.9 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
dwave config create
QFUSION_ENABLE_QPU=1 python app.py
```

Open `http://127.0.0.1:8000` in a browser.

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

QFusion is distributed under `GPL-3.0-only`; see `LICENSE`.
