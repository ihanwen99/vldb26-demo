const state = {
  mode: "graph",
  payload: null,
  defaultMergePlan: null,
  fusionResult: null,
  activeVariable: null,
  activeBoundary: null,
  activePartition: null,
  activeConstructionFilter: null,
  appliedPlannerMode: "default",
};

const palette = ["#005f73", "#ca6702", "#0a9396", "#ae2012", "#94a51f", "#7f5539", "#5c677d", "#9b2226"];

const el = {
  problemSelect: document.getElementById("problemSelect"),
  scaleSlider: document.getElementById("scaleSlider"),
  scaleValue: document.getElementById("scaleValue"),
  partitionSlider: document.getElementById("partitionSlider"),
  partitionValue: document.getElementById("partitionValue"),
  mergeStrategySelect: document.getElementById("mergeStrategySelect"),
  mergeOrderSelect: document.getElementById("mergeOrderSelect"),
  planBtn: document.getElementById("planBtn"),
  mergeBtn: document.getElementById("mergeBtn"),
  graphModeBtn: document.getElementById("graphModeBtn"),
  matrixModeBtn: document.getElementById("matrixModeBtn"),
  refreshBtn: document.getElementById("refreshBtn"),
  leftCanvas: document.getElementById("leftCanvas"),
  constructionState: document.getElementById("constructionState"),
  constructionBlocks: document.getElementById("constructionBlocks"),
  dbSummary: document.getElementById("dbSummary"),
  dbStructure: document.getElementById("dbStructure"),
  partitionCards: document.getElementById("partitionCards"),
  boundaryGroups: document.getElementById("boundaryGroups"),
  boundaryCanvas: document.getElementById("boundaryCanvas"),
  metricsGrid: document.getElementById("metricsGrid"),
  strategyNote: document.getElementById("strategyNote"),
  mergeSteps: document.getElementById("mergeSteps"),
  quboSubtitle: document.getElementById("quboSubtitle"),
  energyChart: document.getElementById("energyChart"),
  conflictChart: document.getElementById("conflictChart"),
  mergeTree: document.getElementById("mergeTree"),
};

async function fetchJson(url) {
  const response = await fetch(url);
  return await response.json();
}

async function bootstrap() {
  const catalog = await fetchJson("/api/catalog");
  fillSelect(el.problemSelect, catalog.problems.map((item) => ({ value: item.id, label: item.label })));
  el.problemSelect.value = "join_order";
  el.scaleSlider.value = "8";
  el.scaleValue.textContent = "8";
  el.partitionSlider.value = "7";
  el.partitionValue.textContent = "7";
  fillSelect(el.mergeStrategySelect, catalog.merge_strategies.map((value) => ({ value, label: friendlyMergeStrategyName(value) })));
  if (el.mergeOrderSelect && Array.isArray(catalog.merge_orders)) {
    fillSelect(el.mergeOrderSelect, catalog.merge_orders.map((value) => ({ value, label: friendlyMergeOrderName(value) })));
  }
  wireEvents();
  await loadPayload();
}

function fillSelect(select, options) {
  if (!select) return;
  select.innerHTML = options.map((option) => `<option value="${option.value}">${option.label}</option>`).join("");
}

function wireEvents() {
  el.partitionSlider.addEventListener("input", () => {
    el.partitionValue.textContent = el.partitionSlider.value;
  });
  el.scaleSlider.addEventListener("input", () => {
    el.scaleValue.textContent = el.scaleSlider.value;
    if (state.payload) renderConstructionState();
  });
  el.problemSelect.addEventListener("change", () => {
    adaptScaleRange();
    resetPlannerState();
    if (state.payload) renderConstructionState();
    loadPayload();
  });
  el.refreshBtn.addEventListener("click", loadPayload);
  if (el.planBtn) {
    el.planBtn.addEventListener("click", planFusionTree);
  }
  el.mergeBtn.addEventListener("click", runFusion);
  el.graphModeBtn.addEventListener("click", () => setMode("graph"));
  el.matrixModeBtn.addEventListener("click", () => setMode("matrix"));
  el.partitionSlider.addEventListener("change", () => {
    resetPlannerState();
    loadPayload();
  });
  el.mergeStrategySelect.addEventListener("change", () => {
    resetPlannerState();
    loadPayload();
  });
  if (el.mergeOrderSelect) {
    el.mergeOrderSelect.addEventListener("change", () => {
      resetPlannerState();
      loadPayload();
    });
  }
}

function resetPlannerState() {
  state.appliedPlannerMode = "default";
  state.defaultMergePlan = null;
}

function setMode(mode) {
  state.mode = mode;
  el.graphModeBtn.classList.toggle("active", mode === "graph");
  el.matrixModeBtn.classList.toggle("active", mode === "matrix");
  render();
}

async function loadPayload() {
  return loadPayloadForPlanner(state.appliedPlannerMode || "default");
}

async function loadPayloadForPlanner(plannerMode) {
  adaptScaleRange();
  const params = new URLSearchParams({
    problem: el.problemSelect.value || "join_order",
    scale: el.scaleSlider.value,
    partitions: el.partitionSlider.value,
    merge_strategy: el.mergeStrategySelect.value || "top2_merge",
    merge_order: selectedMergeOrder(),
    planner_mode: plannerMode,
  });
  state.payload = await fetchJson(`/api/problem?${params.toString()}`);
  state.appliedPlannerMode = plannerMode;
  if (plannerMode === "default") {
    state.defaultMergePlan = JSON.parse(JSON.stringify(state.payload.merge_plan || null));
  }
  state.activeVariable = state.payload.graph.nodes[0]?.id || null;
  state.activeBoundary = state.payload.partitioning.boundary_focus[0] || null;
  state.activePartition = null;
  state.activeConstructionFilter = null;
  state.fusionResult = null;
  render();
}

async function planFusionTree() {
  if (!el.planBtn) return;
  el.planBtn.disabled = true;
  el.planBtn.textContent = "Planning...";
  try {
    await loadPayloadForPlanner("cost_based");
  } finally {
    el.planBtn.disabled = false;
    el.planBtn.textContent = "Plan";
  }
}

async function runFusion() {
  el.mergeBtn.disabled = true;
  el.mergeBtn.textContent = "Running D-Wave...";
  await loadPayload();
  const params = new URLSearchParams({
    problem: el.problemSelect.value || "join_order",
    scale: el.scaleSlider.value,
    partitions: el.partitionSlider.value,
    merge_strategy: el.mergeStrategySelect.value || "top2_merge",
    merge_order: selectedMergeOrder(),
    planner_mode: state.payload?.merge_plan?.planner_mode || state.appliedPlannerMode || "default",
  });
  try {
    state.fusionResult = await fetchJson(`/api/fusion?${params.toString()}`);
  } catch (error) {
    state.fusionResult = {
      supported: false,
      message: `Fusion request failed: ${error.message || error}`,
    };
  } finally {
    el.mergeBtn.disabled = false;
    el.mergeBtn.textContent = "Fusion";
  }
  renderMetrics();
  renderStrategyNote();
  renderMergeSteps();
  renderMergeTree();
  renderFusionCharts();
}

function render() {
  if (!state.payload) return;
  el.quboSubtitle.textContent = state.payload.problem_label;
  renderLeft();
  renderConstructionState();
  renderConstructionBlocks();
  renderDbSummary();
  renderDbStructure();
  renderBoundaryCanvas();
  renderPartitions();
  renderBoundaryGroups();
  renderMetrics();
  renderStrategyNote();
  renderMergeSteps();
  renderMergeTree();
  renderFusionCharts();
}

function currentControlState() {
  return {
    problem: el.problemSelect.value || "join_order",
    scale: Number(el.scaleSlider.value),
    mergeStrategy: el.mergeStrategySelect.value || "top2_merge",
    mergeOrder: selectedMergeOrder(),
    plannerMode: state.payload?.merge_plan?.planner_mode || state.appliedPlannerMode || "default",
  };
}

function selectedMergeOrder() {
  return el.mergeOrderSelect?.value || "left_deep";
}

function selectedPlannerMode() {
  return state.appliedPlannerMode || "default";
}

function displayJoinTag(joinIndex) {
  return `j${Number(joinIndex) + 1}`;
}

function displayJoinStep(joinIndex) {
  return `Join Step ${Number(joinIndex) + 1}`;
}

function renderConstructionState() {
  const controls = currentControlState();
  const selectedProblemLabel = el.problemSelect?.selectedOptions?.[0]?.textContent || pretty(controls.problem);
  const built = {
    problem: state.payload.problem_id,
    label: state.payload.problem_label || pretty(state.payload.problem_id),
    scale: state.payload.scale,
  };
  const pending =
    controls.problem !== built.problem ||
    controls.scale !== built.scale;

  const familySummary = renderFamilySummary(state.payload.partitioning.db_view.summary);
  const statusLabel = pending ? "Build pending" : "Constructed";
  const statusDetail = pending ? `${selectedProblemLabel} | size ${controls.scale}` : "Current view is in sync";

  el.constructionState.innerHTML = `
    <div class="state-main">
      <strong>Constructed: ${built.label} | size ${built.scale}</strong>
      <div class="step-meta">Variables ${state.payload.graph.nodes.length} | Couplings ${state.payload.graph.edges.length}</div>
      <div class="family-row">${familySummary}</div>
    </div>
    <div class="state-badge${pending ? " dirty" : ""}">
      <strong>${statusLabel}</strong>
      <span>${statusDetail}</span>
    </div>
  `;
  el.constructionState.querySelectorAll("[data-family-kind]").forEach((button) => {
    button.addEventListener("click", () => toggleConstructionFilter({
      id: `kind:${button.dataset.familyKind}`,
      label: friendlyKindName(state.payload.problem_id, button.dataset.familyKind),
      kinds: [button.dataset.familyKind],
    }));
  });
}

function adaptScaleRange() {
  const problem = el.problemSelect.value || "join_order";
  if (problem === "join_order") {
    el.scaleSlider.min = "3";
    el.scaleSlider.max = "8";
  } else if (problem === "mqo") {
    el.scaleSlider.min = "2";
    el.scaleSlider.max = "6";
  } else {
    el.scaleSlider.min = "2";
    el.scaleSlider.max = "6";
  }
  if (Number(el.scaleSlider.value) < Number(el.scaleSlider.min)) el.scaleSlider.value = el.scaleSlider.min;
  if (Number(el.scaleSlider.value) > Number(el.scaleSlider.max)) el.scaleSlider.value = el.scaleSlider.max;
  el.scaleValue.textContent = el.scaleSlider.value;
}

function renderLeft() {
  el.leftCanvas.innerHTML = "";
  if (state.mode === "matrix") return renderMatrix();
  if (state.payload.problem_id === "join_order") return renderJoinOrderLayeredGraph();
  if (state.payload.problem_id === "mqo") return renderMqoStructuredGraph();
  if (state.payload.problem_id === "index_selection") return renderIndexSelectionStructuredGraph();
  return renderGraph();
}

function renderMqoStructuredGraph() {
  const width = el.leftCanvas.clientWidth;
  const height = el.leftCanvas.clientHeight;
  const svg = makeSvg(width, height);
  const { nodes, edges } = state.payload.graph;
  const partitionMap = state.payload.partitioning.node_to_partition;
  const positions = mqoLayout(nodes, width, height);
  const queries = state.payload.partitioning.db_view.summary.queries || [];

  queries.forEach((entry) => {
    const x = positions[`column::${entry.query}`];
    const header = document.createElementNS("http://www.w3.org/2000/svg", "text");
    header.setAttribute("x", x - 18);
    header.setAttribute("y", 30);
    header.setAttribute("font-size", "16");
    header.setAttribute("font-family", "Trebuchet MS, sans-serif");
    header.textContent = entry.query.toUpperCase();
    svg.appendChild(header);

    const sub = document.createElementNS("http://www.w3.org/2000/svg", "text");
    sub.setAttribute("x", x - 40);
    sub.setAttribute("y", 48);
    sub.setAttribute("font-size", "11");
    sub.setAttribute("font-family", "Trebuchet MS, sans-serif");
    sub.setAttribute("fill", "#6d695e");
    sub.textContent = `${entry.plans.length} candidate plans`;
    svg.appendChild(sub);
  });

  edges.forEach((edge) => {
    const source = positions[edge.source];
    const target = positions[edge.target];
    const boundary = partitionMap[edge.source] !== partitionMap[edge.target];
    const active = isEdgeActive(edge);
    const inFilter = matchesConstructionFilterEdge(edge);
    const sameColumn = Math.abs(source.x - target.x) < 2;
    const shape = document.createElementNS("http://www.w3.org/2000/svg", "path");
    const bend = sameColumn ? 34 : Math.abs(target.x - source.x) * 0.18;
    const d = sameColumn
      ? `M ${source.x} ${source.y} C ${source.x + bend} ${source.y}, ${target.x + bend} ${target.y}, ${target.x} ${target.y}`
      : `M ${source.x} ${source.y} C ${(source.x + target.x) / 2} ${source.y - bend}, ${(source.x + target.x) / 2} ${target.y - bend}, ${target.x} ${target.y}`;
    shape.setAttribute("d", d);
    shape.setAttribute("fill", "none");
    shape.setAttribute("stroke", boundary ? "#bb3e03" : "#7f8c8d");
    shape.setAttribute("stroke-opacity", active ? "0.96" : inFilter || !state.activeConstructionFilter ? boundary ? "0.76" : "0.22" : "0.06");
    shape.setAttribute("stroke-width", String(Math.max(1.5, Math.min(6.5, Math.abs(edge.value) / 6)) * (inFilter ? 1.15 : 1)));
    shape.addEventListener("mouseenter", () => focusBoundaryFromEdge(edge));
    svg.appendChild(shape);
  });

  nodes.forEach((node) => {
    const pos = positions[node.id];
    const partition = partitionMap[node.id];
    const color = palette[partition % palette.length];
    const inFilter = matchesConstructionFilter(node);
    const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
    group.addEventListener("mouseenter", () => {
      state.activeVariable = node.id;
      renderDbStructure();
      renderLeft();
    });

    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("cx", pos.x);
    circle.setAttribute("cy", pos.y);
    circle.setAttribute("r", node.id === state.activeVariable ? "13" : inFilter ? "11.5" : "10");
    circle.setAttribute("fill", color);
    circle.setAttribute("fill-opacity", inFilter || !state.activeConstructionFilter ? "1" : "0.26");
    circle.setAttribute("stroke", node.id === state.activeVariable ? "#111" : inFilter ? "#0a7d91" : "rgba(0,0,0,0.15)");
    circle.setAttribute("stroke-width", node.id === state.activeVariable ? "3" : inFilter ? "2" : "1");
    group.appendChild(circle);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", pos.x + 14);
    label.setAttribute("y", pos.y + 4);
    label.setAttribute("font-size", "12");
    label.setAttribute("font-family", "Trebuchet MS, sans-serif");
    label.textContent = shortName(node.id);
    group.appendChild(label);
    svg.appendChild(group);
  });

  el.leftCanvas.appendChild(svg);
  el.leftCanvas.appendChild(makeLegend());
}

function renderIndexSelectionStructuredGraph() {
  const width = el.leftCanvas.clientWidth;
  const height = el.leftCanvas.clientHeight;
  const svg = makeSvg(width, height);
  const { nodes, edges } = state.payload.graph;
  const partitionMap = state.payload.partitioning.node_to_partition;
  const positions = indexSelectionLayout(nodes, width, height);
  const tables = (state.payload.partitioning.db_view.summary.tables || []).filter((entry) => entry.table !== "_storage_");
  const storageEntry = (state.payload.partitioning.db_view.summary.tables || []).find((entry) => entry.table === "_storage_");

  tables.forEach((entry) => {
    const x = positions[`column::${entry.table}`];
    const header = document.createElementNS("http://www.w3.org/2000/svg", "text");
    header.setAttribute("x", x);
    header.setAttribute("y", 30);
    header.setAttribute("font-size", "13");
    header.setAttribute("font-family", "Trebuchet MS, sans-serif");
    header.setAttribute("text-anchor", "middle");
    header.textContent = entry.table;
    svg.appendChild(header);
  });

  if (storageEntry) {
    const x = positions["column::_storage_"];
    const header = document.createElementNS("http://www.w3.org/2000/svg", "text");
    header.setAttribute("x", x);
    header.setAttribute("y", 48);
    header.setAttribute("font-size", "13");
    header.setAttribute("font-family", "Trebuchet MS, sans-serif");
    header.setAttribute("text-anchor", "middle");
    header.textContent = "Storage";
    svg.appendChild(header);
  }

  edges.forEach((edge) => {
    const source = positions[edge.source];
    const target = positions[edge.target];
    const boundary = partitionMap[edge.source] !== partitionMap[edge.target];
    const active = isEdgeActive(edge);
    const inFilter = matchesConstructionFilterEdge(edge);
    const shape = document.createElementNS("http://www.w3.org/2000/svg", "path");
    const bend = Math.abs(target.x - source.x) < 2 ? 28 : Math.abs(target.x - source.x) * 0.12;
    const d = `M ${source.x} ${source.y} C ${(source.x + target.x) / 2} ${source.y - bend}, ${(source.x + target.x) / 2} ${target.y - bend}, ${target.x} ${target.y}`;
    shape.setAttribute("d", d);
    shape.setAttribute("fill", "none");
    shape.setAttribute("stroke", boundary ? "#bb3e03" : "#7f8c8d");
    shape.setAttribute("stroke-opacity", active ? "0.9" : inFilter || !state.activeConstructionFilter ? boundary ? "0.48" : "0.1" : "0.03");
    shape.setAttribute("stroke-width", String(Math.max(1.0, Math.min(2.8, Math.abs(edge.value) / 18)) * (inFilter ? 1.1 : 1)));
    shape.addEventListener("mouseenter", () => focusBoundaryFromEdge(edge));
    svg.appendChild(shape);
  });

  nodes.forEach((node) => {
    const pos = positions[node.id];
    const partition = partitionMap[node.id];
    const color = palette[partition % palette.length];
    const inFilter = matchesConstructionFilter(node);
    const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
    group.addEventListener("mouseenter", () => {
      state.activeVariable = node.id;
      renderDbStructure();
      renderLeft();
    });

    const shapeType = node.meta.kind === "storage_fraction" ? "rect" : (node.meta.clustered ? "rect" : "circle");
    const shape = document.createElementNS("http://www.w3.org/2000/svg", shapeType);
    if (shapeType === "circle") {
      shape.setAttribute("cx", pos.x);
      shape.setAttribute("cy", pos.y);
      shape.setAttribute("r", node.id === state.activeVariable ? "12.5" : inFilter ? "11" : "9.5");
    } else {
      const w = node.meta.kind === "storage_fraction" ? (node.id === state.activeVariable ? 34 : 30) : (node.id === state.activeVariable ? 28 : 24);
      const h = node.meta.kind === "storage_fraction" ? (node.id === state.activeVariable ? 24 : 20) : (node.id === state.activeVariable ? 24 : 20);
      shape.setAttribute("x", pos.x - w / 2);
      shape.setAttribute("y", pos.y - h / 2);
      shape.setAttribute("width", w);
      shape.setAttribute("height", h);
      shape.setAttribute("rx", node.meta.kind === "storage_fraction" ? "4" : "6");
    }
    shape.setAttribute("fill", color);
    shape.setAttribute("fill-opacity", inFilter || !state.activeConstructionFilter ? "1" : "0.24");
    shape.setAttribute("stroke", node.id === state.activeVariable ? "#111" : inFilter ? "#0a7d91" : "rgba(0,0,0,0.15)");
    shape.setAttribute("stroke-width", node.id === state.activeVariable ? "3" : inFilter ? "2" : "1");
    group.appendChild(shape);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", pos.x);
    label.setAttribute("y", pos.y + (node.meta.kind === "storage_fraction" ? 22 : 20));
    label.setAttribute("font-size", "9.5");
    label.setAttribute("font-family", "Trebuchet MS, sans-serif");
    label.setAttribute("text-anchor", "middle");
    label.setAttribute("fill", "#3f3a31");
    const labelLines = indexSelectionNodeLabelParts(node);
    labelLines.forEach((line, idx) => {
      const span = document.createElementNS("http://www.w3.org/2000/svg", "tspan");
      span.setAttribute("x", pos.x);
      span.setAttribute("dy", idx === 0 ? "0" : "10");
      span.textContent = line;
      label.appendChild(span);
    });
    group.appendChild(label);
    svg.appendChild(group);
  });

  el.leftCanvas.appendChild(svg);
  el.leftCanvas.appendChild(makeLegend());
}

function renderJoinOrderLayeredGraph() {
  const width = el.leftCanvas.clientWidth;
  const height = el.leftCanvas.clientHeight;
  const svg = makeSvg(width, height);
  const { nodes, edges } = state.payload.graph;
  const partitionMap = state.payload.partitioning.node_to_partition;
  const positions = joinOrderLayout(nodes, width, height);
  const joinIndices = [...new Set(nodes.map((node) => node.meta.join_index).filter((value) => value !== undefined))].sort((a, b) => a - b);

  joinIndices.forEach((joinIndex) => {
    const x = positions[`column::${joinIndex}`];
    const header = document.createElementNS("http://www.w3.org/2000/svg", "text");
    header.setAttribute("x", x - 42);
    header.setAttribute("y", 30);
    header.setAttribute("font-size", "16");
    header.setAttribute("font-family", "Trebuchet MS, sans-serif");
    header.textContent = displayJoinStep(joinIndex);
    svg.appendChild(header);

    const sub = document.createElementNS("http://www.w3.org/2000/svg", "text");
    sub.setAttribute("x", x - 38);
    sub.setAttribute("y", 48);
    sub.setAttribute("font-size", "11");
    sub.setAttribute("font-family", "Trebuchet MS, sans-serif");
    sub.setAttribute("fill", "#6d695e");
    sub.textContent = `Variables tag ${displayJoinTag(joinIndex)}`;
    svg.appendChild(sub);
  });

  const relationBand = document.createElementNS("http://www.w3.org/2000/svg", "text");
  relationBand.setAttribute("x", 26);
  relationBand.setAttribute("y", 76);
  relationBand.setAttribute("font-size", "12");
  relationBand.setAttribute("font-family", "Trebuchet MS, sans-serif");
  relationBand.setAttribute("fill", "#005f73");
  relationBand.textContent = "Relation Variables";
  svg.appendChild(relationBand);

  const predicateBand = document.createElementNS("http://www.w3.org/2000/svg", "text");
  predicateBand.setAttribute("x", 26);
  predicateBand.setAttribute("y", height * 0.58);
  predicateBand.setAttribute("font-size", "12");
  predicateBand.setAttribute("font-family", "Trebuchet MS, sans-serif");
  predicateBand.setAttribute("fill", "#bb3e03");
  predicateBand.textContent = "Predicate Variables";
  svg.appendChild(predicateBand);

  edges.forEach((edge) => {
    const source = positions[edge.source];
    const target = positions[edge.target];
    const boundary = partitionMap[edge.source] !== partitionMap[edge.target];
    const active = isEdgeActive(edge);
    const inFilter = matchesConstructionFilterEdge(edge);
    const line = document.createElementNS("http://www.w3.org/2000/svg", "path");
    const midX = (source.x + target.x) / 2;
    const d = `M ${source.x} ${source.y} C ${midX} ${source.y}, ${midX} ${target.y}, ${target.x} ${target.y}`;
    line.setAttribute("d", d);
    line.setAttribute("fill", "none");
    line.setAttribute("stroke", boundary ? "#bb3e03" : "#7f8c8d");
    line.setAttribute("stroke-opacity", active ? "0.96" : inFilter || !state.activeConstructionFilter ? boundary ? "0.82" : "0.22" : "0.05");
    line.setAttribute("stroke-width", String(Math.max(1.6, Math.min(7.5, Math.abs(edge.value) / 8)) * (inFilter ? 1.15 : 1)));
    line.addEventListener("mouseenter", () => focusBoundaryFromEdge(edge));
    svg.appendChild(line);
  });

  nodes.forEach((node) => {
    const pos = positions[node.id];
    const partition = partitionMap[node.id];
    const color = palette[partition % palette.length];
    const inFilter = matchesConstructionFilter(node);
    const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
    group.addEventListener("mouseenter", () => {
      state.activeVariable = node.id;
      renderDbStructure();
      renderLeft();
    });

    const shape = document.createElementNS("http://www.w3.org/2000/svg", node.meta.kind === "relation_operand_for_join" ? "circle" : "rect");
    if (node.meta.kind === "relation_operand_for_join") {
      shape.setAttribute("cx", pos.x);
      shape.setAttribute("cy", pos.y);
      shape.setAttribute("r", node.id === state.activeVariable ? "12.5" : inFilter ? "11" : "9.5");
    } else {
      shape.setAttribute("x", pos.x - 11);
      shape.setAttribute("y", pos.y - 11);
      shape.setAttribute("width", node.id === state.activeVariable ? "24" : "22");
      shape.setAttribute("height", node.id === state.activeVariable ? "24" : "22");
      shape.setAttribute("rx", "5");
    }
    shape.setAttribute("fill", color);
    shape.setAttribute("fill-opacity", inFilter || !state.activeConstructionFilter ? "1" : "0.26");
    shape.setAttribute("stroke", node.id === state.activeVariable ? "#111" : inFilter ? "#0a7d91" : "rgba(0,0,0,0.15)");
    shape.setAttribute("stroke-width", node.id === state.activeVariable ? "3" : inFilter ? "2" : "1");
    group.appendChild(shape);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", pos.x + 14);
    label.setAttribute("y", pos.y + 4);
    label.setAttribute("font-size", "12");
    label.setAttribute("font-family", "Trebuchet MS, sans-serif");
    label.textContent = shortName(node.id);
    group.appendChild(label);
    svg.appendChild(group);
  });

  el.leftCanvas.appendChild(svg);
  el.leftCanvas.appendChild(makeLegend());
}

function renderGraph() {
  const width = el.leftCanvas.clientWidth;
  const height = el.leftCanvas.clientHeight;
  const svg = makeSvg(width, height);
  const { nodes, edges } = state.payload.graph;
  const positions = radialLayout(nodes, width, height);
  const partitionMap = state.payload.partitioning.node_to_partition;

  edges.forEach((edge) => {
    const source = positions[edge.source];
    const target = positions[edge.target];
    const boundary = partitionMap[edge.source] !== partitionMap[edge.target];
    const active = isEdgeActive(edge);
    const inFilter = matchesConstructionFilterEdge(edge);
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", source.x);
    line.setAttribute("y1", source.y);
    line.setAttribute("x2", target.x);
    line.setAttribute("y2", target.y);
    line.setAttribute("stroke", boundary ? "#bb3e03" : "#7f8c8d");
    line.setAttribute("stroke-opacity", active ? "0.96" : inFilter || !state.activeConstructionFilter ? boundary ? "0.72" : "0.2" : "0.06");
    line.setAttribute("stroke-width", String(Math.max(1.4, Math.min(8, Math.abs(edge.value) / 8)) * (inFilter ? 1.1 : 1)));
    line.addEventListener("mouseenter", () => focusBoundaryFromEdge(edge));
    svg.appendChild(line);
  });

  nodes.forEach((node) => {
    const position = positions[node.id];
    const partition = partitionMap[node.id];
    const color = palette[partition % palette.length];
    const inFilter = matchesConstructionFilter(node);
    const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
    group.addEventListener("mouseenter", () => {
      state.activeVariable = node.id;
      renderDbStructure();
      renderLeft();
    });
    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("cx", position.x);
    circle.setAttribute("cy", position.y);
    circle.setAttribute("r", node.id === state.activeVariable ? "13" : inFilter ? "11.5" : "10");
    circle.setAttribute("fill", color);
    circle.setAttribute("fill-opacity", inFilter || !state.activeConstructionFilter ? "1" : "0.28");
    circle.setAttribute("stroke", node.id === state.activeVariable ? "#111" : inFilter ? "#0a7d91" : "rgba(0,0,0,0.15)");
    circle.setAttribute("stroke-width", node.id === state.activeVariable ? "3" : inFilter ? "2" : "1");
    group.appendChild(circle);
    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", position.x + 14);
    label.setAttribute("y", position.y + 4);
    label.setAttribute("font-size", "12");
    label.setAttribute("font-family", "Trebuchet MS, sans-serif");
    label.textContent = shortName(node.id);
    group.appendChild(label);
    svg.appendChild(group);
  });

  el.leftCanvas.appendChild(svg);
  el.leftCanvas.appendChild(makeLegend());
}

function renderMatrix() {
  const { nodes, edges } = state.payload.graph;
  const nodeIds = nodes.map((node) => node.id);
  const edgeMap = new Map();
  edges.forEach((edge) => edgeMap.set([edge.source, edge.target].sort().join("|"), edge.value));
  const partitionMap = state.payload.partitioning.node_to_partition;
  const size = Math.max(16, Math.min(24, Math.floor((el.leftCanvas.clientWidth - 120) / nodeIds.length)));
  const width = 100 + nodeIds.length * size;
  const height = 100 + nodeIds.length * size;
  const svg = makeSvg(width, height);

  nodeIds.forEach((rowId, row) => {
    nodeIds.forEach((colId, col) => {
      const key = [rowId, colId].sort().join("|");
      const value = rowId === colId ? nodes.find((node) => node.id === rowId).linear : edgeMap.get(key) || 0;
      const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      rect.setAttribute("x", 80 + col * size);
      rect.setAttribute("y", 40 + row * size);
      rect.setAttribute("width", size - 1);
      rect.setAttribute("height", size - 1);
      rect.setAttribute("fill", matrixColor(value));
      const inFilter = rowId === colId
        ? matchesConstructionFilterById(rowId)
        : matchesConstructionFilterEdge({ id: [rowId, colId].sort().join("|"), source: rowId, target: colId });
      rect.setAttribute("fill-opacity", inFilter || !state.activeConstructionFilter ? "1" : "0.26");
      rect.setAttribute("stroke", rowId === state.activeVariable || colId === state.activeVariable ? "#111" : inFilter ? "#0a7d91" : "rgba(0,0,0,0.06)");
      rect.setAttribute("stroke-width", rowId === state.activeVariable || colId === state.activeVariable ? "1.5" : inFilter ? "1.2" : "1");
      svg.appendChild(rect);
    });
  });

  nodeIds.forEach((id, idx) => {
    const color = palette[partitionMap[id] % palette.length];
    const xLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
    xLabel.setAttribute("x", 85 + idx * size);
    xLabel.setAttribute("y", 26);
    xLabel.setAttribute("font-size", "9");
    xLabel.setAttribute("transform", `rotate(-45 ${85 + idx * size} 26)`);
    xLabel.setAttribute("fill", color);
    xLabel.textContent = shortName(id);
    svg.appendChild(xLabel);
    const yLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
    yLabel.setAttribute("x", 8);
    yLabel.setAttribute("y", 52 + idx * size);
    yLabel.setAttribute("font-size", "9");
    yLabel.setAttribute("fill", color);
    yLabel.textContent = shortName(id);
    svg.appendChild(yLabel);
  });
  el.leftCanvas.appendChild(svg);
}

function renderDbSummary() {
  const summary = state.payload.partitioning.db_view.summary;
  const problem = summary.problem || state.payload.problem_id;
  const activeSemantic = activeJoinOrderSemanticFilter();
  const accordions = [];
  if (problem === "join_order") {
    accordions.push(renderJoinOrderSemanticAccordion("Relations", summary.relations || [], "relation", activeSemantic));
    accordions.push(renderJoinOrderSemanticAccordion("Predicates", summary.predicates || [], "predicate", activeSemantic));
  } else if (problem === "mqo") {
    (summary.queries || []).forEach((entry) => {
      accordions.push(renderMqoSemanticAccordion(entry));
    });
  } else if (problem === "index_selection") {
    (summary.tables || []).forEach((entry) => {
      accordions.push(renderIndexSemanticAccordion(entry));
    });
  }
  el.dbSummary.innerHTML = accordions.join("");
  if (problem === "join_order") {
    syncJoinOrderAccordionState(activeSemantic);
    el.dbSummary.querySelectorAll("[data-semantic-type]").forEach((button) => {
      button.addEventListener("click", () => {
        const semanticType = button.dataset.semanticType;
        const semanticName = button.dataset.semanticName;
        toggleConstructionFilter(buildJoinOrderSemanticFilter(semanticType, semanticName));
      });
    });
  } else if (problem === "mqo") {
    el.dbSummary.querySelectorAll("[data-mqo-query]").forEach((button) => {
      button.addEventListener("click", () => {
        toggleConstructionFilter(buildMqoQueryFilter(button.dataset.mqoQuery));
      });
    });
  } else if (problem === "index_selection") {
    el.dbSummary.querySelectorAll("[data-index-table]").forEach((button) => {
      button.addEventListener("click", () => {
        const table = button.dataset.indexTable;
        if (table === "_storage_") {
          toggleConstructionFilter(buildIndexStorageFilter());
        } else {
          toggleConstructionFilter(buildIndexTableFilter(table));
        }
      });
    });
  }
}

function renderConstructionBlocks() {
  const blocks = state.payload.construction.construction_blocks || [];
  el.constructionBlocks.innerHTML = blocks.map((block) => `
    <button type="button" class="block-card${isFilterActive(`block:${block.name}`) ? " active" : ""}" data-block-name="${block.name}">
      <strong>${block.name}: ${block.title}</strong>
      <div class="formula">${block.formula}</div>
      <div class="step-meta">${block.meaning}</div>
      <div class="step-meta">Variables: ${block.variables.join(", ")}</div>
      ${block.focus ? `<div class="focus-note">Nodes: ${block.focus.node_pattern}</div><div class="focus-note">Edges: ${block.focus.edge_pattern}</div>` : ""}
    </button>
  `).join("");
  el.constructionBlocks.querySelectorAll("[data-block-name]").forEach((button) => {
    const block = blocks.find((entry) => entry.name === button.dataset.blockName);
    button.addEventListener("click", () => toggleConstructionFilter({
      id: `block:${block.name}`,
      label: block.name,
      kinds: mapBlockVariablesToKinds(block.variables),
      nodeIds: block.highlight?.node_ids || [],
      edgeIds: block.highlight?.edge_ids || [],
    }));
  });
}

function renderDbStructure() {
  if (state.payload.problem_id === "join_order") {
    renderJoinOrderDbStructure();
    return;
  }
  if (state.payload.problem_id === "mqo") {
    renderMqoDbStructure();
    return;
  }
  if (state.payload.problem_id === "index_selection") {
    renderIndexDbStructure();
    return;
  }
  const items = state.payload.partitioning.db_view.items;
  el.dbStructure.innerHTML = "";
  items.forEach((item) => {
    const meta = state.payload.graph.nodes.find((entry) => entry.id === item.variable).meta;
    const inFilter = matchesConstructionFilterByMeta(meta);
    const card = document.createElement("button");
    card.type = "button";
    card.className = `db-item${item.variable === state.activeVariable ? " active" : ""}`;
    card.style.opacity = inFilter || !state.activeConstructionFilter ? "1" : "0.35";
    card.innerHTML = `
      <strong>${item.label}</strong>
      <div class="step-meta">${pretty(item.type)}</div>
      <div class="step-meta">${summarizeExtra(item.extra)}</div>
    `;
    card.addEventListener("mouseenter", () => {
      state.activeVariable = item.variable;
      renderDbStructure();
      renderLeft();
    });
    el.dbStructure.appendChild(card);
  });
}

function renderMqoDbStructure() {
  const summary = state.payload.partitioning.db_view.summary;
  const queries = summary.queries || [];
  const activeQuery = activeMqoQueryFilter();
  if (!activeQuery) {
    el.dbStructure.innerHTML = `
      <div class="mapping-empty">
        <strong>Select one query</strong>
        <div class="step-meta">Choose a query above to inspect its candidate plans and how they enter the Multiple Query Optimization QUBO.</div>
      </div>
    `;
    return;
  }
  const entry = queries.find((item) => item.query === activeQuery);
  if (!entry) {
    el.dbStructure.innerHTML = "";
    return;
  }
  el.dbStructure.innerHTML = `
    <div class="mapping-section">
      <div class="mapping-header">
        <strong>${entry.query}</strong>
        <span>${entry.plans.length} plans</span>
      </div>
      <div class="mapping-grid">
        ${entry.plans.map((plan) => `
          <button type="button" class="semantic-map-card${plan.variable === state.activeVariable ? " active" : ""}" data-map-variable="${plan.variable}">
            <div class="mapping-header">
              <strong>${plan.plan}</strong>
              <span>cost ${plan.cost}</span>
            </div>
            <div class="step-meta">QUBO variable: ${shortName(plan.variable)}</div>
            <div class="step-meta">One candidate execution plan for ${entry.query}.</div>
          </button>
        `).join("")}
      </div>
    </div>
  `;
  bindMapVariableHover();
}

function renderIndexDbStructure() {
  const summary = state.payload.partitioning.db_view.summary;
  const tables = summary.tables || [];
  const activeTable = activeIndexTableFilter();
  if (!activeTable) {
    el.dbStructure.innerHTML = `
      <div class="mapping-empty">
        <strong>Select one table or storage group</strong>
        <div class="step-meta">Choose a table above to inspect index candidates, or choose Storage Encoding to inspect capacity variables.</div>
      </div>
    `;
    return;
  }
  const entry = tables.find((item) => item.table === activeTable);
  if (!entry) {
    el.dbStructure.innerHTML = "";
    return;
  }
  const label = entry.table === "_storage_" ? "Storage Encoding" : entry.table;
  const indexCards = (entry.indices || []).map((item) => `
    <button type="button" class="semantic-map-card${item.variable === state.activeVariable ? " active" : ""}" data-map-variable="${item.variable}">
      <div class="mapping-header">
        <strong>${item.index}</strong>
        <span>storage ${item.storage}</span>
      </div>
      <div class="step-meta">${item.clustered ? "Clustered index candidate" : "Non-clustered index candidate"}</div>
      <div class="step-meta">QUBO variable: ${shortName(item.variable)}</div>
    </button>
  `).join("");
  const storageCards = (entry.storage_vars || []).map((item) => `
    <button type="button" class="semantic-map-card${item.variable === state.activeVariable ? " active" : ""}" data-map-variable="${item.variable}">
      <div class="mapping-header">
        <strong>Capacity ${item.fraction}</strong>
        <span>auxiliary</span>
      </div>
      <div class="step-meta">QUBO variable: ${shortName(item.variable)}</div>
      <div class="step-meta">Binary storage-fraction variable used by the capacity constraint.</div>
    </button>
  `).join("");

  el.dbStructure.innerHTML = `
    <div class="mapping-section">
      <div class="mapping-header">
        <strong>${label}</strong>
        <span>${(entry.indices || []).length + (entry.storage_vars || []).length} variables</span>
      </div>
      <div class="mapping-grid">
        ${indexCards}
        ${storageCards}
      </div>
    </div>
  `;
  bindMapVariableHover();
}

function renderJoinOrderDbStructure() {
  const summary = state.payload.partitioning.db_view.summary;
  const joinPrefixes = summary.join_prefixes || [];
  const activeSemantic = activeJoinOrderSemanticFilter();
  if (!activeSemantic) {
    el.dbStructure.innerHTML = `
      <div class="mapping-empty">
        <strong>Select one semantic object</strong>
        <div class="step-meta">Choose a relation or predicate above to inspect how it expands into QUBO variable copies across join steps.</div>
      </div>
    `;
    return;
  }

  const relationNames = activeSemantic.type === "relation" ? [activeSemantic.name] : [];
  const predicateNames = activeSemantic.type === "predicate" ? [activeSemantic.name] : [];

  const sections = [];
  if (relationNames.length) {
    sections.push(`
      <div class="mapping-section">
        <div class="mapping-header">
          <strong>Relation Mapping</strong>
          <span>${relationNames.length} object${relationNames.length === 1 ? "" : "s"}</span>
        </div>
        <div class="mapping-grid">
          ${relationNames.map((name) => renderJoinOrderSemanticMapCard("relation", name, joinPrefixes)).join("")}
        </div>
      </div>
    `);
  }
  if (predicateNames.length) {
    sections.push(`
      <div class="mapping-section">
        <div class="mapping-header">
          <strong>Predicate Mapping</strong>
          <span>${predicateNames.length} object${predicateNames.length === 1 ? "" : "s"}</span>
        </div>
        <div class="mapping-grid">
          ${predicateNames.map((name) => renderJoinOrderSemanticMapCard("predicate", name, joinPrefixes)).join("")}
        </div>
      </div>
    `);
  }

  el.dbStructure.innerHTML = sections.join("");
  el.dbStructure.querySelectorAll("[data-semantic-type]").forEach((button) => {
    if (button.dataset.mapVariable) return;
    button.addEventListener("click", () => {
      toggleConstructionFilter(buildJoinOrderSemanticFilter(button.dataset.semanticType, button.dataset.semanticName));
    });
  });
  el.dbStructure.querySelectorAll("[data-map-variable]").forEach((button) => {
    const focusVariable = () => {
      state.activeVariable = button.dataset.mapVariable;
      renderDbStructure();
      renderLeft();
    };
    button.addEventListener("mouseenter", focusVariable);
    button.addEventListener("click", focusVariable);
  });
}

function renderJoinOrderSemanticMapCard(semanticType, semanticName, joinPrefixes) {
  const filter = buildJoinOrderSemanticFilter(semanticType, semanticName);
  const isActive = isFilterActive(filter.id);
  const rows = joinPrefixes
    .map((prefix) => {
      if (semanticType === "relation") {
        const match = (prefix.relations || []).find((item) => item.name === semanticName);
        if (!match) return "";
        return renderSemanticMapRow(displayJoinStep(prefix.join_index), displayJoinTag(prefix.join_index), match.variable, semanticType, semanticName);
      }
      const match = (prefix.predicates || []).find((item) => item.name === semanticName);
      if (!match) return "";
      return renderSemanticMapRow(displayJoinStep(prefix.join_index), displayJoinTag(prefix.join_index), match.variable, semanticType, semanticName, match.relations);
    })
    .filter(Boolean)
    .join("");

  const relationHint = semanticType === "predicate"
    ? findPredicateRelations(joinPrefixes, semanticName)
    : [];
  const subtitle = semanticType === "relation"
    ? "Expands into one QUBO relation variable at each join step."
    : `Connects ${relationHint.join(" - ")} and expands into one QUBO predicate variable at each join step.`;

  return `
    <section class="semantic-map-card${isActive ? " active" : ""}">
      <button type="button" class="semantic-map-title" data-semantic-type="${semanticType}" data-semantic-name="${semanticName}">
        <strong>${semanticName}</strong>
        <span>${pretty(semanticType)}</span>
      </button>
      <div class="step-meta">${subtitle}</div>
      <div class="semantic-map-rows">${rows}</div>
    </section>
  `;
}

function renderSemanticMapRow(stepLabel, tagLabel, variableId, semanticType, semanticName, relations = []) {
  const active = variableId === state.activeVariable ? " active" : "";
  const relationTag = semanticType === "predicate" && relations.length
    ? `<span class="mini-tag">${relations.join(" - ")}</span>`
    : "";
  return `
    <button type="button" class="semantic-map-row${active}" data-map-variable="${variableId}" data-semantic-type="${semanticType}" data-semantic-name="${semanticName}">
      <span class="semantic-map-prefix">${stepLabel}</span>
      <span class="semantic-map-var">${shortName(variableId)}</span>
      <span class="semantic-map-partition">${tagLabel}</span>
      ${relationTag}
    </button>
  `;
}

function findPredicateRelations(joinPrefixes, predicateName) {
  for (const prefix of joinPrefixes) {
    const match = (prefix.predicates || []).find((item) => item.name === predicateName);
    if (match) return match.relations || [];
  }
  return [];
}

function renderFamilySummary(summary) {
  const problem = state.payload.problem_id;
  const orderedKinds = Object.keys(summary.by_kind_counts || {}).sort((left, right) => familyOrder(problem, left) - familyOrder(problem, right));
  return orderedKinds.map((kind) => `
    <button type="button" class="family-chip${isFilterActive(`kind:${kind}`) ? " active" : ""}" data-family-kind="${kind}">
      ${friendlyKindName(problem, kind)} (${summary.by_kind_counts[kind]})
    </button>
  `).join("");
}

function renderAccordion(title, items) {
  return `
    <details class="accordion" open>
      <summary>${title}</summary>
      <div class="accordion-body">
        ${items.map((item) => `<div class="semantic-pill">${item}</div>`).join("")}
      </div>
    </details>
  `;
}

function renderJoinOrderSemanticAccordion(title, items, semanticType, activeSemantic) {
  const isOpen = !activeSemantic || activeSemantic.type === semanticType;
  return `
    <details class="accordion" data-semantic-group="${semanticType}"${isOpen ? " open" : ""}>
      <summary>${title}</summary>
      <div class="accordion-body">
        ${items.map((item) => {
          const filterId = `semantic:${semanticType}:${item}`;
          return `
            <button type="button" class="semantic-pill${isFilterActive(filterId) ? " active" : ""}" data-semantic-type="${semanticType}" data-semantic-name="${item}">
              ${item}
            </button>
          `;
        }).join("")}
      </div>
    </details>
  `;
}

function renderMqoSemanticAccordion(entry) {
  const filterId = `semantic:mqo:query:${entry.query}`;
  return `
    <details class="accordion" open>
      <summary>${entry.query}</summary>
      <div class="accordion-body">
        <button type="button" class="semantic-pill${isFilterActive(filterId) ? " active" : ""}" data-mqo-query="${entry.query}">
          ${entry.query} (${entry.plans.length} plans)
        </button>
      </div>
    </details>
  `;
}

function renderIndexSemanticAccordion(entry) {
  const label = entry.table === "_storage_" ? "Storage Encoding" : entry.table;
  const filterId = entry.table === "_storage_" ? "semantic:index:storage" : `semantic:index:table:${entry.table}`;
  const count = (entry.indices || []).length + (entry.storage_vars || []).length;
  return `
    <details class="accordion" open>
      <summary>${label}</summary>
      <div class="accordion-body">
        <button type="button" class="semantic-pill${isFilterActive(filterId) ? " active" : ""}" data-index-table="${entry.table}">
          ${label} (${count})
        </button>
      </div>
    </details>
  `;
}

function syncJoinOrderAccordionState(activeSemantic) {
  const detailsList = [...el.dbSummary.querySelectorAll("[data-semantic-group]")];
  detailsList.forEach((details) => {
    if (!activeSemantic) {
      details.open = true;
      return;
    }
    details.open = details.dataset.semanticGroup === activeSemantic.type;
  });
}

function buildJoinOrderSemanticFilter(semanticType, semanticName) {
  const nodes = state.payload.graph.nodes;
  const nodeIds = nodes
    .filter((node) => {
      if (semanticType === "relation") {
        return node.meta.relation === semanticName || node.meta.db_element?.relations?.includes(semanticName);
      }
      if (semanticType === "predicate") {
        return node.meta.predicate === semanticName;
      }
      return false;
    })
    .map((node) => node.id);
  const edgeIds = state.payload.graph.edges
    .filter((edge) => nodeIds.includes(edge.source) || nodeIds.includes(edge.target))
    .map((edge) => edge.id);
  return {
    id: `semantic:${semanticType}:${semanticName}`,
    label: semanticName,
    nodeIds,
    edgeIds,
  };
}

function buildMqoQueryFilter(queryName) {
  const nodeIds = state.payload.graph.nodes
    .filter((node) => node.meta.query === queryName)
    .map((node) => node.id);
  const edgeIds = state.payload.graph.edges
    .filter((edge) => nodeIds.includes(edge.source) || nodeIds.includes(edge.target))
    .map((edge) => edge.id);
  return {
    id: `semantic:mqo:query:${queryName}`,
    label: queryName,
    nodeIds,
    edgeIds,
  };
}

function buildIndexTableFilter(tableName) {
  const nodeIds = state.payload.graph.nodes
    .filter((node) => node.meta.table === tableName)
    .map((node) => node.id);
  const edgeIds = state.payload.graph.edges
    .filter((edge) => nodeIds.includes(edge.source) || nodeIds.includes(edge.target))
    .map((edge) => edge.id);
  return {
    id: `semantic:index:table:${tableName}`,
    label: tableName,
    nodeIds,
    edgeIds,
  };
}

function buildIndexStorageFilter() {
  const nodeIds = state.payload.graph.nodes
    .filter((node) => node.meta.kind === "storage_fraction")
    .map((node) => node.id);
  const edgeIds = state.payload.graph.edges
    .filter((edge) => nodeIds.includes(edge.source) || nodeIds.includes(edge.target))
    .map((edge) => edge.id);
  return {
    id: "semantic:index:storage",
    label: "storage",
    nodeIds,
    edgeIds,
  };
}

function activeJoinOrderSemanticFilter() {
  const filter = state.activeConstructionFilter;
  if (!filter || !filter.id || !filter.id.startsWith("semantic:")) return null;
  const [, type, name] = filter.id.split(":");
  return { type, name };
}

function activeMqoQueryFilter() {
  const filter = state.activeConstructionFilter;
  if (!filter || !filter.id || !filter.id.startsWith("semantic:mqo:query:")) return null;
  return filter.id.split(":")[3];
}

function activeIndexTableFilter() {
  const filter = state.activeConstructionFilter;
  if (!filter || !filter.id || !filter.id.startsWith("semantic:index:")) return null;
  const parts = filter.id.split(":");
  if (parts[2] === "storage") return "_storage_";
  return parts[3];
}

function bindMapVariableHover() {
  el.dbStructure.querySelectorAll("[data-map-variable]").forEach((button) => {
    const focusVariable = () => {
      state.activeVariable = button.dataset.mapVariable;
      renderDbStructure();
      renderLeft();
    };
    button.addEventListener("mouseenter", focusVariable);
    button.addEventListener("click", focusVariable);
  });
}

function renderPartitions() {
  el.partitionCards.innerHTML = state.payload.partitioning.partitions.map((partition) => {
    const summary = summarizePartitionCard(partition);
    return `
    <button type="button" class="partition-card${state.activePartition === partition.id ? " active" : ""}" data-partition-id="${partition.id}">
      <strong>Partition ${partition.id + 1}</strong>
      <div class="step-meta">Variables: ${partition.size}</div>
      <div class="step-meta">Variable Weight: ${format(summary.variableWeight)}</div>
      <div class="step-meta">${summary.label}</div>
      ${summary.detail ? `<div class="step-meta">${summary.detail}</div>` : ""}
    </button>
  `;
  }).join("");
  el.partitionCards.querySelectorAll("[data-partition-id]").forEach((button) => {
    button.addEventListener("click", () => {
      const partitionId = Number(button.dataset.partitionId);
      state.activePartition = state.activePartition === partitionId ? null : partitionId;
      renderPartitions();
      renderBoundaryCanvas();
      renderBoundaryGroups();
    });
  });
}

function summarizePartitionCard(partition) {
  const nodes = partition.nodes || [];
  const graphNodeMap = new Map((state.payload?.graph?.nodes || []).map((node) => [node.id, node]));
  const metas = nodes
    .map((name) => graphNodeMap.get(name))
    .filter(Boolean)
    .map((node) => node.meta);
  const variableWeight = Number.isFinite(partition.linear_weight_sum)
    ? partition.linear_weight_sum
    : nodes.reduce((total, name) => total + Math.abs(graphNodeMap.get(name)?.linear || 0), 0);
  const label = formatPartitionSummaryLabel(partition.summary_label || fallbackPartitionLabel(metas, state.payload.problem_id), metas, state.payload.problem_id);
  const detail = partition.summary_detail || fallbackPartitionDetail(metas, state.payload.problem_id);
  return {
    variableWeight,
    label: label || "QUBO variables",
    detail,
  };
}

function formatPartitionSummaryLabel(label, metas, problemId) {
  if (problemId !== "join_order" || !metas.length) return label;
  const steps = [...new Set(metas.map((meta) => meta.join_index).filter((value) => value !== undefined))].sort((a, b) => a - b);
  if (steps.length === 1) {
    return `${displayJoinStep(steps[0])} (${displayJoinTag(steps[0])})`;
  }
  const tags = steps.map((step) => displayJoinTag(step)).join(", ");
  return `${label} [${tags}]`;
}

function fallbackPartitionLabel(metas, problemId) {
  if (!metas.length) return "QUBO variables";
  if (problemId === "join_order") {
    const steps = [...new Set(metas.map((meta) => meta.join_index).filter((value) => value !== undefined))].sort((a, b) => a - b);
    if (steps.length === 1) return `${displayJoinStep(steps[0])} variables`;
    return `Join steps ${steps.map((step) => step + 1).join(", ")}`;
  }
  if (problemId === "mqo") {
    const queries = [...new Set(metas.map((meta) => meta.query).filter(Boolean))];
    if (queries.length === 1) return `Query ${queries[0]}`;
    return `${queries.length} query groups`;
  }
  if (problemId === "index_selection") {
    if (metas.every((meta) => meta.kind === "storage_fraction")) return "Storage variables";
    const tables = [...new Set(metas.map((meta) => meta.table).filter(Boolean))];
    if (tables.length === 1) return `${tables[0]} indexes`;
    return `${tables.length} tables`;
  }
  return "QUBO variables";
}

function fallbackPartitionDetail(metas, problemId) {
  if (!metas.length) return "";
  if (problemId === "join_order") {
    const relations = [...new Set(metas.filter((meta) => meta.kind === "relation_operand_for_join").map((meta) => meta.relation))].sort();
    const predicates = [...new Set(metas.filter((meta) => meta.kind === "predicate_applicable_for_join").map((meta) => meta.predicate))].sort();
    const relationText = relations.length ? `Relations: ${relations.slice(0, 4).join(", ")}${relations.length > 4 ? " ..." : ""}` : "";
    const predicateText = predicates.length ? `Predicates: ${predicates.slice(0, 4).join(", ")}${predicates.length > 4 ? " ..." : ""}` : "";
    return [relationText, predicateText].filter(Boolean).join(" | ");
  }
  if (problemId === "mqo") {
    const plans = [...new Set(metas.map((meta) => meta.plan).filter(Boolean))].sort();
    return plans.slice(0, 4).join(", ") + (plans.length > 4 ? " ..." : "");
  }
  if (problemId === "index_selection") {
    if (metas.every((meta) => meta.kind === "storage_fraction")) {
      const fractions = [...new Set(metas.map((meta) => meta.fraction).filter((value) => value !== undefined))].sort((a, b) => a - b);
      return fractions.map((fraction) => `S${fraction}`).join(", ");
    }
    const labels = [...new Set(metas.map((meta) => {
      if (!meta.index) return null;
      if (meta.index.includes("_main")) return "main";
      if (meta.index.includes("_clustered_a")) return "clustered A";
      if (meta.index.includes("_clustered_b")) return "clustered B";
      return meta.index;
    }).filter(Boolean))];
    return labels.slice(0, 4).join(", ") + (labels.length > 4 ? " ..." : "");
  }
  return "";
}

function renderBoundaryCanvas() {
  el.boundaryCanvas.innerHTML = "";
  const width = el.boundaryCanvas.clientWidth;
  const height = el.boundaryCanvas.clientHeight;
  const svg = makeSvg(width, height);
  const { nodes, edges } = state.payload.graph;
  const partitionMap = state.payload.partitioning.node_to_partition;
  const positions = radialLayout(nodes, width, height);

  edges.forEach((edge) => {
    const source = positions[edge.source];
    const target = positions[edge.target];
    const boundary = partitionMap[edge.source] !== partitionMap[edge.target];
    const partitionActive = matchesActivePartitionEdge(edge, partitionMap);
    const boundaryFocused = isEdgeActive(edge);
    const shape = document.createElementNS("http://www.w3.org/2000/svg", "line");
    shape.setAttribute("x1", source.x);
    shape.setAttribute("y1", source.y);
    shape.setAttribute("x2", target.x);
    shape.setAttribute("y2", target.y);
    shape.setAttribute("stroke", boundaryFocused ? "#9b2226" : boundary ? "#c97b63" : "#bcc5c8");
    shape.setAttribute("stroke-opacity",
      boundaryFocused ? "0.98" :
      partitionActive ? (boundary ? "0.78" : "0.22") :
      boundary ? "0.36" : "0.08"
    );
    shape.setAttribute("stroke-width",
      boundaryFocused ? String(Math.max(2.6, Math.min(5.2, Math.abs(edge.value) / 10))) :
      partitionActive ? (boundary ? String(Math.max(1.8, Math.min(3.4, Math.abs(edge.value) / 14))) : "1.1") :
      boundary ? String(Math.max(1.0, Math.min(2.2, Math.abs(edge.value) / 18))) : "0.9"
    );
    shape.addEventListener("mouseenter", () => focusBoundaryFromEdge(edge));
    svg.appendChild(shape);
  });

  nodes.forEach((node) => {
    const pos = positions[node.id];
    const partition = partitionMap[node.id];
    const color = palette[partition % palette.length];
    const active = matchesActiveBoundaryNode(node.id);
    const partitionActive = state.activePartition === null || partition === state.activePartition;
    const shape = document.createElementNS("http://www.w3.org/2000/svg", node.meta.kind === "predicate_applicable_for_join" ? "rect" : "circle");
    if (shape.tagName === "circle") {
      shape.setAttribute("cx", pos.x);
      shape.setAttribute("cy", pos.y);
      shape.setAttribute("r", active ? "12.5" : "10");
    } else {
      shape.setAttribute("x", pos.x - 10);
      shape.setAttribute("y", pos.y - 10);
      shape.setAttribute("width", active ? "24" : "20");
      shape.setAttribute("height", active ? "24" : "20");
      shape.setAttribute("rx", "5");
    }
    shape.setAttribute("fill", color);
    shape.setAttribute("fill-opacity", partitionActive ? "1" : "0.18");
    shape.setAttribute("stroke", active ? "#111" : "rgba(0,0,0,0.14)");
    shape.setAttribute("stroke-width", active ? "2.8" : "1");
    svg.appendChild(shape);
  });

  el.boundaryCanvas.appendChild(svg);
  el.boundaryCanvas.appendChild(makeBoundaryLegend());
}

function renderBoundaryGroups() {
  const groups = state.payload.partitioning.boundary_groups || [];
  el.boundaryGroups.innerHTML = "";
  groups
    .filter((group) => state.activePartition === null || group.pair.includes(state.activePartition))
    .forEach((group) => {
    const strongest = group.strongest_edge;
    const card = document.createElement("button");
    card.type = "button";
    const isActive = state.activeBoundary && strongest &&
      strongest.source === state.activeBoundary.source &&
      strongest.target === state.activeBoundary.target;
    card.className = `boundary-card${isActive ? " active" : ""}`;
    card.innerHTML = `
      <strong>Boundary P${group.pair[0] + 1} <-> P${group.pair[1] + 1}</strong>
      <div class="step-meta">Edges: ${group.edge_count} | Coupling Strength: ${formatCompact(group.weight_sum)}</div>
      <div class="step-meta">Types: ${Object.entries(group.type_counts).map(([k, v]) => `${friendlyBoundaryTypeName(k)} ${v}`).join(" | ")}</div>
      <div class="step-meta">Strongest interaction: ${strongest ? `${formatBoundaryNode(strongest.source)} <-> ${formatBoundaryNode(strongest.target)}` : "-"}</div>
    `;
    card.addEventListener("mouseenter", () => {
      if (strongest) {
        state.activeBoundary = strongest;
        renderBoundaryGroups();
        renderBoundaryCanvas();
      }
    });
    el.boundaryGroups.appendChild(card);
  });
}

function renderMergeTree() {
  const svg = el.mergeTree;
  svg.innerHTML = "";
  const steps = state.payload.merge_plan.steps || [];
  const partitions = state.payload.partitioning.partitions || [];
  const width = 520;
  const height = 280;
  const paddingX = 44;
  const leafY = height - 34;
  const innerStepGap = partitions.length > 1 ? 170 / partitions.length : 80;

  const clusters = new Map();
  const changedSteps = changedMergeSteps();
  partitions.forEach((partition, index) => {
    const x = paddingX + (index * (width - paddingX * 2)) / Math.max(1, partitions.length - 1);
    const key = clusterKey([partition.id]);
    clusters.set(key, { key, ids: [partition.id], x, y: leafY, label: `P${partition.id + 1}` });
  });

  steps.forEach((step, index) => {
    const clusterIds = [...step.cluster].sort((a, b) => a - b);
    const pair = resolveMergeChildren(clusterIds, clusters);
    if (!pair) return;
    const [leftChild, rightChild] = pair;
    const y = leafY - (index + 1) * innerStepGap;
    const x = (leftChild.x + rightChild.x) / 2;

    drawTreeLink(svg, leftChild.x, leftChild.y, x, y);
    drawTreeLink(svg, rightChild.x, rightChild.y, x, y);
    drawTreeNode(svg, x, y, `${step.step}`, index === steps.length - 1, changedSteps.has(index));
    clusters.set(clusterKey(clusterIds), { key: clusterKey(clusterIds), ids: clusterIds, x, y, label: `${step.step}` });
  });

  partitions.forEach((partition, index) => {
    const x = paddingX + (index * (width - paddingX * 2)) / Math.max(1, partitions.length - 1);
    drawTreeLeaf(svg, x, leafY, `P${partition.id + 1}`);
  });

  const orderLabel = friendlyMergeOrderName(selectedMergeOrder());
  const plannerLabel = friendlyPlannerModeName(state.payload.merge_plan?.planner_mode || selectedPlannerMode());
  const changedCount = changedSteps.size;
  const summary = state.fusionResult?.supported
    ? `${plannerLabel} ${orderLabel.toLowerCase()} tree executed with the selected D-Wave fusion strategy.`
    : plannerLabel === "Cost-Based"
      ? (changedCount
        ? `${plannerLabel} ${orderLabel.toLowerCase()} tree with ${changedCount} changed merge step${changedCount === 1 ? "" : "s"}.`
        : `${plannerLabel} ${orderLabel.toLowerCase()} tree kept the same merge structure.`)
      : `${plannerLabel} ${orderLabel.toLowerCase()} tree with ${steps.length} merge steps.`;
  const note = document.getElementById("mergeTreeNote");
  if (note) note.textContent = summary;
}

function changedMergeSteps() {
  const changed = new Set();
  const currentSteps = state.payload?.merge_plan?.steps || [];
  const defaultSteps = state.defaultMergePlan?.steps || [];
  currentSteps.forEach((step, index) => {
    const currentKey = JSON.stringify(step.cluster || []);
    const defaultKey = JSON.stringify(defaultSteps[index]?.cluster || []);
    if (currentKey !== defaultKey) changed.add(index);
  });
  return changed;
}

function joinOrderLayout(nodes, width, height) {
  const map = {};
  const joinIndices = [...new Set(nodes.map((node) => node.meta.join_index))].sort((a, b) => a - b);
  const columnGap = (width - 180) / Math.max(1, joinIndices.length - 1 || 1);
  joinIndices.forEach((joinIndex, idx) => {
    const x = 90 + idx * columnGap;
    map[`column::${joinIndex}`] = x;
    const relationNodes = nodes
      .filter((node) => node.meta.join_index === joinIndex && node.meta.kind === "relation_operand_for_join")
      .sort((a, b) => a.meta.relation.localeCompare(b.meta.relation));
    const predicateNodes = nodes
      .filter((node) => node.meta.join_index === joinIndex && node.meta.kind === "predicate_applicable_for_join")
      .sort((a, b) => a.meta.predicate.localeCompare(b.meta.predicate));

    relationNodes.forEach((node, relIdx) => {
      map[node.id] = {
        x,
        y: 90 + relIdx * ((height * 0.38) / Math.max(1, relationNodes.length - 1 || 1)),
      };
    });
    predicateNodes.forEach((node, predIdx) => {
      map[node.id] = {
        x,
        y: height * 0.62 + predIdx * ((height * 0.22) / Math.max(1, predicateNodes.length - 1 || 1)),
      };
    });
  });
  return map;
}

function mqoLayout(nodes, width, height) {
  const map = {};
  const queries = [...new Set(nodes.map((node) => node.meta.query).filter(Boolean))].sort();
  const columnGap = (width - 180) / Math.max(1, queries.length - 1 || 1);
  queries.forEach((query, idx) => {
    const x = 90 + idx * columnGap;
    map[`column::${query}`] = x;
    const queryNodes = nodes
      .filter((node) => node.meta.query === query)
      .sort((a, b) => a.meta.plan.localeCompare(b.meta.plan));
    queryNodes.forEach((node, nodeIdx) => {
      map[node.id] = {
        x,
        y: 110 + nodeIdx * ((height - 180) / Math.max(1, queryNodes.length - 1 || 1)),
      };
    });
  });
  return map;
}

function indexSelectionLayout(nodes, width, height) {
  const map = {};
  const tableNames = [...new Set(nodes.map((node) => node.meta.table).filter(Boolean))].sort();
  const storageNodes = nodes
    .filter((node) => node.meta.kind === "storage_fraction")
    .sort((a, b) => (a.meta.fraction || 0) - (b.meta.fraction || 0));
  const indexTables = tableNames.filter((name) => name !== "_storage_");
  const tableStartRatio = 0.12;
  const tableEndRatio = storageNodes.length ? 0.70 : 0.88;

  indexTables.forEach((table, idx) => {
    const ratio = indexTables.length === 1
      ? (tableStartRatio + tableEndRatio) / 2
      : tableStartRatio + idx * ((tableEndRatio - tableStartRatio) / (indexTables.length - 1));
    const x = width * ratio;
    map[`column::${table}`] = x;
    const tableNodes = nodes
      .filter((node) => node.meta.table === table)
      .sort((left, right) => {
        const leftRank = left.meta.clustered ? 1 : 0;
        const rightRank = right.meta.clustered ? 1 : 0;
        if (leftRank !== rightRank) return leftRank - rightRank;
        return (left.meta.index || "").localeCompare(right.meta.index || "");
      });
    const topY = 104;
    const bottomY = height - 92;
    const rowGap = (bottomY - topY) / Math.max(1, tableNodes.length - 1 || 1);
    tableNodes.forEach((node, nodeIdx) => {
      map[node.id] = {
        x,
        y: topY + nodeIdx * rowGap,
      };
    });
  });

  const storageX = width * 0.9;
  map["column::_storage_"] = storageX;
  const storageTopY = 118;
  const storageBottomY = height - 104;
  const storageGap = (storageBottomY - storageTopY) / Math.max(1, storageNodes.length - 1 || 1);
  storageNodes.forEach((node, idx) => {
    map[node.id] = {
      x: storageX,
      y: storageTopY + idx * storageGap,
    };
  });
  return map;
}

function indexSelectionNodeLabelParts(node) {
  if (node.meta.kind === "storage_fraction") {
    return [`S${node.meta.fraction}`];
  }
  const table = (node.meta.table || "").slice(0, 3);
  const indexName = node.meta.index || "";
  let suffix = "idx";
  if (indexName.includes("_main")) suffix = "main";
  else if (indexName.includes("_clustered_a")) suffix = "cA";
  else if (indexName.includes("_clustered_b")) suffix = "cB";
  return [table, suffix];
}

function toggleConstructionFilter(filter) {
  if (state.activeConstructionFilter && state.activeConstructionFilter.id === filter.id) {
    state.activeConstructionFilter = null;
  } else {
    state.activeConstructionFilter = filter;
  }
  render();
}

function isFilterActive(id) {
  return state.activeConstructionFilter && state.activeConstructionFilter.id === id;
}

function mapBlockVariablesToKinds(variables) {
  const mapping = {
    roj: "relation_operand_for_join",
    paj: "predicate_applicable_for_join",
    plan: "plan_selection",
    index: "index_selection",
    cap: "storage_fraction",
  };
  return variables.map((name) => mapping[name]).filter(Boolean);
}

function matchesConstructionFilter(node) {
  if (!state.activeConstructionFilter) return true;
  return matchesConstructionFilterByMeta(node.meta);
}

function matchesConstructionFilterById(nodeId) {
  const node = state.payload.graph.nodes.find((entry) => entry.id === nodeId);
  return matchesConstructionFilterByMeta(node.meta);
}

function matchesConstructionFilterByMeta(meta) {
  if (!state.activeConstructionFilter) return true;
  const filter = state.activeConstructionFilter;
  if (filter.nodeIds && filter.nodeIds.length) {
    return filter.nodeIds.includes(meta.name);
  }
  if (filter.kinds && filter.kinds.includes(meta.kind)) return true;
  if (filter.dbTypes && filter.dbTypes.includes(meta.db_element.type)) return true;
  return false;
}

function matchesConstructionFilterEdge(edge) {
  if (!state.activeConstructionFilter) return true;
  const filter = state.activeConstructionFilter;
  if (filter.edgeIds && filter.edgeIds.length) {
    return filter.edgeIds.includes(edge.id);
  }
  if (filter.nodeIds && filter.nodeIds.length) {
    return filter.nodeIds.includes(edge.source) || filter.nodeIds.includes(edge.target);
  }
  return matchesConstructionFilterById(edge.source) || matchesConstructionFilterById(edge.target);
}

function friendlyKindName(problem, kind) {
  const maps = {
    join_order: {
      relation_operand_for_join: "Relation",
      predicate_applicable_for_join: "Predicate",
    },
    mqo: {
      plan_selection: "Plan",
    },
    index_selection: {
      index_selection: "Index",
      storage_fraction: "Storage",
    },
  };
  return maps[problem]?.[kind] || pretty(kind);
}

function familyOrder(problem, kind) {
  const orders = {
    join_order: {
      relation_operand_for_join: 0,
      predicate_applicable_for_join: 1,
    },
    mqo: {
      plan_selection: 0,
    },
    index_selection: {
      index_selection: 0,
      storage_fraction: 1,
    },
  };
  return orders[problem]?.[kind] ?? 99;
}

function renderMetrics() {
  const metrics = state.payload.metrics;
  const fusion = state.fusionResult;
  const cards = [
    ["Partitions", metrics.partition_count],
    ["Boundary Size", metrics.boundary_size],
    ["Boundary Weight", formatCompact(metrics.boundary_weight_sum)],
    ["Final Energy", fusion?.supported ? formatCompact(fusion.energy) : "--"],
    ["Conflicts", fusion?.supported ? fusion.conflict_count : "--"],
    ["Conflict Weight", fusion?.supported ? formatCompact(fusion.conflict_weight) : "--"],
    ["Merge Depth", metrics.merge_depth],
    ["Sample Time (ms)", fusion?.supported ? formatCompact(fusion.sample_ms) : "--"],
    ["Fusion Time (ms)", fusion?.supported ? formatCompact(fusion.fusion_ms) : "--"],
    ["Runtime (ms)", fusion?.supported ? formatCompact(fusion.total_runtime_ms) : "--"],
  ];
  el.metricsGrid.innerHTML = cards.map(([label, value]) => `
    <div class="metric-card"><span>${label}</span><strong>${value}</strong></div>
  `).join("");
}

function renderStrategyNote() {
  const selectedStrategy = el.mergeStrategySelect.value || "top2_merge";
  const notes = {
    direct_fusion: {
      text: "Directly combine variables from both side after sampling.",
    },
    top2_merge: {
      text: "Use the top-2 candidate pairs from the two sides and keep the merged assignment with the lowest energy.",
    },
    conditioned_fusion: {
      text: "Fixed the sampling decision for the left side, then conditioning the decision for the boundary variables for the right side.",
    },
  };
  const note = notes[selectedStrategy] || { text: "" };
  const fusion = state.fusionResult;
  let detail = "";
  if (fusion) {
    detail = fusion.supported
      ? `${fusion.message} Assignment: ${fusion.assignment_size} variables.`
      : fusion.message;
  }
  el.strategyNote.innerHTML = `
    <div class="step-meta">${note.text}</div>
    ${detail ? `<div class="step-meta">${detail}</div>` : ""}
  `;
}

function renderMergeSteps() {
  const fusion = state.fusionResult;
  const changedSteps = changedMergeSteps();
  el.mergeSteps.innerHTML = "";
  if (fusion?.supported && Array.isArray(fusion.execution_steps) && fusion.execution_steps.length) {
    fusion.execution_steps.forEach((step, index) => {
      const card = document.createElement("div");
      card.className = `step-card${step.type === "result" ? " active" : ""}`;
      const extra = [];
      if (step.type === "result") {
        extra.push(`Final energy ${formatCompact(step.energy)}`);
        extra.push(`Conflicts ${step.conflicts}`);
      }
      card.innerHTML = `
        <h4>Step ${index + 1}: ${step.label}</h4>
        <div class="step-meta">${friendlyExecutionStepType(step.type)} | Runtime ${formatCompact(step.runtime_ms)}ms</div>
        ${extra.length ? `<div class="step-meta">${extra.join(" | ")}</div>` : ""}
      `;
      el.mergeSteps.appendChild(card);
    });
    return;
  }

  const steps = state.payload.merge_plan.steps;
  steps.forEach((step, index) => {
    const card = document.createElement("div");
    const plannerChanged = changedSteps.has(index);
    card.className = `step-card${index === steps.length - 1 ? " active" : ""}${plannerChanged ? " active" : ""}`;
    card.innerHTML = `
      <h4>Step ${step.step}: merge [${step.cluster.map((value) => value + 1).join(", ")}]</h4>
      <div class="step-meta">Planned merge scope ${step.scope_size}</div>
      ${plannerChanged ? `<div class="step-meta">Planner changed this merge step.</div>` : ""}
    `;
    el.mergeSteps.appendChild(card);
  });
}

function friendlyExecutionStepType(type) {
  const labels = {
    sampling: "Actual sampling",
    fusion: "Actual fusion",
    result: "Merged result",
  };
  return labels[type] || pretty(type);
}

function renderFusionCharts() {
  const fusion = state.fusionResult;
  if (fusion?.supported) {
    renderRuntimeBreakdownChart(el.energyChart, fusion);
    renderResultSummaryChart(el.conflictChart, fusion);
    return;
  }
  el.energyChart.innerHTML = "";
  el.conflictChart.innerHTML = "";
}

function renderRuntimeBreakdownChart(svg, fusion) {
  const items = [
    { label: "Sampling", value: fusion.sample_ms, color: "#005f73" },
    { label: "Fusion", value: fusion.fusion_ms, color: "#ca6702" },
    { label: "Total", value: fusion.total_runtime_ms, color: "#0a9396" },
  ];
  renderMetricBars(svg, items, "Runtime (ms)");
}

function renderResultSummaryChart(svg, fusion) {
  const items = [
    { label: "Energy", value: fusion.energy, color: "#ae2012" },
    { label: "Conflicts", value: fusion.conflict_count, color: "#bb3e03" },
    { label: "Weight", value: fusion.conflict_weight, color: "#7f5539" },
  ];
  renderMetricBars(svg, items, "Final Result");
}

function renderMetricBars(svg, items, title) {
  svg.innerHTML = "";
  const width = 320;
  const height = 170;
  const left = 34;
  const right = 18;
  const top = 30;
  const bottom = 28;
  const chartWidth = width - left - right;
  const chartHeight = height - top - bottom;
  const max = Math.max(...items.map((item) => Number(item.value) || 0), 1);
  const barWidth = Math.min(52, chartWidth / Math.max(items.length * 1.8, 1));
  const gap = items.length > 1 ? (chartWidth - items.length * barWidth) / (items.length - 1) : 0;

  const baseline = document.createElementNS("http://www.w3.org/2000/svg", "line");
  baseline.setAttribute("x1", left);
  baseline.setAttribute("y1", height - bottom);
  baseline.setAttribute("x2", width - right);
  baseline.setAttribute("y2", height - bottom);
  baseline.setAttribute("stroke", "rgba(0,0,0,0.18)");
  svg.appendChild(baseline);

  items.forEach((item, index) => {
    const value = Number(item.value) || 0;
    const barHeight = (value / max) * chartHeight;
    const x = left + index * (barWidth + gap);
    const y = height - bottom - barHeight;

    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("x", x);
    rect.setAttribute("y", y);
    rect.setAttribute("width", barWidth);
    rect.setAttribute("height", Math.max(barHeight, 1));
    rect.setAttribute("rx", "6");
    rect.setAttribute("fill", item.color);
    rect.setAttribute("fill-opacity", "0.88");
    svg.appendChild(rect);

    const valueLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
    valueLabel.setAttribute("x", x + barWidth / 2);
    valueLabel.setAttribute("y", Math.max(y - 8, top + 8));
    valueLabel.setAttribute("text-anchor", "middle");
    valueLabel.setAttribute("font-size", "10");
    valueLabel.setAttribute("font-family", "Trebuchet MS, sans-serif");
    valueLabel.textContent = formatCompactMetric(value);
    svg.appendChild(valueLabel);

    const nameLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
    nameLabel.setAttribute("x", x + barWidth / 2);
    nameLabel.setAttribute("y", height - 10);
    nameLabel.setAttribute("text-anchor", "middle");
    nameLabel.setAttribute("font-size", "10");
    nameLabel.setAttribute("font-family", "Trebuchet MS, sans-serif");
    nameLabel.textContent = item.label;
    svg.appendChild(nameLabel);
  });

  const chartTitle = document.createElementNS("http://www.w3.org/2000/svg", "text");
  chartTitle.setAttribute("x", 16);
  chartTitle.setAttribute("y", 18);
  chartTitle.setAttribute("font-size", "12");
  chartTitle.setAttribute("font-family", "Trebuchet MS, sans-serif");
  chartTitle.textContent = title;
  svg.appendChild(chartTitle);
}

function formatCompactMetric(value) {
  const abs = Math.abs(value);
  if (abs >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
  if (abs >= 1000) return `${(value / 1000).toFixed(1)}k`;
  if (abs >= 100) return `${Math.round(value)}`;
  return formatCompact(value);
}

function radialLayout(nodes, width, height) {
  const map = {};
  const cx = width / 2;
  const cy = height / 2;
  const radius = Math.min(width, height) * 0.33;
  nodes.forEach((node, index) => {
    const angle = (Math.PI * 2 * index) / Math.max(1, nodes.length);
    map[node.id] = { x: cx + Math.cos(angle) * radius, y: cy + Math.sin(angle) * radius };
  });
  return map;
}

function focusBoundaryFromEdge(edge) {
  const partitionMap = state.payload.partitioning.node_to_partition;
  state.activeBoundary = {
    source: edge.source,
    target: edge.target,
    value: edge.value,
    left_partition: partitionMap[edge.source],
    right_partition: partitionMap[edge.target],
    abs_value: Math.abs(edge.value),
    type: inferBoundaryType(edge),
  };
  renderBoundaryGroups();
}

function matchesActiveBoundaryNode(nodeId) {
  const boundary = state.activeBoundary;
  return !!boundary && (boundary.source === nodeId || boundary.target === nodeId);
}

function matchesActivePartitionEdge(edge, partitionMap) {
  if (state.activePartition === null) return false;
  return partitionMap[edge.source] === state.activePartition || partitionMap[edge.target] === state.activePartition;
}

function inferBoundaryType(edge) {
  const nodeMap = Object.fromEntries(state.payload.graph.nodes.map((node) => [node.id, node]));
  const leftKind = nodeMap[edge.source].meta.kind;
  const rightKind = nodeMap[edge.target].meta.kind;
  return [leftKind, rightKind].sort().join("-");
}

function formatBoundaryNode(nodeId) {
  const node = state.payload.graph.nodes.find((entry) => entry.id === nodeId);
  if (!node) return shortName(nodeId);
  const meta = node.meta;
  if (meta.kind === "relation_operand_for_join") {
    return `${meta.relation} @ Step ${meta.join_index + 1}`;
  }
  if (meta.kind === "predicate_applicable_for_join") {
    return `${meta.predicate} @ Step ${meta.join_index + 1}`;
  }
  if (meta.kind === "plan_selection") {
    return `${meta.plan}`;
  }
  if (meta.kind === "index_selection") {
    return `${meta.index}`;
  }
  if (meta.kind === "storage_fraction") {
    return `cap ${meta.fraction}`;
  }
  return shortName(nodeId);
}

function friendlyBoundaryTypeName(type) {
  const labels = {
    "prefix-propagation": "Relation Carry-Forward",
    "predicate-to-relation": "Predicate Applicability",
    "plan-sharing": "Shared Savings",
    "index-to-storage": "Storage Constraint",
    "index-conflict": "Index Conflict",
  };
  return labels[type] || pretty(type);
}

function isEdgeActive(edge) {
  const activeBoundary = state.activeBoundary;
  if (!activeBoundary) return false;
  return (
    (edge.source === activeBoundary.source && edge.target === activeBoundary.target) ||
    (edge.source === activeBoundary.target && edge.target === activeBoundary.source)
  );
}

function makeSvg(width, height) {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  return svg;
}

function makeLegend() {
  const wrapper = document.createElement("div");
  wrapper.className = "legend";
  if (state.payload?.problem_id === "join_order" && state.mode === "graph") {
    wrapper.innerHTML = `
      <div class="chip"><span class="swatch" style="background:#bb3e03"></span>Boundary edge</div>
      <div class="chip"><span class="swatch" style="background:#005f73"></span>Partition color</div>
      <div class="chip"><span class="shape-circle"></span>Relation node</div>
      <div class="chip"><span class="shape-rect"></span>Predicate node</div>
      <div class="chip"><span class="swatch" style="background:#111"></span>Current focus</div>
    `;
    return wrapper;
  }
  wrapper.innerHTML = `
    <div class="chip"><span class="swatch" style="background:#bb3e03"></span>Boundary edge</div>
    <div class="chip"><span class="swatch" style="background:#005f73"></span>Partition color</div>
    <div class="chip"><span class="swatch" style="background:#111"></span>Current focus</div>
  `;
  return wrapper;
}

function makeBoundaryLegend() {
  const wrapper = document.createElement("div");
  wrapper.className = "legend";
  wrapper.innerHTML = `
    <div class="chip"><span class="swatch" style="background:#bb3e03"></span>Cross-partition edge</div>
    <div class="chip"><span class="swatch" style="background:#a7b0b2"></span>Internal edge</div>
    <div class="chip"><span class="swatch" style="background:#111"></span>Focused boundary endpoint</div>
  `;
  return wrapper;
}

function clusterKey(ids) {
  return [...ids].sort((a, b) => a - b).join("-");
}

function resolveMergeChildren(clusterIds, clusters) {
  const candidates = [...clusters.values()]
    .filter((entry) => entry.ids.every((id) => clusterIds.includes(id)))
    .sort((left, right) => right.ids.length - left.ids.length);
  for (let i = 0; i < candidates.length; i += 1) {
    for (let j = i + 1; j < candidates.length; j += 1) {
      const left = candidates[i];
      const right = candidates[j];
      if (left.ids.some((id) => right.ids.includes(id))) continue;
      const union = [...left.ids, ...right.ids].sort((a, b) => a - b);
      if (clusterKey(union) === clusterKey(clusterIds)) return [left, right];
    }
  }
  return null;
}

function drawTreeLink(svg, childX, childY, parentX, parentY) {
  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.setAttribute("d", `M ${childX} ${childY} C ${childX} ${(childY + parentY) / 2}, ${parentX} ${(childY + parentY) / 2}, ${parentX} ${parentY}`);
  path.setAttribute("fill", "none");
  path.setAttribute("stroke", "#7f8c8d");
  path.setAttribute("stroke-width", "2");
  svg.appendChild(path);
}

function drawTreeNode(svg, x, y, label, active, changed = false) {
  const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
  circle.setAttribute("cx", x);
  circle.setAttribute("cy", y);
  circle.setAttribute("r", active ? "14" : "12");
  circle.setAttribute("fill", active ? "#005f73" : changed ? "#f6d8b8" : "#eff9f6");
  circle.setAttribute("stroke", changed ? "#ca6702" : "#005f73");
  circle.setAttribute("stroke-width", active ? "3" : "2");
  svg.appendChild(circle);

  const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
  text.setAttribute("x", x - 8);
  text.setAttribute("y", y + 4);
  text.setAttribute("font-size", "11");
  text.setAttribute("font-family", "Trebuchet MS, sans-serif");
  text.setAttribute("fill", active ? "#fff" : changed ? "#8a4f00" : "#005f73");
  text.textContent = label;
  svg.appendChild(text);
}

function drawTreeLeaf(svg, x, y, label) {
  const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  rect.setAttribute("x", x - 20);
  rect.setAttribute("y", y - 14);
  rect.setAttribute("width", "40");
  rect.setAttribute("height", "28");
  rect.setAttribute("rx", "8");
  rect.setAttribute("fill", "#fffdf8");
  rect.setAttribute("stroke", "#7f8c8d");
  rect.setAttribute("stroke-width", "1.5");
  svg.appendChild(rect);

  const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
  text.setAttribute("x", x - 8);
  text.setAttribute("y", y + 4);
  text.setAttribute("font-size", "12");
  text.setAttribute("font-family", "Trebuchet MS, sans-serif");
  text.textContent = label;
  svg.appendChild(text);
}

function shortName(name) { return name.replace(/^.*::/, "").slice(0, 18); }
function pretty(text) { return text.replaceAll("_", " ").replace(/\b\w/g, (char) => char.toUpperCase()); }
function friendlyMergeStrategyName(value) {
  const labels = {
    direct_fusion: "Direct Fusion",
    top2_merge: "Top-2 Merge",
    conditioned_fusion: "Conditioned Fusion",
  };
  return labels[value] || pretty(value);
}
function friendlyMergeOrderName(value) {
  const labels = {
    left_deep: "Left-Deep",
    bushy: "Bushy",
  };
  return labels[value] || pretty(value);
}
function friendlyPlannerModeName(value) {
  const labels = {
    default: "Default",
    cost_based: "Cost-Based",
  };
  return labels[value] || pretty(value);
}
function summarizeExtra(extra) {
  const priorityKeys = ["query", "plan", "table", "join_index", "relation", "predicate", "storage", "utility", "clustered"];
  return priorityKeys.filter((key) => extra[key] !== undefined).map((key) => `${key}: ${extra[key]}`).join(" | ");
}
function matrixColor(value) {
  if (value === 0) return "rgba(255,255,255,0.92)";
  const alpha = Math.min(0.85, 0.18 + Math.abs(value) / 90);
  return value > 0 ? `rgba(187, 62, 3, ${alpha})` : `rgba(0, 95, 115, ${alpha})`;
}
function format(value) { return Number(value).toFixed(2); }

function formatCompact(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  if (Number.isInteger(num)) return String(num);
  return num.toFixed(2).replace(/\.?0+$/, "");
}

bootstrap();
