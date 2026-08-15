(function () {
  let currentScenario = null;
  let currentSequence = 0;
  let maxSequence = 0;

  // DOM Elements
  const scenarioBtns = document.querySelectorAll(".btn-scenario");
  const summaryBanner = document.getElementById("summaryBanner");
  const scenarioStatusPill = document.getElementById("scenarioStatusPill");
  const summaryTitle = document.getElementById("summaryTitle");
  const summaryDesc = document.getElementById("summaryDesc");
  const summaryMeta = document.getElementById("summaryMeta");

  const eventCountBadge = document.getElementById("eventCountBadge");
  const timelineContainer = document.getElementById("timelineContainer");

  const sequenceSlider = document.getElementById("sequenceSlider");
  const seqIndicator = document.getElementById("seqIndicator");
  const sliderTicks = document.getElementById("sliderTicks");
  const btnPrevSeq = document.getElementById("btnPrevSeq");
  const btnNextSeq = document.getElementById("btnNextSeq");

  const dynamicContextBox = document.getElementById("dynamicContextBox");
  const liveStateViewer = document.getElementById("liveStateViewer");
  const stateDiffViewer = document.getElementById("stateDiffViewer");
  const materializedViewViewer = document.getElementById("materializedViewViewer");

  const invariantsList = document.getElementById("invariantsList");
  const diagCountBadge = document.getElementById("diagCountBadge");
  const diagnosticsList = document.getElementById("diagnosticsList");

  const storageBadge = document.getElementById("storageBadge");
  const sessionBadge = document.getElementById("sessionBadge");
  const storageAdapterVal = document.getElementById("storageAdapterVal");
  const storageDbFileVal = document.getElementById("storageDbFileVal");
  const aggregateIdVal = document.getElementById("aggregateIdVal");
  const commandIdVal = document.getElementById("commandIdVal");

  const tabBtns = document.querySelectorAll(".tab-btn");
  const tabContentLiveState = document.getElementById("tabContentLiveState");
  const tabContentStateDiff = document.getElementById("tabContentStateDiff");
  const tabContentMaterializedView = document.getElementById("tabContentMaterializedView");

  // Tab switching
  tabBtns.forEach((btn) => {
    btn.addEventListener("click", () => {
      tabBtns.forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");

      const tab = btn.dataset.tab;
      tabContentLiveState.style.display = tab === "liveState" ? "block" : "none";
      tabContentStateDiff.style.display = tab === "stateDiff" ? "block" : "none";
      tabContentMaterializedView.style.display = tab === "materializedView" ? "block" : "none";
    });
  });

  // Attach scenario buttons
  scenarioBtns.forEach((btn) => {
    btn.addEventListener("click", () => {
      const scenarioType = btn.dataset.scenario;
      let options = {};
      try {
        if (btn.dataset.options) {
          options = JSON.parse(btn.dataset.options);
        }
      } catch {
        options = {};
      }
      runScenario(scenarioType, options);
    });
  });

  async function runScenario(scenarioType, options = {}) {
    setLoadingState(true);

    try {
      const res = await fetch("/lab/api/scenarios/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ scenarioType, storageType: "sqlite", options }),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.message || `HTTP ${res.status}`);
      }

      const data = await res.json();
      currentScenario = data;
      maxSequence = data.events?.length || 0;
      currentSequence = maxSequence;

      renderScenarioResult(data);
    } catch (err) {
      alert(`Scenario execution error: ${err.message}`);
    } finally {
      setLoadingState(false);
    }
  }

  function setLoadingState(loading) {
    scenarioBtns.forEach((btn) => (btn.disabled = loading));
    if (loading) {
      summaryTitle.textContent = "Executing scenario on isolated SQLite database...";
      summaryDesc.textContent = "Engine is processing saga steps and writing to WAL log...";
    }
  }

  function renderScenarioResult(data) {
    // 1. Header Badges & Summary
    sessionBadge.textContent = `SESSION: ${data.scenarioId}`;
    storageBadge.textContent = `STORAGE: ${data.storage?.adapter === "sqlite" ? "SQLite WAL" : "In-Memory"}`;
    storageAdapterVal.textContent = data.storage?.adapter === "sqlite" ? "SQLite (WAL)" : "In-Memory";
    storageDbFileVal.textContent = data.storage?.databaseFile || "-";
    aggregateIdVal.textContent = data.aggregateId ?? "-";
    commandIdVal.textContent = data.commandId ?? "-";

    scenarioStatusPill.className = "scenario-status-pill";
    scenarioStatusPill.classList.add(`pill-${data.status}`);
    scenarioStatusPill.textContent = (data.status || "COMPLETED").toUpperCase().replace(/_/g, " ");

    summaryTitle.textContent = data.summary?.headline || data.scenarioType;
    summaryDesc.textContent = data.summary?.description || "";
    summaryMeta.textContent = `Events: ${data.events?.length || 0} | Aggregate: ${data.aggregateId ?? "None"}`;

    // 2. Timeline
    renderTimeline(data.events || []);

    // 3. Scrubber Setup
    setupScrubber(maxSequence);

    // 4. State Viewers
    liveStateViewer.textContent = JSON.stringify(data.finalState || { status: "empty" }, null, 2);
    materializedViewViewer.textContent = JSON.stringify(data.materializedState || { view: "unpopulated" }, null, 2);

    // Initial Diff from previous sequence
    if (maxSequence > 0) {
      fetchAndRenderSequenceState(maxSequence);
    } else {
      stateDiffViewer.innerHTML = `<div class="empty-placeholder">No events committed in this scenario.</div>`;
    }

    // 5. Invariants
    renderInvariants(data.invariants || {});

    // 6. Diagnostics
    renderDiagnostics(data.diagnostics || []);

    // 7. Dynamic Context Box
    renderDynamicContext(data);
  }

  function renderTimeline(events) {
    eventCountBadge.textContent = `${events.length} events`;
    timelineContainer.innerHTML = "";

    if (!events || events.length === 0) {
      timelineContainer.innerHTML = `<div class="empty-placeholder">No events in this stream.</div>`;
      return;
    }

    const compensationTypes = new Set([
      "PAYMENT_REFUNDED",
      "INVENTORY_RELEASED",
      "ORDER_ROLLED_BACK",
      "ORDER_DELETED",
    ]);

    events.forEach((evt) => {
      const isCompensation = compensationTypes.has(evt.eventType);
      const item = document.createElement("div");
      item.className = `timeline-item ${isCompensation ? "compensation" : "forward"}`;
      item.dataset.sequence = evt.sequence;

      if (evt.sequence === currentSequence) {
        item.classList.add("active");
      }

      item.innerHTML = `
        <div class="timeline-item-header">
          <span class="timeline-seq">#${evt.sequence}</span>
          <span class="timeline-type ${isCompensation ? "type-compensation" : "type-forward"}">${evt.eventType}</span>
        </div>
        <div class="timeline-meta-row">
          <span>Event ID:</span>
          <span>${evt.eventId?.slice(0, 16)}...</span>
        </div>
        <div class="timeline-meta-row">
          <span>Timestamp:</span>
          <span>${evt.timestamp ? new Date(evt.timestamp).toLocaleTimeString() : "-"}</span>
        </div>
        <details class="timeline-details">
          <summary style="cursor:pointer; color: var(--text-dim);">Inspect Payload &amp; Metadata</summary>
          <pre class="timeline-json">${JSON.stringify({ payload: evt.payload, metadata: evt.metadata }, null, 2)}</pre>
        </details>
      `;

      item.addEventListener("click", () => {
        setSequence(evt.sequence);
      });

      timelineContainer.appendChild(item);
    });
  }

  function setupScrubber(maxSeq) {
    sequenceSlider.min = "0";
    sequenceSlider.max = String(maxSeq);
    sequenceSlider.value = String(maxSeq);
    sequenceSlider.disabled = maxSeq === 0;

    btnPrevSeq.disabled = maxSeq === 0 || currentSequence === 0;
    btnNextSeq.disabled = maxSeq === 0 || currentSequence === maxSeq;

    seqIndicator.textContent = `Seq ${currentSequence} / ${maxSeq}`;

    // Render ticks
    sliderTicks.innerHTML = "";
    for (let i = 0; i <= maxSeq; i++) {
      const tick = document.createElement("span");
      tick.textContent = i;
      sliderTicks.appendChild(tick);
    }
  }

  sequenceSlider.addEventListener("input", (e) => {
    setSequence(Number(e.target.value));
  });

  btnPrevSeq.addEventListener("click", () => {
    if (currentSequence > 0) setSequence(currentSequence - 1);
  });

  btnNextSeq.addEventListener("click", () => {
    if (currentSequence < maxSequence) setSequence(currentSequence + 1);
  });

  function setSequence(seq) {
    currentSequence = seq;
    sequenceSlider.value = String(seq);
    seqIndicator.textContent = `Seq ${seq} / ${maxSequence}`;

    btnPrevSeq.disabled = seq === 0;
    btnNextSeq.disabled = seq === maxSequence;

    // Highlight timeline item
    document.querySelectorAll(".timeline-item").forEach((el) => {
      if (Number(el.dataset.sequence) === seq) {
        el.classList.add("active");
        el.scrollIntoView({ behavior: "smooth", block: "nearest" });
      } else {
        el.classList.remove("active");
      }
    });

    fetchAndRenderSequenceState(seq);
  }

  async function fetchAndRenderSequenceState(seq) {
    if (!currentScenario?.scenarioId) return;

    try {
      const res = await fetch(`/lab/api/scenarios/${currentScenario.scenarioId}/state/${seq}`);
      if (!res.ok) return;

      const data = await res.json();
      liveStateViewer.textContent = JSON.stringify(data.state || {}, null, 2);

      // Render Diff
      renderStateDiff(data.previousState, data.state);
    } catch {
      // Ignore sequence fetch errors
    }
  }

  function renderStateDiff(prev, curr) {
    if (!prev && !curr) {
      stateDiffViewer.innerHTML = `<div class="empty-placeholder">No state difference.</div>`;
      return;
    }

    const diffRows = computeJsonDiff(prev || {}, curr || {});
    if (diffRows.length === 0) {
      stateDiffViewer.innerHTML = `<div class="diff-row"><span class="diff-tag">UNCHANGED</span><span>No properties modified at this sequence.</span></div>`;
      return;
    }

    let html = "";
    diffRows.forEach((row) => {
      html += `
        <div class="diff-row diff-${row.type}">
          <span class="diff-tag">${row.type.toUpperCase()}</span>
          <span><strong>${row.path}:</strong> ${formatValue(row.oldVal)} ➔ ${formatValue(row.newVal)}</span>
        </div>
      `;
    });

    stateDiffViewer.innerHTML = html;
  }

  function formatValue(v) {
    if (v === undefined) return "<em>none</em>";
    if (typeof v === "object" && v !== null) return JSON.stringify(v);
    return String(v);
  }

  function computeJsonDiff(obj1, obj2, prefix = "") {
    const diffs = [];
    const allKeys = new Set([...Object.keys(obj1 || {}), ...Object.keys(obj2 || {})]);

    allKeys.forEach((key) => {
      const path = prefix ? `${prefix}.${key}` : key;
      const v1 = obj1 ? obj1[key] : undefined;
      const v2 = obj2 ? obj2[key] : undefined;

      if (v1 === undefined && v2 !== undefined) {
        diffs.push({ type: "added", path, oldVal: undefined, newVal: v2 });
      } else if (v1 !== undefined && v2 === undefined) {
        diffs.push({ type: "removed", path, oldVal: v1, newVal: undefined });
      } else if (typeof v1 === "object" && typeof v2 === "object" && v1 !== null && v2 !== null) {
        diffs.push(...computeJsonDiff(v1, v2, path));
      } else if (v1 !== v2) {
        diffs.push({ type: "changed", path, oldVal: v1, newVal: v2 });
      }
    });

    return diffs;
  }

  function renderInvariants(invariants) {
    invariantsList.innerHTML = "";
    const entries = Object.entries(invariants);

    if (entries.length === 0) {
      invariantsList.innerHTML = `<div class="empty-placeholder">No invariant calculations available.</div>`;
      return;
    }

    entries.forEach(([key, inv]) => {
      const item = document.createElement("div");
      item.className = "invariant-item";
      item.innerHTML = `
        <div class="invariant-header">
          <span class="invariant-label">${inv.label || key}</span>
          <span class="invariant-status ${inv.passed ? "inv-pass" : "inv-fail"}">${inv.passed ? "PASSED ✓" : "VIOLATED ✗"}</span>
        </div>
        <div class="invariant-desc">${inv.details || ""}</div>
      `;
      invariantsList.appendChild(item);
    });
  }

  function renderDiagnostics(diagnostics) {
    diagCountBadge.textContent = `${diagnostics.length} signals`;
    diagnosticsList.innerHTML = "";

    if (!diagnostics || diagnostics.length === 0) {
      diagnosticsList.innerHTML = `<div class="empty-placeholder">No diagnostic entries.</div>`;
      return;
    }

    diagnostics.slice(-10).reverse().forEach((d) => {
      const item = document.createElement("div");
      item.className = "diagnostic-item";
      item.innerHTML = `
        <div class="diagnostic-type">${d.type} [${d.status}]</div>
        <div class="diagnostic-msg">${d.details ? JSON.stringify(d.details) : `Aggregate ${d.aggregateId || "-"}`}</div>
      `;
      diagnosticsList.appendChild(item);
    });
  }

  function renderDynamicContext(data) {
    dynamicContextBox.style.display = "none";
    dynamicContextBox.innerHTML = "";

    // 1. Read Model Drift Context
    if (data.scenarioType === "read_model_drift") {
      dynamicContextBox.style.display = "block";
      if (data.drift?.detected) {
        dynamicContextBox.innerHTML = `
          <div class="alert-box alert-amber">
            <div class="alert-title">⚡ Logical Read-Model Drift Active</div>
            <div>The materialized cache table contains corrupted item <code>"${data.drift.materializedItem}"</code>, while the authoritative Event Store log retains <code>"${data.drift.authoritativeItem}"</code>.</div>
            <button class="alert-action-btn" id="btnRepairDrift">Run Authoritative Read &amp; Self-Heal</button>
          </div>
        `;
        document.getElementById("btnRepairDrift").addEventListener("click", triggerDriftRepair);
      } else if (data.drift?.repaired) {
        dynamicContextBox.innerHTML = `
          <div class="alert-box alert-emerald">
            <div class="alert-title">✓ Read Model Successfully Repaired</div>
            <div>Authoritative read detected divergence, replayed the authoritative event stream, and synchronized the materialized SQLite table with item <code>"${data.drift.materializedItem}"</code>.</div>
          </div>
        `;
      }
    }

    // 2. Restart Scenario Context
    if (data.scenarioType === "process_restart_durability" && data.restart) {
      dynamicContextBox.style.display = "block";
      dynamicContextBox.innerHTML = `
        <div class="alert-box alert-emerald">
          <div class="alert-title">✓ Process Restart Durability Verified Across OS Boundary</div>
          <div style="margin-top: 4px;">
            <strong>Process A:</strong> ${data.restart.processA.action} (Exit Code: ${data.restart.processA.exitCode})<br>
            <strong>Process B:</strong> ${data.restart.processB.action} (Exit Code: ${data.restart.processB.exitCode})<br>
            <strong>State Equality:</strong> Replayed state from Process B matches Process A perfectly.
          </div>
        </div>
      `;
    }

    // 3. Post-Commit Reconciliation Context
    if (data.scenarioType === "post_commit_reconciliation" && data.reconciliation) {
      dynamicContextBox.style.display = "block";
      dynamicContextBox.innerHTML = `
        <div class="alert-box alert-cyan">
          <div class="alert-title">🛡 Post-Commit Reconciliation Guard Verified</div>
          <div style="margin-top: 4px;">
            Crash occurred before completion ACK. On retry with same Idempotency-Key, existing committed event was found.
            <strong>Result:</strong> Callback was NOT re-executed. Zero duplicate events produced.
          </div>
        </div>
      `;
    }

    // 4. Boundary Scenario Context
    if (data.scenarioType === "processing_zero_events_boundary" && data.boundary) {
      dynamicContextBox.style.display = "block";
      dynamicContextBox.innerHTML = `
        <div class="alert-box alert-slate">
          <div class="alert-title">⚠ Known Reliability Boundary (processing + 0 events)</div>
          <div style="margin-top: 4px;">
            ${data.boundary.explanation}<br>
            <strong>Engine Response:</strong> <code>${data.boundary.errorCode}</code> (<code>${data.boundary.retryAction}</code>)
          </div>
        </div>
      `;
    }
  }

  async function triggerDriftRepair() {
    if (!currentScenario?.scenarioId) return;

    try {
      const res = await fetch(`/lab/api/scenarios/${currentScenario.scenarioId}/repair`, {
        method: "POST",
      });
      if (!res.ok) throw new Error("Repair request failed");

      const repairedData = await res.json();
      currentScenario = repairedData;
      renderScenarioResult(repairedData);
    } catch (err) {
      alert(`Repair error: ${err.message}`);
    }
  }
})();
