(() => {
  "use strict";

  const POLL_INTERVAL_MS = 5000;
  const state = {
    status: null,
    health: { livez: null, readyz: null },
    metrics: { available: false, names: [], series: null, error: null },
    lastUpdated: null,
    polling: true,
    refreshing: false,
    statusError: null,
    expandedPairs: new Set(),
    selectedMemoryKey: null,
  };

  const $ = (id) => document.getElementById(id);
  const numberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });

  function isNumber(value) {
    return value !== null && value !== undefined && value !== "" && Number.isFinite(Number(value));
  }

  function setText(id, value) {
    const element = $(id);
    if (element) element.textContent = value;
  }

  function formatNumber(value) {
    return isNumber(value) ? numberFormatter.format(Number(value)) : "—";
  }

  function formatBytes(bytes) {
    const value = Number(bytes);
    if (!isNumber(bytes)) return "—";
    if (value < 1024) return `${Math.round(value)} B`;
    const units = ["KB", "MB", "GB", "TB"];
    let amount = value;
    let unit = -1;
    while (amount >= 1024 && unit < units.length - 1) {
      amount /= 1024;
      unit += 1;
    }
    return `${amount.toFixed(amount >= 100 ? 0 : amount >= 10 ? 1 : 2)} ${units[unit]}`;
  }

  function formatDuration(seconds) {
    if (!isNumber(seconds)) return "—";
    const value = Number(seconds);
    const total = Math.max(0, Math.floor(value));
    const days = Math.floor(total / 86400);
    const hours = Math.floor((total % 86400) / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    const secs = total % 60;
    if (days) return `${days}d ${hours}h`;
    if (hours) return `${hours}h ${minutes}m`;
    if (minutes) return `${minutes}m ${secs}s`;
    return `${secs}s`;
  }

  function formatPercent(value) {
    if (!isNumber(value)) return "—";
    const number = Number(value);
    return `${number.toFixed(number >= 10 ? 0 : 1)}%`;
  }

  function formatAge(date) {
    if (!date) return "No sample yet";
    const age = Math.max(0, Math.round((Date.now() - date.getTime()) / 1000));
    if (age < 2) return "Updated just now";
    return `Updated ${age}s ago`;
  }

  async function request(url, options = {}) {
    const response = await fetch(url, { cache: "no-store", ...options });
    if (!response.ok) throw new Error(`${url} returned ${response.status}`);
    return response;
  }

  async function readStatus() {
    try {
      const response = await request("/api/v1/status");
      return { ok: true, value: await response.json() };
    } catch (error) {
      return { ok: false, error };
    }
  }

  async function readHealth(path) {
    try {
      const response = await request(path);
      return { ok: true, status: response.status };
    } catch (error) {
      return { ok: false, error };
    }
  }

  async function readMetrics() {
    try {
      const response = await request("/metrics");
      const text = await response.text();
      return { ok: true, value: parsePrometheus(text) };
    } catch (error) {
      return { ok: false, error };
    }
  }

  function parsePrometheus(text) {
    const names = [];
    const seen = new Set();
    let series = 0;
    const lines = String(text || "").split(/\r?\n/);
    for (const line of lines) {
      if (!line || line.startsWith("#")) continue;
      const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{[^}]*\})?\s+[-+]?\d/);
      if (!match) continue;
      series += 1;
      if (!seen.has(match[1])) {
        seen.add(match[1]);
        names.push(match[1]);
      }
    }
    return { available: true, names: names.slice(0, 18), series };
  }

  function normalizeStatus(value) {
    const status = value && typeof value === "object" ? value : {};
    return {
      version: status.version,
      ready: status.ready === true,
      uptime_seconds: status.uptime_seconds,
      resource_pairs: Array.isArray(status.resource_pairs) ? status.resource_pairs : [],
      totals: status.totals && typeof status.totals === "object" ? status.totals : {},
      process: status.process && typeof status.process === "object" ? status.process : {},
      scrape: status.scrape && typeof status.scrape === "object" ? status.scrape : {},
      crd_metric_cardinality: Array.isArray(status.crd_metric_cardinality) ? status.crd_metric_cardinality : [],
      counters: status.counters && typeof status.counters === "object" ? status.counters : {},
      remote_write: status.remote_write && typeof status.remote_write === "object" ? status.remote_write : {},
    };
  }

  function setHealth(id, result) {
    const dot = $(`${id}Dot`);
    const value = $(`${id}Health`);
    const row = value?.closest(".health-row");
    if (!dot || !value || !row) return;
    row.classList.toggle("is-ok", Boolean(result?.ok));
    row.classList.toggle("is-error", result ? !result.ok : false);
    value.textContent = result ? (result.ok ? "ok" : "unavailable") : "checking";
  }

  function renderConnection() {
    const element = $("connectionState");
    if (!element) return;
    const statusOk = Boolean(state.status);
    const readyzOk = Boolean(state.health.readyz?.ok);
    const livezOk = Boolean(state.health.livez?.ok);
    const ready = statusOk && state.status.ready && readyzOk;
    const degraded = (statusOk && !ready) || (livezOk && !readyzOk);
    element.className = `connection-state ${ready ? "connection-state-ready" : degraded ? "connection-state-degraded" : "connection-state-offline"}`;
    setText("connectionLabel", ready ? "Ready to collect" : degraded ? "Collection degraded" : "Agent unreachable");
    setText("connectionDetail", ready ? `v${state.status.version || "unknown"} · ${formatAge(state.lastUpdated).toLowerCase()}` : state.statusError ? "Check the agent process and API route" : "Waiting for health confirmation");
  }

  function renderInventory() {
    const status = state.status;
    const totals = status?.totals || {};
    const pairs = status?.resource_pairs || [];
    setText("crdObjects", formatNumber(totals.crd_objects));
    setText("resourcePairs", formatNumber(pairs.length));
    const series = isNumber(totals.metric_series) ? Number(totals.metric_series) : state.metrics.series;
    setText("metricSeries", formatNumber(series));
    setText("metricSeriesNote", isNumber(totals.metric_series) ? "in the last Rush payload" : state.metrics.series !== null ? "counted from /metrics" : "in the last Rush payload");
    const scrape = status?.scrape || {};
    setText("scrapeTargets", formatNumber(scrape.targets));
    setText("scrapeTargetsNote", isNumber(scrape.healthy_targets) ? `${formatNumber(scrape.healthy_targets)} healthy` : "waiting for discovery");
    setText("scrapeSamples", formatNumber(scrape.samples));
    setText("uptime", formatDuration(status?.uptime_seconds));
    setText("agentVersion", `version ${status?.version || "—"}`);
    setText("lastUpdated", formatAge(state.lastUpdated));
    setText("collectionCount", pairs.length ? `${pairs.length} pair${pairs.length === 1 ? "" : "s"}` : "0 pairs");
    setText("collectionSummary", status ? `${formatNumber(totals.crd_objects)} CRD object${Number(totals.crd_objects) === 1 ? "" : "s"} across ${pairs.length} active resource pair${pairs.length === 1 ? "" : "s"}.` : "Waiting for the first collection sample.");
  }

  function availabilityCell(available) {
    const span = document.createElement("span");
    span.className = `availability ${available ? "availability-yes" : "availability-no"}`;
    const dot = document.createElement("span");
    dot.className = "availability-dot";
    dot.setAttribute("aria-hidden", "true");
    span.append(dot, document.createTextNode(available ? "available" : "not seen"));
    return span;
  }

  function textCell(text, className = "") {
    const cell = document.createElement("td");
    if (className) cell.className = className;
    cell.textContent = text;
    return cell;
  }

  function objectList(title, objects, source, pairName) {
    const section = document.createElement("section");
    section.className = "crd-source-section";
    const heading = document.createElement("div");
    heading.className = "crd-source-heading";
    const label = document.createElement("strong");
    label.textContent = title;
    const count = document.createElement("span");
    const estimatedBytes = objects.reduce(
      (total, object) => total + (Number(object.estimated_memory_bytes) || 0),
      0,
    );
    count.textContent = `${objects.length} object${objects.length === 1 ? "" : "s"} · ${formatBytes(estimatedBytes)} estimated`;
    heading.append(label, count);
    section.append(heading);
    if (!objects.length) {
      const empty = document.createElement("p");
      empty.className = "crd-detail-empty";
      empty.textContent = `${source} has no objects in the active watch set.`;
      section.append(empty);
      return section;
    }
    const list = document.createElement("ul");
    list.className = "crd-object-list";
    for (const object of objects) {
      const item = document.createElement("li");
      item.className = "crd-object-item";
      item.tabIndex = 0;
      item.setAttribute("role", "button");
      const identity = document.createElement("code");
      identity.textContent = `${object.namespace || "cluster"}/${object.name}`;
      const meta = document.createElement("span");
      meta.className = "crd-object-meta";
      const state = object.converted ? "converted" : "native";
      meta.textContent = `${state} · ${formatBytes(object.estimated_memory_bytes)} est.`;
      item.append(identity, meta);
      list.append(item);
      const entry = {
        source,
        pair: pairName,
        namespace: object.namespace || "cluster",
        name: object.name || "unnamed",
        converted: object.converted === true,
        bytes: Number(object.estimated_memory_bytes) || 0,
        key: `${source.toLowerCase()}:${pairName}:${object.namespace || "cluster"}/${object.name || "unnamed"}`,
      };
      const select = () => openMetricDrawer(entry);
      item.addEventListener("click", select);
      item.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          select();
        }
      });
    }
    section.append(list);
    return section;
  }

  function pairDetailRow(pair) {
    const row = document.createElement("tr");
    row.className = "pair-detail-row";
    const cell = document.createElement("td");
    cell.colSpan = 5;
    const detail = document.createElement("div");
    detail.className = "pair-detail";
    const heading = document.createElement("div");
    heading.className = "pair-detail-heading";
    const title = document.createElement("strong");
    title.textContent = `${pair.name} objects`;
    const note = document.createElement("span");
    note.textContent = "Memory is an estimate of serialized watch-cache data per object, not an exact OS allocation. Target samples are collected from the discovered scrape configuration and delivered to Rush.";
    heading.append(title, note);
    detail.append(heading);
    detail.append(objectList("Prometheus CRDs", pair.prometheus_objects || [], "Prometheus", pair.name));
    detail.append(objectList("Victoria CRDs", pair.victoria_objects || [], "Victoria", pair.name));
    cell.append(detail);
    row.append(cell);
    return row;
  }

  function togglePair(pair, row) {
    const expanded = state.expandedPairs.has(pair.name);
    if (expanded) state.expandedPairs.delete(pair.name);
    else state.expandedPairs.add(pair.name);
    row.setAttribute("aria-expanded", String(!expanded));
    renderPairs();
  }

  function renderPairs() {
    const rows = $("pairRows");
    const empty = $("pairEmpty");
    if (!rows || !empty) return;
    rows.replaceChildren();
    const pairs = state.status?.resource_pairs || [];
    empty.hidden = pairs.length > 0;
    if (!pairs.length) {
      const row = document.createElement("tr");
      row.className = "table-state-row";
      const cell = textCell(state.status ? "No resource pairs reported" : "Waiting for collection data…");
      cell.colSpan = 5;
      row.append(cell);
      rows.append(row);
      return;
    }
    for (const pair of pairs) {
      const row = document.createElement("tr");
      row.className = "pair-row";
      row.tabIndex = 0;
      row.setAttribute("role", "button");
      row.setAttribute("aria-expanded", String(state.expandedPairs.has(pair.name)));
      row.setAttribute("aria-label", `Inspect ${pair.name} CRDs`);
      const name = document.createElement("td");
      const wrapper = document.createElement("div");
      wrapper.className = "pair-name";
      const title = document.createElement("strong");
      title.textContent = pair.name || "Unnamed resource pair";
      const subtitle = document.createElement("span");
      subtitle.textContent = `${formatNumber(pair.prometheus_count)} Prometheus · ${formatNumber(pair.victoria_count)} Victoria`;
      wrapper.append(title, subtitle);
      name.append(wrapper);
      row.append(name);
      for (const key of ["prometheus_available", "victoria_available"]) {
        const cell = document.createElement("td");
        cell.append(availabilityCell(pair[key] === true));
        row.append(cell);
      }
      row.append(textCell(formatNumber(pair.native_count), "count-cell"));
      row.append(textCell(formatNumber(pair.converted_count), "count-cell"));
      rows.append(row);
      row.addEventListener("click", () => togglePair(pair, row));
      row.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          togglePair(pair, row);
        }
      });
      if (state.expandedPairs.has(pair.name)) rows.append(pairDetailRow(pair));
    }
  }

  function renderMemoryConsumers() {
    const list = $("memoryList");
    if (!list) return;
    const entries = [];
    for (const pair of state.status?.resource_pairs || []) {
      for (const [source, objects] of [["Victoria", pair.victoria_objects || []], ["Prometheus", pair.prometheus_objects || []]]) {
        for (const object of objects) {
          entries.push({
            source,
            pair: pair.name,
            namespace: object.namespace || "cluster",
            name: object.name || "unnamed",
            converted: object.converted === true,
            bytes: Number(object.estimated_memory_bytes) || 0,
            key: `${source.toLowerCase()}:${pair.name}:${object.namespace || "cluster"}/${object.name || "unnamed"}`,
          });
        }
      }
    }
    entries.sort((left, right) => right.bytes - left.bytes || left.name.localeCompare(right.name));
    const top = entries.slice(0, 15);
    list.replaceChildren();
    if (!top.length) {
      const empty = document.createElement("div");
      empty.className = "memory-empty";
      empty.textContent = state.status ? "No CRD objects reported" : "Waiting for collection data…";
      list.append(empty);
      setText("memoryCount", "0 objects");
      return;
    }
    const maximum = top[0].bytes || 1;
    setText("memoryCount", `${top.length} of ${entries.length}`);
    for (const [index, entry] of top.entries()) {
      const row = document.createElement("div");
      row.className = "memory-row";
      row.classList.toggle("is-selected", state.selectedMemoryKey === entry.key);
      row.tabIndex = 0;
      row.setAttribute("role", "button");
      row.setAttribute("aria-label", `Inspect largest metric series for ${entry.name}`);
      const rank = document.createElement("span");
      rank.className = "memory-rank";
      rank.textContent = String(index + 1).padStart(2, "0");
      const identity = document.createElement("div");
      identity.className = "memory-identity";
      const title = document.createElement("strong");
      title.textContent = entry.name;
      const subtitle = document.createElement("span");
      subtitle.textContent = `${entry.namespace}/${entry.pair}${entry.converted ? " · converted" : ""}`;
      identity.append(title, subtitle);
      const source = document.createElement("span");
      source.className = `memory-source memory-source-${entry.source.toLowerCase()}`;
      source.textContent = entry.source;
      const value = document.createElement("strong");
      value.className = "memory-value";
      value.textContent = formatBytes(entry.bytes);
      const track = document.createElement("div");
      track.className = "memory-track";
      const fill = document.createElement("span");
      fill.style.width = `${Math.max(2, (entry.bytes / maximum) * 100)}%`;
      track.append(fill);
      row.append(rank, identity, source, value, track);
      list.append(row);
      const select = () => {
        openMetricDrawer(entry);
      };
      row.addEventListener("click", select);
      row.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          select();
        }
      });
    }
  }

  function openMetricDrawer(entry) {
    state.selectedMemoryKey = entry.key;
    renderMemoryConsumers();
    renderMetricDrawer(entry);
    const drawer = $("metricDrawer");
    if (!drawer) return;
    drawer.hidden = false;
    drawer.setAttribute("aria-hidden", "false");
    requestAnimationFrame(() => drawer.classList.add("is-open"));
    $("metricDrawerClose")?.focus();
  }

  function closeMetricDrawer() {
    const drawer = $("metricDrawer");
    if (!drawer) return;
    drawer.classList.remove("is-open");
    drawer.setAttribute("aria-hidden", "true");
    setTimeout(() => {
      if (!drawer.classList.contains("is-open")) drawer.hidden = true;
    }, 220);
    state.selectedMemoryKey = null;
    renderMemoryConsumers();
  }

  function renderMetricDrawer(entry) {
    const list = $("metricDrawerList");
    if (!list) return;
    const detail = (state.status?.crd_metric_cardinality || []).find((item) => item.key === entry.key);
    list.replaceChildren();
    setText("metricDrawerTitle", entry.name);
    setText("metricDrawerSubtitle", `${entry.namespace}/${entry.pair} · ${entry.source}${entry.converted ? " · converted" : ""}`);
    setText("drawerMemory", formatBytes(entry.bytes));
    setText("drawerSeries", detail ? formatNumber(detail.total_series) : "No scrape data");
    setText("drawerSamples", detail ? formatNumber(detail.samples) : "No scrape data");
    const metrics = detail?.top_metrics || [];
    setText("drawerMetricCount", metrics.length ? `${metrics.length} metrics` : "no samples");
    if (!metrics.length) {
      const empty = document.createElement("div");
      empty.className = "memory-empty";
      empty.textContent = "This CRD is being watched, but no successful metric scrape is associated with it yet.";
      list.append(empty);
      return;
    }
    const maximum = metrics[0]?.series || 1;
    for (const [index, metric] of metrics.entries()) {
      const row = document.createElement("div");
      row.className = "metric-detail-row";
      const rank = document.createElement("span");
      rank.className = "memory-rank";
      rank.textContent = String(index + 1).padStart(2, "0");
      const name = document.createElement("code");
      name.className = "metric-detail-name";
      name.textContent = metric.name;
      const value = document.createElement("strong");
      value.className = "memory-value";
      value.textContent = `${formatNumber(metric.series)} series`;
      const track = document.createElement("div");
      track.className = "memory-track";
      const fill = document.createElement("span");
      fill.style.width = `${Math.max(2, (Number(metric.series) / maximum) * 100)}%`;
      track.append(fill);
      row.append(rank, name, value, track);
      list.append(row);
    }
  }

  function renderRuntime() {
    const process = state.status?.process || {};
    const cpu = Number(process.cpu_percent);
    const memory = Number(process.memory_bytes);
    setText("cpuValue", formatPercent(cpu));
    setText("memoryValue", formatBytes(memory));
    const cpuMeter = $("cpuMeter");
    if (cpuMeter) cpuMeter.style.width = `${Math.min(100, Math.max(0, isNumber(process.cpu_percent) ? cpu : 0))}%`;
    const cpuTrack = cpuMeter?.parentElement;
    if (cpuTrack) cpuTrack.setAttribute("aria-valuenow", isNumber(process.cpu_percent) ? String(Math.min(100, Math.max(0, cpu))) : "0");
  }

  function renderCounters() {
    const counters = state.status?.counters || {};
    setText("reconciliations", formatNumber(counters.reconciliations));
    setText("patchesVictoria", formatNumber(counters.patches_victoria));
    setText("patchesPrometheus", formatNumber(counters.patches_prometheus));
    setText("errors", formatNumber(counters.errors));
  }

  function renderSignals() {
    const empty = $("signalsEmpty");
    const list = $("signalList");
    if (!empty || !list) return;
    list.replaceChildren();
    const names = state.metrics.names;
    empty.hidden = names.length > 0;
    list.hidden = names.length === 0;
    for (const name of names) {
      const signal = document.createElement("code");
      signal.className = "signal-name";
      signal.textContent = name;
      list.append(signal);
    }
    setText("seriesSource", state.metrics.available ? `${state.metrics.series} health samples · /metrics` : "agent health · /metrics");
  }

  function renderError() {
    const banner = $("errorBanner");
    if (!banner) return;
    banner.hidden = !state.statusError;
    if (state.statusError) setText("errorMessage", "The control surface could not reach /api/v1/status. Health endpoints may still report independently.");
  }

  function render() {
    renderConnection();
    renderInventory();
    renderPairs();
    renderMemoryConsumers();
    renderRuntime();
    renderCounters();
    renderSignals();
    renderError();
    setHealth("live", state.health.livez);
    setHealth("ready", state.health.readyz);
    setHealth("metrics", state.metrics.available ? { ok: true } : state.metrics.error ? { ok: false } : null);
    const remoteWrite = state.status?.remote_write;
    const remoteWriteResult = remoteWrite?.enabled
      ? { ok: Boolean(remoteWrite.last_publish_at && !remoteWrite.last_error) }
      : null;
    setHealth("remoteWrite", remoteWriteResult);
    setText("remoteWriteHealth", !remoteWrite?.enabled ? "not configured" : remoteWriteResult.ok ? "ok" : remoteWrite?.last_error ? "error" : "waiting");
  }

  async function refresh() {
    if (state.refreshing) return;
    state.refreshing = true;
    document.body.classList.add("is-refreshing");
    setText("pollStatus", "Fetching live sample");
    const [statusResult, livez, readyz, metricsResult] = await Promise.all([readStatus(), readHealth("/livez"), readHealth("/readyz"), readMetrics()]);
    state.status = statusResult.ok ? normalizeStatus(statusResult.value) : null;
    state.statusError = statusResult.ok ? null : statusResult.error;
    state.health.livez = livez;
    state.health.readyz = readyz;
    state.metrics = metricsResult.ok ? { ...metricsResult.value, error: null } : { available: false, names: [], series: null, error: metricsResult.error };
    if (statusResult.ok) state.lastUpdated = new Date();
    render();
    state.refreshing = false;
    document.body.classList.remove("is-refreshing");
    setText("pollStatus", state.polling ? `Polling every ${POLL_INTERVAL_MS / 1000}s` : "Polling paused");
  }

  function setPolling(enabled) {
    state.polling = enabled;
    const button = $("pollToggle");
    if (button) {
      button.textContent = enabled ? "Pause polling" : "Resume polling";
      button.setAttribute("aria-pressed", String(!enabled));
    }
    setText("pollStatus", enabled ? `Polling every ${POLL_INTERVAL_MS / 1000}s` : "Polling paused");
  }

  $("refreshButton")?.addEventListener("click", refresh);
  $("errorRetry")?.addEventListener("click", refresh);
  $("pollToggle")?.addEventListener("click", () => setPolling(!state.polling));
  $("metricDrawerClose")?.addEventListener("click", closeMetricDrawer);
  $("metricDrawerBackdrop")?.addEventListener("click", closeMetricDrawer);
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && state.selectedMemoryKey) closeMetricDrawer();
  });
  setInterval(() => { if (state.polling) refresh(); }, POLL_INTERVAL_MS);
  refresh();
})();
