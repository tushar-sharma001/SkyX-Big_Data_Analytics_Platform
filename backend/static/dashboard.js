const EVENT_COLORS = {
  rainfall: "#1f8ac0", flood: "#d94b4b", heatwave: "#ff8c42",
  dust_storm: "#b98a3a", cold_wave: "#4a6fa5", thunderstorm: "#7a5cbf", cyclone: "#c0396f"
};

const map = L.map("map").setView([22.9734, 78.6569], 5);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: "&copy; OpenStreetMap contributors",
  maxZoom: 12
}).addTo(map);
let markerLayer = L.layerGroup().addTo(map);

let chartEvent, chartStatus, chartTimeline;

function currentFilters() {
  return {
    event_type: document.getElementById("f-event").value,
    state: document.getElementById("f-state").value,
    status: document.getElementById("f-status").value,
    date_from: document.getElementById("f-date-from").value,
    date_to: document.getElementById("f-date-to").value,
    q: document.getElementById("f-q").value,
  };
}

function buildQuery(params) {
  const q = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => { if (v) q.set(k, v); });
  return q.toString();
}

async function loadStates() {
  const res = await fetch("/api/states");
  const states = await res.json();
  const sel = document.getElementById("f-state");
  states.forEach(s => {
    const opt = document.createElement("option");
    opt.value = s; opt.textContent = s;
    sel.appendChild(opt);
  });
}

async function loadStats() {
  const res = await fetch("/api/stats");
  const stats = await res.json();

  document.getElementById("kpi-total").textContent = stats.total;
  const verified = (stats.by_status.find(s => s.status === "verified") || {}).c || 0;
  document.getElementById("kpi-verified").textContent = verified;
  document.getElementById("kpi-fake").textContent = stats.avg_fake_probability;
  document.getElementById("kpi-imd").textContent = stats.imd_corroborated_pct + "%";

  renderEventChart(stats.by_event);
  renderStatusChart(stats.by_status);
  renderTimelineChart(stats.timeline);
}

function renderEventChart(byEvent) {
  const ctx = document.getElementById("chart-event");
  const labels = byEvent.map(r => r.e || "unknown");
  const data = byEvent.map(r => r.c);
  const colors = labels.map(l => EVENT_COLORS[l] || "#999");
  if (chartEvent) chartEvent.destroy();
  chartEvent = new Chart(ctx, {
    type: "doughnut",
    data: { labels, datasets: [{ data, backgroundColor: colors }] },
    options: { plugins: { legend: { position: "bottom", labels: { boxWidth: 10, font: { size: 10 } } } } }
  });
}

function renderStatusChart(byStatus) {
  const ctx = document.getElementById("chart-status");
  const labels = byStatus.map(r => r.s || "unknown");
  const data = byStatus.map(r => r.c);
  if (chartStatus) chartStatus.destroy();
  chartStatus = new Chart(ctx, {
    type: "bar",
    data: { labels, datasets: [{ data, backgroundColor: "#1f8ac0" }] },
    options: { plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } }
  });
}

function renderTimelineChart(timeline) {
  const ctx = document.getElementById("chart-timeline");
  const labels = timeline.map(r => r.day);
  const data = timeline.map(r => r.c);
  if (chartTimeline) chartTimeline.destroy();
  chartTimeline = new Chart(ctx, {
    type: "line",
    data: { labels, datasets: [{ data, borderColor: "#0b2545", backgroundColor: "rgba(31,138,192,.15)", fill: true, tension: .25 }] },
    options: { plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } }
  });
}

async function loadReports() {
  const qs = buildQuery({ ...currentFilters(), limit: 300 });
  const res = await fetch(`/api/reports?${qs}`);
  const data = await res.json();
  renderMap(data.reports);
  renderFeed(data.reports);
}

function renderMap(reports) {
  markerLayer.clearLayers();
  reports.forEach(r => {
    if (!r.latitude || !r.longitude) return;
    const color = EVENT_COLORS[r.predicted_event_type] || "#999";
    const marker = L.circleMarker([r.latitude, r.longitude], {
      radius: r.status === "verified" ? 6 : 4,
      color, fillColor: color, fillOpacity: r.status === "flagged_fake" ? 0.25 : 0.75,
      weight: r.status === "duplicate" ? 1 : 2,
    });
    const imdBadge = r.corroboration_verdict === "corroborated" ? "✅ IMD-corroborated"
      : r.corroboration_verdict === "contradicted" ? "⚠️ contradicts IMD bulletin"
      : "— no official match yet";
    marker.bindPopup(`
      <strong>${r.predicted_event_type}</strong> (${(r.event_confidence*100).toFixed(0)}% conf.)<br>
      ${r.district}, ${r.state}<br>
      <em>${r.text}</em><br>
      source: ${r.source} · status: <strong>${r.status}</strong><br>
      fake score: ${(r.fake_probability*100).toFixed(0)}% · ${imdBadge}
    `);
    marker.addTo(markerLayer);
  });
}

function renderFeed(reports) {
  const body = document.getElementById("feed-body");
  body.innerHTML = "";
  document.getElementById("feed-count").textContent = reports.length;
  reports.slice(0, 100).forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.timestamp}</td>
      <td>${r.predicted_event_type}</td>
      <td>${r.district}, ${r.state}</td>
      <td>${r.source}</td>
      <td><span class="status-pill status-${r.status}">${r.status.replace('_',' ')}</span></td>
      <td>${(r.fake_probability*100).toFixed(0)}%</td>
      <td>${r.corroboration_verdict === "corroborated" ? "✅" : (r.corroboration_verdict === "contradicted" ? "⚠️" : "—")}</td>
      <td>${r.text}</td>
    `;
    body.appendChild(tr);
  });
}

async function refreshAll() {
  await loadStats();
  await loadReports();
}

document.getElementById("apply-filters").addEventListener("click", loadReports);
document.getElementById("clear-filters").addEventListener("click", () => {
  ["f-event", "f-state", "f-status"].forEach(id => document.getElementById(id).value = "");
  ["f-date-from", "f-date-to", "f-q"].forEach(id => document.getElementById(id).value = "");
  loadReports();
});

(async function init() {
  await loadStates();
  await refreshAll();
  setInterval(refreshAll, 6000); // live feed refresh
})();
