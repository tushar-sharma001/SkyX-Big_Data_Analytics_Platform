async function loadQueue() {
  const status = document.getElementById("a-status").value;
  const qs = new URLSearchParams();
  if (status) qs.set("status", status);
  qs.set("limit", 200);
  const res = await fetch(`/api/reports?${qs.toString()}`);
  const data = await res.json();
  document.getElementById("a-total").textContent = data.count;
  renderTable(data.reports);
}

function renderTable(reports) {
  const body = document.getElementById("admin-body");
  body.innerHTML = "";
  reports.forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.timestamp}</td>
      <td>${r.predicted_event_type} (${(r.event_confidence*100).toFixed(0)}%)</td>
      <td>${r.district}, ${r.state}</td>
      <td>${r.source}</td>
      <td>${(r.source_credibility*100).toFixed(0)}%</td>
      <td>${(r.fake_probability*100).toFixed(0)}%</td>
      <td>${r.corroboration_verdict === "corroborated" ? `✅ ${r.matched_bulletin_id || ""}` : (r.corroboration_verdict === "contradicted" ? "⚠️ contradicted" : "—")}</td>
      <td>${r.duplicate_of || "-"}</td>
      <td><span class="status-pill status-${r.status}">${r.status.replace('_',' ')}</span></td>
      <td style="max-width:280px">${r.text}</td>
      <td>
        <button class="btn-verify" data-id="${r.report_id}" data-status="verified">Verify</button>
        <button class="btn-reject" data-id="${r.report_id}" data-status="rejected">Reject</button>
        <button class="btn-dup" data-id="${r.report_id}" data-status="duplicate">Mark dup</button>
      </td>
    `;
    body.appendChild(tr);
  });

  body.querySelectorAll("button").forEach(btn => {
    btn.addEventListener("click", async () => {
      await fetch("/api/admin/verify", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ report_id: btn.dataset.id, status: btn.dataset.status })
      });
      loadQueue();
    });
  });
}

document.getElementById("a-refresh").addEventListener("click", loadQueue);
document.getElementById("a-status").addEventListener("change", loadQueue);

loadQueue();
setInterval(loadQueue, 8000);
