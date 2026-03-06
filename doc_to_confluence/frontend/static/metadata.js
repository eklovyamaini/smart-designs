/* ── Metadata Manager Frontend ──────────────────────────────────────────────
 *
 * 3-step workflow:
 *  Step 1 — Enter parent page URLs + credentials → Preview Scope
 *  Step 2 — Review table of pages in scope (with Module column) → Apply
 *  Step 3 — Live SSE progress stream → Summary
 *
 * Additional: Tracking Page panel (collapsible, inline in Step 1)
 */

// ─── State ────────────────────────────────────────────────────────────────────

let _credentials = { baseUrl: "", user: "", apiToken: "" };

// ─── Helpers ──────────────────────────────────────────────────────────────────

function showStep(n) {
  document.querySelectorAll(".step-panel").forEach(el => {
    el.classList.toggle("hidden", !el.id.endsWith(String(n)));
    el.classList.toggle("active", el.id.endsWith(String(n)));
  });
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function setHint(msg, id = "step1-hint") {
  const el = document.getElementById(id);
  if (el) el.textContent = msg;
}

function appendLog(text, cls = "info") {
  const log = document.getElementById("progress-log");
  if (!log) return;
  const line = document.createElement("p");
  line.className = `log-line ${cls}`;
  line.textContent = text;
  log.appendChild(line);
  log.scrollTop = log.scrollHeight;
}

// ─── Step 1 Init — Load defaults ─────────────────────────────────────────────

async function loadDefaults() {
  try {
    const resp = await fetch("/defaults");
    if (!resp.ok) return;
    const data = await resp.json();

    const baseUrlEl   = document.getElementById("meta-base-url");
    const userEl      = document.getElementById("meta-user");
    const approversEl = document.getElementById("meta-default-approvers");
    const badgeEl     = document.getElementById("defaults-source-badge");

    if (data.confluence_base_url)            baseUrlEl.value   = data.confluence_base_url;
    if (data.confluence_user)                userEl.value      = data.confluence_user;
    if (data.metadata_default_approvers)     approversEl.value = data.metadata_default_approvers;
    // Never pre-fill the token visually; keep it in state only
    _credentials.apiToken = data.confluence_api_token || "";

    // Pre-fill tracker space key from default_space_key
    const spaceKeyEl = document.getElementById("tracker-space-key");
    if (spaceKeyEl && data.default_space_key) spaceKeyEl.value = data.default_space_key;

    if (data.confluence_base_url || data.confluence_user) {
      badgeEl && badgeEl.classList.remove("hidden");
    }
  } catch (_) {
    // Silently ignore — user can fill manually
  }
}

// ─── Tracker Panel Toggle ─────────────────────────────────────────────────────

function initTrackerToggle() {
  const btn  = document.getElementById("tracker-toggle");
  const body = document.getElementById("tracker-body");
  if (!btn || !body) return;

  btn.addEventListener("click", () => {
    const isOpen = body.classList.toggle("open");
    btn.classList.toggle("open", isOpen);
  });
}

// ─── Create / Update Tracker Page ────────────────────────────────────────────

async function handleCreateTracker() {
  const baseUrl   = document.getElementById("meta-base-url").value.trim();
  const user      = document.getElementById("meta-user").value.trim();
  const apiToken  = document.getElementById("meta-api-token").value.trim() || _credentials.apiToken;
  const spaceKey  = document.getElementById("tracker-space-key").value.trim();
  const title     = document.getElementById("tracker-title").value.trim();
  const parentId  = document.getElementById("tracker-parent-id").value.trim() || null;
  const label     = (document.getElementById("tracker-label")?.value.trim()) || "ds-tracked";
  const resultEl  = document.getElementById("tracker-result");
  const btn       = document.getElementById("create-tracker-btn");

  if (!baseUrl || !user || !apiToken) {
    resultEl.className = "tracker-result error";
    resultEl.textContent = "Please fill in all Confluence connection fields first.";
    resultEl.classList.remove("hidden");
    return;
  }
  if (!spaceKey) {
    resultEl.className = "tracker-result error";
    resultEl.textContent = "Space Key is required.";
    resultEl.classList.remove("hidden");
    return;
  }

  btn.disabled = true;
  btn.textContent = "Creating…";
  resultEl.className = "tracker-result";
  resultEl.textContent = "";
  resultEl.classList.remove("hidden");

  try {
    const resp = await fetch("/metadata/create-tracker", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        space_key: spaceKey,
        tracker_title: title || "DS Review Tracking Dashboard",
        parent_page_id: parentId,
        label: label,
        confluence_base_url: baseUrl,
        confluence_user: user,
        confluence_api_token: apiToken,
      }),
    });

    const data = await resp.json();

    if (!resp.ok) {
      resultEl.className = "tracker-result error";
      resultEl.textContent = `Error: ${data.error || "Unknown error"}`;
    } else {
      resultEl.className = "tracker-result";
      resultEl.innerHTML =
        `✓ Tracker page ${data.id ? "updated" : "created"} — ` +
        `<a href="${data.url}" target="_blank" rel="noopener">${data.title}</a>`;
    }
  } catch (err) {
    resultEl.className = "tracker-result error";
    resultEl.textContent = `Request failed: ${err.message}`;
  } finally {
    btn.disabled = false;
    btn.innerHTML = "&#128202; Create / Update Tracker";
  }
}

// ─── Step 1 → Step 2: Preview ────────────────────────────────────────────────

async function handlePreview() {
  const baseUrl       = document.getElementById("meta-base-url").value.trim();
  const user          = document.getElementById("meta-user").value.trim();
  const apiToken      = document.getElementById("meta-api-token").value.trim() || _credentials.apiToken;
  const defaultApprovers = document.getElementById("meta-default-approvers").value.trim();
  const urlsRaw       = document.getElementById("parent-urls").value.trim();

  if (!baseUrl || !user || !apiToken) {
    setHint("Please fill in all Confluence connection fields.");
    return;
  }
  if (!urlsRaw) {
    setHint("Please enter at least one parent page URL.");
    return;
  }

  _credentials = { baseUrl, user, apiToken };
  setHint("Fetching pages in scope…");

  const parentUrls = urlsRaw.split("\n").map(s => s.trim()).filter(Boolean);

  const btn = document.getElementById("preview-btn");
  btn.disabled = true;
  btn.textContent = "Loading…";

  try {
    const resp = await fetch("/metadata/preview", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        parent_urls: parentUrls,
        confluence_base_url: baseUrl,
        confluence_user: user,
        confluence_api_token: apiToken,
        default_approvers: defaultApprovers,
      }),
    });

    const data = await resp.json();

    if (!resp.ok) {
      setHint(`Error: ${data.error || "Unknown error"}`);
      return;
    }

    renderPreviewTable(data.pages || []);
    showStep(2);
  } catch (err) {
    setHint(`Request failed: ${err.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = "Preview Scope →";
  }
}

function renderPreviewTable(pages) {
  const tbody  = document.getElementById("preview-tbody");
  const summary = document.getElementById("preview-summary");
  tbody.innerHTML = "";

  const willApply = pages.filter(p => !p.has_blocks && !p.error).length;
  const already   = pages.filter(p => p.has_blocks).length;
  const errors    = pages.filter(p => p.error).length;
  const forceOn   = document.getElementById("force-apply").checked;

  summary.textContent =
    `${pages.length} pages found — ` +
    `${willApply} will receive blocks` +
    (already ? `, ${already} already have blocks${forceOn ? " (will re-apply)" : " (will skip)"}` : "") +
    (errors  ? `, ${errors} errors` : "");

  for (const page of pages) {
    const tr = document.createElement("tr");

    // Title cell
    const tdTitle = document.createElement("td");
    tdTitle.textContent = page.title || page.page_id || "—";
    tr.appendChild(tdTitle);

    // Module cell
    const tdModule = document.createElement("td");
    if (page.module) {
      const badge = document.createElement("span");
      badge.style.cssText =
        "display:inline-block;padding:0.125rem 0.5rem;border-radius:99px;" +
        "background:#f1f5f9;color:#475569;font-size:0.75rem;font-weight:500;";
      badge.textContent = page.module;
      tdModule.appendChild(badge);
    } else {
      tdModule.textContent = "—";
      tdModule.style.color = "var(--color-text-light)";
    }
    tr.appendChild(tdModule);

    // URL cell
    const tdUrl = document.createElement("td");
    if (page.url) {
      const a = document.createElement("a");
      a.href = page.url;
      a.target = "_blank";
      a.rel = "noopener";
      a.textContent = page.url;
      a.style.cssText = "color:var(--color-primary);font-size:0.75rem;word-break:break-all;";
      tdUrl.appendChild(a);
    } else {
      tdUrl.textContent = "—";
    }
    tr.appendChild(tdUrl);

    // Status badge cell
    const tdStatus = document.createElement("td");
    const badge = document.createElement("span");
    badge.className = "status-badge";
    if (page.error) {
      badge.classList.add("error");
      badge.textContent = "⚠ Error";
      badge.title = page.error;
    } else if (page.has_blocks) {
      badge.classList.add("has-blocks");
      badge.textContent = forceOn ? "✓ Will re-apply" : "✓ Already applied";
    } else {
      badge.classList.add("will-apply");
      badge.textContent = "→ Will apply";
    }
    tdStatus.appendChild(badge);
    tr.appendChild(tdStatus);

    tbody.appendChild(tr);
  }
}

// ─── Step 2 → Step 3: Apply (SSE stream) ─────────────────────────────────────

async function handleApply() {
  showStep(3);

  const log = document.getElementById("progress-log");
  log.innerHTML = "";

  const urlsRaw          = document.getElementById("parent-urls").value.trim();
  const parentUrls       = urlsRaw.split("\n").map(s => s.trim()).filter(Boolean);
  const force            = document.getElementById("force-apply").checked;
  const defaultApprovers = document.getElementById("meta-default-approvers").value.trim();

  const formData = new FormData();
  parentUrls.forEach(url => formData.append("parent_urls", url));
  formData.append("force", force ? "true" : "false");
  formData.append("confluence_base_url", _credentials.baseUrl);
  formData.append("confluence_user", _credentials.user);
  formData.append("confluence_api_token", _credentials.apiToken);
  formData.append("default_approvers", defaultApprovers);
  formData.append("label", (document.getElementById("tracker-label")?.value.trim()) || "ds-tracked");

  try {
    const resp = await fetch("/metadata/apply", {
      method: "POST",
      body: formData,
    });

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({ error: resp.statusText }));
      appendLog(`Error: ${err.error || "Unknown error"}`, "error");
      return;
    }

    const reader  = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });

      const lines = buf.split("\n");
      buf = lines.pop();

      for (const line of lines) {
        if (!line.startsWith("data:")) continue;
        const raw = line.slice(5).trim();
        if (!raw) continue;
        let event;
        try { event = JSON.parse(raw); }
        catch (_) { continue; }
        handleEvent(event);
      }
    }
  } catch (err) {
    appendLog(`Connection error: ${err.message}`, "error");
  }
}

function handleEvent(event) {
  switch (event.type) {
    case "start":
      appendLog(`Starting — ${event.total} pages in scope`, "info");
      break;

    case "progress": {
      const icon = event.status === "applied" ? "✓" :
                   event.status === "skipped" ? "–" : "✗";
      const cls  = event.status === "applied" ? "applied" :
                   event.status === "skipped" ? "skipped" : "error";
      const suffix = event.error
        ? ` — ${event.error}`
        : event.label_warning
          ? ` \u26a0 ${event.label_warning}`
          : "";
      appendLog(
        `[${event.current}/${event.total}] ${icon} ${event.title || event.page_id}${suffix}`,
        cls
      );
      break;
    }

    case "complete": {
      appendLog(
        `Done — ${event.applied} applied, ${event.skipped} skipped, ${event.errors} errors`,
        "info"
      );
      const bar = document.getElementById("summary-bar");
      bar.classList.remove("hidden");
      document.getElementById("sum-applied").textContent  = event.applied;
      document.getElementById("sum-skipped").textContent  = event.skipped;
      document.getElementById("sum-errors").textContent   = event.errors;
      document.getElementById("sum-total").textContent    = event.total;
      document.getElementById("restart-btn").classList.remove("hidden");
      break;
    }

    case "error":
      appendLog(`Error: ${event.message}`, "error");
      break;
  }
}

// ─── Auto-Label Pages ─────────────────────────────────────────────────────────

async function handleAutoLabel() {
  const btn      = document.getElementById("auto-label-btn");
  const statusEl = document.getElementById("auto-label-status");
  const log      = document.getElementById("auto-label-log");

  // Read credentials from the form fields directly (same pattern as handlePreview),
  // falling back to cached _credentials so saved/loaded defaults still work.
  const baseUrl  = document.getElementById("meta-base-url").value.trim()  || _credentials.baseUrl;
  const user     = document.getElementById("meta-user").value.trim()      || _credentials.user;
  const apiToken = document.getElementById("meta-api-token").value.trim() || _credentials.apiToken;

  const urlsRaw = (document.getElementById("auto-label-parent-urls")?.value || "").trim();
  if (!urlsRaw) {
    statusEl.textContent = "✕ Please enter at least one parent URL";
    statusEl.style.color = "var(--color-danger)";
    return;
  }
  const parent_urls = urlsRaw.split("\n").map(s => s.trim()).filter(Boolean);

  btn.disabled = true;
  btn.textContent = "⏳ Labeling…";
  statusEl.textContent = "";
  statusEl.style.color = "";
  log.innerHTML = "";
  log.classList.remove("hidden");

  try {
    const resp = await fetch("/metadata/auto-label", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        confluence_base_url:  baseUrl,
        confluence_user:      user,
        confluence_api_token: apiToken,
        parent_urls,
      }),
    });

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      statusEl.textContent = `✕ ${err.error || resp.statusText}`;
      statusEl.style.color = "var(--color-danger)";
      return;
    }

    const reader  = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split("\n\n");
      for (let i = 0; i < parts.length - 1; i++) {
        const raw = parts[i].trim();
        if (!raw.startsWith("data:")) continue;
        try {
          const event = JSON.parse(raw.slice(5).trim());
          if (event.type === "start") {
            statusEl.textContent = `Labeling ${event.total} page(s)…`;
            statusEl.style.color = "";
          } else if (event.type === "progress") {
            const newLbls = (event.new_labels || []).join(", ") || "—";
            const icon    = event.status === "error"     ? "✕"
                          : event.status === "unchanged" ? "–"
                          : "✓";
            const line = document.createElement("div");
            line.className = `log-line ${event.status === "labeled" ? "labeled" : event.status === "unchanged" ? "unchanged" : "error"}`;
            line.textContent =
              `${icon} [${event.current}/${event.total}] ${event.title}` +
              (event.status === "error"
                ? ` — ${event.error}`
                : ` — added: [${newLbls}]`);
            log.appendChild(line);
            log.scrollTop = log.scrollHeight;
          } else if (event.type === "complete") {
            statusEl.textContent =
              `✓ Done — ${event.labeled} labeled, ${event.unchanged} unchanged` +
              (event.errors ? `, ${event.errors} errors` : "");
            statusEl.style.color = event.errors ? "var(--color-warning, orange)" : "var(--color-success, #34d399)";
          } else if (event.type === "error") {
            statusEl.textContent = `✕ ${event.message}`;
            statusEl.style.color = "var(--color-danger)";
          }
        } catch (_) {}
      }
      buffer = parts[parts.length - 1];
    }
    reader.releaseLock();
  } catch (err) {
    statusEl.textContent = `✕ ${err.message}`;
    statusEl.style.color = "var(--color-danger)";
  } finally {
    btn.disabled = false;
    btn.textContent = "🏷️ Add Labels";
  }
}

// ─── Event Listeners ──────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  loadDefaults();
  initTrackerToggle();

  document.getElementById("preview-btn").addEventListener("click", handlePreview);
  document.getElementById("back-btn").addEventListener("click", () => showStep(1));
  document.getElementById("apply-btn").addEventListener("click", handleApply);
  document.getElementById("create-tracker-btn").addEventListener("click", handleCreateTracker);
  document.getElementById("auto-label-btn").addEventListener("click", handleAutoLabel);

  document.getElementById("restart-btn").addEventListener("click", () => {
    document.getElementById("progress-log").innerHTML = "";
    document.getElementById("summary-bar").classList.add("hidden");
    document.getElementById("restart-btn").classList.add("hidden");
    showStep(1);
  });

  // Update preview badge labels live when force checkbox toggles
  document.getElementById("force-apply").addEventListener("change", () => {
    const step2 = document.getElementById("step-2");
    if (!step2.classList.contains("active")) return;
    const forceOn = document.getElementById("force-apply").checked;
    document.querySelectorAll("#preview-tbody .status-badge.has-blocks").forEach(badge => {
      badge.textContent = forceOn ? "✓ Will re-apply" : "✓ Already applied";
    });
  });
});
