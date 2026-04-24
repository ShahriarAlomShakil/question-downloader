// ─── STATE ────────────────────────────────────────────────────────────────────
let currentBatch = null; // batch object from server
// jobId -> { index: liveRecord } — live per-question data for comparison views
const jobRecords = new Map();
const STORAGE_KEY = "qdl-defaults-v1";
const QUEUE_KEY = "qdl-queue-v1";

// ─── UTIL ─────────────────────────────────────────────────────────────────────
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => [...document.querySelectorAll(sel)];

const esc = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const saveDefaults = () => {
  const d = {
    class: $("#def-class").value,
    subject: $("#def-subject").value,
    paper: $("#def-paper").value,
    chapter_no: $("#def-chapter_no").value,
    chapter_name: $("#def-chapter_name").value,
    uploadImages: $("#def-uploadImages").checked,
    enhanceText: $("#def-enhanceText").checked,
  };
  localStorage.setItem(STORAGE_KEY, JSON.stringify(d));
};

const loadDefaults = () => {
  try {
    const d = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}");
    if (d.class) $("#def-class").value = d.class;
    if (d.subject) $("#def-subject").value = d.subject;
    if (d.paper) $("#def-paper").value = d.paper;
    if (d.chapter_no) $("#def-chapter_no").value = d.chapter_no;
    if (d.chapter_name) $("#def-chapter_name").value = d.chapter_name;
    if (d.uploadImages !== undefined) $("#def-uploadImages").checked = d.uploadImages;
    if (d.enhanceText !== undefined) $("#def-enhanceText").checked = d.enhanceText;
  } catch {}
};

const saveQueue = () => {
  const rows = readQueue();
  localStorage.setItem(QUEUE_KEY, JSON.stringify(rows));
};

const loadQueue = () => {
  try {
    const rows = JSON.parse(localStorage.getItem(QUEUE_KEY) || "[]");
    for (const r of rows) addRow(r);
  } catch {}
};

// ─── QUEUE ROWS ───────────────────────────────────────────────────────────────
const addRow = (preset) => {
  const tpl = $("#queue-row-template");
  const tr = tpl.content.firstElementChild.cloneNode(true);
  const tbody = $("#queue-body");
  tbody.appendChild(tr);

  const p = preset || {
    link: "",
    class: $("#def-class").value,
    subject: $("#def-subject").value,
    paper: $("#def-paper").value,
    chapter_no: $("#def-chapter_no").value,
    chapter_name: $("#def-chapter_name").value,
    uploadImages: $("#def-uploadImages").checked,
    enhanceText: $("#def-enhanceText").checked,
  };

  tr.querySelector(".q-link").value = p.link || "";
  tr.querySelector(".q-class").value = p.class || "";
  tr.querySelector(".q-subject").value = p.subject || "";
  tr.querySelector(".q-paper").value = p.paper || "";
  tr.querySelector(".q-chapter_no").value = p.chapter_no || "";
  tr.querySelector(".q-chapter_name").value = p.chapter_name || "";
  tr.querySelector(".q-uploadImages").checked = !!p.uploadImages;
  tr.querySelector(".q-enhanceText").checked = !!p.enhanceText;

  tr.querySelector(".q-del").addEventListener("click", () => {
    tr.remove();
    renumberRows();
    saveQueue();
  });

  tr.querySelectorAll("input").forEach((inp) => inp.addEventListener("change", saveQueue));

  renumberRows();
};

const renumberRows = () => {
  $$("#queue-body tr").forEach((tr, i) => {
    tr.querySelector(".row-num").textContent = i + 1;
  });
};

const readQueue = () => {
  return $$("#queue-body tr").map((tr) => ({
    link: tr.querySelector(".q-link").value.trim(),
    class: tr.querySelector(".q-class").value.trim(),
    subject: tr.querySelector(".q-subject").value.trim(),
    paper: tr.querySelector(".q-paper").value.trim(),
    chapter_no: tr.querySelector(".q-chapter_no").value.trim(),
    chapter_name: tr.querySelector(".q-chapter_name").value.trim(),
    uploadImages: tr.querySelector(".q-uploadImages").checked,
    enhanceText: tr.querySelector(".q-enhanceText").checked,
  }));
};

// ─── API ──────────────────────────────────────────────────────────────────────
const api = async (path, opts = {}) => {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
};

const loadEnvStatus = async () => {
  try {
    const c = await api("/api/config");
    const missing = [];
    if (!c.daricommaToken) missing.push("DARICOMMA_TOKEN");
    if (!c.cloudinaryApiKey) missing.push("CLOUDINARY_API_KEY");
    if (!c.openrouterApiKey) missing.push("OPENROUTER_API_KEY");
    $("#env-status").textContent = missing.length
      ? `⚠ missing env: ${missing.join(", ")}`
      : `env loaded — token ${c.daricommaToken} · saves under ${c.outputRoot}`;
    $("#env-status").style.color = missing.length ? "var(--err)" : "var(--muted)";
  } catch (e) {
    $("#env-status").textContent = "failed to load /api/config";
    $("#env-status").style.color = "var(--err)";
  }
};

// ─── START BATCH ──────────────────────────────────────────────────────────────
const startBatch = async () => {
  const rows = readQueue().filter((r) => r.link);
  if (!rows.length) {
    alert("Add at least one link.");
    return;
  }

  const invalid = rows.find((r) => !r.class || !r.subject || !r.chapter_no);
  if (invalid) {
    alert("Every row needs Class, Subject and Chapter # filled in.");
    return;
  }

  const jobs = rows.map((r) => ({
    link: r.link,
    meta: {
      class: r.class,
      subject: r.subject,
      paper: r.paper,
      chapter_no: r.chapter_no,
      chapter_name: r.chapter_name,
    },
    uploadImages: r.uploadImages,
    enhanceText: r.enhanceText,
  }));

  try {
    const { batchId } = await api("/api/batches", { method: "POST", body: JSON.stringify({ jobs }) });
    // batch_created will arrive via SSE; this just confirms the POST succeeded.
    $("#start-batch").disabled = true;
    $("#cancel-batch").disabled = false;
    $("#batch-status").textContent = `Batch ${batchId.slice(0, 8)}… started`;
  } catch (err) {
    alert("Failed to start batch: " + err.message);
  }
};

const cancelBatch = async () => {
  if (!currentBatch) return;
  if (!confirm("Cancel the running batch? The current link will stop on its next checkpoint.")) return;
  try {
    await api(`/api/batches/${currentBatch.id}/cancel`, { method: "POST" });
  } catch (err) {
    alert("Cancel failed: " + err.message);
  }
};

// ─── RENDER JOBS LIST ─────────────────────────────────────────────────────────
const jobCard = (job, batchId) => {
  const card = document.createElement("div");
  card.className = "job-card";
  card.dataset.jobId = job.id;
  renderJobCard(card, job, batchId);
  return card;
};

const progressCell = (label, value) =>
  `<div class="progress-cell"><div class="label">${label}</div><div class="value">${value}</div></div>`;

const renderJobCard = (card, job, batchId) => {
  const p = job.progress || {};
  const fetchLine = p.total
    ? `${p.fetched || 0}/${p.total}`
    : p.fetched
    ? `${p.fetched}`
    : "—";
  const transformLine = p.total || p.done !== undefined ? `${p.done || 0}/${p.total || "?"}` : "—";
  const geminiLine = p.total !== undefined && p.done !== undefined ? `${p.done}/${p.total}` : "—";

  // Derive per-phase progress:
  const fetchDone = p.fetched && p.total ? Math.min(1, p.fetched / p.total) : 0;

  const chapterLabel = `${job.meta.class}/${job.meta.subject} — Ch${job.meta.chapter_no}${
    job.meta.chapter_name ? ` (${esc(job.meta.chapter_name)})` : ""
  }`;

  const statsRow = job.result
    ? `<div class="stats-row">
        <div class="stat ok"><strong>${job.result.totalQuestions}</strong> saved</div>
        <div class="stat dup"><strong>${job.result.duplicates}</strong> dup</div>
        <div class="stat err"><strong>${job.result.imageFailures}</strong> img err</div>
        <div class="stat warn"><strong>${job.result.aiFailures}</strong> ai err</div>
      </div>`
    : "";

  const errorBlock = job.error ? `<div class="error-msg">Error: ${esc(job.error)}</div>` : "";

  const actions =
    job.status === "done"
      ? `<div class="job-actions">
          <button data-action="view" data-batch="${batchId}" data-job="${job.id}">View results</button>
          <button data-action="download" data-batch="${batchId}" data-job="${job.id}">Download JSON</button>
        </div>`
      : "";

  card.innerHTML = `
    <div class="job-header">
      <div>
        <div class="job-title">${esc(job.link)}</div>
        <div class="job-meta">${chapterLabel}</div>
      </div>
      <span class="status-pill ${job.status}">${job.status}</span>
    </div>

    <div class="progress-grid">
      ${progressCell("Fetch", fetchLine)}
      ${progressCell("Transform", transformLine)}
      ${progressCell("Gemini", geminiLine)}
    </div>
    ${
      fetchDone > 0 && job.status === "running"
        ? `<div class="bar"><div class="bar-fill" style="width:${fetchDone * 100}%"></div></div>`
        : ""
    }
    ${statsRow}
    ${errorBlock}
    ${actions}
    <div class="live-questions" data-job-live="${job.id}"></div>
    ${
      job.logs && job.logs.length
        ? `<div class="logs">${job.logs.map((l) => `<div>${esc(l)}</div>`).join("")}</div>`
        : ""
    }
  `;

  card.querySelectorAll("button[data-action]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const action = btn.dataset.action;
      if (action === "view") openDetail(btn.dataset.batch, btn.dataset.job);
      if (action === "download") window.location.href = `/api/batches/${btn.dataset.batch}/jobs/${btn.dataset.job}/download`;
    });
  });

  renderLiveQuestions(job.id, batchId);
};

// ─── LIVE QUESTIONS PANEL ─────────────────────────────────────────────────────
const PHASE_LABEL = {
  transformed: "transformed",
  ai_enhancing: "enhancing",
  ai_enhancing_explanation: "enhancing expl",
  ai_enhancing_answer: "enhancing ans",
  ai_done: "ai done",
  ai_skipped: "no ai",
  ai_failed: "ai failed",
};

const contentPreview = (content, max = 100) => {
  if (!Array.isArray(content)) return "";
  let out = "";
  for (const item of content) {
    if (item.type === "text") out += item.value + " ";
    else if (item.type === "image") out += "[img] ";
    if (out.length > max) break;
  }
  return out.trim().slice(0, max);
};

const renderLiveQuestions = (jobId, batchId) => {
  const container = document.querySelector(`[data-job-live="${jobId}"]`);
  if (!container) return;
  const recMap = jobRecords.get(jobId);
  if (!recMap || recMap.size === 0) {
    container.innerHTML = "";
    return;
  }

  const records = [...recMap.values()].sort((a, b) => a.index - b.index);
  const rows = records
    .map((r) => {
      const phase = r.phase || "transformed";
      const typeBadge = r.is_mcq
        ? '<span class="badge mcq">MCQ</span>'
        : r.is_cq
        ? '<span class="badge cq">CQ</span>'
        : `<span class="badge">${esc(r.question_type_slug || "?")}</span>`;
      const aiFlags = [
        r.ai_enhanced_explanation ? '<span class="badge ai">expl</span>' : "",
        r.ai_enhanced_answer ? '<span class="badge ai">ans</span>' : "",
      ]
        .filter(Boolean)
        .join("");
      const errFlag = r.ai_error ? '<span class="badge err-b">err</span>' : "";
      return `<div class="lq-row" data-idx="${r.index}" data-job="${jobId}" data-batch="${batchId}">
        <span class="lq-idx">#${r.index + 1}</span>
        <span class="phase-pill ${phase}">${PHASE_LABEL[phase] || phase}</span>
        ${typeBadge}
        <span class="lq-preview">${esc(contentPreview(r.question, 120))}</span>
        <span class="lq-flags">${aiFlags}${errFlag}</span>
      </div>`;
    })
    .join("");

  container.innerHTML = `
    <div class="lq-header">
      <span>Questions (${records.length})</span>
      <span class="muted">click a row to compare original vs AI</span>
    </div>
    <div class="lq-list">${rows}</div>
  `;

  container.querySelectorAll(".lq-row").forEach((el) => {
    el.addEventListener("click", () => {
      const idx = parseInt(el.dataset.idx, 10);
      const rec = jobRecords.get(el.dataset.job)?.get(idx);
      if (rec) openComparison(rec);
    });
  });
};

const handleQuestionRecord = (data) => {
  let bag = jobRecords.get(data.jobId);
  if (!bag) {
    bag = new Map();
    jobRecords.set(data.jobId, bag);
  }
  bag.set(data.record.index, data.record);
  renderLiveQuestions(data.jobId, data.batchId);

  // Keep any open comparison modal in sync.
  if (comparisonState && comparisonState.jobId === data.jobId && comparisonState.index === data.record.index) {
    renderComparison(data.record);
  }
};

const renderBatch = (batch) => {
  currentBatch = batch;
  const list = $("#jobs-list");
  list.innerHTML = "";
  if (!batch || !batch.jobs.length) {
    list.innerHTML = `<div class="empty">No batch running.</div>`;
    return;
  }
  for (const job of batch.jobs) list.appendChild(jobCard(job, batch.id));

  const doneStatuses = new Set(["done", "failed", "cancelled"]);
  const allDone = batch.jobs.every((j) => doneStatuses.has(j.status));
  $("#start-batch").disabled = !allDone;
  $("#cancel-batch").disabled = allDone;
  $("#batch-status").textContent = `Batch ${batch.id.slice(0, 8)} — ${batch.status}${
    batch.currentIndex >= 0 ? ` (link ${batch.currentIndex + 1}/${batch.jobs.length})` : ""
  }`;
};

const updateJobInPlace = (jobId, mutator) => {
  if (!currentBatch) return;
  const job = currentBatch.jobs.find((j) => j.id === jobId);
  if (!job) return;
  mutator(job);
  const card = $(`.job-card[data-job-id="${jobId}"]`);
  if (card) renderJobCard(card, job, currentBatch.id);
};

// ─── SSE ──────────────────────────────────────────────────────────────────────
const connectSSE = () => {
  const es = new EventSource("/api/events");
  es.onmessage = (msg) => {
    let data;
    try { data = JSON.parse(msg.data); } catch { return; }

    if (data.type === "batch_created") {
      renderBatch(data.batch);
    } else if (data.type === "batch_update") {
      renderBatch(data.batch);
    } else if (data.type === "job_update") {
      if (!currentBatch || currentBatch.id !== data.batchId) return;
      const idx = currentBatch.jobs.findIndex((j) => j.id === data.job.id);
      if (idx >= 0) currentBatch.jobs[idx] = data.job;
      renderBatch(currentBatch);
    } else if (data.type === "job_progress") {
      updateJobInPlace(data.jobId, (j) => {
        j.progress = { ...j.progress, ...data.progress };
      });
    } else if (data.type === "job_log") {
      updateJobInPlace(data.jobId, (j) => {
        j.logs = j.logs || [];
        j.logs.push(data.line);
        if (j.logs.length > 50) j.logs.shift();
      });
    } else if (data.type === "question_record") {
      handleQuestionRecord(data);
    }
  };
  es.onerror = () => {
    // Browser will auto-reconnect
  };
};

// ─── DETAIL MODAL ─────────────────────────────────────────────────────────────
let detailCache = null;

const openDetail = async (batchId, jobId) => {
  try {
    const [{ job }, recordsRes] = await Promise.all([
      api(`/api/batches/${batchId}/jobs/${jobId}`),
      api(`/api/batches/${batchId}/jobs/${jobId}/records`).catch(() => ({ records: [] })),
    ]);
    // Hydrate jobRecords so comparison modal has originals for completed jobs.
    const bag = new Map();
    for (const r of recordsRes.records || []) bag.set(r.index, r);
    jobRecords.set(jobId, bag);

    detailCache = { batchId, jobId, job };
    renderDetail(job);
    $("#modal").classList.remove("hidden");
  } catch (err) {
    alert("Failed to load detail: " + err.message);
  }
};

const renderDetail = (job) => {
  $("#modal-content").innerHTML = `
    <h3>${esc(job.meta.class)}/${esc(job.meta.subject)} — Chapter ${esc(job.meta.chapter_no)}</h3>
    <div class="stats-row">
      <div class="stat ok"><strong>${job.questions.length}</strong> questions</div>
      <div class="stat dup"><strong>${job.duplicates.length}</strong> duplicates</div>
      <div class="stat err"><strong>${job.imageFailures.length}</strong> image errors</div>
      <div class="stat warn"><strong>${job.aiFailures.length}</strong> Gemini errors</div>
    </div>
    <div class="detail-tabs">
      <button data-tab="questions" class="active">Questions</button>
      <button data-tab="duplicates">Duplicates (${job.duplicates.length})</button>
      <button data-tab="imgerr">Image errors (${job.imageFailures.length})</button>
      <button data-tab="aierr">AI errors (${job.aiFailures.length})</button>
      <button data-tab="logs">Logs</button>
    </div>
    <div class="detail-tab-content active" data-tab="questions">${renderQuestionsTab(job)}</div>
    <div class="detail-tab-content" data-tab="duplicates">${renderDuplicatesTab(job)}</div>
    <div class="detail-tab-content" data-tab="imgerr">${renderImgErrTab(job)}</div>
    <div class="detail-tab-content" data-tab="aierr">${renderAiErrTab(job)}</div>
    <div class="detail-tab-content" data-tab="logs">${renderLogsTab(job)}</div>
  `;

  $("#modal-content .detail-tabs").addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-tab]");
    if (!btn) return;
    $$("#modal-content .detail-tabs button").forEach((b) => b.classList.toggle("active", b === btn));
    $$("#modal-content .detail-tab-content").forEach((c) =>
      c.classList.toggle("active", c.dataset.tab === btn.dataset.tab)
    );
  });

  wireQuestionFilter(job);
};

const renderQuestionsTab = (job) => {
  const typeOptions = [...new Set(job.questions.map((q) => q.question_type_slug).filter(Boolean))]
    .map((t) => `<option value="${esc(t)}">${esc(t)}</option>`)
    .join("");
  const tagOptions = [...new Set(job.questions.flatMap((q) => q.tags || []))]
    .map((t) => `<option value="${esc(t)}">${esc(t)}</option>`)
    .join("");

  return `
    <div class="filter-row">
      <input id="q-search" placeholder="Search preview text…" style="min-width:220px" />
      <select id="q-type"><option value="">All types</option>${typeOptions}</select>
      <select id="q-tag"><option value="">All tags</option>${tagOptions}</select>
      <label style="flex-direction:row;color:var(--text);font-size:11px;align-items:center;gap:4px">
        <input type="checkbox" id="q-imgonly" /> has images
      </label>
    </div>
    <div style="max-height:55vh;overflow:auto;">
      <table class="q-table" id="q-table">
        <thead>
          <tr>
            <th>#</th>
            <th>Type</th>
            <th>Preview</th>
            <th>Topic</th>
            <th>Tags</th>
            <th>IMG</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  `;
};

const wireQuestionFilter = (job) => {
  const renderRows = () => {
    const search = $("#q-search").value.trim().toLowerCase();
    const type = $("#q-type").value;
    const tag = $("#q-tag").value;
    const imgOnly = $("#q-imgonly").checked;

    const rows = job.questions.filter((q) => {
      if (search) {
        const hay = `${q.question_preview} ${q.answer_preview} ${q.explanation_preview}`.toLowerCase();
        if (!hay.includes(search)) return false;
      }
      if (type && q.question_type_slug !== type) return false;
      if (tag && !(q.tags || []).includes(tag)) return false;
      if (imgOnly && !q.has_question_image && !q.has_answer_image && !q.has_explanation_image) return false;
      return true;
    });

    const tbody = $("#q-table tbody");
    tbody.innerHTML = rows
      .map((q) => {
        const typeBadge = q.is_mcq ? '<span class="badge mcq">MCQ</span>' : q.is_cq ? '<span class="badge cq">CQ</span>' : `<span class="badge">${esc(q.question_type_slug || "?")}</span>`;
        const imgBadge =
          q.has_question_image || q.has_answer_image || q.has_explanation_image
            ? '<span class="badge img">IMG</span>'
            : "";
        const tags = (q.tags || []).map((t) => `<span class="badge">${esc(t)}</span>`).join("");
        return `<tr data-idx="${q.index}">
          <td>${q.index + 1}</td>
          <td>${typeBadge}</td>
          <td class="q-preview">${esc(q.question_preview)}</td>
          <td>${esc(q.topic || "")}</td>
          <td>${tags}</td>
          <td>${imgBadge}</td>
        </tr>`;
      })
      .join("");

    tbody.querySelectorAll("tr").forEach((tr) => {
      tr.addEventListener("click", () => openQuestionModal(parseInt(tr.dataset.idx, 10)));
    });
  };

  ["q-search", "q-type", "q-tag", "q-imgonly"].forEach((id) => {
    const el = $(`#${id}`);
    el.addEventListener("input", renderRows);
    el.addEventListener("change", renderRows);
  });
  renderRows();
};

const renderContentBlocks = (content) => {
  if (!Array.isArray(content) || !content.length) return '<div style="color:var(--muted)">(empty)</div>';
  return content
    .map((item) => {
      if (item.type === "image") return `<img src="${esc(item.value)}" alt="image" />`;
      return `<div class="text-chunk">${esc(item.value)}</div>`;
    })
    .join("");
};

const contentEqual = (a, b) => JSON.stringify(a || []) === JSON.stringify(b || []);

// ─── COMPARISON MODAL (original vs AI) ────────────────────────────────────────
let comparisonState = null; // { jobId, index }

const openComparison = (rec, jobId) => {
  comparisonState = { jobId: jobId || null, index: rec.index };
  renderComparison(rec);
};

const renderComparison = (rec) => {
  const existing = document.getElementById("cmp-modal");
  if (existing) existing.remove();

  const optionsHtml =
    rec.options && rec.options.length
      ? `<div class="view-block">
          <h4>Options</h4>
          <div class="options-list">
            ${rec.options
              .map(
                (opt, i) => `
              <div class="option-row ${i === rec.correct_option_index ? "correct" : ""}">
                <div class="option-label">${String.fromCharCode(65 + i)}.</div>
                <div>${renderContentBlocks(opt)}</div>
              </div>
            `
              )
              .join("")}
          </div>
        </div>`
      : "";

  const phase = rec.phase || "transformed";
  const explSame = contentEqual(rec.explanation_original, rec.explanation);
  const ansSame = contentEqual(rec.answer_original, rec.answer);

  const diffBlock = (title, originalContent, enhancedContent, enhancedFlag, sameFlag) => `
    <div class="diff-block">
      <h4>${esc(title)}
        ${
          enhancedFlag
            ? '<span class="badge ai">AI enhanced</span>'
            : sameFlag
            ? '<span class="badge">unchanged</span>'
            : '<span class="badge">not yet run</span>'
        }
      </h4>
      <div class="diff-grid">
        <div class="diff-col">
          <div class="diff-col-title">Original</div>
          <div class="diff-col-body">${renderContentBlocks(originalContent)}</div>
        </div>
        <div class="diff-col">
          <div class="diff-col-title">AI enhanced</div>
          <div class="diff-col-body ${enhancedFlag ? "enhanced" : ""}">${renderContentBlocks(enhancedContent)}</div>
        </div>
      </div>
    </div>
  `;

  const errBlock = rec.ai_error
    ? `<div class="error-msg">AI error: ${esc(rec.ai_error)}</div>`
    : "";

  const html = `
    <h3>Question ${rec.index + 1}
      <span class="phase-pill ${phase}">${PHASE_LABEL[phase] || phase}</span>
      — ${esc(rec.question_type || "")}
    </h3>
    ${errBlock}
    <div class="view-block"><h4>Question</h4>${renderContentBlocks(rec.question)}</div>
    ${optionsHtml}
    ${diffBlock("Answer", rec.answer_original, rec.answer, rec.ai_enhanced_answer, ansSame)}
    ${diffBlock("Explanation", rec.explanation_original, rec.explanation, rec.ai_enhanced_explanation, explSame)}
  `;

  const modal = document.createElement("div");
  modal.className = "modal";
  modal.id = "cmp-modal";
  modal.innerHTML = `<div class="modal-body"><button class="modal-close" id="cmp-close">×</button>${html}</div>`;
  document.body.appendChild(modal);
  $("#cmp-close").addEventListener("click", () => {
    modal.remove();
    comparisonState = null;
  });
  modal.addEventListener("click", (e) => {
    if (e.target === modal) {
      modal.remove();
      comparisonState = null;
    }
  });
};

const openQuestionModal = async (index) => {
  if (!detailCache) return;
  // Prefer the live record (has originals for side-by-side) if we have it.
  const rec = jobRecords.get(detailCache.jobId)?.get(index);
  if (rec) {
    openComparison(rec, detailCache.jobId);
    return;
  }
  try {
    const { question } = await api(`/api/batches/${detailCache.batchId}/jobs/${detailCache.jobId}/questions/${index}`);

    const optionsHtml = question.options && question.options.length
      ? `<div class="view-block">
          <h4>Options</h4>
          <div class="options-list">
            ${question.options.map((opt, i) => `
              <div class="option-row ${i === question.correct_option_index ? "correct" : ""}">
                <div class="option-label">${String.fromCharCode(65 + i)}.</div>
                <div>${renderContentBlocks(opt)}</div>
              </div>
            `).join("")}
          </div>
        </div>`
      : "";

    const html = `
      <h3>Question ${index + 1} — ${esc(question.question_type)}</h3>
      <div class="view-block"><h4>Question</h4>${renderContentBlocks(question.question)}</div>
      ${optionsHtml}
      <div class="view-block"><h4>Answer</h4>${renderContentBlocks(question.answer)}</div>
      <div class="view-block"><h4>Explanation</h4>${renderContentBlocks(question.explanation)}</div>
      <details>
        <summary style="cursor:pointer;color:var(--muted);font-size:11px">Raw JSON</summary>
        <pre class="json-view">${esc(JSON.stringify(question, null, 2))}</pre>
      </details>
    `;

    // Stack a second modal over the detail one
    const existing = document.getElementById("q-modal");
    if (existing) existing.remove();
    const qModal = document.createElement("div");
    qModal.className = "modal";
    qModal.id = "q-modal";
    qModal.innerHTML = `<div class="modal-body"><button class="modal-close" id="q-close">×</button>${html}</div>`;
    document.body.appendChild(qModal);
    $("#q-close").addEventListener("click", () => qModal.remove());
    qModal.addEventListener("click", (e) => { if (e.target === qModal) qModal.remove(); });
  } catch (err) {
    alert("Failed to load question: " + err.message);
  }
};

const renderDuplicatesTab = (job) => {
  if (!job.duplicates.length) return '<div class="empty">No duplicates were skipped.</div>';
  return `<table class="q-table">
    <thead><tr><th>#</th><th>Source ID</th><th>Text preview</th></tr></thead>
    <tbody>${job.duplicates.map((d, i) =>
      `<tr><td>${i + 1}</td><td>${esc(d.id || "—")}</td><td>${esc(d.fingerprint_preview)}</td></tr>`
    ).join("")}</tbody>
  </table>`;
};

const renderImgErrTab = (job) => {
  if (!job.imageFailures.length) return '<div class="empty">No image errors.</div>';
  return `<table class="q-table">
    <thead><tr><th>#</th><th>URL</th><th>Reason</th></tr></thead>
    <tbody>${job.imageFailures.map((f, i) =>
      `<tr><td>${i + 1}</td><td style="word-break:break-all">${esc(f.url)}</td><td>${esc(f.reason)}</td></tr>`
    ).join("")}</tbody>
  </table>`;
};

const renderAiErrTab = (job) => {
  if (!job.aiFailures.length) return '<div class="empty">No Gemini errors.</div>';
  return `<table class="q-table">
    <thead><tr><th>#</th><th>Source ID</th><th>Reason</th></tr></thead>
    <tbody>${job.aiFailures.map((f, i) =>
      `<tr><td>${i + 1}</td><td>${esc(f.source_id || "—")}</td><td>${esc(f.reason)}</td></tr>`
    ).join("")}</tbody>
  </table>`;
};

const renderLogsTab = (job) =>
  `<div class="logs" style="max-height:55vh;">${(job.logs || []).map((l) => `<div>${esc(l)}</div>`).join("")}</div>`;

// ─── INIT ─────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  loadDefaults();
  loadQueue();
  if (!$("#queue-body").children.length) addRow();

  $$(".defaults-card input").forEach((inp) => inp.addEventListener("change", saveDefaults));
  $("#add-row").addEventListener("click", () => { addRow(); saveQueue(); });
  $("#clear-queue").addEventListener("click", () => {
    if (!confirm("Clear all queue rows?")) return;
    $("#queue-body").innerHTML = "";
    saveQueue();
  });
  $("#add-many").addEventListener("click", () => $("#paste-modal").classList.remove("hidden"));
  $("#paste-close").addEventListener("click", () => $("#paste-modal").classList.add("hidden"));
  $("#paste-apply").addEventListener("click", () => {
    const lines = $("#paste-area").value.split("\n").map((l) => l.trim()).filter(Boolean);
    for (const link of lines) addRow({ link, class: $("#def-class").value, subject: $("#def-subject").value, paper: $("#def-paper").value, chapter_no: $("#def-chapter_no").value, chapter_name: $("#def-chapter_name").value, uploadImages: $("#def-uploadImages").checked, enhanceText: $("#def-enhanceText").checked });
    $("#paste-area").value = "";
    $("#paste-modal").classList.add("hidden");
    saveQueue();
  });

  $("#start-batch").addEventListener("click", startBatch);
  $("#cancel-batch").addEventListener("click", cancelBatch);

  $("#modal-close").addEventListener("click", () => $("#modal").classList.add("hidden"));
  $("#modal").addEventListener("click", (e) => {
    if (e.target === $("#modal")) $("#modal").classList.add("hidden");
  });

  loadEnvStatus();
  connectSSE();
});
