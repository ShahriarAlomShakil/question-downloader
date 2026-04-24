require("dotenv").config();

const express = require("express");
const path = require("path");
const fs = require("fs");
const os = require("os");
const { randomUUID } = require("crypto");
const { JobRunner } = require("./downloader");

const app = express();
app.use(express.json({ limit: "5mb" }));
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3939;
const OUTPUT_ROOT = path.join(os.homedir(), "Downloads");

// ─── CREDS FROM .env ──────────────────────────────────────────────────────────
const getCreds = () => ({
  daricommaToken: process.env.DARICOMMA_TOKEN || "",
  cloudinaryCloudName: process.env.CLOUDINARY_CLOUD_NAME || "",
  cloudinaryApiKey: process.env.CLOUDINARY_API_KEY || "",
  cloudinaryApiSecret: process.env.CLOUDINARY_API_SECRET || "",
  openrouterApiKey: process.env.OPENROUTER_API_KEY || "",
  geminiModel: process.env.GEMINI_MODEL || "google/gemini-2.0-flash-001",
  geminiRateLimitMs: process.env.GEMINI_RATE_LIMIT_MS || "0",
  geminiConcurrency: process.env.GEMINI_CONCURRENCY || "8",
});

// ─── IN-MEMORY STATE ──────────────────────────────────────────────────────────
// batches: id -> { id, jobs: [job], status, createdAt, currentIndex, cancelled }
// jobs have: { id, link, meta, uploadImages, enhanceText, status, logs[], progress{}, result?, error? }
const batches = new Map();
const sseClients = new Set();

const broadcast = (event) => {
  const payload = `data: ${JSON.stringify(event)}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(payload);
    } catch {}
  }
};

const publicJob = (job) => ({
  id: job.id,
  link: job.link,
  meta: job.meta,
  uploadImages: job.uploadImages,
  enhanceText: job.enhanceText,
  status: job.status,
  progress: job.progress,
  logs: job.logs.slice(-50),
  error: job.error || null,
  recordCount: job.records ? job.records.length : 0,
  result: job.result
    ? {
        outputPath: job.result.outputPath,
        totalQuestions: job.result.questions.length,
        duplicates: job.result.duplicates.length,
        imageFailures: job.result.imageFailures.length,
        aiFailures: job.result.aiFailures.length,
      }
    : null,
});

const publicBatch = (batch) => ({
  id: batch.id,
  status: batch.status,
  createdAt: batch.createdAt,
  currentIndex: batch.currentIndex,
  jobs: batch.jobs.map(publicJob),
});

// Live record payload — full content (question/answer/explanation/options + originals)
// so the UI can show a side-by-side comparison without another round trip.
const liveRecord = (index, rec) => ({
  index,
  source_id: rec._source_id || null,
  phase: rec._phase || "transformed",
  question_type: rec.question_type,
  question_type_slug: rec.question_type_slug,
  is_mcq: !!rec.is_mcq,
  is_cq: !!rec.is_cq,
  topic: rec.topic || "",
  tags: rec.tags || [],
  question: rec.question || [],
  options: rec.options || [],
  correct_option_index: rec.correct_option_index ?? null,
  answer: rec.answer || [],
  explanation: rec.explanation || [],
  answer_original: rec._answer_original || [],
  explanation_original: rec._explanation_original || [],
  ai_enhanced_answer: !!rec._ai_enhanced_answer,
  ai_enhanced_explanation: !!rec._ai_enhanced_explanation,
  ai_error: rec._ai_error || null,
});

// ─── SSE ──────────────────────────────────────────────────────────────────────
app.get("/api/events", (req, res) => {
  res.set({
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });
  res.flushHeaders();
  res.write(`: connected\n\n`);

  sseClients.add(res);
  req.on("close", () => sseClients.delete(res));
});

// ─── CONFIG ENDPOINTS ─────────────────────────────────────────────────────────
app.get("/api/config", (req, res) => {
  const creds = getCreds();
  const mask = (s) => (s ? s.slice(0, 6) + "..." + s.slice(-4) : "");
  res.json({
    daricommaToken: mask(creds.daricommaToken),
    cloudinaryCloudName: creds.cloudinaryCloudName,
    cloudinaryApiKey: mask(creds.cloudinaryApiKey),
    openrouterApiKey: mask(creds.openrouterApiKey),
    geminiModel: creds.geminiModel,
    geminiRateLimitMs: creds.geminiRateLimitMs,
    geminiConcurrency: creds.geminiConcurrency,
    outputRoot: OUTPUT_ROOT,
  });
});

// ─── BATCH CREATE ─────────────────────────────────────────────────────────────
app.post("/api/batches", async (req, res) => {
  const { jobs } = req.body || {};
  if (!Array.isArray(jobs) || jobs.length === 0) {
    return res.status(400).json({ error: "jobs array required" });
  }

  for (const j of jobs) {
    if (!j.link || typeof j.link !== "string") {
      return res.status(400).json({ error: "each job needs a link" });
    }
    if (!j.meta || !j.meta.class || !j.meta.subject || !j.meta.chapter_no) {
      return res.status(400).json({ error: "each job needs meta.class, meta.subject, meta.chapter_no" });
    }
  }

  const batchId = randomUUID();
  const batch = {
    id: batchId,
    status: "running",
    createdAt: Date.now(),
    currentIndex: -1,
    cancelled: false,
    activeRunners: new Set(),
    jobs: jobs.map((j) => ({
      id: randomUUID(),
      link: j.link,
      meta: j.meta,
      uploadImages: j.uploadImages !== false,
      enhanceText: j.enhanceText !== false,
      status: "pending",
      progress: {},
      logs: [],
      records: [],
      result: null,
      error: null,
    })),
  };
  batches.set(batchId, batch);

  broadcast({ type: "batch_created", batch: publicBatch(batch) });

  // Kick off async processing — don't await
  processBatch(batch).catch((err) => {
    console.error("Batch failed:", err);
    batch.status = "failed";
    broadcast({ type: "batch_update", batch: publicBatch(batch) });
  });

  res.json({ batchId });
});

// ─── BATCH PROCESSOR ──────────────────────────────────────────────────────────
const processBatch = async (batch) => {
  const creds = getCreds();
  const pendingPosts = [];

  for (let i = 0; i < batch.jobs.length; i++) {
    if (batch.cancelled) break;

    batch.currentIndex = i;
    const job = batch.jobs[i];
    job.status = "running";
    broadcast({ type: "job_update", batchId: batch.id, job: publicJob(job) });

    const runner = new JobRunner(
      { link: job.link, meta: job.meta, uploadImages: job.uploadImages, enhanceText: job.enhanceText },
      creds,
      OUTPUT_ROOT
    );
    batch.activeRunners.add(runner);

    runner.on("log", (msg) => {
      const line = `[${new Date().toLocaleTimeString()}] ${msg}`;
      job.logs.push(line);
      if (job.logs.length > 500) job.logs.shift();
      broadcast({ type: "job_log", batchId: batch.id, jobId: job.id, line });
    });

    runner.on("progress", (p) => {
      job.progress = { ...job.progress, ...p };
      broadcast({ type: "job_progress", batchId: batch.id, jobId: job.id, progress: job.progress });
    });

    runner.on("question_record", ({ index, record }) => {
      const payload = liveRecord(index, record);
      job.records[index] = payload;
      broadcast({
        type: "question_record",
        batchId: batch.id,
        jobId: job.id,
        record: payload,
      });
    });

    let fetchFailed = false;
    try {
      await runner.fetchPhase();
    } catch (err) {
      fetchFailed = true;
      job.error = err.message;
      job.status = err.message === "Cancelled" ? "cancelled" : "failed";
      broadcast({ type: "job_update", batchId: batch.id, job: publicJob(job) });
      batch.activeRunners.delete(runner);
    }

    if (!fetchFailed) {
      // Post-processing runs in the background so the next job's fetch can start immediately.
      const postPromise = runner
        .postPhase()
        .then((result) => {
          job.result = result;
          job.status = "done";
          broadcast({ type: "job_update", batchId: batch.id, job: publicJob(job) });
        })
        .catch((err) => {
          job.error = err.message;
          job.status = err.message === "Cancelled" ? "cancelled" : "failed";
          broadcast({ type: "job_update", batchId: batch.id, job: publicJob(job) });
        })
        .finally(() => {
          batch.activeRunners.delete(runner);
        });
      pendingPosts.push(postPromise);
    }
  }

  // Wait for all background post-processing to finish before marking the batch done.
  await Promise.all(pendingPosts);

  batch.status = batch.cancelled ? "cancelled" : "done";
  broadcast({ type: "batch_update", batch: publicBatch(batch) });
};

// ─── BATCH OPS ────────────────────────────────────────────────────────────────
app.get("/api/batches", (req, res) => {
  const list = [...batches.values()]
    .sort((a, b) => b.createdAt - a.createdAt)
    .map(publicBatch);
  res.json({ batches: list });
});

app.get("/api/batches/:id", (req, res) => {
  const batch = batches.get(req.params.id);
  if (!batch) return res.status(404).json({ error: "not found" });
  res.json({ batch: publicBatch(batch) });
});

app.post("/api/batches/:id/cancel", (req, res) => {
  const batch = batches.get(req.params.id);
  if (!batch) return res.status(404).json({ error: "not found" });
  batch.cancelled = true;
  for (const runner of batch.activeRunners) runner.cancel();
  res.json({ ok: true });
});

// Live records for a job — includes originals for side-by-side comparison.
app.get("/api/batches/:batchId/jobs/:jobId/records", (req, res) => {
  const batch = batches.get(req.params.batchId);
  if (!batch) return res.status(404).json({ error: "batch not found" });
  const job = batch.jobs.find((j) => j.id === req.params.jobId);
  if (!job) return res.status(404).json({ error: "job not found" });
  res.json({ records: (job.records || []).filter(Boolean) });
});

// Full job detail (with all questions/duplicates/errors — not sent over SSE)
app.get("/api/batches/:batchId/jobs/:jobId", (req, res) => {
  const batch = batches.get(req.params.batchId);
  if (!batch) return res.status(404).json({ error: "batch not found" });
  const job = batch.jobs.find((j) => j.id === req.params.jobId);
  if (!job) return res.status(404).json({ error: "job not found" });

  res.json({
    job: {
      ...publicJob(job),
      logs: job.logs,
      questions: job.result ? job.result.questions.map(summarizeQuestion) : [],
      duplicates: job.result ? job.result.duplicates : [],
      imageFailures: job.result ? job.result.imageFailures : [],
      aiFailures: job.result ? job.result.aiFailures : [],
    },
  });
});

const contentSnippet = (content, maxLen = 200) => {
  if (!Array.isArray(content)) return "";
  let out = "";
  for (const item of content) {
    if (item.type === "text") out += item.value + " ";
    else if (item.type === "image") out += "[image] ";
    if (out.length > maxLen) break;
  }
  return out.trim().slice(0, maxLen);
};

const hasText = (content) =>
  Array.isArray(content) && content.some((item) => item.type === "text" && item.value.trim());

const summarizeQuestion = (q, idx) => ({
  index: idx,
  source_id: q._source_id,
  question_type: q.question_type,
  question_type_slug: q.question_type_slug,
  is_mcq: q.is_mcq,
  is_cq: q.is_cq,
  topic: q.topic,
  tags: q.tags,
  has_question_image: q.has_question_image,
  has_answer_image: q.has_answer_image,
  has_explanation_image: q.has_explanation_image,
  has_explanation: q.has_explanation,
  has_answer: hasText(q.answer),
  option_count: q.option_count,
  correct_option_index: q.correct_option_index,
  question_preview: contentSnippet(q.question),
  answer_preview: contentSnippet(q.answer),
  explanation_preview: contentSnippet(q.explanation),
});

// Full question payload for the "view" modal
app.get("/api/batches/:batchId/jobs/:jobId/questions/:index", (req, res) => {
  const batch = batches.get(req.params.batchId);
  if (!batch) return res.status(404).json({ error: "batch not found" });
  const job = batch.jobs.find((j) => j.id === req.params.jobId);
  if (!job || !job.result) return res.status(404).json({ error: "job not found / no result" });
  const idx = parseInt(req.params.index, 10);
  const q = job.result.questions[idx];
  if (!q) return res.status(404).json({ error: "question not found" });
  const { _source_id, ...rest } = q;
  res.json({ question: rest });
});

// Download written JSON file
app.get("/api/batches/:batchId/jobs/:jobId/download", (req, res) => {
  const batch = batches.get(req.params.batchId);
  if (!batch) return res.status(404).json({ error: "batch not found" });
  const job = batch.jobs.find((j) => j.id === req.params.jobId);
  if (!job?.result?.outputPath) return res.status(404).json({ error: "no output file" });
  if (!fs.existsSync(job.result.outputPath)) return res.status(404).json({ error: "file missing" });
  res.download(job.result.outputPath);
});

app.listen(PORT, () => {
  console.log(`\nQuestion downloader UI → http://localhost:${PORT}`);
  console.log(`Output root: ${OUTPUT_ROOT}\n`);
});
