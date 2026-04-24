const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { EventEmitter } = require("events");

const OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions";

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

// ─── FINGERPRINT / DEDUP HELPERS ──────────────────────────────────────────────
const qFingerprint = (q) =>
  (q.question_text?.blocks || [])
    .map((b) => (b.text || "").trim())
    .filter(Boolean)
    .join("|");


// ─── CONTENT EXTRACTION ───────────────────────────────────────────────────────
const extractContent = (richText) => {
  if (typeof richText === "string") {
    const t = richText.trim();
    return t ? [{ type: "text", value: t }] : [];
  }
  if (!richText || !Array.isArray(richText.blocks)) return [];

  const entityMap = richText.entityMap || {};
  const content = [];

  const pushText = (t) => {
    const c = String(t || "").trim();
    if (c) content.push({ type: "text", value: c });
  };

  const pushImage = (key) => {
    const entity = entityMap[String(key)] || entityMap[key];
    const src = entity?.data?.src;
    if (entity?.type === "IMAGE" && src) {
      content.push({ type: "image", value: src });
      return true;
    }
    return false;
  };

  for (const block of richText.blocks) {
    const text = block?.text || "";
    const ranges = Array.isArray(block?.entityRanges)
      ? [...block.entityRanges].sort((a, b) => a.offset - b.offset)
      : [];

    if (!ranges.length) {
      pushText(text);
      continue;
    }

    let cursor = 0,
      usedImage = false;
    for (const r of ranges) {
      const offset = Math.max(0, r?.offset ?? 0);
      const length = Math.max(0, r?.length ?? 0);
      if (offset > cursor) pushText(text.slice(cursor, offset));
      const isImg = pushImage(r?.key);
      if (!isImg && length > 0) pushText(text.slice(offset, offset + length));
      usedImage = usedImage || isImg;
      cursor = Math.max(cursor, offset + length);
    }
    if (cursor < text.length) pushText(text.slice(cursor));
    if (!usedImage && block?.type === "atomic") ranges.forEach((r) => pushImage(r?.key));
  }

  return content;
};

const extractRawPreview = (q) => ({
  source_id: q.id || null,
  question_type: q.question_type?.name || "",
  question: extractContent(q.question_text),
  answer: extractContent(q.answer_text),
  explanation: extractContent(q.explanation_text),
  options: (q.option || []).map((o) => extractContent(o)),
  correct_option_index: q.mcq_solution_index ?? null,
});

const richHasText = (richText) => {
  if (!richText) return false;
  if (typeof richText === "string") return richText.trim().length > 0;
  if (!Array.isArray(richText.blocks)) return false;
  return richText.blocks.some((b) => b.text?.trim().length > 0);
};

const richHasImage = (richText) => {
  if (!richText?.entityMap) return false;
  return Object.values(richText.entityMap).some((e) => e.type === "IMAGE");
};

// Answer+Explanation=3, Answer=2, Explanation=1, None=0
const qPriority = (q) => {
  const hasAnswer = richHasText(q.answer_text);
  const hasExpl = richHasText(q.explanation_text);
  if (hasAnswer && hasExpl) return 3;
  if (hasAnswer) return 2;
  if (hasExpl) return 1;
  return 0;
};

const contentToText = (content) => {
  let n = 0;
  return content.map((item) => (item.type === "image" ? `[[IMG_${n++}]]` : item.value)).join("\n");
};

const textToContent = (enhancedText, originalContent) => {
  const images = originalContent.filter((item) => item.type === "image");
  if (images.length === 0) return [{ type: "text", value: enhancedText.trim() }];

  const result = [];
  let remaining = enhancedText;

  images.forEach((imgItem, i) => {
    const ph = `[[IMG_${i}]]`;
    const idx = remaining.indexOf(ph);
    if (idx !== -1) {
      const before = remaining.slice(0, idx).trim();
      if (before) result.push({ type: "text", value: before });
      result.push(imgItem);
      remaining = remaining.slice(idx + ph.length);
    } else {
      result.push(imgItem);
    }
  });

  const after = remaining.trim();
  if (after) result.push({ type: "text", value: after });
  return result.length > 0 ? result : originalContent;
};

// ─── TAG DERIVATION ───────────────────────────────────────────────────────────
const ENGG_CODES = new Set(["BUET", "RUET", "KUET", "CUET", "IUT", "BUTEX", "BUTex"]);
const AGRI_CODES = new Set(["BAU", "CVASU", "SBAU", "BSFMSTU", "PASTU", "SAU", "JGVC", "HSTU"]);
const MED_CODES = new Set(["BMA", "BMDC", "MMCH"]);

const deriveTags = (subsources) => {
  const tags = new Set();
  for (const s of subsources) {
    const srcType = s.sub_source?.source?.name || "";
    const code = (s.sub_source?.name || "").trim();
    const base = code.split("-")[0];

    if (srcType === "Board Exam Question") {
      tags.add("Board Exam QB");
      continue;
    }
    if (srcType === "Test Exam Question") {
      tags.add("Test Exam QB");
      continue;
    }
    if (srcType === "অনুশীলনীর প্রশ্ন") {
      tags.add("NCTB Book");
      continue;
    }

    if (srcType === "Admission Exam Question") {
      if (code === "ACAS") {
        tags.add("ACAS QB");
        continue;
      }
      if (code.startsWith("CKRUET")) {
        tags.add("CKRUET QB");
        continue;
      }
      if (code.startsWith("GST")) {
        tags.add("GST QB");
        continue;
      }
      if (ENGG_CODES.has(base)) {
        tags.add("Engg QB");
        continue;
      }
      if (AGRI_CODES.has(base)) {
        tags.add("Agri QB");
        continue;
      }
      if (MED_CODES.has(base)) {
        tags.add("Medical QB");
        continue;
      }
      if (code.startsWith("SAT") || code.startsWith("SAU")) {
        tags.add("SAT QB");
        continue;
      }
      tags.add("Admission QB");
    }
  }
  return [...tags].sort();
};

// ─── URL HELPER ───────────────────────────────────────────────────────────────
const urlMutator = (url, page) => {
  const u = new URL(url);
  u.searchParams.set("page", page);
  return u.toString();
};

// ─── MAIN DOWNLOADER CLASS ────────────────────────────────────────────────────
class JobRunner extends EventEmitter {
  constructor(job, creds, outputRoot) {
    super();
    this.job = job;
    this.creds = creds;
    this.outputRoot = outputRoot;
    this.cancelled = false;

    this.questionsRaw = [];
    this.duplicates = [];
    this.imageFailures = [];
    this.aiFailures = [];
    this.uploadCache = new Map();
    this.seenIds = new Set();
    this.seenTexts = new Set();
    this.seenIdMap = new Map();
    this.seenFpMap = new Map();
  }

  log(msg) {
    this.emit("log", msg);
  }

  progress(phase, data = {}) {
    this.emit("progress", { phase, ...data });
  }

  emitQuestion(index, record) {
    this.emit("question_record", { index, record });
  }

  cancel() {
    this.cancelled = true;
  }

  // ─── CLOUDINARY ─────────────────────────────────────────────────────────────
  async uploadToCloudinary(imageUrl, folder) {
    if (this.uploadCache.has(imageUrl)) return this.uploadCache.get(imageUrl);

    const timestamp = Math.round(Date.now() / 1000);
    const paramsToSign = `folder=${folder}&timestamp=${timestamp}`;
    const signature = crypto
      .createHash("sha1")
      .update(paramsToSign + this.creds.cloudinaryApiSecret)
      .digest("hex");

    const buildForm = (file) => {
      const f = new FormData();
      f.append("file", file);
      f.append("folder", folder);
      f.append("timestamp", timestamp);
      f.append("api_key", this.creds.cloudinaryApiKey);
      f.append("signature", signature);
      return f;
    };

    try {
      let res = await fetch(
        `https://api.cloudinary.com/v1_1/${this.creds.cloudinaryCloudName}/image/upload`,
        { method: "POST", body: buildForm(imageUrl) }
      );
      let data = await res.json();

      if (!data.secure_url) {
        const imgRes = await fetch(imageUrl);
        if (!imgRes.ok) throw new Error(`Download failed: ${imgRes.status}`);
        const b64 = Buffer.from(await imgRes.arrayBuffer()).toString("base64");
        const mimeType = imgRes.headers.get("content-type") || "image/png";
        res = await fetch(
          `https://api.cloudinary.com/v1_1/${this.creds.cloudinaryCloudName}/image/upload`,
          { method: "POST", body: buildForm(`data:${mimeType};base64,${b64}`) }
        );
        data = await res.json();
      }

      if (!data.secure_url) throw new Error(`Upload failed: ${JSON.stringify(data)}`);
      this.uploadCache.set(imageUrl, data.secure_url);
      return data.secure_url;
    } catch (err) {
      this.imageFailures.push({ url: imageUrl, reason: err.message });
      this.log(`Skipping image (${err.message}): ${imageUrl}`);
      return imageUrl;
    }
  }

  async processContent(richText, folder, uploadImages) {
    const content = extractContent(richText);
    if (!uploadImages) return content;
    return Promise.all(
      content.map(async (item) =>
        item.type === "image"
          ? { type: "image", value: await this.uploadToCloudinary(item.value, folder) }
          : item
      )
    );
  }

  async processOption(o, folder, uploadImages) {
    if (o == null || typeof o !== "object") return [{ type: "text", value: String(o ?? "") }];
    return this.processContent(o, folder, uploadImages);
  }

  // ─── GEMINI ─────────────────────────────────────────────────────────────────
  async callGemini(prompt, retries = 5) {
    const response = await fetch(OPENROUTER_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${this.creds.openrouterApiKey}`,
      },
      body: JSON.stringify({
        model: this.creds.geminiModel,
        messages: [{ role: "user", content: prompt }],
        temperature: 0.3,
      }),
    });
    const data = await response.json();

    if (data.error) {
      const msg = data.error.message || JSON.stringify(data.error);
      const retryMatch = msg.match(/retry in ([\d.]+)s/i);
      if (retryMatch && retries > 0) {
        const waitMs = Math.ceil(parseFloat(retryMatch[1]) * 1000) + 2000;
        this.log(`Rate limited — waiting ${Math.ceil(waitMs / 1000)}s before retry...`);
        await sleep(waitMs);
        return this.callGemini(prompt, retries - 1);
      }
      throw new Error(msg);
    }

    return data.choices?.[0]?.message?.content?.trim() || null;
  }

  async enhanceExplanation(content, meta) {
    const text = contentToText(content);
    if (!text.trim()) return content;
    const prompt = `You are an expert ${meta.subject} teacher for ${meta.class} level students.

Rewrite the following explanation to make it easy to understand.

Rules:
- Keep the same language (Bengali if Bengali, English if English)
- Use LaTeX for all math/chemistry expressions: inline → $...$, block/display → $$...$$
- Keep it well-structured and clear
- Do NOT change the meaning or add new information
- Keep [[IMG_N]] placeholders exactly as they are — they mark image positions
- Return ONLY the rewritten explanation, nothing else

Chapter: ${meta.chapter_name}

Explanation:
${text}`;

    const result = await this.callGemini(prompt);
    return result ? textToContent(result, content) : content;
  }

  async polishSolution(content, meta) {
    const text = contentToText(content);
    if (!text.trim()) return content;
    const prompt = `You are an expert ${meta.subject} teacher for ${meta.class} level students.

Slightly vary the writing pattern of the following solution so it does not match 100% with the original.

Rules:
- Keep the same language (Bengali if Bengali, English if English)
- Keep all math/chemistry expressions exactly as they are — do NOT touch LaTeX
- Only change word choices, sentence structure, or phrasing slightly
- Do NOT change the meaning, steps, or any calculated values
- Keep [[IMG_N]] placeholders exactly as they are — they mark image positions
- The output must be at least 95% similar to the input
- Return ONLY the modified solution, nothing else

Chapter: ${meta.chapter_name}

Solution:
${text}`;

    const result = await this.callGemini(prompt);
    return result ? textToContent(result, content) : content;
  }

  // ─── TRANSFORM ──────────────────────────────────────────────────────────────
  async transformQuestion(q, meta, uploadImages) {
    const folder = `${meta.class}/${meta.subject}/paper-${meta.paper}/chapter-${meta.chapter_no}`;

    const [question, answer, explanation] = await Promise.all([
      this.processContent(q.question_text, folder, uploadImages),
      this.processContent(q.answer_text, folder, uploadImages),
      this.processContent(q.explanation_text, folder, uploadImages),
    ]);

    const options = await Promise.all(
      (q.option || []).map((o) => this.processOption(o, folder, uploadImages))
    );

    const subsources = q.question_subsources || [];
    const boardCodes = [...new Set(subsources.map((s) => s.sub_source?.name).filter(Boolean))];
    const boardNames = [...new Set(subsources.map((s) => s.sub_source?.description).filter(Boolean))];
    const years = [...new Set(subsources.map((s) => s.year?.name).filter(Boolean))];
    const sourceTypes = [...new Set(subsources.map((s) => s.sub_source?.source?.name).filter(Boolean))];
    const tags = deriveTags(subsources);

    const questionTypeName = q.question_type?.name || "";
    const isCq = questionTypeName.includes("সৃজনশীল");
    const isMcq =
      !isCq &&
      (questionTypeName.includes("বহুনির্বাচনি") || questionTypeName.toLowerCase().includes("mcq"));
    const isAdmission = sourceTypes.includes("Admission Exam Question");

    const levelName = q.question_level?.name || "";
    const levelCode = levelName.split(":")[0].trim().toLowerCase();
    const questionTypeSlug = isMcq ? "mcq" : isCq ? "cq" : levelCode || "other";

    const hasQuestionImage = richHasImage(q.question_text);
    const hasAnswerImage = richHasImage(q.answer_text);
    const hasExplanationImage = richHasImage(q.explanation_text);

    const finalOptions = isCq ? [] : options;
    const finalCorrectIndex = isMcq ? q.mcq_solution_index ?? null : null;

    return {
      _source_id: q.id || null,
      _answer_original: answer,
      _explanation_original: explanation,
      _ai_enhanced_answer: false,
      _ai_enhanced_explanation: false,
      _ai_error: null,
      _phase: "transformed",

      class: meta.class || "",
      subject: meta.subject || "",
      subject_slug: (meta.subject || "").toLowerCase().replace(/\s+/g, "-"),
      subject_id: null,
      paper: parseInt(meta.paper, 10) || 0,
      chapter_no: meta.chapter_no || "",
      chapter_name: meta.chapter_name || "",

      question,
      answer,
      explanation,
      options: finalOptions,
      correct_option_index: finalCorrectIndex,

      question_type: questionTypeName,
      question_type_slug: questionTypeSlug,
      is_mcq: isMcq,
      is_cq: isCq,
      is_admission: isAdmission,
      marks: q.question_type?.mark ?? 0,
      level: 5,
      topic: q.topic?.name || "",

      sources: subsources.map((qs) => ({
        board_code: qs.sub_source?.name || "",
        board_name: qs.sub_source?.description || "",
        source_type: qs.sub_source?.source?.name || "",
        year: qs.year?.name || null,
      })),
      board_codes: boardCodes,
      board_names: boardNames,
      years,
      source_types: sourceTypes,
      tags,

      has_options: finalOptions.length > 0,
      option_count: finalOptions.length,
      has_explanation: richHasText(q.explanation_text),
      has_question_image: hasQuestionImage,
      has_answer_image: hasAnswerImage,
      has_explanation_image: hasExplanationImage,
      has_any_image: hasQuestionImage || hasAnswerImage || hasExplanationImage,
    };
  }

  // ─── FETCH LOOP ─────────────────────────────────────────────────────────────
  async fetchAllPages() {
    let page = 1;
    let total = 0;
    let fetched = 0;

    while (true) {
      if (this.cancelled) throw new Error("Cancelled");

      const url = urlMutator(this.job.link, page);
      const response = await fetch(url, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${this.creds.daricommaToken}`,
          "Content-Type": "application/json",
        },
      });
      if (!response.ok) throw new Error(`HTTP ${response.status} on page ${page}`);

      const data = await response.json();
      const questions = data.data?.questions || [];
      total = data.data?.total_questions ?? total;

      let dupes = 0;
      for (const q of questions) {
        const id = q.id;
        const fp = qFingerprint(q);
        const isDup = (id && this.seenIds.has(id)) || (fp && this.seenTexts.has(fp));

        if (isDup) {
          dupes++;
          const existingQ = (id && this.seenIdMap.get(id)) || (fp && this.seenFpMap.get(fp)) || null;
          const incomingPri = qPriority(q);
          const existingPri = existingQ ? qPriority(existingQ) : -1;

          if (incomingPri > existingPri && existingQ) {
            // Incoming is better — replace existing in questionsRaw
            const rawIdx = this.questionsRaw.indexOf(existingQ);
            if (rawIdx !== -1) this.questionsRaw[rawIdx] = q;
            if (id) this.seenIdMap.set(id, q);
            if (fp) this.seenFpMap.set(fp, q);
            this.duplicates.push({
              id: existingQ.id || null,
              fingerprint_preview: (qFingerprint(existingQ) || "").slice(0, 100),
              duplicate_data: extractRawPreview(existingQ),
              original_data: extractRawPreview(q),
            });
          } else {
            this.duplicates.push({
              id: id || null,
              fingerprint_preview: (fp || "").slice(0, 100),
              duplicate_data: extractRawPreview(q),
              original_data: existingQ ? extractRawPreview(existingQ) : null,
            });
          }
          continue;
        }

        if (id) { this.seenIds.add(id); this.seenIdMap.set(id, q); }
        if (fp) { this.seenTexts.add(fp); this.seenFpMap.set(fp, q); }
        this.questionsRaw.push(q);
      }

      fetched += questions.length;
      this.progress("fetch", { page, fetched, total, dupes });
      this.log(
        `Fetched page ${page} (${fetched}/${total})${dupes > 0 ? ` — ${dupes} duplicate(s) skipped` : ""}`
      );

      if (fetched >= total || questions.length === 0) break;
      page++;
      await sleep(1000);
    }
  }

  // ─── RUN ────────────────────────────────────────────────────────────────────
  async run() {
    const { meta, uploadImages, enhanceText } = this.job;

    this.log(`Starting job for ${meta.class}/${meta.subject} — Chapter ${meta.chapter_no}`);

    // Phase 1: fetch
    await this.fetchAllPages();

    if (this.cancelled) throw new Error("Cancelled");

    // Phase 2: transform
    this.log(`Transforming ${this.questionsRaw.length} questions${uploadImages ? " & uploading images" : ""}...`);
    const transformed = [];
    for (let i = 0; i < this.questionsRaw.length; i++) {
      if (this.cancelled) throw new Error("Cancelled");
      const rec = await this.transformQuestion(this.questionsRaw[i], meta, uploadImages);
      transformed.push(rec);
      this.emitQuestion(i, rec);
      this.progress("transform", { done: i + 1, total: this.questionsRaw.length });
    }

    // Phase 3: Gemini
    let finalQuestions = transformed;
    if (enhanceText) {
      const hasTextIn = (content) =>
        Array.isArray(content) && content.some((item) => item.type === "text" && item.value.trim());

      const pendingIndices = [];
      for (let i = 0; i < transformed.length; i++) {
        const doExplanation = hasTextIn(transformed[i].explanation);
        const doAnswer = hasTextIn(transformed[i].answer);
        if (!doExplanation && !doAnswer) {
          transformed[i]._phase = "ai_skipped";
          this.emitQuestion(i, transformed[i]);
        } else {
          pendingIndices.push(i);
        }
      }

      const concurrency = Math.max(1, parseInt(this.creds.geminiConcurrency, 10) || 8);
      const rateLimitMs = Math.max(0, parseInt(this.creds.geminiRateLimitMs, 10) || 0);
      this.log(
        `Gemini pass: ${pendingIndices.length} questions to process (concurrency=${concurrency}` +
          (rateLimitMs ? `, delay=${rateLimitMs}ms` : "") +
          `)`
      );

      let done = 0;
      let cursor = 0;
      const total = pendingIndices.length;

      const worker = async () => {
        while (true) {
          if (this.cancelled) return;
          const myTurn = cursor++;
          if (myTurn >= pendingIndices.length) return;
          const i = pendingIndices[myTurn];

          const doExplanation = hasTextIn(transformed[i].explanation);
          const doAnswer = hasTextIn(transformed[i].answer);

          transformed[i]._phase = "ai_enhancing";
          this.emitQuestion(i, transformed[i]);

          try {
            const [enhanced, polished] = await Promise.all([
              doExplanation ? this.enhanceExplanation(transformed[i].explanation, meta) : null,
              doAnswer ? this.polishSolution(transformed[i].answer, meta) : null,
            ]);

            if (enhanced) {
              transformed[i] = {
                ...transformed[i],
                explanation: enhanced,
                _ai_enhanced_explanation: true,
              };
            }
            if (polished) {
              transformed[i] = {
                ...transformed[i],
                answer: polished,
                _ai_enhanced_answer: true,
              };
            }
            transformed[i]._phase = "ai_done";
            this.emitQuestion(i, transformed[i]);
          } catch (err) {
            this.aiFailures.push({
              index: i,
              source_id: transformed[i]._source_id,
              reason: err.message,
            });
            this.log(`Gemini failed at index ${i}: ${err.message} — keeping original`);
            transformed[i]._phase = "ai_failed";
            transformed[i]._ai_error = err.message;
            this.emitQuestion(i, transformed[i]);
          }

          done++;
          this.progress("gemini", { done, total });
          if (rateLimitMs > 0 && done < total) await sleep(rateLimitMs);
        }
      };

      const workerCount = Math.min(concurrency, pendingIndices.length) || 1;
      await Promise.all(Array.from({ length: workerCount }, worker));
      if (this.cancelled) throw new Error("Cancelled");

      finalQuestions = transformed;
    } else {
      // No AI phase — mark all transformed records as finalized.
      for (let i = 0; i < transformed.length; i++) {
        transformed[i]._phase = "ai_skipped";
        this.emitQuestion(i, transformed[i]);
      }
    }

    // Phase 4: write file
    const paper = parseInt(meta.paper, 10) || 1;
    const dirPath = path.join(this.outputRoot, meta.class, meta.subject, `paper-${paper}`);
    fs.mkdirSync(dirPath, { recursive: true });
    const outputPath = path.join(dirPath, `CHAPTER ${meta.chapter_no}.json`);

    // Strip internal (underscore-prefixed) fields before writing
    const toWrite = finalQuestions.map((q) => {
      const out = {};
      for (const [k, v] of Object.entries(q)) {
        if (!k.startsWith("_")) out[k] = v;
      }
      return out;
    });
    fs.writeFileSync(outputPath, JSON.stringify(toWrite, null, 2), "utf-8");

    this.log(`Saved ${toWrite.length} questions to ${outputPath}`);

    return {
      outputPath,
      questions: finalQuestions,
      duplicates: this.duplicates,
      imageFailures: this.imageFailures,
      aiFailures: this.aiFailures,
    };
  }
}

module.exports = { JobRunner };
