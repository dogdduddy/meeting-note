const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const { spawn } = require("child_process");
const path = require("path");
const fs = require("fs");

// ─── 설정 ───────────────────────────────────────────
const PORT = 3456;

const WHISPER_CLI = "build/bin/whisper-cli";
const WHISPER_STREAM = "build/bin/whisper-stream";
const WHISPER_MODEL = "models/ggml-large-v3-turbo-q5_0.bin";
const MEETINGS_DIR = "meetings";

// ─── KST 시간 유틸 ──────────────────────────────────
function kstNow() {
  return new Date(Date.now() + 9 * 60 * 60 * 1000);
}

function kstDirName() {
  const now = kstNow();
  const y = now.getUTCFullYear();
  const mo = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const h = String(now.getUTCHours()).padStart(2, "0");
  const mi = String(now.getUTCMinutes()).padStart(2, "0");
  return `${y}${mo}${d}_${h}${mi}`;
}

function kstISO() {
  const now = kstNow();
  return now.toISOString().replace("Z", "+09:00");
}

// ─── STT 프롬프트 생성 ──────────────────────────────
const MODE_PROMPTS = {
  company:
    "회사 업무 회의를 시작하겠습니다. 참석자 여러분 안녕하세요. 오늘 안건을 논의하겠습니다. 스프린트, 배포, API, 서버, 클라이언트, 백엔드, 프론트엔드, 일정, 마일스톤, QA, 리뷰, PR, 머지, 디자인, 기획, KPI, OKR, 회고.",
  personal:
    "개인 메모를 녹음합니다. 일상 대화, 아이디어 정리, 독서 메모, 일기, 감상, 계획, 할 일 목록.",
};

function buildWhisperPrompt(mode, topic) {
  let prompt = MODE_PROMPTS[mode] || MODE_PROMPTS.company;
  if (topic && topic.trim().length > 0) {
    prompt += ` 오늘 주제: ${topic.trim()}.`;
  }
  return prompt;
}

// ─── 환각 필터 ──────────────────────────────────────
const HALLUCINATION_PATTERNS = [
  /MBC/i, /KBS/i, /SBS/i, /YTN/i, /JTBC/i, /TV조선/i, /채널A/i,
  /뉴스입니다/, /뉴스였습니다/, /앵커/, /리포터/,
  /구독.*좋아요/, /채널.*구독/, /좋아요.*구독/, /알림.*설정/,
  /시청해/, /시청자/, /영상.*끝/,
  /자막.*제공/, /번역.*자막/, /자막.*번역/,
  /감사합니다\.*$/, /고맙습니다\.*$/,
  /^\(.*\)$/, /^\[.*\]$/,
  /^\.+$/, /^,+$/, /^!+$/,
  /^(아|어|음|으|응|네|예)+$/,
  /空/, /♪/, /♫/, /🎵/,
  /(.)\1{3,}/,
  /^.{1,2}$/,
];

function isHallucination(text) {
  const trimmed = text.trim();
  if (trimmed.length < 3) return true;
  if (HALLUCINATION_PATTERNS.some((p) => p.test(trimmed))) return true;
  return false;
}

// ─── 상태 ───────────────────────────────────────────
let soxProcess = null;
let whisperStreamProc = null;
let isRecording = false;
let isPaused = false;
let currentMeetingDir = null;
let currentPrompt = "";
let currentMeta = null;
let realtimeLines = [];   // 실시간 STT 결과
let transcriptLines = []; // whisper-cli 정밀 결과 (기존 호환)
let fileSizeInterval = null;
let recordingWavPath = "";

// ─── Express + WebSocket ────────────────────────────
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

function broadcast(type, data) {
  const msg = JSON.stringify({ type, ...data });
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(msg);
  });
}

// ─── whisper-stream 실시간 STT ──────────────────────
function startWhisperStream(prompt) {
  if (!fs.existsSync(WHISPER_STREAM)) {
    console.log("[System] whisper-stream 바이너리 없음 — 실시간 STT 건너뜀");
    return;
  }

  const args = [
    "-m", WHISPER_MODEL,
    "-l", "ko",
    "--step", "3000",
    "--length", "8000",
    "-vth", "0.6",
    "--keep", "200",
    "-c", "1",
  ];
  if (prompt) args.push("--prompt", prompt);

  console.log(`[System] whisper-stream 시작`);

  whisperStreamProc = spawn(WHISPER_STREAM, args, { cwd: __dirname });

  let buffer = "";
  whisperStreamProc.stdout.on("data", (chunk) => {
    buffer += chunk.toString();
    const lines = buffer.split("\n");
    buffer = lines.pop(); // 마지막 불완전한 라인은 버퍼 유지

    for (const raw of lines) {
      const trimmed = raw.trim();
      if (!trimmed) continue;

      // whisper-stream 출력: [00:00:00.000 --> 00:00:03.000]  텍스트
      const match = trimmed.match(/\[.*?\]\s*(.*)/);
      const text = match ? match[1].trim() : trimmed;

      if (text.length < 2) continue;
      if (/^\[.*\]$/.test(text)) continue;
      if (isHallucination(text)) continue;

      const entry = { time: kstISO(), text, source: "realtime" };
      realtimeLines.push(entry);
      broadcast("transcript", { line: entry });
    }
  });

  whisperStreamProc.stderr.on("data", (chunk) => {
    const msg = chunk.toString().trim();
    if (msg) console.log(`[whisper-stream] ${msg}`);
  });

  whisperStreamProc.on("close", (code) => {
    console.log(`[System] whisper-stream 종료 (code: ${code})`);
    whisperStreamProc = null;
  });

  whisperStreamProc.on("error", (err) => {
    console.error(`[whisper-stream Error] ${err.message}`);
    whisperStreamProc = null;
  });
}

function stopWhisperStream() {
  return new Promise((resolve) => {
    if (!whisperStreamProc) { resolve(); return; }
    whisperStreamProc.on("close", () => resolve());
    whisperStreamProc.kill("SIGTERM");
    setTimeout(() => {
      if (whisperStreamProc) {
        whisperStreamProc.kill("SIGKILL");
        resolve();
      }
    }, 3000);
  });
}

// ─── sox 녹음 ───────────────────────────────────────
function startSoxRecording(wavPath) {
  soxProcess = spawn("sox", ["-d", "-r", "16000", "-c", "1", "-b", "16", wavPath], {
    cwd: __dirname,
  });

  soxProcess.stderr.on("data", (chunk) => {
    const msg = chunk.toString().trim();
    if (msg) console.log(`[Sox] ${msg}`);
  });

  soxProcess.on("error", (err) => {
    console.error(`[Sox Error] ${err.message}`);
    broadcast("recording-error", {
      message: `녹음 실패: ${err.message}. sox가 설치되어 있는지 확인하세요 (brew install sox)`,
    });
  });

  soxProcess.on("close", (code) => {
    console.log(`[System] Sox 프로세스 종료 (exit code: ${code})`);
    soxProcess = null;
  });

  // 파일 크기 모니터링
  let lastSize = 0;
  let staleCount = 0;

  fileSizeInterval = setInterval(() => {
    try {
      if (fs.existsSync(wavPath)) {
        const stat = fs.statSync(wavPath);
        const sizeMB = (stat.size / 1024 / 1024).toFixed(2);
        if (stat.size > lastSize) {
          staleCount = 0;
          broadcast("recording-status", { fileSize: sizeMB, growing: true });
        } else {
          staleCount++;
          if (staleCount >= 3) {
            broadcast("recording-status", {
              fileSize: sizeMB,
              growing: false,
              warning: "파일 크기가 변하지 않고 있습니다. 마이크를 확인하세요.",
            });
          }
        }
        lastSize = stat.size;
      }
    } catch {}
  }, 1000);
}

function stopSoxRecording() {
  return new Promise((resolve) => {
    if (fileSizeInterval) {
      clearInterval(fileSizeInterval);
      fileSizeInterval = null;
    }
    if (!soxProcess) { resolve(); return; }
    soxProcess.on("close", () => resolve());
    soxProcess.kill("SIGINT");
    setTimeout(() => {
      if (soxProcess) { soxProcess.kill("SIGKILL"); resolve(); }
    }, 5000);
  });
}

// ─── whisper-cli 정밀 변환 ──────────────────────────
function runWhisperCli(wavPath, prompt) {
  return new Promise((resolve, reject) => {
    const args = [
      "-m", WHISPER_MODEL,
      "-f", wavPath,
      "-l", "ko",
      "-bs", "5",
      "-bo", "5",
      "--no-timestamps",
    ];
    if (prompt) args.push("--prompt", prompt);

    console.log(`[System] Whisper-cli 실행: ${args.join(" ")}`);

    const whisper = spawn(WHISPER_CLI, args, { cwd: __dirname });
    let stdout = "";
    let stderr = "";

    whisper.stdout.on("data", (chunk) => {
      const text = chunk.toString();
      stdout += text;

      // 정밀 변환 결과도 실시간으로 라인별 파싱
      const lines = text.split("\n").filter((l) => l.trim());
      for (const line of lines) {
        const cleaned = line
          .replace(/\[\d{2}:\d{2}[:\.][\d.]+ --> \d{2}:\d{2}[:\.][\d.]+\]\s*/g, "")
          .replace(/\[.*?\]/g, "")
          .replace(/^[\s\-–—]+/, "")
          .trim();
        if (cleaned.length >= 3 && !isHallucination(cleaned)) {
          transcriptLines.push({ time: kstISO(), text: cleaned });
        }
      }
    });

    whisper.stderr.on("data", (chunk) => {
      const msg = chunk.toString().trim();
      stderr += msg;
      if (msg.includes("progress")) {
        broadcast("transcribing", { status: "progress", message: msg });
      }
    });

    whisper.on("close", (code) => {
      console.log(`[System] Whisper-cli 종료 (exit code: ${code})`);
      if (code === 0) resolve(stdout);
      else reject(new Error(`whisper-cli 실패 (code: ${code}): ${stderr}`));
    });

    whisper.on("error", (err) => reject(err));
  });
}

// ─── Gemini CLI: 텍스트 병합 (교차 검증) ────────────
function runGeminiMerge(realtimeText, preciseText, meta) {
  return new Promise((resolve, reject) => {
    const modeLabel = meta.mode === "personal" ? "개인 메모" : "회사 업무 회의";
    const topicLine = meta.topic ? `주제: ${meta.topic}` : "";

    const prompt = `당신은 음성 인식 텍스트 교정 전문가입니다.

같은 음성을 두 가지 방식으로 STT 변환한 결과가 있습니다:

1. **실시간 STT** (whisper-stream): 녹음 중 실시간 변환. 빠르지만 부정확할 수 있음.
2. **정밀 STT** (whisper-cli): 녹음 완료 후 전체 파일 정밀 변환. 더 정확하지만 여전히 오류 가능.

두 텍스트를 교차 비교하여 **하나의 최종 교정 텍스트**를 만들어주세요.

규칙:
- 양쪽에 공통으로 나타나는 내용 → 높은 신뢰도로 채택
- 한쪽에만 있는 내용 → 문맥상 자연스러우면 포함, 환각이면 제거
- 맞춤법·띄어쓰기 교정
- 전문 용어·고유명사는 더 정확해 보이는 쪽 채택
- 시간 순서 유지, 중복 제거
- 뉴스 앵커 멘트, 유튜브 구독 문구 등 환각 패턴 완전 제거
- **결과는 순수 텍스트만 출력** (마크다운 서식·설명·주석 없이)

맥락:
- 유형: ${modeLabel}
${topicLine ? `- ${topicLine}` : ""}

---
## 실시간 STT 결과:
${realtimeText || "(없음)"}

---
## 정밀 STT 결과:
${preciseText || "(없음)"}

---
위 두 텍스트를 교차 검증하여 최종 교정 텍스트만 출력하세요.`;

    // 프롬프트 저장
    if (currentMeetingDir) {
      fs.writeFileSync(path.join(currentMeetingDir, "merge_prompt.txt"), prompt, "utf-8");
    }

    console.log("[System] Gemini 텍스트 병합 시작");

    const gemini = spawn("gemini", ["--output-format", "json"], {
      cwd: currentMeetingDir || __dirname,
      stdio: ["pipe", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";

    gemini.stdout.on("data", (chunk) => { stdout += chunk.toString(); });
    gemini.stderr.on("data", (chunk) => { stderr += chunk.toString(); });

    gemini.on("close", (code) => {
      if (code === 0) {
        let result = stdout.trim();
        try {
          const parsed = JSON.parse(result);
          result = parsed.response || parsed.text || parsed.candidates?.[0]?.content?.parts?.[0]?.text || result;
        } catch {}
        console.log("[System] Gemini 텍스트 병합 완료");
        resolve(typeof result === "string" ? result.trim() : JSON.stringify(result));
      } else {
        reject(new Error(`Gemini merge 실패 (code: ${code}): ${stderr}`));
      }
    });

    gemini.on("error", (err) => reject(err));
    gemini.stdin.write(prompt);
    gemini.stdin.end();
  });
}

// ─── Gemini CLI: 요약 ───────────────────────────────
function runGeminiSummary(text, meta) {
  return new Promise((resolve, reject) => {
    let metaContext = "";
    if (meta.topic) metaContext += `회의 주제: ${meta.topic}\n`;
    if (meta.mode === "personal") metaContext += `이것은 개인 메모/녹음이다.\n`;
    else metaContext += `이것은 회사 업무 회의이다.\n`;

    const prompt = `아래는 회의 트랜스크립트이다.
도구를 사용하지 말고 텍스트로만 응답해라.
화자별로 정리하고 핵심 논의사항과 액션아이템을 요약해서 마크다운으로 작성해라.
${metaContext}
---

${text}`;

    if (currentMeetingDir) {
      fs.writeFileSync(path.join(currentMeetingDir, "summary_prompt.txt"), prompt, "utf-8");
    }

    const gemini = spawn("gemini", ["--output-format", "json"], {
      cwd: currentMeetingDir || __dirname,
      stdio: ["pipe", "pipe", "pipe"],
    });

    let stdout = "";
    gemini.stdout.on("data", (chunk) => { stdout += chunk.toString(); });
    gemini.stderr.on("data", () => {});

    gemini.on("close", (code) => {
      let summary = stdout.trim();
      try {
        const parsed = JSON.parse(summary);
        summary = parsed.response || parsed.text || parsed.candidates?.[0]?.content?.parts?.[0]?.text || summary;
      } catch {}
      if (typeof summary !== "string") summary = JSON.stringify(summary);

      if (currentMeetingDir) {
        fs.writeFileSync(path.join(currentMeetingDir, "summary.md"), summary, "utf-8");
      }
      resolve(summary);
    });

    gemini.on("error", (err) => reject(err));
    gemini.stdin.write(prompt);
    gemini.stdin.end();
  });
}

// ─── API: 상태 확인 ─────────────────────────────────
app.get("/api/status", (req, res) => {
  res.json({
    isRecording,
    isPaused,
    currentMeetingDir,
    lineCount: realtimeLines.length,
    whisperExists: fs.existsSync(WHISPER_CLI),
    whisperStreamExists: fs.existsSync(WHISPER_STREAM),
    modelExists: fs.existsSync(WHISPER_MODEL),
    whisperDir: __dirname,
  });
});

// ─── API: 녹음 시작 ─────────────────────────────────
app.post("/api/start", (req, res) => {
  if (isRecording && !isPaused) return res.status(400).json({ error: "이미 녹음 중" });

  if (!fs.existsSync(WHISPER_CLI)) {
    return res.status(500).json({ error: `whisper-cli를 찾을 수 없습니다: ${WHISPER_CLI}` });
  }
  if (!fs.existsSync(WHISPER_MODEL)) {
    return res.status(500).json({ error: `모델 파일을 찾을 수 없습니다: ${WHISPER_MODEL}` });
  }

  if (!isPaused) {
    // 새 세션
    currentMeetingDir = path.join(MEETINGS_DIR, kstDirName());
    fs.mkdirSync(currentMeetingDir, { recursive: true });
    realtimeLines = [];
    transcriptLines = [];

    const mode = req.body.mode || "company";
    const topic = req.body.topic || "";
    currentPrompt = buildWhisperPrompt(mode, topic);
    currentMeta = { mode, topic, prompt: currentPrompt, startedAt: kstISO() };

    console.log(`[System] 모드: ${mode}, 주제: "${topic}"`);
    fs.writeFileSync(path.join(currentMeetingDir, "meta.json"), JSON.stringify(currentMeta, null, 2), "utf-8");

    // whisper-stream 실시간 STT 시작
    startWhisperStream(currentPrompt);
  } else {
    // 일시정지에서 재개 — whisper-stream도 재시작
    startWhisperStream(currentPrompt);
  }

  // sox 녹음 시작
  const partNum = fs.readdirSync(currentMeetingDir).filter((f) => f.startsWith("part_") && f.endsWith(".wav")).length;
  recordingWavPath = path.join(currentMeetingDir, `part_${String(partNum).padStart(3, "0")}.wav`);

  isRecording = true;
  isPaused = false;

  startSoxRecording(recordingWavPath);

  broadcast("status", { isRecording: true, isPaused: false });
  res.json({ ok: true, meetingDir: currentMeetingDir });
});

// ─── API: 일시정지 ──────────────────────────────────
app.post("/api/pause", async (req, res) => {
  if (!isRecording || isPaused) {
    return res.status(400).json({ error: "녹음 중이 아니거나 이미 일시정지됨" });
  }

  isPaused = true;
  await stopSoxRecording();
  await stopWhisperStream();

  broadcast("status", { isRecording: true, isPaused: true });
  res.json({ ok: true });
});

// ─── API: 재개 ──────────────────────────────────────
app.post("/api/resume", (req, res) => {
  if (!isRecording || !isPaused) {
    return res.status(400).json({ error: "일시정지 상태가 아닙니다" });
  }

  isPaused = false;

  const partNum = fs.readdirSync(currentMeetingDir).filter((f) => f.startsWith("part_") && f.endsWith(".wav")).length;
  recordingWavPath = path.join(currentMeetingDir, `part_${String(partNum).padStart(3, "0")}.wav`);

  startSoxRecording(recordingWavPath);
  startWhisperStream(currentPrompt);

  broadcast("status", { isRecording: true, isPaused: false });
  res.json({ ok: true });
});

// ─── API: 녹음 종료 → 정밀 STT → Gemini 병합 ──────
app.post("/api/stop", async (req, res) => {
  if (!isRecording) return res.status(400).json({ error: "녹음 중이 아닙니다" });

  isRecording = false;

  if (!isPaused) {
    await stopSoxRecording();
    await stopWhisperStream();
  }
  isPaused = false;

  // ① 실시간 STT 결과 저장
  const realtimeText = realtimeLines.map((l) => l.text).join("\n");
  if (currentMeetingDir) {
    fs.writeFileSync(path.join(currentMeetingDir, "realtime.txt"), realtimeText, "utf-8");
  }

  // WAV 파트 합치기
  const parts = fs
    .readdirSync(currentMeetingDir)
    .filter((f) => f.startsWith("part_") && f.endsWith(".wav"))
    .sort()
    .map((f) => path.join(currentMeetingDir, f));

  if (parts.length === 0) {
    broadcast("status", { isRecording: false, isPaused: false });
    return res.status(400).json({ error: "녹음된 파일이 없습니다" });
  }

  const fullWavPath = path.join(currentMeetingDir, "full.wav");

  try {
    if (parts.length === 1) {
      fs.copyFileSync(parts[0], fullWavPath);
    } else {
      await new Promise((resolve, reject) => {
        const concat = spawn("sox", [...parts, fullWavPath], { cwd: __dirname });
        concat.on("close", (code) => {
          if (code === 0) resolve();
          else reject(new Error(`sox concat 실패 (code: ${code})`));
        });
        concat.on("error", reject);
      });
    }

    const stat = fs.statSync(fullWavPath);
    const durationSec = Math.floor(stat.size / (16000 * 2));
    console.log(`[System] 녹음 완료: ${(stat.size / 1024 / 1024).toFixed(1)}MB, ~${durationSec}초`);

    broadcast("status", { isRecording: false, isPaused: false });
    res.json({ ok: true, duration: durationSec, fileSize: stat.size });

    // ─── 백그라운드 파이프라인 ───
    // STEP 1: 정밀 STT
    broadcast("transcribing", {
      status: "start",
      step: "precise",
      duration: durationSec,
      message: `정밀 STT 변환 중... (~${Math.ceil(durationSec / 60)}분 분량)`,
    });

    transcriptLines = [];
    await runWhisperCli(fullWavPath, currentPrompt);

    const preciseText = transcriptLines.map((l) => l.text).join("\n");
    fs.writeFileSync(path.join(currentMeetingDir, "precise.txt"), preciseText, "utf-8");

    broadcast("transcribing", {
      status: "precise-done",
      step: "precise",
      message: "정밀 STT 완료",
    });

    // ② 정밀 텍스트를 프론트에 전송 (비교용)
    broadcast("precise-transcript", {
      text: preciseText,
      lineCount: transcriptLines.length,
    });

    // STEP 2: Gemini 교차 검증 병합
    broadcast("transcribing", {
      status: "merging",
      step: "merge",
      message: "Gemini가 두 텍스트를 교차 검증 중...",
    });

    let mergedText = preciseText; // 폴백
    try {
      mergedText = await runGeminiMerge(realtimeText, preciseText, currentMeta);
    } catch (err) {
      console.error(`[System] Gemini 병합 실패, 정밀 텍스트로 폴백: ${err.message}`);
      broadcast("transcribing", {
        status: "merge-fallback",
        message: "Gemini 병합 실패 — 정밀 STT 텍스트를 사용합니다",
      });
    }

    fs.writeFileSync(path.join(currentMeetingDir, "merged.txt"), mergedText, "utf-8");

    // ③ 최종 병합 텍스트를 프론트에 전송
    broadcast("merged-transcript", {
      text: mergedText,
      lineCount: mergedText.split("\n").filter((l) => l.trim()).length,
    });

    broadcast("transcribing", { status: "done" });
  } catch (err) {
    console.error(`[Error] ${err.message}`);
    broadcast("status", { isRecording: false, isPaused: false });
    broadcast("transcribing", { status: "error", message: err.message });
  }
});

// ─── API: Gemini 요약 ───────────────────────────────
app.post("/api/summarize", async (req, res) => {
  if (isRecording) return res.status(400).json({ error: "녹음 중에는 요약 불가" });

  // merged > precise > realtime 순서로 텍스트 결정
  let text = "";
  if (currentMeetingDir) {
    const mergedPath = path.join(currentMeetingDir, "merged.txt");
    const precisePath = path.join(currentMeetingDir, "precise.txt");
    const realtimePath = path.join(currentMeetingDir, "realtime.txt");

    if (fs.existsSync(mergedPath)) text = fs.readFileSync(mergedPath, "utf-8");
    else if (fs.existsSync(precisePath)) text = fs.readFileSync(precisePath, "utf-8");
    else if (fs.existsSync(realtimePath)) text = fs.readFileSync(realtimePath, "utf-8");
  }

  if (!text || text.trim().length < 10) {
    return res.status(400).json({ error: "요약할 텍스트가 부족합니다" });
  }

  let meta = currentMeta || { mode: "company", topic: "" };
  if (currentMeetingDir) {
    const metaPath = path.join(currentMeetingDir, "meta.json");
    if (fs.existsSync(metaPath)) {
      try { meta = JSON.parse(fs.readFileSync(metaPath, "utf-8")); } catch {}
    }
  }

  broadcast("status", { summarizing: true });
  res.json({ ok: true });

  try {
    const summary = await runGeminiSummary(text, meta);
    broadcast("status", { summarizing: false });
    broadcast("summary", { content: summary });
  } catch (err) {
    console.error(`[Error] 요약 실패: ${err.message}`);
    broadcast("status", { summarizing: false });
    broadcast("recording-error", { message: `요약 실패: ${err.message}` });
  }
});

// ─── API: 과거 회의 목록 ────────────────────────────
app.get("/api/meetings", (req, res) => {
  if (!fs.existsSync(MEETINGS_DIR)) return res.json([]);

  const dirs = fs
    .readdirSync(MEETINGS_DIR)
    .filter((d) => fs.statSync(path.join(MEETINGS_DIR, d)).isDirectory())
    .sort()
    .reverse();

  const meetings = dirs.map((d) => {
    const dir = path.join(MEETINGS_DIR, d);
    const item = {
      id: d,
      hasSummary: fs.existsSync(path.join(dir, "summary.md")),
      hasTranscript:
        fs.existsSync(path.join(dir, "merged.txt")) ||
        fs.existsSync(path.join(dir, "precise.txt")) ||
        fs.existsSync(path.join(dir, "realtime.txt")),
    };
    const metaPath = path.join(dir, "meta.json");
    if (fs.existsSync(metaPath)) {
      try {
        const meta = JSON.parse(fs.readFileSync(metaPath, "utf-8"));
        item.mode = meta.mode;
        item.topic = meta.topic;
      } catch {}
    }
    return item;
  });

  res.json(meetings);
});

// ─── API: 회의 상세 조회 ────────────────────────────
app.get("/api/meetings/:id", (req, res) => {
  const dir = path.join(MEETINGS_DIR, req.params.id);
  if (!fs.existsSync(dir)) return res.status(404).json({ error: "회의를 찾을 수 없습니다" });

  const result = { id: req.params.id };

  const metaPath = path.join(dir, "meta.json");
  if (fs.existsSync(metaPath)) {
    try { result.meta = JSON.parse(fs.readFileSync(metaPath, "utf-8")); } catch {}
  }

  // merged > precise > realtime 순서
  if (fs.existsSync(path.join(dir, "merged.txt"))) {
    result.transcript = fs.readFileSync(path.join(dir, "merged.txt"), "utf-8");
    result.transcriptSource = "merged";
  } else if (fs.existsSync(path.join(dir, "precise.txt"))) {
    result.transcript = fs.readFileSync(path.join(dir, "precise.txt"), "utf-8");
    result.transcriptSource = "precise";
  } else if (fs.existsSync(path.join(dir, "realtime.txt"))) {
    result.transcript = fs.readFileSync(path.join(dir, "realtime.txt"), "utf-8");
    result.transcriptSource = "realtime";
  }

  if (fs.existsSync(path.join(dir, "summary.md"))) {
    result.summary = fs.readFileSync(path.join(dir, "summary.md"), "utf-8");
  }

  // 과거 회의 열람 시 현재 디렉토리 업데이트 (요약 버튼용)
  currentMeetingDir = dir;

  res.json(result);
});

// ─── API: 요약 파일 다운로드 ────────────────────────
app.get("/api/meetings/:id/download", (req, res) => {
  const dir = path.join(MEETINGS_DIR, req.params.id);
  const summaryPath = path.join(dir, "summary.md");
  if (!fs.existsSync(summaryPath)) {
    return res.status(404).json({ error: "요약 파일이 없습니다" });
  }
  const dateStr = req.params.id.replace(/(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})/, "$1-$2-$3_$4시$5분");
  res.download(summaryPath, `회의록_${dateStr}.md`);
});

// ─── 서버 시작 ──────────────────────────────────────
server.listen(PORT, () => {
  console.log("");
  console.log("  ╔══════════════════════════════════════════╗");
  console.log("  ║   📝 Meeting Minutes AI  (Dual STT)     ║");
  console.log(`  ║   http://localhost:${PORT}                 ║`);
  console.log("  ╠══════════════════════════════════════════╣");
  console.log(`  ║  whisper-cli:    ${fs.existsSync(WHISPER_CLI) ? "✅ found" : "❌ not found"}              ║`);
  console.log(`  ║  whisper-stream: ${fs.existsSync(WHISPER_STREAM) ? "✅ found" : "❌ not found"}              ║`);
  console.log(`  ║  Model:          ${fs.existsSync(WHISPER_MODEL) ? "✅ found" : "❌ not found"}              ║`);
  console.log("  ╚══════════════════════════════════════════╝");
  console.log("");
  console.log("  파이프라인: 실시간STT → 정밀STT → Gemini병합 → 요약");
  console.log("");
});
