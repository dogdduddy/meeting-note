const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const { spawn } = require("child_process");
const path = require("path");
const fs = require("fs");

// ─── 설정 ───────────────────────────────────────────
const PORT = 3456;

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

let lastTranscriptText = "";

function isHallucination(text) {
  const trimmed = text.trim();
  if (trimmed.length < 3) return true;
  if (trimmed === lastTranscriptText) return true;
  if (HALLUCINATION_PATTERNS.some((p) => p.test(trimmed))) return true;
  return false;
}

// ─── 상태 ───────────────────────────────────────────
let whisperProcess = null;
let isRecording = false;
let isPaused = false;
let currentMeetingDir = null;
let transcriptLines = [];

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

// ─── 라인 파싱 (공통) ───────────────────────────────
function processRawLine(rawLine) {
  let stripped = rawLine.replace(/\x1b\[[0-9;]*[A-Za-z]/g, "");

  if (stripped.includes("\r")) {
    const segments = stripped.split("\r");
    stripped = segments[segments.length - 1];
  }

  stripped = stripped.replace(/\[\d{2}:\d{2}[:\.][\d.]+ --> \d{2}:\d{2}[:\.][\d.]+\]\s*/g, "");
  stripped = stripped.replace(/\[.*?\]/g, "");
  const cleaned = stripped.replace(/^[\s\-–—]+/, "").trim();

  if (cleaned.length >= 3 && !isHallucination(cleaned)) {
    const entry = { time: kstISO(), text: cleaned };
    transcriptLines.push(entry);
    lastTranscriptText = cleaned;
    broadcast("transcript", { line: entry });
  }
}

// ─── whisper-stream 프로세스 시작 (공통) ─────────────
let whisperBuffer = "";
let whisperClosePromise = null;

function spawnWhisper() {
  whisperBuffer = "";

  whisperProcess = spawn(WHISPER_STREAM, [
    "-m", WHISPER_MODEL,
    "-l", "ko",
    "-t", "4",
    "--step", "500",
    "--length", "5000",
    "-vth", "0.8",
  ], {
    cwd: __dirname,
  });

  // close 시 resolve될 promise
  whisperClosePromise = new Promise((resolve) => {
    whisperProcess.stdout.on("data", (chunk) => {
      whisperBuffer += chunk.toString();

      while (whisperBuffer.includes("\n")) {
        const idx = whisperBuffer.indexOf("\n");
        const rawLine = whisperBuffer.slice(0, idx);
        whisperBuffer = whisperBuffer.slice(idx + 1);
        processRawLine(rawLine);
      }
    });

    whisperProcess.stderr.on("data", (chunk) => {
      const msg = chunk.toString().trim();
      if (msg) {
        console.log(`[Whisper Log] ${msg}`);
        broadcast("log", { message: msg });
      }
    });

    whisperProcess.on("close", (code) => {
      console.log(`[System] Whisper 프로세스 종료 (exit code: ${code})`);

      // 버퍼에 남은 마지막 텍스트 처리
      if (whisperBuffer.trim().length > 0) {
        console.log(`[System] 버퍼 잔여 텍스트 처리: ${whisperBuffer.trim().length}자`);
        processRawLine(whisperBuffer);
        whisperBuffer = "";
      }

      whisperProcess = null;
      if (!isPaused) {
        isRecording = false;
        broadcast("status", { isRecording: false, isPaused: false, code });
      }

      resolve(code);
    });
  });
}

// ─── API: 상태 확인 ─────────────────────────────────
app.get("/api/status", (req, res) => {
  res.json({
    isRecording,
    isPaused,
    currentMeetingDir,
    lineCount: transcriptLines.length,
    whisperExists: fs.existsSync(WHISPER_STREAM),
    modelExists: fs.existsSync(WHISPER_MODEL),
    whisperDir: __dirname,
  });
});

// ─── API: 녹음 시작 ─────────────────────────────────
app.post("/api/start", (req, res) => {
  if (isRecording && !isPaused) return res.status(400).json({ error: "이미 녹음 중" });

  if (!fs.existsSync(WHISPER_STREAM)) {
    return res.status(500).json({ error: `whisper-stream을 찾을 수 없습니다: ${WHISPER_STREAM}` });
  }
  if (!fs.existsSync(WHISPER_MODEL)) {
    return res.status(500).json({ error: `모델 파일을 찾을 수 없습니다: ${WHISPER_MODEL}` });
  }

  if (!isPaused) {
    currentMeetingDir = path.join(MEETINGS_DIR, kstDirName());
    fs.mkdirSync(currentMeetingDir, { recursive: true });
    transcriptLines = [];
    lastTranscriptText = "";
  }

  isRecording = true;
  isPaused = false;
  spawnWhisper();

  broadcast("status", { isRecording: true, isPaused: false });
  res.json({ ok: true, meetingDir: currentMeetingDir });
});

// ─── API: 일시정지 ──────────────────────────────────
app.post("/api/pause", async (req, res) => {
  if (!isRecording || isPaused) {
    return res.status(400).json({ error: "녹음 중이 아니거나 이미 일시정지됨" });
  }

  isPaused = true;

  // SIGINT 보내고, 프로세스가 완전히 종료될 때까지 대기 (버퍼 flush)
  if (whisperProcess) {
    whisperProcess.kill("SIGINT");
    if (whisperClosePromise) await whisperClosePromise;
  }

  transcriptLines.push({ time: kstISO(), text: "── 일시정지 ──", isPauseMarker: true });
  broadcast("transcript", { line: transcriptLines[transcriptLines.length - 1] });
  broadcast("status", { isRecording: true, isPaused: true });
  res.json({ ok: true });
});

// ─── API: 재개 ──────────────────────────────────────
app.post("/api/resume", (req, res) => {
  if (!isRecording || !isPaused) {
    return res.status(400).json({ error: "일시정지 상태가 아닙니다" });
  }

  isPaused = false;
  transcriptLines.push({ time: kstISO(), text: "── 재개 ──", isPauseMarker: true });
  broadcast("transcript", { line: transcriptLines[transcriptLines.length - 1] });

  spawnWhisper();

  broadcast("status", { isRecording: true, isPaused: false });
  res.json({ ok: true });
});

// ─── API: 녹음 종료 ─────────────────────────────────
app.post("/api/stop", async (req, res) => {
  if (!isRecording) return res.status(400).json({ error: "녹음 중이 아닙니다" });

  const wasPaused = isPaused;
  isPaused = false;
  isRecording = false;

  // 일시정지 상태가 아니었으면 프로세스 종료 대기 (버퍼 flush)
  if (!wasPaused && whisperProcess) {
    whisperProcess.kill("SIGINT");
    if (whisperClosePromise) await whisperClosePromise;
  }

  // 버퍼 flush 완료 후 파일 저장
  const transcriptPath = path.join(currentMeetingDir, "realtime.txt");
  const content = transcriptLines.filter((l) => !l.isPauseMarker).map((l) => l.text).join("\n");
  fs.writeFileSync(transcriptPath, content, "utf-8");

  broadcast("status", { isRecording: false, isPaused: false });
  res.json({ ok: true, lineCount: transcriptLines.length, transcriptPath });
});

// ─── API: Gemini 요약 ───────────────────────────────
app.post("/api/summarize", async (req, res) => {
  if (isRecording) return res.status(400).json({ error: "녹음 중에는 요약 불가" });
  if (transcriptLines.length === 0) return res.status(400).json({ error: "트랜스크립트가 비어있음" });

  broadcast("status", { summarizing: true });

  const transcript = transcriptLines.filter((l) => !l.isPauseMarker).map((l) => l.text).join("\n");

  const prompt = `아래는 회의 실시간 트랜스크립트이다.
도구를 사용하지 말고 텍스트로만 응답해라.
화자별로 정리하고 핵심 논의사항과 액션아이템을 요약해서 마크다운으로 작성해라.

---

${transcript}`;

  const promptPath = path.join(currentMeetingDir, "prompt.txt");
  fs.writeFileSync(promptPath, prompt, "utf-8");

  try {
    const gemini = spawn("gemini", ["--output-format", "json"], {
      cwd: currentMeetingDir,
      stdio: ["pipe", "pipe", "pipe"],
    });

    gemini.stdin.write(prompt);
    gemini.stdin.end();

    let stdout = "";
    gemini.stdout.on("data", (chunk) => { stdout += chunk.toString(); });
    gemini.stderr.on("data", () => {});

    gemini.on("close", () => {
      let summary = "";
      try {
        const parsed = JSON.parse(stdout);
        summary = parsed.response || stdout;
      } catch {
        summary = stdout;
      }

      const summaryPath = path.join(currentMeetingDir, "summary.md");
      fs.writeFileSync(summaryPath, summary, "utf-8");

      broadcast("status", { summarizing: false });
      broadcast("summary", { content: summary, path: summaryPath });
      res.json({ ok: true, summary, path: summaryPath });
    });

    gemini.on("error", (err) => {
      broadcast("status", { summarizing: false });
      res.status(500).json({ error: `Gemini 실행 실패: ${err.message}` });
    });
  } catch (err) {
    broadcast("status", { summarizing: false });
    res.status(500).json({ error: err.message });
  }
});

// ─── API: 과거 회의 목록 ────────────────────────────
app.get("/api/meetings", (req, res) => {
  if (!fs.existsSync(MEETINGS_DIR)) return res.json([]);

  const dirs = fs.readdirSync(MEETINGS_DIR)
    .filter((d) => fs.statSync(path.join(MEETINGS_DIR, d)).isDirectory())
    .sort()
    .reverse();

  const meetings = dirs.map((d) => {
    const dir = path.join(MEETINGS_DIR, d);
    return {
      id: d,
      hasSummary: fs.existsSync(path.join(dir, "summary.md")),
      hasTranscript: fs.existsSync(path.join(dir, "realtime.txt")),
    };
  });

  res.json(meetings);
});

// ─── API: 회의 상세 조회 ────────────────────────────
app.get("/api/meetings/:id", (req, res) => {
  const dir = path.join(MEETINGS_DIR, req.params.id);
  if (!fs.existsSync(dir)) return res.status(404).json({ error: "회의를 찾을 수 없습니다" });

  const result = { id: req.params.id };

  const transcriptPath = path.join(dir, "realtime.txt");
  if (fs.existsSync(transcriptPath)) {
    result.transcript = fs.readFileSync(transcriptPath, "utf-8");
  }

  const summaryPath = path.join(dir, "summary.md");
  if (fs.existsSync(summaryPath)) {
    result.summary = fs.readFileSync(summaryPath, "utf-8");
  }

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
  console.log("  ╔══════════════════════════════════════╗");
  console.log("  ║   📝 Meeting Minutes AI             ║");
  console.log(`  ║   http://localhost:${PORT}             ║`);
  console.log("  ╠══════════════════════════════════════╣");
  console.log(`  ║  Whisper: ${fs.existsSync(WHISPER_STREAM) ? "✅ found" : "❌ not found"}                  ║`);
  console.log(`  ║  Model:   ${fs.existsSync(WHISPER_MODEL) ? "✅ found" : "❌ not found"}                  ║`);
  console.log("  ╚══════════════════════════════════════╝");
  console.log("");
});
