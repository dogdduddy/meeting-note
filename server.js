const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const { spawn, execSync } = require("child_process");
const path = require("path");
const fs = require("fs");

// ─── 설정 ───────────────────────────────────────────
const PORT = 3456;

// whisper.cpp 경로 (환경변수 또는 기본값)
const WHISPER_STREAM = "build/bin/whisper-stream";
const WHISPER_MODEL = "models/ggml-large-v3-turbo-q5_0.bin";
const MEETINGS_DIR = "meetings";

// 환각 필터 패턴
const HALLUCINATION_PATTERNS = [
  // 뉴스/방송
  /MBC/i, /KBS/i, /SBS/i, /YTN/i, /JTBC/i, /TV조선/i, /채널A/i,
  /뉴스입니다/, /뉴스였습니다/, /앵커/, /리포터/,
  // 유튜브/영상
  /구독.*좋아요/, /채널.*구독/, /좋아요.*구독/, /알림.*설정/,
  /시청해/, /시청자/, /영상.*끝/,
  // 자막/번역
  /자막.*제공/, /번역.*자막/, /자막.*번역/,
  // 일반 환각
  /감사합니다\.*$/, /고맙습니다\.*$/,
  /^\(.*\)$/, /^\[.*\]$/,  // 괄호만 있는 줄
  /^\.+$/, /^,+$/, /^!+$/,  // 구두점만
  /^(아|어|음|으|응|네|예)+$/,  // 감탄사만 반복
  /空/, /♪/, /♫/, /🎵/,  // 특수 기호
  // 중복/반복 (같은 글자가 4번 이상)
  /(.)\1{3,}/,
  // 의미 없는 짧은 조각
  /^.{1,2}$/,
];

// 직전 줄과 동일하면 중복으로 판단
let lastTranscriptText = "";

function isHallucination(text) {
  const trimmed = text.trim();
  if (trimmed.length < 3) return true;
  if (trimmed === lastTranscriptText) return true;  // 중복 제거
  if (HALLUCINATION_PATTERNS.some((p) => p.test(trimmed))) return true;
  return false;
}

// ─── 상태 ───────────────────────────────────────────
let whisperProcess = null;
let isRecording = false;
let currentMeetingDir = null;
let transcriptLines = [];

// ─── Express + WebSocket ────────────────────────────
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// 모든 WebSocket 클라이언트에게 전송
function broadcast(type, data) {
  const msg = JSON.stringify({ type, ...data });
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(msg);
  });
}

// ─── API: 상태 확인 ─────────────────────────────────
app.get("/api/status", (req, res) => {
  // whisper-stream 바이너리 존재 확인
  const whisperExists = fs.existsSync(WHISPER_STREAM);
  const modelExists = fs.existsSync(WHISPER_MODEL);

  res.json({
    isRecording,
    currentMeetingDir,
    lineCount: transcriptLines.length,
    whisperExists,
    modelExists,
    whisperDir: __dirname,
  });
});

// ─── API: 녹음 시작 ─────────────────────────────────
app.post("/api/start", (req, res) => {
  if (isRecording) return res.status(400).json({ error: "이미 녹음 중" });

  if (!fs.existsSync(WHISPER_STREAM)) {
    return res.status(500).json({
      error: `whisper-stream을 찾을 수 없습니다: ${WHISPER_STREAM}`,
    });
  }
  if (!fs.existsSync(WHISPER_MODEL)) {
    return res.status(500).json({
      error: `모델 파일을 찾을 수 없습니다: ${WHISPER_MODEL}`,
    });
  }

  // 회의 디렉토리 생성
  const now = new Date();
  const dirName = now.toISOString().replace(/[-:T]/g, "").slice(0, 13);
  currentMeetingDir = path.join(MEETINGS_DIR, dirName);
  fs.mkdirSync(currentMeetingDir, { recursive: true });

  transcriptLines = [];
  isRecording = true;
  lastTranscriptText = "";

  // whisper-stream 실행
  whisperProcess = spawn(WHISPER_STREAM, [
    "-m", WHISPER_MODEL,
    "-l", "ko",
    "-t", "4",
    "--step", "500",
    "--length", "5000",
    "-vth", "0.8",
//    "--no-fallback",
  ], {
    cwd: __dirname,
  });

  let buffer = "";

  whisperProcess.stdout.on("data", (chunk) => {
    buffer += chunk.toString();

    // 줄바꿈이 있을 때만 처리 (완성된 줄)
    while (buffer.includes("\n")) {
      const idx = buffer.indexOf("\n");
      const rawLine = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 1);

      // 1) ANSI 이스케이프 코드 전부 제거 (\x1b[2K 등)
      let stripped = rawLine.replace(/\x1b\[[0-9;]*[A-Za-z]/g, "");

      // 2) \r로 덮어쓴 부분 결과 → 마지막 것만 취함
      if (stripped.includes("\r")) {
        const segments = stripped.split("\r");
        stripped = segments[segments.length - 1];
      }

      // 3) 타임스탬프 [00:00:00.000 --> 00:00:05.000] 제거
      stripped = stripped.replace(/\[\d{2}:\d{2}[:\.][\d.]+ --> \d{2}:\d{2}[:\.][\d.]+\]\s*/g, "");

      // 4) 남은 대괄호 내용 제거
      stripped = stripped.replace(/\[.*?\]/g, "");

      // 5) 양쪽 공백 및 특수문자 정리
      const cleaned = stripped.replace(/^[\s\-–—]+/, "").trim();

      if (cleaned.length >= 3 && !isHallucination(cleaned)) {
        const entry = {
          time: new Date().toISOString(),
          text: cleaned,
        };
        transcriptLines.push(entry);
        lastTranscriptText = cleaned;
        broadcast("transcript", { line: entry });
      }
    }
  });

whisperProcess.stderr.on("data", (chunk) => {
    const msg = chunk.toString().trim();
    if (msg) {
      // 터미널에서도 에러/상태를 바로 볼 수 있게 추가
      console.log(`[Whisper Log] ${msg}`);
      broadcast("log", { message: msg });
    }
  });

  whisperProcess.on("close", (code) => {
    // 코드가 0이 아니라면 비정상 종료된 것입니다.
    console.log(`[System] Whisper 프로세스 종료 (exit code: ${code})`);
    isRecording = false;
    whisperProcess = null;
    broadcast("status", { isRecording: false, code });
  });

  broadcast("status", { isRecording: true });
  res.json({ ok: true, meetingDir: currentMeetingDir });
});

// ─── API: 녹음 중지 ─────────────────────────────────
app.post("/api/stop", (req, res) => {
  if (!isRecording || !whisperProcess) {
    return res.status(400).json({ error: "녹음 중이 아닙니다" });
  }

  whisperProcess.kill("SIGINT");
  isRecording = false;

  // 트랜스크립트 저장
  const transcriptPath = path.join(currentMeetingDir, "realtime.txt");
  const content = transcriptLines.map((l) => l.text).join("\n");
  fs.writeFileSync(transcriptPath, content, "utf-8");

  res.json({
    ok: true,
    lineCount: transcriptLines.length,
    transcriptPath,
  });
});

// ─── API: Gemini 요약 ───────────────────────────────
app.post("/api/summarize", async (req, res) => {
  if (isRecording) return res.status(400).json({ error: "녹음 중에는 요약 불가" });
  if (transcriptLines.length === 0) return res.status(400).json({ error: "트랜스크립트가 비어있음" });

  broadcast("status", { summarizing: true });

  const transcript = transcriptLines.map((l) => l.text).join("\n");

  const prompt = `아래는 회의 실시간 트랜스크립트이다.
도구를 사용하지 말고 텍스트로만 응답해라.
화자별로 정리하고 핵심 논의사항과 액션아이템을 요약해서 마크다운으로 작성해라.

---

${transcript}`;

  // prompt를 임시 파일로 저장
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
    let stderr = "";

    gemini.stdout.on("data", (chunk) => { stdout += chunk.toString(); });
    gemini.stderr.on("data", (chunk) => { stderr += chunk.toString(); });

    gemini.on("close", (code) => {
      let summary = "";

      try {
        const parsed = JSON.parse(stdout);
        summary = parsed.response || stdout;
      } catch {
        summary = stdout;
      }

      // summary.md 저장
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
    const hasSummary = fs.existsSync(path.join(dir, "summary.md"));
    const hasTranscript = fs.existsSync(path.join(dir, "realtime.txt"));
    let summary = "";
    if (hasSummary) {
      summary = fs.readFileSync(path.join(dir, "summary.md"), "utf-8");
    }
    return { id: d, hasSummary, hasTranscript, summary };
  });

  res.json(meetings);
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
