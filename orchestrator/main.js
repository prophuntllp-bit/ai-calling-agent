const express = require("express");
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const https = require("https");
const WebSocket = require("ws");
const axios = require("axios");
const FormData = require("form-data");
const Redis = require("ioredis");
const { Counter, Histogram, Registry, collectDefaultMetrics } = require("prom-client");
const { LanguageManager } = require("./language-manager");
const { v2: cloudinary } = require("cloudinary");
const { AgniBridge, createAgniSession } = require("./agni-bridge");

if (process.env.CLOUDINARY_CLOUD_NAME) {
  cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET,
  });
}

const config = {
  port: parseInt(process.env.PORT || "8000", 10),
  services: {
    vad: process.env.VAD_URL || "http://vad:8001",
    stt: process.env.STT_URL || "http://stt:8002",
    tts: process.env.TTS_URL || "http://tts:8003",
    llm: process.env.LLM_URL || "http://llm:11434",
    crmAdapter: process.env.CRM_ADAPTER_URL || "http://crm-adapter:8010",
    knowledge: process.env.KNOWLEDGE_SERVICE_URL || "http://knowledge-service:8011",
    platformApi: process.env.PLATFORM_API_URL || "http://platform-api:8013",
  },
  redisUrl: process.env.REDIS_URL || "redis://redis:6379",
  internalToken: process.env.ORCHESTRATOR_INTERNAL_TOKEN || "local-dev-internal-token",
  recordingsDir: process.env.RECORDINGS_DIR || "/data/recordings",
  maxConcurrentCalls: parseInt(process.env.MAX_CONCURRENT || "50", 10),
  callTimeoutMs: parseInt(process.env.CALL_TIMEOUT_MS || `${5 * 60 * 1000}`, 10),
  sttTimeoutMs: parseInt(process.env.STT_REQUEST_TIMEOUT_MS || "45000", 10),
  enablex: {
    appId: process.env.ENABLEX_APP_ID || "",
    appKey: process.env.ENABLEX_APP_KEY || "",
    fromNumber: process.env.ENABLEX_FROM_NUMBER || "",
    baseUrl: (process.env.ENABLEX_VOICE_BASE_URL || "https://api.enablex.io/voice/v1").replace(/\/$/, ""),
  },
  telephonyProvider: (process.env.TELEPHONY_PROVIDER || "enablex").toLowerCase(),
  // Ravan.ai Agni — set both vars to enable; leave blank to use local STT/LLM/TTS
  agni: {
    apiKey: process.env.AGNI_API_KEY || "",
    agentId: process.env.AGNI_AGENT_ID || "",
    get enabled() { return !!(this.apiKey && this.agentId); },
  },
};

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const registry = new Registry();
collectDefaultMetrics({ register: registry });

const callsTotal = new Counter({
  name: "calls_total",
  help: "Total number of calls handled",
  labelNames: ["status"],
  registers: [registry],
});

const callDuration = new Histogram({
  name: "call_duration_seconds",
  help: "End to end call duration",
  buckets: [5, 15, 30, 60, 120, 300],
  registers: [registry],
});

const serviceLatency = new Histogram({
  name: "service_latency_ms",
  help: "Latency by dependency",
  labelNames: ["service"],
  buckets: [25, 50, 100, 250, 500, 1000, 3000, 5000],
  registers: [registry],
});

const redis = new Redis(config.redisUrl, { lazyConnect: false, maxRetriesPerRequest: 2 });
const sessions = new Map();
const languageManager = new LanguageManager();
let acceptingTraffic = true;
const enablexAuthHeader = config.enablex.appId && config.enablex.appKey
  ? `Basic ${Buffer.from(`${config.enablex.appId}:${config.enablex.appKey}`).toString("base64")}`
  : "";

fs.mkdirSync(config.recordingsDir, { recursive: true });

app.use(express.json({ limit: "5mb" }));
app.use(express.urlencoded({ extended: true }));

// CORS — allow dashboard (Vercel) and localhost to call all HTTP endpoints
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Internal-Token');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});
// Recordings endpoint — Redis-first so files survive container restarts / redeploys.
// Falls back to local disk for files written this session that haven't been cached yet.
app.get("/recordings/:callSid/mixed.wav", async (req, res) => {
  const { callSid } = req.params;
  try {
    const b64 = await redis.get(`recording:${callSid}`);
    if (b64) {
      const buf = Buffer.from(b64, "base64");
      res.set("Content-Type", "audio/wav");
      res.set("Content-Length", buf.length);
      res.set("Cache-Control", "public, max-age=86400");
      return res.send(buf);
    }
  } catch { /* fall through to disk */ }
  // Disk fallback (works within the same container session)
  const diskPath = path.join(config.recordingsDir, safeRecordingId(callSid), "mixed.wav");
  if (fs.existsSync(diskPath)) return res.sendFile(diskPath);
  return res.status(404).json({ error: "Recording not found" });
});
// Serve other recording files (caller.wav, agent.wav, timeline.json) from disk
app.use("/recordings", express.static(config.recordingsDir));

function getPublicBaseUrl(req) {
  const host = process.env.PUBLIC_HOST || req.get("host") || "localhost:8000";
  const protocol = req.get("x-forwarded-proto") || (host.includes("localhost") ? "http" : "https");
  return `${protocol}://${host}`.replace(/\/$/, "");
}

function getPublicWsBaseUrl(req) {
  return getPublicBaseUrl(req).replace(/^http/i, "ws");
}

function getConfiguredPublicBaseUrl() {
  const host = process.env.PUBLIC_HOST || `localhost:${config.port}`;
  const protocol = host.includes("localhost") ? "http" : "https";
  return `${protocol}://${host}`.replace(/\/$/, "");
}

function getConfiguredPublicWsBaseUrl() {
  return getConfiguredPublicBaseUrl().replace(/^http/i, "ws");
}

function resolveTelephonyProvider(requestedProvider) {
  const provider = String(requestedProvider || config.telephonyProvider || "enablex").trim().toLowerCase();
  return provider === "enablex" ? "enablex" : "simulated";
}

function hasEnablexConfig() {
  return Boolean(enablexAuthHeader && config.enablex.fromNumber);
}

function buildEnablexOpeningLine(leadName = "there") {
  return `Hello, this is Priya from Prophunt. I am calling regarding your interest in our project. Is this a good time to talk for thirty seconds, ${leadName}?`;
}

function normalizeEnablexPhoneNumber(phone) {
  const digits = String(phone || "").replace(/\D/g, "");
  if (digits.length === 10) return `91${digits}`;
  return digits;
}

async function placeEnablexOutboundCall({ lead, session, openingLine }) {
  if (!hasEnablexConfig()) {
    throw new Error("EnableX credentials or caller number are missing");
  }

  const publicBaseUrl = getConfiguredPublicBaseUrl();
  const payload = {
    name: "Prophunt AI Voice Agent",
    owner_ref: session.callSid,
    auto_record: false,
    from: normalizeEnablexPhoneNumber(config.enablex.fromNumber),
    to: normalizeEnablexPhoneNumber(lead.phone),
    event_url: `${publicBaseUrl}/call/enablex/events`,
  };

  let response;
  try {
    response = await timed("enablex", () =>
      axios.post(`${config.enablex.baseUrl}/call`, payload, {
        headers: {
          Authorization: enablexAuthHeader,
          "Content-Type": "application/json",
        },
        timeout: 45000,
      })
    );
  } catch (error) {
    console.error("[enablex] outbound call failed", {
      message: error.message,
      status: error.response?.status,
      data: error.response?.data,
      from: payload.from,
      to: payload.to,
    });
    throw error;
  }

  const data = response.data || {};
  console.log("[enablex] outbound call response", {
    status: response.status,
    voice_id: data.voice_id,
    state: data.state,
    msg: data.msg,
  });
  return {
    provider_call_id: data.voice_id || data.call_id || data.callId || data.id || data.sid || session.callSid,
    provider_status: data.state || data.status || "initiated",
    raw: data,
  };
}

async function callEnablexApi(method, pathName, payload = null, options = {}) {
  if (!enablexAuthHeader) {
    throw new Error("EnableX credentials are missing");
  }
  const response = await timed("enablex", () =>
    axios({
      method,
      url: `${config.enablex.baseUrl}${pathName}`,
      data: payload,
      headers: {
        Authorization: enablexAuthHeader,
        "Content-Type": "application/json",
      },
      timeout: options.timeout || 45000,
    })
  );
  const data = response.data;
  if (
    data &&
    (data.statusCode >= 400 ||
      data.result >= 400 ||
      /not found|not allowed|failed|error/i.test(String(data.msg || data.playstate || data.state || "")))
  ) {
    const error = new Error(data.msg || data.playstate || data.state || "EnableX API rejected the request");
    error.response = { status: data.statusCode || data.result || response.status, data };
    throw error;
  }
  return data;
}

async function callEnablexDeleteRaw(pathName) {
  if (!enablexAuthHeader) {
    throw new Error("EnableX credentials are missing");
  }
  const endpoint = new URL(`${config.enablex.baseUrl}${pathName}`);
  return timed("enablex", () =>
    new Promise((resolve, reject) => {
      const req = https.request(
        {
          protocol: endpoint.protocol,
          hostname: endpoint.hostname,
          port: endpoint.port || 443,
          path: `${endpoint.pathname}${endpoint.search}`,
          method: "DELETE",
          headers: {
            Authorization: enablexAuthHeader,
            "Content-Type": "application/json",
          },
        },
        (res) => {
          let body = "";
          res.on("data", (chunk) => {
            body += chunk;
          });
          res.on("end", () => {
            if (res.statusCode >= 200 && res.statusCode < 300) {
              resolve(body);
              return;
            }
            const error = new Error(`EnableX delete failed with status ${res.statusCode}`);
            error.response = { status: res.statusCode, data: body };
            reject(error);
          });
        }
      );
      req.on("error", reject);
      req.end("");
    })
  );
}

async function startEnablexStream(voiceId) {
  const wssHost = `${getConfiguredPublicWsBaseUrl()}/audio/enablex/${encodeURIComponent(voiceId)}`;
  console.log("[enablex-media] starting stream", { voice_id: voiceId, wss_host: wssHost });
  return callEnablexApi(
    "put",
    `/call/${encodeURIComponent(voiceId)}/stream`,
    { wss_host: wssHost },
    { timeout: 10000 }
  );
}

async function stopEnablexStream(voiceId) {
  return callEnablexDeleteRaw(`/call/${encodeURIComponent(voiceId)}/stream`);
}

async function hangupEnablexCall(voiceId) {
  return callEnablexDeleteRaw(`/call/${encodeURIComponent(voiceId)}`);
}

const ENABLEX_STREAM_READY_STATUSES = new Set([
  "answered",
  "answer",
  "connected",
  "in-progress",
  "in_progress",
  "live",
  "ongoing",
  "active",
  "bridged",
]);

function extractEnablexCallSid(payload = {}) {
  return payload.voice_id || payload.call_id || payload.callId || payload.id || payload.sid || payload.call_sid;
}

function normalizeEnablexStatus(payload = {}) {
  const rawStatus = payload.status || payload.state || payload.event || payload.call_status || payload.callStatus || "";
  return String(rawStatus).toLowerCase();
}

function shouldStartEnablexStream(callStatus) {
  return ENABLEX_STREAM_READY_STATUSES.has(String(callStatus || "").toLowerCase());
}

function scheduleEnablexStreamStart(session, reason = "scheduled", options = {}) {
  const force = options.force === true;
  if (!session?.callSid || session.closed || session.telephony?.streamStarted || (!force && session.telephony?.streamStartScheduled)) {
    return;
  }
  session.telephony = {
    ...(session.telephony || {}),
    provider: "enablex",
    streamStartScheduled: true,
    streamStartInFlight: false,
    streamStartReason: reason,
  };

  const voiceId = session.callSid;
  // post-dial: only 3 quick attempts (call may already be connected by the time we dial)
  // event-connected: single immediate attempt — EnableX is ready at this point
  const isPostDial = reason === "post-dial";
  const delays = isPostDial
    ? [0, 1500, 4000]           // 3 attempts only — event-connected handles the rest
    : [0, 1000, 3000, 6000, 10000, 15000, 21000, 28000]; // robust retry after connected event
  delays.forEach((delayMs, index) => {
    setTimeout(async () => {
      const current = sessions.get(voiceId);
      if (!current || current.closed || current.telephony?.streamStarted || current.telephony?.streamStartInFlight) return;
      try {
        current.telephony = {
          ...(current.telephony || {}),
          provider: "enablex",
          streamStartInFlight: true,
        };
        console.log("[enablex-media] stream start attempt", { voice_id: voiceId, attempt: index + 1, reason });
        const streamResponse = await startEnablexStream(voiceId);
        console.log("[enablex-media] stream start accepted", { voice_id: voiceId, attempt: index + 1, response: streamResponse });
        current.telephony = {
          ...(current.telephony || {}),
          provider: "enablex",
          streamStartResponse: streamResponse,
          streamStartInFlight: false,
          streamStartScheduled: false,
          streamStarted: true,
        };
        await persistSession(current);
      } catch (streamError) {
        const errorPayload = streamError.response?.data || streamError.message;
        console.error("[enablex-media] stream start failed", {
          voice_id: voiceId,
          attempt: index + 1,
          reason,
          error: errorPayload,
        });
        current.telephony = {
          ...(current.telephony || {}),
          provider: "enablex",
          streamStartError: errorPayload,
          streamStartInFlight: false,
          streamStartScheduled: index < delays.length - 1,
        };
        await persistSession(current).catch(() => {});
      }
    }, delayMs);
  });
}

function nowIso() {
  return new Date().toISOString();
}

function buildSystemPrompt(lead, knowledgeContext, language, agentConfig = {}) {
  const hasKB = knowledgeContext && knowledgeContext.trim().length > 30;
  const kbBlock = hasKB
    ? `PROJECT KNOWLEDGE BASE — Answer ALL questions directly from this. Never say "I will check" or "let me verify":\n${knowledgeContext}`
    : `PROJECT: ${lead.project || "our project"}`;

  // ── PRIORITY: use system prompt authored in the Agents tab ───────────────
  // The frontend generates the full prompt with {{placeholders}}; we fill them here.
  if (agentConfig.systemPrompt && agentConfig.systemPrompt.trim().length > 50) {
    return agentConfig.systemPrompt
      .replace(/\{\{KNOWLEDGE_BASE\}\}/g,  kbBlock)
      .replace(/\{\{LEAD_NAME\}\}/g,       lead.name         || "ji")
      .replace(/\{\{PROJECT_NAME\}\}/g,    lead.project      || "the project")
      .replace(/\{\{LEAD_BUDGET\}\}/g,     lead.budget       || "not discussed yet");
  }

  // ── FALLBACK: auto-generate (used when no agent is configured in dashboard) ──
  const lang = normalizeLanguageToISO(language || lead.language_preference || lead.language || "auto");
  const langNames = { hi: "Hindi", mr: "Marathi", ta: "Tamil", te: "Telugu", pa: "Punjabi", bn: "Bengali", gu: "Gujarati", kn: "Kannada", ml: "Malayalam", en: "English" };
  const langLabel = langNames[lang];

  // ── Agent config with defaults ────────────────────────────────────────────
  const agentName      = agentConfig.agentName      || "Priya";
  const wordCap        = parseInt(agentConfig.wordCap || "30", 10);
  const pitchTone      = agentConfig.pitchTone      || "balanced";       // aggressive | balanced | consultative
  const langStrictness = agentConfig.langStrictness  || "auto";          // auto | hinglish | pure-hindi
  const escalationLine = agentConfig.escalationLine  ||
    "Iske liye main aapko hamare sales expert se connect karti hoon jo bilkul sahi detail de sakenge.";

  // ── Language instruction — fully adaptive, no language barrier ───────────
  // ElevenLabs TTS speaks any language the LLM writes — no need to force Hindi.
  const languageInstruction = `LANGUAGE RULE: Always mirror the language the lead is speaking.
- If they speak Hindi → reply in Hindi (Devanagari script, natural conversational tone)
- If they speak English → reply in English
- If they speak Hinglish (mixed Hindi-English) → reply in Hinglish
- If they speak Marathi, Tamil, Telugu, Punjabi, Bengali, Gujarati, or any other language → reply in that same language
- NEVER force Hindi on someone who is not speaking Hindi
- Numbers, prices, and project names can always be spoken naturally in the detected language
- Keep responses SHORT — max ${wordCap} words — one clear point per reply`;

  // ── Sales pitch philosophy based on tone ─────────────────────────────────
  const pitchBlock = {
    aggressive: `SALES PHILOSOPHY — AGGRESSIVE CLOSER:
You are a confident, results-driven closer. Every conversation must move toward a site visit.
- After giving any project info: immediately bridge to site visit — "Main abhi 30-minute visit arrange kar sakti hoon, kya aaj ya kal theek rahega?"
- After FIRST soft refusal ("sochna hai", "baad mein"): persist once — "${agentName}: Main bilkul samajhti hoon. Lekin bina dekhe decision lena mushkil hota hai — ek 20-minute visit mein sab clear ho jayega. Kaisa rahega?"
- After SECOND refusal: close gracefully and end the call.
OBJECTION SCRIPTS:
• "Budget tight hai" → "EMI option bhi available hai — mujhe exact EMI figure pata hai, kya bata doon?"
• "Sochna hai" → "Zaroor sochiye — lekin slots limited hain. Ek tentative visit book kar lein, cancel karna free hai."
• "Abhi time nahi" → "20 minute — bas itna hi chahiye. Weekend mein bhi visit ho sakti hai."`,

    balanced: `SALES PITCH FLOW — 3-step natural progression:
STEP 1 — ANSWER & DISCOVER: Answer the lead's question fully using KB. Ask one focused discovery question (BHK, budget, or purpose).
STEP 2 — BUILD VALUE: Once BHK and budget are clear, share specifics — layout sizes, price, key USPs. Use urgency naturally: "Yeh limited inventory hai" / "Launch price mein mil raha hai — baad mein 10-15% badh sakti hai."
STEP 3 — INVITE SITE VISIT: After covering BHK + price, make one confident ask: "Ek baar personally dekhenge toh sab clear ho jayega — model flat, views, amenities sab live. Main 30-minute visit arrange kar sakti hoon, kya aap is weekend free hain?"
After ONE soft refusal: gently re-ask once. After second refusal: close warmly.`,

    consultative: `SALES APPROACH — TRUSTED ADVISOR:
You are a helpful consultant, not a pusher. Your goal is to understand the lead's needs and guide them honestly.
- First, understand: purpose (investment/self-use), budget range, preferred BHK, timeline.
- Answer all questions completely and honestly from the KB.
- Only invite for a site visit when the lead signals genuine interest (asks about pricing, possession, or visiting).
- NEVER mention site visit more than once if they show hesitation.
- If not interested: "Theek hai, koi pressure nahi. Aap kabhi bhi hamare office aa sakte hain ya humse call kar sakte hain."
- Build trust; a good experience today leads to a referral tomorrow.`,
  }[pitchTone] || pitchBlock?.balanced;

  return `You are ${agentName}, a friendly real estate consultant calling on behalf of Prop Hunt.

${kbBlock}

LEAD INFO:
- Name: ${lead.name}
- Project Interest: ${lead.project || "Unknown"}
- Budget: ${lead.budget || "not discussed yet"}

${languageInstruction}

${pitchBlock || `SALES PITCH FLOW — 3-step natural progression:
STEP 1 — ANSWER & DISCOVER: Answer the lead's question fully using KB. Ask one focused discovery question.
STEP 2 — BUILD VALUE: Share BHK details, price, key USPs. Create urgency: "Limited inventory" / "Launch price — will rise soon."
STEP 3 — INVITE SITE VISIT: After BHK + price covered, offer: "Ek baar personally dekhenge — model flat, views, amenities live. 30-minute visit arrange kar sakti hoon."`}

━━━ CONVERSATION FORMULA — use this pattern every single turn ━━━

① REACT (1-3 words) — Emotionally acknowledge FIRST, before any info:
   Great news / interest  → "वाह!", "बहुत बढ़िया!", "एकदम सही!", "अरे वाह!", "शानदार!", "परफेक्ट!"
   Budget shared          → "समझ गया!", "बिल्कुल.", "अच्छा बताया."
   Concern / objection    → "सही बात है.", "समझ सकती हूँ.", "ज़रूर."
   Acknowledgment only    → SKIP react and ask a follow-up question immediately — never repeat "समझ आया" twice
   Silence / "hello?"     → "हाँ, बोलिए!" then one quick fact + question

② REFLECT (2-4 words) — Echo their key words so they feel heard:
   e.g., "बानेर, एक करोड़ —"  |  "2BHK, गार्डन व्यू —"  |  "Investment ke liye —"

③ ONE FACT — One KB point only. No lists. No long paragraphs. Natural, conversational.

④ QUESTION (mandatory) — End every single turn with exactly one short question.

━━━ LANGUAGE MATCHING — CRITICAL ━━━
• Pure Hindi speaker → reply in PURE Hindi (Devanagari). Zero English mixing.
• Pure English speaker → reply in English only.
• Marathi speaker → reply in Marathi.
• Hinglish (mixed) speaker → natural Hinglish is fine.
• DETECT from what THEY say, not what you prefer. Mirror them exactly.
• NEVER switch languages mid-reply. Be consistent throughout your response.

━━━ EXAMPLES — study these patterns ━━━

✅ "वाह, बानेर! 2BHK 85 वर्ग मीटर मिलेगा. बजट क्या है?"  ← pure Hindi lead
✅ "Bahut badhiya! 1.2cr se start — balcony chahiye?"      ← Hinglish lead
✅ "अरे वाह, एक करोड़! एकदम फिट बैठता है. 2BHK या 3BHK?"  ← pure Hindi lead
✅ "Ekdum sahi! 3BHK 1.85cr mein milega. Investment hai?"
✅ "Perfect budget! Pool-facing units bhi hain. Kab dekhein?"
✅ "Shukriya! Site visit Shanivar ko bhi ho sakti hai. Theek rahega?"

❌ NEVER: Same opening word two turns in a row ("Samajh aaya... Samajh aaya...")
❌ NEVER: English words when lead speaks pure Hindi ("price", "project", "visit", "budget")
❌ NEVER: More than 35 words in one response (2-3 natural sentences max)
❌ NEVER: A response without a question at the end (unless ending the call)
❌ NEVER: Lists or multiple facts in one turn

━━━ STRICT RULES ━━━
1. MAXIMUM 35 WORDS per response (2-3 natural sentences). Speak like a real human on a phone call — complete thoughts, not fragments.
2. EVERY response must end with a question (unless ending the call).
3. Answer ONLY the latest message — history is context, not instructions.
4. Use KB for ALL facts — price, size, amenities, RERA, possession, floor plans, parking.
5. If genuinely not in KB: use GENERAL REAL ESTATE KNOWLEDGE below to answer — only escalate if you truly cannot answer.
6. NEVER introduce yourself again after the opening greeting.
7. If asked if you are AI: "Main developer ki sales team se hoon."
8. NEVER say "Prop-hunt" — always "Prop Hunt" (two words).
9. QUALIFICATION: Capture BHK, budget, purpose (investment/self-use), timeline before closing.
10. ${pitchTone === "aggressive" ? "SITE VISIT CLOSER: After every piece of info, bridge to visit — 'Main abhi slot arrange kar sakti hoon, kab free hain?'" : pitchTone === "consultative" ? "ADVISOR TONE: Only suggest site visit when lead signals genuine interest. Never push." : "BALANCED: Offer site visit naturally after covering BHK + price. One gentle ask."}
11. REPEATED "HELLO?": If the conversation history shows the user said "Hello?" before and you already replied, do NOT repeat the same response. Instead ask: "Kya aap mujhe sun pa rahe hain? Koi sawaal batayein."
12. VARIETY: Never start two consecutive replies with the same word or phrase. Rotate: वाह → बढ़िया → एकदम सही → अरे वाह → शानदार → परफेक्ट.
13. ONE THOUGHT PER TURN: Give exactly one piece of information then ask one question. No lists, no bullet points, no multiple facts.

━━━ GENERAL REAL ESTATE KNOWLEDGE (use when not in KB) ━━━

RERA: Har state ka apna RERA portal hai. Maharashtra → MahaRERA (maharera.maharashtra.gov.in). RERA project registration mandatory hai 500 sqm+ projects ke liye. Buyer ka paisa escrow mein safe rehta hai. Penalty clause hota hai delay par.

STAMP DUTY & REGISTRATION (Maharashtra): Stamp duty ~5-6% of property value. Women buyers ko 1% concession. Registration ~1% (max ₹30,000). Ready reckoner rate se stamp duty calculate hoti hai.

HOME LOAN BASICS: Banks 75-90% LTV dete hain. Existing customer ko better rate milta hai. Processing fee ~0.5-1%. Pre-EMI vs full EMI — possession ke baad full EMI start hoti hai. Tax benefit: 80C (principal), 24B (interest up to ₹2L).

CARPET vs BUILT-UP vs SUPER BUILT-UP: Carpet = usable floor area (walls ke andar). Built-up = carpet + walls (~10-15% more). Super built-up = built-up + common areas (typically 25-35% loading). RERA mandates carpet area mention karna.

OC / CC: Occupancy Certificate (OC) = builder ke paas hona chahiye possession ke time. Completion Certificate (CC) = municipality se milta hai. OC ke bina possession lena risky hai — bank loan, electricity connection mein problem hoti hai.

FLOOR RISE CHARGES: Higher floors par ₹50-200 per sqft extra charge hota hai. Typically 2nd floor se start, har floor pe ₹25-50/sqft increment.

PLC (Preferential Location Charges): Corner unit, garden facing, road facing, club/pool facing ke liye ₹100-500/sqft extra. Builder se confirm karein kaun se units PLC-free hain.

POSSESSION TIMELINE: Under-construction projects mein builder typically 2-3 year timeline deta hai. RERA completion date mention hoti hai. Grace period 6 months allowed hai. Delay hone par builder ko interest pay karna padta hai (typically SBI MCLR + 2%).

INVESTMENT vs SELF-USE: Investment ke liye rental yield (typically 2-3% in metros) aur capital appreciation dekhein. Self-use ke liye connectivity, school/hospital proximity, builder track record important hai.

Return this JSON silently when closing:
OUTCOME:{"status":"interested","site_visit":false,"callback_date":null,"qualification":{"bhk":"","budget_range":"","purpose":"","timeline":""},"notes":""}`;
}

async function timed(service, fn) {
  const start = Date.now();
  try {
    return await fn();
  } finally {
    serviceLatency.labels(service).observe(Date.now() - start);
  }
}

async function callCrmAdapter(method, url, payload) {
  const response = await timed("crm_adapter", () =>
    axios({
      method,
      url: `${config.services.crmAdapter}${url}`,
      data: payload,
      timeout: 10000,
    })
  );
  return response.data;
}

async function fetchLeadByPhone(phone) {
  return callCrmAdapter("get", `/api/leads/by-phone/${encodeURIComponent(phone)}`);
}

async function fetchDialableLeads(campaignId, limit, filters = {}) {
  const data = await callCrmAdapter("post", "/api/leads/fetch-dialable", {
    campaign_id: campaignId,
    limit,
    filters,
  });
  return data.leads || [];
}

async function pushToCRM(leadId, outcome) {
  return callCrmAdapter("patch", `/api/leads/${leadId}/update`, { outcome });
}

async function persistCallLog(session, outcome, durationSec, finalStatus) {
  const tenantId =
    session.campaign?.tenant_id ||
    session.campaign?.tenantId ||
    session.lead?.tenant_id ||
    session.lead?.tenantId ||
    process.env.DEFAULT_TENANT_ID ||
    "";
  if (!tenantId) {
    console.warn("[call-log] skipped platform persistence because tenant_id was not available", {
      call_sid: session.callSid,
      lead_id: session.lead?.id,
    });
    return null;
  }
  const payload = {
    tenant_id: tenantId,
    campaign_id: session.campaign?.id || session.campaign?.campaign_id || null,
    lead_id: session.lead?.id || null,
    phone: session.lead?.phone || "unknown",
    status: finalStatus,
    call_metadata: {
      provider: session.telephony?.provider || "simulated",
      call_id: session.telephony?.voiceId || session.telephony?.callSid || session.callSid,
      duration_sec: durationSec,
      started_at: session.startedAt,
      ended_at: session.endedAt,
      outcome,
      transcript_summary: outcome.transcript_summary,
      full_transcript: outcome.full_transcript,
      recording_url: outcome.recording_url,
      recordings: session.recordings || {},
      lead_name: session.lead?.name || null,
    },
  };
  try {
    const response = await axios.post(`${config.services.platformApi}/internal/calls`, payload, {
      headers: {
        "Content-Type": "application/json",
        "X-Internal-Token": config.internalToken,
      },
      timeout: 10000,
    });
    return response.data;
  } catch (error) {
    console.warn("[call-log] platform persistence failed", {
      call_sid: session.callSid,
      error: error.response?.data || error.message,
    });
    return null;
  }
}

async function getKnowledgeContext(projectId, transcript) {
  if (!projectId || !transcript) {
    return "";
  }
  try {
    const response = await timed("knowledge_service", () =>
      axios.get(`${config.services.knowledge}/projects/${projectId}/query`, {
        params: { q: transcript },
        timeout: 10000,
      })
    );
    const matches = response.data.matches || [];
    return matches.map((match) => `[${match.section}] ${match.text}`).join("\n");
  } catch {
    return "";
  }
}

// ── In-process VAD — RMS energy + zero-crossing rate (~0.05ms vs ~15ms HTTP) ──
// Eliminates one HTTP round-trip per 20ms audio frame. Tune VAD_THRESHOLD env var.
const VAD_RMS_THRESHOLD     = parseInt(process.env.VAD_THRESHOLD      || "420", 10);
const VAD_ZCR_THRESHOLD     = parseFloat(process.env.VAD_ZCR_THRESHOLD || "0.08");

function detectSpeech(pcm16Buffer) {
  if (!pcm16Buffer || pcm16Buffer.length < 4) return false;
  const samples = Math.floor(pcm16Buffer.length / 2);
  let sumSq = 0, zeroCrossings = 0;
  let prev = 0;
  for (let i = 0; i < pcm16Buffer.length - 1; i += 2) {
    const s = pcm16Buffer.readInt16LE(i);
    sumSq += s * s;
    if ((s >= 0) !== (prev >= 0)) zeroCrossings++;
    prev = s;
  }
  const rms = Math.sqrt(sumSq / samples);
  const zcr = zeroCrossings / samples;
  // Speech has both energy (rms) AND frequency content (zcr).
  // Pure silence has low rms. Background noise has low zcr.
  return rms > VAD_RMS_THRESHOLD && zcr > VAD_ZCR_THRESHOLD;
}

async function detectLanguage(audioBuffer) {
  const form = new FormData();
  form.append("audio", ensureWavBuffer(audioBuffer), { filename: "sample.wav", contentType: "audio/wav" });
  const response = await timed("stt", () =>
    axios.post(`${config.services.stt}/detect-language`, form, {
      headers: form.getHeaders(),
      timeout: Math.min(config.sttTimeoutMs, 15000),
    })
  );
  return response.data;
}

async function transcribeAudio(audioBuffer, language = "auto") {
  const form = new FormData();
  form.append("audio", ensureWavBuffer(audioBuffer), { filename: "audio.wav", contentType: "audio/wav" });
  form.append("language", language);
  const response = await timed("stt", () =>
    axios.post(`${config.services.stt}/transcribe`, form, {
      headers: form.getHeaders(),
      timeout: config.sttTimeoutMs,
    })
  );
  return response.data;
}

// ── Direct Sarvam STT — bypasses internal STT microservice, saves one hop ────
// Sarvam accepts: POST /speech-to-text  multipart { file, model, language_code }
// Response: { transcript, language_code, ... }
const SARVAM_LANG_MAP = {
  "hi": "hi-IN", "en": "en-IN", "mr": "mr-IN",
  "ta": "ta-IN", "te": "te-IN", "kn": "kn-IN",
  "gu": "gu-IN", "bn": "bn-IN", "pa": "pa-IN",
};

async function transcribeAudioDirect(audioBuffer, language = "auto") {
  const elevenKey = process.env.ELEVENLABS_API_KEY;
  const sarvamKey = process.env.SARVAM_API_KEY;

  // ── ElevenLabs Scribe STT (preferred when key is set) ──────────────────────
  if (elevenKey) {
    const wav = ensureWavBuffer(audioBuffer);
    const form = new FormData();
    form.append("file", wav, { filename: "audio.wav", contentType: "audio/wav" });
    form.append("model_id", "scribe_v1");
    // Map short codes to BCP-47; "auto" → let ElevenLabs auto-detect
    const ELEVEN_LANG_MAP = {
      "hi": "hi", "en": "en", "mr": "mr", "ta": "ta",
      "te": "te", "kn": "kn", "gu": "gu", "bn": "bn", "pa": "pa",
    };
    const langCode = language === "auto" ? null : (ELEVEN_LANG_MAP[language] || language);
    if (langCode) form.append("language_code", langCode);

    try {
      const t0 = Date.now();
      const response = await timed("stt_elevenlabs", () =>
        axios.post(
          "https://api.elevenlabs.io/v1/speech-to-text",
          form,
          {
            headers: { ...form.getHeaders(), "xi-api-key": elevenKey },
            timeout: 15000,
          }
        )
      );
      const d = response.data;
      const detectedLang = d.language_code?.split("-")[0] || language;
      console.log(`[stt-elevenlabs] latency=${Date.now()-t0}ms lang=${detectedLang} text="${(d.text || "").slice(0, 80)}"`);
      return {
        text:     d.text || "",
        language: detectedLang,
      };
    } catch (err) {
      console.warn("[stt-elevenlabs] failed, falling back to Sarvam:", err.message);
      // fall through to Sarvam below
    }
  }

  // ── Sarvam STT fallback ────────────────────────────────────────────────────
  if (!sarvamKey) return transcribeAudio(audioBuffer, language);

  const wav = ensureWavBuffer(audioBuffer);
  const form = new FormData();
  form.append("file", wav, { filename: "audio.wav", contentType: "audio/wav" });
  form.append("model", "saarika:v2.5");

  const langCode = SARVAM_LANG_MAP[language] || (language === "auto" ? undefined : language);
  if (langCode) form.append("language_code", langCode);

  try {
    const t0 = Date.now();
    const response = await timed("stt_direct", () =>
      axios.post(
        `${process.env.SARVAM_API_URL || "https://api.sarvam.ai"}/speech-to-text`,
        form,
        {
          headers: { ...form.getHeaders(), "api-subscription-key": sarvamKey },
          timeout: 12000,
        }
      )
    );
    const d = response.data;
    console.log(`[stt-direct] latency=${Date.now()-t0}ms lang=${d.language_code}`);
    return {
      text:     d.transcript || "",
      language: d.language_code?.split("-")[0] || language,
    };
  } catch (err) {
    console.warn("[stt-direct] failed, falling back to microservice:", err.message);
    return transcribeAudio(audioBuffer, language);  // graceful fallback
  }
}

// Extract a short price snippet from KB context (used to make guided replies KB-aware)
function extractPriceFromKB(knowledgeBase = "") {
  if (!knowledgeBase) return null;
  // Look for price patterns: ₹X Cr, ₹X lakh, X crore, X lacs, etc.
  const priceMatch = knowledgeBase.match(/(?:starting|starts?|from|price|rate|cost)[^\n.]{0,60}(?:₹|rs\.?|inr)\s*[\d,.]+\s*(?:cr(?:ore)?|lakh?|lac|l)/i)
    || knowledgeBase.match(/(?:₹|rs\.?|inr)\s*[\d,.]+\s*(?:cr(?:ore)?|lakh?|lac|l)[^\n.]{0,60}/i)
    || knowledgeBase.match(/(?:2bhk|3bhk|two bhk|three bhk)[^\n.]{0,80}(?:₹|rs\.?|inr)/i);
  return priceMatch ? priceMatch[0].trim() : null;
}

function buildRuleBasedReply(session, userText = "") {
  const text = String(userText || "").toLowerCase();
  const project = session.lead?.project || session.campaign?.project_name || "the project";
  const lang = languageManager.getBaseLanguage(session.callSid);
  const isHindi = lang === "hi";
  const kbPriceSnippet = extractPriceFromKB(session.dynamicVariables?.knowledge_base || "");

  // ── Helpers — MUST be defined before any usage ────────────────────────────
  const T = (en, hi) => isHindi ? hi : en;

  // ── Universal farewell — end call immediately regardless of state ───────────
  // Catches: "thank you", "थैंक यू", "धन्यवाद", "bye", "chalo", etc.
  const universalFarewell = /\b(thank you|thanks|bye|goodbye|alvida|ok bye|ok thanks|chalo ab|ab chalta|achha theek|chalta hoon|chalti hoon|chalte hain)\b|थैंक\s*यू|धन्यवाद|शुक्रिया|अलविदा|बाय\b|चलो\s*अब|ठीक\s*है\s*चलते|चलते\s*हैं/.test(text);
  if (universalFarewell) {
    session.guidedState = "closed";
    return T(
      `Thank you for your time. Have a great day. Goodbye!`,
      `Bahut shukriya aapka waqt dene ke liye. Aapka din shubh ho. Namaste!`
    );
  }

  // ── Intent patterns — Latin (Romanised Hindi) + Devanagari (Sarvam STT output) ──
  const wantsConfiguration = /(?:\b|[^a-z0-9])(?:1|one|ek|2|two|do|3|three|teen|4|four|char)\s*(?:b|v|d)?\s*h\s*k\b|bhk|vhk|dhk|dbhk|vbhk|configuration|config|flat size|carpet|sq ?ft|बीएचके|बी\.?एच\.?के|bhk/.test(text);
  const wantsTwoBhk = /(?:2|two|to|too|do|d)\s*(?:b|v|d)?\s*h\s*k|dbhk|2bhk|two bhk|do bhk|दो\s*(?:बीएचके|बी\s*एच\s*के|bhk)|2\s*(?:बीएचके|bhk)/.test(text);
  const wantsThreeBhk = /(?:3|three|tree|free|teen)\s*(?:b|v|d)?\s*h\s*k|3vhk|3bhk|three bhk|teen bhk|तीन\s*(?:बीएचके|बी\s*एच\s*के|bhk)|3\s*(?:बीएचके|bhk)/.test(text);
  // Positive — Latin Romanised + Devanagari
  const positiveIntent = /yes|yeah|yep|sure|proceed|tell me|go ahead|interested|ok|okay|alright|all right|hello|hi|speaking|here|haan|ji\b|bilkul|theek|sahi|zaroor|batao|bataiye|हाँ|हां|जी|ठीक|बिल्कुल|ज़रूर|जरूर|बताओ|बताइए|बोलिए|सुनिए|सुनें|हा\b/.test(text);
  // Explicit farewell — Latin + Devanagari
  const explicitFarewell = /\b(bye|goodbye|good bye|not interested|no thank|stop calling|remove|alvida|band karo|chhodo|mujhe nahi chahiye)\b|अलविदा|बंद करो|नहीं चाहिए|छोड़ो/.test(text);
  // Negative — Latin + Devanagari
  const negativeIntent = /bye|not interested|stop|later|no\b|not now|busy|nahi\b|nahin\b|na\b|mat\b|baad mein|abhi nahi|नहीं|नही|ना\b|मत\b|बाद में|अभी नहीं|व्यस्त|बिज़ी/.test(text);
  const guidedState = session.guidedState || null;

  if (/price|cost|rate|budget|how much|pricing|daam|kimat|kitna|kitne|paisa|qeemat|रेट|दाम|कीमत|क़ीमत|कितना|कितने|पैसे|रुपए|रुपये|प्राइस|बजट/.test(text)) {
    if (kbPriceSnippet) {
      session.guidedState = "awaiting_site_visit";
      return T(
        `For ${project}: ${kbPriceSnippet}. Premium gated township with clubhouse, pool, gym, and 24/7 security — at a great launch price. Would you like to book a site visit to see it in person?`,
        `${project} mein ${kbPriceSnippet}. Premium gated township hai — clubhouse, pool, gym aur 24/7 security ke saath, abhi launch kimat mein. Kya site visit book karein taki aap personally dekh sakein?`
      );
    }
    session.guidedState = "awaiting_configuration";
    return T(
      `For ${project}, are you looking at 2 BHK or 3 BHK pricing?`,
      `${project} mein do BHK ka rate chahiye ya teen BHK ka?`
    );
  }
  // ── BHK query — give real info first, DON'T jump to callback/site-visit yet ──
  if (wantsTwoBhk || wantsThreeBhk || wantsConfiguration) {
    // If already past BHK info stage, fall through to LLM for follow-up questions
    if (["price_discussed", "awaiting_site_visit", "site_visit_confirmed",
         "awaiting_callback_confirmation", "callback_confirmed"].includes(guidedState)) {
      return null;
    }
    const bhkLabel = isHindi
      ? (wantsThreeBhk ? "teen BHK" : wantsTwoBhk ? "do BHK" : "BHK")
      : (wantsThreeBhk ? "3 BHK" : wantsTwoBhk ? "2 BHK" : "BHK");
    session._bhkType = wantsTwoBhk ? "2" : wantsThreeBhk ? "3" : "any";
    session.guidedState = "bhk_discussed";
    return T(
      `${project} has beautiful ${bhkLabel} apartments in two layouts — Compact and Classic — available in Wings J and K with great views. Ready-to-move units are also available. Want me to share the current pricing?`,
      `${project} mein ${bhkLabel} ke do options hain — Compact aur Classic layout, Wings J aur K mein sundar views ke saath. Ready-to-move units bhi hain. Kya main current kimat bata doon?`
    );
  }

  // ── bhk_discussed → user wants price or follow-up ──────────────────────────
  if (guidedState === "bhk_discussed") {
    const wantsPrice = /price|cost|rate|kitna|kimat|rupaye|budget|lakh|crore|paisa|qeemat|रेट|दाम|कीमत|कितना|कितने|रुपए/.test(text);
    if (wantsPrice || positiveIntent) {
      session.guidedState = "awaiting_site_visit";
      if (kbPriceSnippet) {
        return T(
          `For ${project}: ${kbPriceSnippet}. It's a premium gated community with clubhouse, pool, gym, and 24/7 security — and these are launch prices that will go up soon. Shall I book a site visit so you can see it in person?`,
          `${project} mein ${kbPriceSnippet}. Yeh ek premium gated community hai — clubhouse, pool, gym aur 24/7 security ke saath. Abhi launch price mein mil raha hai, baad mein daam badhenge. Kya main ek site visit arrange karoon taki aap personally dekh sakein?`
        );
      }
      // No KB price snippet — let LLM answer the price, but track state
      session.guidedState = "awaiting_site_visit";
      return null; // LLM will answer with KB price, then we're in awaiting_site_visit
    }
    // Any other question in bhk_discussed — LLM handles with KB
    return null;
  }

  // ── awaiting_site_visit → respond to yes/no on site visit ─────────────────
  if (guidedState === "awaiting_site_visit") {
    // If the user is asking a question or requesting information → let LLM answer
    // Don't treat informational requests like "bataiye/बताइए/kya hai/hospital/college" as "yes"
    const isInfoRequest = /\b(bataiye|batao|bata|kya|kaise|kaisa|kaisi|kitna|kitne|kimat|price|cost|hospital|college|school|mall|metro|location|jagah|amenity|amenities|pool|gym|parking|floor|possession|ready|handover|loan|emi|bank|discount|offer)\b|बताइए|बताओ|क्या|कैसा|कितना|कितने|कीमत|हॉस्पिटल|कॉलेज|स्कूल|मेट्रो|लोकेशन|पार्किंग|पज़ेशन|लोन|ईएमआई|डिस्काउंट/i.test(text);
    if (isInfoRequest) {
      // User is asking a real question — don't push site visit, let LLM answer from KB
      return null;
    }
    // Explicit yes/confirmation — require ≥3 words OR a single known affirmative word.
    // Short/garbage STT (e.g. "ठीक है मोजर") must not auto-confirm a site visit.
    // The regex tests the START of the trimmed text for a clear affirmative.
    const wordCountSV = text.trim().split(/\s+/).filter(w => w.length > 0).length;
    const explicitYes = /^(haan|ha\b|yes|ji\b|bilkul|theek|acha|accha|zaroor|sure|ok\b|okay|chalo|kar do|book karo|book kar|karo|kijiye|lelo|le lo|confirm|done)\b|^हाँ|^हां|^जी\b|^बिल्कुल|^ठीक|^अच्छा|^ज़रूर|^जरूर/i.test(text.trim());
    // Guard: don't confirm from very short fragments (< 2 words) — likely echo/noise
    const cleanConfirm = explicitYes && wordCountSV >= 2;
    if (cleanConfirm) {
      // Include full goodbye in this response — set state to "closed" immediately so
      // the call ends cleanly with goodbye rather than hanging up mid-conversation.
      session.guidedState = "closed";
      return T(
        `Wonderful! I have noted your site visit request for ${project}. Our team will call you within 24 hours to confirm the date and time. You will see the model apartment, views, and all amenities live. It was great speaking with you. Have a lovely day! Goodbye.`,
        `Bahut achha! ${project} ke liye aapki site visit book ho gayi. Hamari team 24 ghante mein call karke time fix kar legi. Aap model flat, views aur saari amenities live dekhenge. Bahut achha laga aapase baat karke. Aapka din shubh ho! Namaste.`
      );
    }
    if (negativeIntent) {
      session.guidedState = "price_discussed";
      return T(
        `No problem at all. Is there anything else you would like to know — amenities, location, possession date, or floor plans?`,
        `Koi baat nahi. Kya kuch aur jaanna chahenge — amenities, jagah, possession date ya floor plan ke baare mein?`
      );
    }
    // Anything else — let LLM continue the conversation naturally with KB context
    return null;
  }

  // ── price_discussed → continue conversation or offer site visit ────────────
  if (guidedState === "price_discussed") {
    if (positiveIntent) {
      session.guidedState = "awaiting_site_visit";
      return T(
        `I can arrange a site visit at ${project} for you. Our team will confirm the timing. Shall I book it?`,
        `Main ${project} ke liye site visit arrange kar sakti hoon. Hamari team timing confirm kar legi. Karoon book?`
      );
    }
    // Let LLM continue if they have more questions
    return null;
  }

  // ── awaiting_configuration — legacy state, keep for backward compat ────────
  if (guidedState === "awaiting_configuration" && !negativeIntent) {
    const impliedTwo   = /\b(2|do|dono|two|to\b|too\b)\b/.test(text);
    const impliedThree = /\b(3|teen|three|tin)\b/.test(text);
    if (impliedTwo || impliedThree) {
      session._bhkType = impliedTwo ? "2" : "3";
      session.guidedState = "bhk_discussed";
      const bhkLabel = isHindi ? (impliedThree ? "teen BHK" : "do BHK") : (impliedThree ? "3 BHK" : "2 BHK");
      return T(
        `${project} has ${bhkLabel} in Compact and Classic layouts in Wings J and K with great views. Shall I share the pricing?`,
        `${project} mein ${bhkLabel} Compact aur Classic layout mein Wings J aur K mein available hai. Kimat bata doon?`
      );
    }
    session._configAsks = (session._configAsks || 0) + 1;
    if (session._configAsks >= 2) return null;
    return T(
      `Please tell me, are you interested in 2 BHK or 3 BHK?`,
      `Batayein, do BHK mein interest hai ya teen BHK mein?`
    );
  }

  // ── awaiting_callback_confirmation — legacy, redirect to site visit ────────
  if (guidedState === "awaiting_callback_confirmation") {
    if (positiveIntent) {
      session.guidedState = "site_visit_confirmed";
      return T(
        `I have noted your site visit request for ${project}. Our team will call you today to confirm the time. Thank you!`,
        `${project} ke liye site visit note kar li hai. Hamari team aaj call karegi time fix karne ke liye. Shukriya!`
      );
    }
    if (negativeIntent) {
      session.guidedState = "callback_declined";
      return T(
        `No problem. Thank you for your time. Have a great day!`,
        `Koi baat nahi. Aapka shukriya. Aapka din shubh ho!`
      );
    }
    return T(
      `Shall I book a site visit at ${project} for you today?`,
      `Kya main aaj ${project} ke liye site visit book kar doon?`
    );
  }
  if (guidedState === "awaiting_close_confirmation") {
    if (explicitFarewell) {
      session.guidedState = "closed";
      return T(
        `No problem. Have a great day. Goodbye.`,
        `Koi baat nahi. Aapka din shubh ho. Namaste.`
      );
    }
    session.guidedState = "awaiting_configuration";
    return T(
      `Of course! Are you interested in a two BHK or a three BHK at ${project}?`,
      `Zaroor! ${project} mein do BHK mein interest hai ya teen BHK mein?`
    );
  }
  if (positiveIntent) {
    session.guidedState = "open_discovery";
    return T(
      `I can help with price, location, or site visit details for ${project}. What would you like to know first?`,
      `Main ${project} ke baare mein rate, location ya site visit ki jaankari de sakti hoon. Pehle kya jaanna chahenge?`
    );
  }
  if (/location|where|near|connectivity|area|kahan|jagah|लोकेशन|कहाँ|कहां|जगह|स्थान|एड्रेस|पता|नज़दीक|पास में/.test(text)) {
    session.guidedState = "location_shared";
    return T(
      `${project} is in Pune with strong city connectivity. Would you like the pricing next?`,
      `${project} Pune mein hai, city connectivity bahut acchi hai. Ab rate bata doon?`
    );
  }
  if (/visit|site|schedule|appointment|callback|dekhna|milna|विज़िट|विजिट|साइट|देखना|मिलना|अपॉइंटमेंट/.test(text)) {
    session.guidedState = "awaiting_visit_day";
    return T(
      `Sure. I can note a site visit request. Which works better, today or tomorrow?`,
      `Zaroor. Main site visit request note kar sakti hoon. Aaj aayenge ya kal?`
    );
  }
  if (negativeIntent) {
    if (!guidedState || guidedState === "open_discovery" || guidedState === "location_shared") {
      session.guidedState = "awaiting_close_confirmation";
      return T(
        `I understand. Just before I let you go — would you like to know the pricing for ${project}? It only takes a moment.`,
        `Samajh gayi. Jaane se pehle ek kaam — ${project} ka rate ek baar sun lein, sirf ek minute lagega?`
      );
    }
    session.guidedState = "closed";
    return T(
      `No problem. Thank you for your time. Goodbye.`,
      `Koi baat nahi. Aapka shukriya. Namaste.`
    );
  }
  // If already in open_discovery and lead's reply is unclear, move conversation forward
  if (guidedState === "open_discovery") {
    session.guidedState = "awaiting_configuration";
    return T(
      `Are you interested in a two BHK or three BHK at ${project}? I can share the current pricing.`,
      `${project} mein do BHK ka interest hai ya teen BHK ka? Main rate bata sakti hoon.`
    );
  }
  // Generic fallback — only reached if guidedState is null and nothing matched
  session.guidedState = "open_discovery";
  return T(
    `I can help with price, location, or site visit details for ${project}. What would you like to know?`,
    `Main ${project} ke baare mein rate, location ya site visit ki jaankari de sakti hoon. Kya jaanna chahenge?`
  );
}

function isTerminalGuidedState(session) {
  // site_visit_confirmed removed — goodbye is now included in the confirmation response
  // itself (state jumps directly to "closed"), so we don't need a separate terminal check.
  return ["callback_confirmed", "callback_declined", "closed"].includes(session?.guidedState || "");
}

function shouldUseGuidedReply(session, userText = "") {
  const text = String(userText || "").toLowerCase().trim();
  const guidedState = session?.guidedState || null;

  // Terminal states — guided wraps up cleanly
  if (["callback_confirmed", "callback_declined", "site_visit_confirmed", "closed"].includes(guidedState)) return true;

  // Awaiting yes/no on site visit or legacy callback — guided handles
  if (["awaiting_callback_confirmation", "awaiting_site_visit"].includes(guidedState)) return true;

  // In bhk_discussed state — guided handles price follow-up and positive affirmations
  if (guidedState === "bhk_discussed") return true;

  // price_discussed — guided handles positive/close, LLM handles further questions
  if (guidedState === "price_discussed" && /yes|haan|ji\b|sure|okay|ok|theek|bilkul|zaroor|ha\b/.test(text)) return true;

  // Clear goodbye / not interested — guided ends the call gracefully
  if (/\b(bye|goodbye|alvida|band karo|nahi chahiye|not interested|baad mein karana|later call|mujhe nahi chahiye|thank you|thanks|ok bye|ok thanks|theek hai ab|chalta hoon|chalti hoon|achha chalta|chalte hain)\b|थैंक\s*यू|धन्यवाद|शुक्रिया|अलविदा|चलते\s*हैं|चलता\s*हूँ|बाय/.test(text)) return true;

  // BHK / configuration questions — route to guided so LLM can't inject payment-plan tangents
  const hasBhkQuery = /(?:2|two|to\b|too\b|do\b|3|three|teen|4|four|char|1|one|ek)\s*(?:b\s*h\s*k|bhk|vhk|dhk)\b|(?:bhk|vhk|dhk)\b|configuration\b|flat\s+(?:size|type)|बीएचके|बी\.?एच\.?के/.test(text);
  if (hasBhkQuery) return true;

  // Everything else (amenities, location, possession date, open-ended Qs) → LLM with KB
  return false;
}

// ── LLM response — Groq fast path (50–150ms TTFT) with Ollama fallback ──────
async function getLLMResponse(session, userText) {
  const language = languageManager.getLanguage(session.callSid);
  session.history.push({ role: "user", content: userText });
  session.history = session.history.slice(-10);  // keep last 5 turns — enough context, avoids history replay

  // Guided reply path — pure in-memory, ~0ms (handles pricing/BHK/location/callback)
  // Returns null when it wants LLM to take over (e.g. user is confused, not answering config question)
  if (shouldUseGuidedReply(session, userText)) {
    const reply = buildRuleBasedReply(session, userText);
    if (reply !== null) {
      session.history.push({ role: "assistant", content: reply });
      return reply;
    }
    // null → fall through to LLM
  }

  // Early-call affirmation shortcut — if the lead says "haan / ji / yes / okay"
  // as their very first response after the opening, they are confirming they can
  // talk — NOT asking a question. Skip LLM and ask a natural qualifying question.
  const userTurns = session.history.filter(h => h.role === "user").length;
  const isSimpleAffirmation = /^(haan|ha|yes|ji|okay|ok|theek|acha|accha|bilkul|zaroor|sure|haan ji|ha ji|theek hai|theek h|sahi|chal|chalo|bolo|batao|bol)[\.\!\s,]*$/i.test(userText.trim());
  if (userTurns <= 2 && isSimpleAffirmation) {
    const project = session.lead?.project || session.campaign?.name || "is project";
    const reply = `${project} ke baare mein kya jaanna chahenge aap — price, location, ya BHK options?`;
    session.history.push({ role: "assistant", content: reply });
    return reply;
  }

  // Knowledge context — always fetch so LLM can answer any project question
  // Prefer pre-loaded KB in session, fallback to live fetch; cap at 4000 chars for GPT-4o-mini
  const knowledgeContext = (
    session.dynamicVariables?.knowledge_base ||
    (await getKnowledgeContext(session.campaign?.project_id || session.lead.project_id, userText))
  ).slice(0, 3500);  // 3500 chars — includes pricing section. (was 1500: pricing was cut off → agent said "not discussed")

  // Resolve language — prefer detected language over "auto" placeholder
  const resolvedLanguage = (language === "auto" || language === "auto-IN" || !language)
    ? (languageManager.getBaseLanguage(session.callSid) || "hi")
    : language;

  const systemPrompt = buildSystemPrompt(session.lead, knowledgeContext, resolvedLanguage, session.agentConfig || {});

  // Send only last 3 turns (6 messages) — enough context, far fewer tokens
  const historyContext = session.history.slice(-6).slice(0, -1);
  const currentTurn   = { role: "user", content: `[CURRENT — respond to this only]: ${userText}` };
  const messages = [{ role: "system", content: systemPrompt }, ...historyContext, currentTurn];

  // ── Safe error body serializer — avoids circular JSON from stream responses ──
  // When responseType:'stream', err.response?.data is an IncomingMessage (TLSSocket)
  // which cannot be JSON.stringified. This helper detects streams and returns '[stream]'.
  function safeErrBody(data) {
    if (!data) return "{}";
    if (typeof data === "object" && typeof data.on === "function") return "[stream]";
    try { return JSON.stringify(data).slice(0, 200); } catch { return "[unstringifiable]"; }
  }

  // ── Streaming SSE helper — collects all chunks into a full reply string ──────
  // stream:true delivers first bytes sooner (lower TTFT) even when we wait for the
  // full response. For 90-token replies this saves ~80-150ms vs stream:false.
  //
  // CRITICAL: TCP chunks can split mid-line. We carry a `remainder` string so that
  // a JSON line broken across two chunks is re-assembled before parsing.
  // Without this, split lines are silently skipped → garbled / truncated text.
  async function collectStreamingReply(axiosResponse) {
    let fullText = "";
    let remainder = "";
    return new Promise((resolve, reject) => {
      axiosResponse.data.on("data", (chunk) => {
        // Prepend any incomplete line carried over from the previous chunk
        const text = remainder + chunk.toString("utf8");
        const lines = text.split("\n");
        // The last element may be an incomplete line — carry it to the next chunk
        remainder = lines.pop() || "";
        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          const data = line.slice(6).trim();
          if (data === "[DONE]") return;
          try {
            const parsed = JSON.parse(data);
            fullText += parsed.choices?.[0]?.delta?.content || "";
          } catch {}
        }
      });
      axiosResponse.data.on("end", () => {
        // Flush any remaining incomplete line
        if (remainder.startsWith("data: ")) {
          const data = remainder.slice(6).trim();
          if (data && data !== "[DONE]") {
            try {
              const parsed = JSON.parse(data);
              fullText += parsed.choices?.[0]?.delta?.content || "";
            } catch {}
          }
        }
        resolve(fullText.trim());
      });
      axiosResponse.data.on("error", reject);
    });
  }

  // ── Groq primary when LLM_PREFER_GROQ=true OR no OpenAI key ─────────────────
  // Groq llama-3.1-8b-instant: 50–150ms TTFT vs OpenAI 300–800ms.
  // Set LLM_PREFER_GROQ=true in Railway env to enable Groq-first routing.
  const preferGroq = process.env.LLM_PREFER_GROQ === "true";

  // ── OpenAI (primary unless preferGroq=true) ───────────────────────────────
  if (process.env.OPENAI_API_KEY && !preferGroq) {
    try {
      const t0 = Date.now();
      const response = await timed("openai", () =>
        axios.post(
          "https://api.openai.com/v1/chat/completions",
          {
            model: process.env.OPENAI_MODEL || "gpt-4o-mini",
            messages,
            temperature: 0.3,
            max_tokens: 180,  // 2-3 natural sentences — fuller, more human responses
            stream: true,    // streaming: first bytes arrive faster, lower TTFT
          },
          {
            headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
            responseType: "stream",
            timeout: 8000,
          }
        )
      );
      const reply = await collectStreamingReply(response);
      console.log(`[openai] callSid=${session.callSid} latency=${Date.now()-t0}ms model=${process.env.OPENAI_MODEL || "gpt-4o-mini"} reply="${reply.slice(0,60)}"`);
      session.history.push({ role: "assistant", content: reply });
      const match = reply.match(/OUTCOME:({.*})/s);
      if (match) { try { session.outcome = JSON.parse(match[1]); } catch {} }
      return reply.replace(/OUTCOME:({.*})/s, "").trim();
    } catch (err) {
      const statusCode = err.response?.status;
      const errBody = safeErrBody(err.response?.data);
      console.warn(`[openai] failed (HTTP ${statusCode || "?"}) falling back to Groq: ${err.message} — ${errBody}`);
    }
  }

  // ── Groq (primary when preferGroq=true, otherwise fallback) ──────────────
  if (process.env.GROQ_API_KEY) {
    try {
      const t0 = Date.now();
      const response = await timed("groq", () =>
        axios.post(
          "https://api.groq.com/openai/v1/chat/completions",
          {
            model: process.env.GROQ_MODEL || "llama-3.1-8b-instant",
            messages,
            temperature: 0.2,
            max_tokens: 180,  // 2-3 natural sentences — fuller responses
            stream: true,    // Groq streaming: even faster first-token delivery
          },
          {
            headers: { Authorization: `Bearer ${process.env.GROQ_API_KEY}` },
            responseType: "stream",
            timeout: 4000,
          }
        )
      );
      const reply = await collectStreamingReply(response);
      console.log(`[groq] callSid=${session.callSid} latency=${Date.now()-t0}ms reply="${reply.slice(0,60)}"`);
      session.history.push({ role: "assistant", content: reply });
      const match = reply.match(/OUTCOME:({.*})/s);
      if (match) { try { session.outcome = JSON.parse(match[1]); } catch {} }
      return reply.replace(/OUTCOME:({.*})/s, "").trim();
    } catch (err) {
      const statusCode = err.response?.status;
      const data = err.response?.data;
      // Read actual error body from stream so we can diagnose 400/401/429 causes
      let errBody = "{}";
      if (data && typeof data.on === "function") {
        try {
          const chunks = [];
          await new Promise((res) => { data.on("data", c => chunks.push(c)); data.on("end", res); data.on("error", res); });
          errBody = Buffer.concat(chunks).toString().slice(0, 300);
        } catch { errBody = "[stream-read-failed]"; }
      } else {
        errBody = safeErrBody(data);
      }
      console.warn(`[groq] failed (HTTP ${statusCode || "?"}) falling back: ${err.message} — ${errBody}`);
    }
  }

  // ── OpenAI as last LLM resort when preferGroq=true but Groq failed ───────
  if (process.env.OPENAI_API_KEY && preferGroq) {
    try {
      const t0 = Date.now();
      const response = await timed("openai_fallback", () =>
        axios.post(
          "https://api.openai.com/v1/chat/completions",
          { model: process.env.OPENAI_MODEL || "gpt-4o-mini", messages, temperature: 0.3, max_tokens: 180, stream: true },
          { headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}` }, responseType: "stream", timeout: 8000 }
        )
      );
      const reply = await collectStreamingReply(response);
      console.log(`[openai_fallback] callSid=${session.callSid} latency=${Date.now()-t0}ms reply="${reply.slice(0,60)}"`);
      session.history.push({ role: "assistant", content: reply });
      const match = reply.match(/OUTCOME:({.*})/s);
      if (match) { try { session.outcome = JSON.parse(match[1]); } catch {} }
      return reply.replace(/OUTCOME:({.*})/s, "").trim();
    } catch (err) {
      console.warn(`[openai_fallback] failed: ${err.message}`);
    }
  }

  // ── Last resort: rule-based reply ───────────────────────────────────────────
  {
    console.warn("[llm] all LLM paths failed, using guided fallback", { callSid: session.callSid, message: "no LLM available" });
    const reply = buildRuleBasedReply(session, userText);
    session.history.push({ role: "assistant", content: reply });
    return reply;
  }
}

async function getOpeningMessage(session) {
  const explicitOpening =
    session.campaign?.opening_line ||
    session.campaign?.openingLine ||
    session.campaign?.opening_text ||
    session.campaign?.opening_message ||
    session.lead?.opening_text ||
    process.env.DEFAULT_OPENING_TEXT ||
    "";

  // Use explicit opening if set, but cap it to the first 2 sentences so it
  // never runs longer than ~5 seconds. Long openings get cut off by impatient
  // callers — a brief greeting is more effective.
  // Interpolate template placeholders before normalizing
  const leadName = session.lead?.name || "ji";
  const projectName = session.lead?.project || session.campaign?.name || "hamare project";
  const interpolated = explicitOpening.trim()
    .replace(/\{[\s]*lead[\s_]*name[\s]*\}/gi, leadName)
    .replace(/\{[\s]*name[\s]*\}/gi, leadName)
    .replace(/\{[\s]*lead[\s]*\}/gi, leadName)
    .replace(/\[Lead Name\]/gi, leadName)
    .replace(/\{[\s]*project[\s_]*name[\s]*\}/gi, projectName)
    .replace(/\{[\s]*project[\s]*\}/gi, projectName)
    .replace(/\[Project Name\]/gi, projectName);

  const rawOpening = normalizeTtsText(interpolated);
  const opening = rawOpening
    ? (() => {
        // Allow up to 3 sentences, cap at 30 words.
        // ElevenLabs Hindi TTS: ~3.5 words/sec → 30 words ≈ 8.5s audio — acceptable for opening.
        // 20-word cap was cutting configured opening lines mid-sentence when templates
        // had more than one sentence of introduction (e.g. name + company + project intro).
        const sentences = rawOpening.split(/(?<=[.!?।])\s+/);
        const threeSentences = sentences.slice(0, 3).join(" ").trim();
        return capReplyWords(threeSentences, 30);
      })()
    : (() => {
        // Short hardcoded fallback — only used if opening line field is completely empty
        const fallback = `Namaste ${leadName} ji! Main Priya hoon Prop Hunt se. Aapko ek project ke baare mein batana tha.`;
        return capReplyWords(fallback, 30);
      })();

  // Seed history so subsequent LLM turns have context of how the call started
  session.history.push({ role: "user",      content: "[CALL_STARTED]" });
  session.history.push({ role: "assistant", content: opening });
  session.history = session.history.slice(-12);
  return opening;
}

function emotionFromContext(text = "", state = {}) {
  const lowered = text.toLowerCase();
  if (state.stage === "opening") return "warm";
  // Excitement triggers — user expressing interest, mentioning nice features, positives
  if (/(benefit|amenity|feature|offer|launch|badhiya|achha|accha|acha|wah|vah|pasand|sundar|bढ़िया|शानदार|बढ़िया|अच्छा|वाह|पसंद|सुंदर|interest|good|great|nice|love|like)/.test(lowered)) return "excited";
  // Empathy triggers — concerns, price sensitivity, hesitation
  if (/(price|budget|expensive|concern|issue|problem|sochna|baad|later|costly|mehenga|महंगा|सोचना|बाद में|परेशान|दिक्कत|theek nahi|nahi chahiye|nahi chahie)/.test(lowered)) return "empathetic";
  // Professional triggers — site visit, booking, scheduling
  if (/(visit|schedule|book|callback|meeting|dekhna|site|confirm|date|time|slot|aana|aaun|आना|देखना|बुक|कब)/.test(lowered)) return "professional";
  // Default — warm (not neutral) — real estate calls benefit from a warm baseline
  return "warm";
}

// Sarvam AI voice roster — female & male per language
// All voice IDs are lowercase as required by Sarvam API
const SARVAM_VOICE_MAP = {
  en: { female: "priya",  male: "shubh"  },  // English
  hi: { female: "ritu",   male: "rahul"  },  // Hindi
  mr: { female: "roopa",  male: "anand"  },  // Marathi
  ta: { female: "kavya",  male: "kavya"  },  // Tamil  (no dedicated male — kavya works)
  te: { female: "kavya",  male: "vijay"  },  // Telugu
  pa: { female: "simran", male: "simran" },  // Punjabi (no dedicated male)
  bn: { female: "shreya", male: "shreya" },  // Bengali (no dedicated male)
  gu: { female: "priya",  male: "shubh"  },  // Gujarati — fall back to EN voices
  kn: { female: "priya",  male: "shubh"  },  // Kannada  — fall back to EN voices
  ml: { female: "priya",  male: "shubh"  },  // Malayalam — fall back to EN voices
};

// Split reply into natural sentence chunks for streaming delivery
function splitIntoSentences(text) {
  // Split on Hindi/English sentence endings: . ! ? । and ellipsis
  const parts = text.split(/(?<=[.!?।…])\s+/).map(s => s.trim()).filter(Boolean);
  if (parts.length <= 1) return [text];
  // Merge sentences that are too short (< 6 words) with the next one
  const merged = [];
  let buf = '';
  for (const s of parts) {
    buf = buf ? buf + ' ' + s : s;
    if (buf.split(/\s+/).length >= 6 || s === parts[parts.length - 1]) {
      merged.push(buf.trim());
      buf = '';
    }
  }
  if (buf) merged.push(buf.trim());
  return merged.length ? merged : [text];
}

// Hard-cap reply to MAX_WORDS words to prevent long TTS audio.
// ElevenLabs Hindi TTS speaks at ~1.4 words/sec (NOT 2.5 — that's English).
// 12 words × (1/1.4) ≈ 8.6s audio — acceptable for a phone turn.
// Ends with "." so TTS reads it as a complete sentence, not mid-cut.
function capReplyWords(text, maxWords = 12) {
  const words = text.trim().split(/\s+/);
  if (words.length <= maxWords) return text;
  // Strip trailing punctuation from the last kept word, then add period
  const trimmed = words.slice(0, maxWords).join(" ").replace(/[,;—–\s]+$/, "");
  return trimmed + ".";
}

// Stream reply sentence-by-sentence — lead hears first sentence ~200ms sooner
async function synthesizeAndStreamReply(ws, session, fullText) {
  // Hard word-cap before anything else — prevents long audio chunks.
  // ElevenLabs Hindi TTS: ~1.4 words/sec → 12 words ≈ 8.6s audio.
  const capped = capReplyWords(fullText, parseInt(process.env.TTS_MAX_WORDS || "35", 10));

  // Allow up to 3 sentences — lets the agent speak naturally with flow.
  // Word cap above (35 words) keeps total audio under ~10s which is fine for phone calls.
  const allSentences = splitIntoSentences(capped);
  const sentences = allSentences.slice(0, 3);
  let firstSent = false;
  let lastKnownGeneration = session.telephony?.outGeneration || 0;

  for (const sentence of sentences) {
    if (!sentence || session.closed || session.telephony?.hangupScheduled) break;

    // ── Barge-in guard: if outGeneration changed since the last send, a barge-in
    // fired during our wait and cleared the audio queue. Stop streaming — continuing
    // would send the next sentence onto a cleared channel and produce a skip/jump.
    if (firstSent && (session.telephony?.outGeneration || 0) !== lastKnownGeneration) {
      console.log(`[synthesize] barge-in detected mid-stream, stopping early callSid=${session.callSid}`);
      break;
    }

    const audio = await synthesizeSpeech(session, sentence);
    if (!audio) continue;

    if (!firstSent) {
      clearEnablexMedia(ws, session);  // cancel any previous audio
      firstSent = true;
    }

    if (ws.readyState !== WebSocket.OPEN) break;
    await recordAgentAudio(session, audio, "agent-reply");
    sendEnablexMedia(ws, session, audio, "streaming-sentence");
    // Snapshot generation right after send — sendEnablexMedia increments it
    lastKnownGeneration = session.telephony?.outGeneration || 0;

    // Wait for THIS sentence's playback to finish (use its own duration, not last audio's)
    // lastPlaybackMs is updated inside sendEnablexMedia for the just-sent chunk
    const playMs = session.telephony?.lastPlaybackMs || 800;
    await new Promise(r => setTimeout(r, Math.min(playMs + 80, 4000)));  // cap at 4s
  }

  return firstSent;
}

// Known Sarvam voice IDs (all lowercase)
const SARVAM_KNOWN_VOICES = new Set(["priya","shubh","ritu","rahul","roopa","anand","kavya","simran","shreya","vijay"]);

// Infer gender from a Sarvam voice name selected in the dashboard
function inferGenderFromVoiceName(name = "") {
  const male = ["shubh", "rahul", "anand", "vijay"];
  return male.includes(name.toLowerCase()) ? "male" : "female";
}

// Normalise text before TTS: expand abbreviations and fix known mispronunciations
function normalizeTtsText(text) {
  return (text || "")
    // ── Brand names ─────────────────────────────────────────────────────
    .replace(/\bProphunt\b/gi,   "Prop Hunt")
    .replace(/\bProphunts?\b/gi, "Prop Hunt")
    .replace(/\bprop-hunt\b/gi,  "Prop Hunt")
    .replace(/\bArthaleads?\b/gi, "Artha Leads")

    // ── Currency: ₹ / Rs. / INR → "rupaye" ─────────────────────────────
    .replace(/₹\s*/g,     "rupaye ")
    .replace(/Rs\.\s*/gi, "rupaye ")
    .replace(/\bRs\b/gi,  "rupaye")
    .replace(/\bINR\b/g,  "rupaye")

    // ── Area units ──────────────────────────────────────────────────────
    .replace(/sq\.?\s*ft\.?/gi,              "square feet")
    .replace(/\bsqft\b/gi,                   "square feet")
    .replace(/sq\.?\s*f(?:eet|oot)\.?/gi,    "square feet")
    .replace(/sq\.?\s*m(?:t|tr|eter)?\.?/gi, "square meter")
    .replace(/\bsqmt\b/gi,                   "square meter")

    // ── Large Indian number suffixes ─────────────────────────────────────
    // e.g. "1.5Cr" → "1.5 crore",  "80L" / "80 lac" → "80 lakh"
    .replace(/\b(\d+(?:\.\d+)?)\s*[Cc]r\.?\b/g,           "$1 crore")
    .replace(/\b(\d+(?:\.\d+)?)\s*[Ll](?:ac|akh)?\.?\b/g, "$1 lakh")

    // ── Percentage ───────────────────────────────────────────────────────
    .replace(/(\d)\s*%/g, "$1 percent")

    // ── Floor notation: G+12 → "Ground plus 12" ─────────────────────────
    .replace(/\bG\+(\d+)\b/g, "Ground plus $1")

    // ── BHK: space out letters so TTS reads them individually ────────────
    .replace(/\b(\d)\s*BHK\b/g, "$1 B H K");
}

async function synthesizeSpeech(session, text) {
  const normalizedText = normalizeTtsText(text);
  // gender: from campaign (set by dashboard voice selection) → lead → default female
  const gender = session.campaign?.voice_gender || session.lead?.voice_gender || "female";

  // Language-detected voice ID pattern from language-manager (e.g. "hi_female_01")
  const resolvedVoiceId = session.campaign?.voice_id || languageManager.resolveVoice(session.callSid, gender);

  const language = languageManager.getLanguage(session.callSid);
  const lang = languageManager.getBaseLanguage(session.callSid) || "en";

  let voiceId;
  if (SARVAM_KNOWN_VOICES.has(resolvedVoiceId?.toLowerCase())) {
    // Dashboard passed an explicit Sarvam voice name — but auto-switch by language
    // Keep the gender preference; pick the matching voice for the CURRENT detected language
    voiceId = SARVAM_VOICE_MAP[lang]?.[gender] || SARVAM_VOICE_MAP["en"][gender] || "priya";
  } else if (/^([a-z]{2})_(male|female)_\d{2}$/i.test(resolvedVoiceId)) {
    // Language-manager placeholder (e.g. hi_female_01) → resolve to real Sarvam voice
    voiceId = SARVAM_VOICE_MAP[lang]?.[gender] || SARVAM_VOICE_MAP["en"][gender] || "priya";
  } else {
    // Explicit custom voice ID passed (e.g. from Agni config) — use as-is
    voiceId = resolvedVoiceId || "priya";
  }
  voiceId = voiceId.toLowerCase();
  const emotion = emotionFromContext(text, { stage: session.stage });
  try {
    const response = await timed("tts", () =>
      axios.post(
        `${config.services.tts}/synthesize`,
        {
          text: normalizedText,
          voice_id: voiceId,
          language,
          gender,
          emotion,
          context: { stage: session.stage, lead_status: session.outcome?.status || session.lead.status || "new" },
        },
        { responseType: "arraybuffer", timeout: parseInt(process.env.TTS_REQUEST_TIMEOUT_MS || "25000", 10) }
      )
    );
    return Buffer.from(response.data);
  } catch (error) {
    console.warn("[tts] synthesis failed", {
      callSid: session.callSid,
      voiceId,
      language,
      message: error.message,
      status: error.response?.status,
    });
    return null;
  }
}

async function persistSession(session) {
  const serializable = { ...session, startedAt: session.startedAt, updatedAt: nowIso() };
  // Remove non-serializable objects — WebSocket and timer have circular refs that break JSON.stringify
  delete serializable.timer;
  delete serializable.deepgramWs;      // WebSocket → TLSSocket → HTTPParser (circular)
  delete serializable.inboundAudio;    // Buffers can be large — not needed in Redis
  delete serializable.recordings;      // PCM buffer arrays — not needed in Redis
  try {
    await redis.set(`session:${session.callSid}`, JSON.stringify(serializable), "EX", Math.ceil(config.callTimeoutMs / 1000));
  } catch (err) {
    console.warn(`[persist] JSON.stringify failed callSid=${session.callSid}: ${err.message}`);
  }
}

function safeRecordingId(callSid) {
  return String(callSid || crypto.randomUUID()).replace(/[^a-zA-Z0-9_.-]/g, "_");
}

function writeWavFile(filePath, pcm16Buffer, sampleRate = 16000) {
  fs.writeFileSync(filePath, createWavBuffer(pcm16Buffer, sampleRate));
}

function createWavBuffer(pcm16Buffer, sampleRate = 16000) {
  const header = Buffer.alloc(44);
  const byteRate = sampleRate * 2;
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcm16Buffer.length, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(1, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcm16Buffer.length, 40);
  return Buffer.concat([header, pcm16Buffer]);
}

function ensureWavBuffer(audioBuffer, sampleRate = 16000) {
  if (!audioBuffer?.length) return audioBuffer;
  return audioBuffer.subarray(0, 4).toString("ascii") === "RIFF" ? audioBuffer : createWavBuffer(audioBuffer, sampleRate);
}

function recordingUrl(callSid, fileName) {
  return `${getConfiguredPublicBaseUrl()}/recordings/${safeRecordingId(callSid)}/${fileName}`;
}

function getRecordingState(session) {
  if (!session.recording) {
    const recordingId = safeRecordingId(session.callSid);
    const dir = path.join(config.recordingsDir, recordingId);
    fs.mkdirSync(dir, { recursive: true });
    session.recording = {
      id: recordingId,
      dir,
      callerPcmPath: path.join(dir, "caller.pcm"),
      agentPcmPath: path.join(dir, "agent.pcm"),
      mixedPcmPath: path.join(dir, "mixed.pcm"),
      timelinePath: path.join(dir, "timeline.json"),
      timeline: [],
      startedAt: nowIso(),
    };
  }
  return session.recording;
}

async function appendRecordingAudio(session, speaker, pcm16Buffer, label = "audio") {
  if (!session || !pcm16Buffer?.length) return null;
  const recording = getRecordingState(session);
  const targetPath = speaker === "agent" ? recording.agentPcmPath : recording.callerPcmPath;
  await fs.promises.appendFile(targetPath, pcm16Buffer);
  await fs.promises.appendFile(recording.mixedPcmPath, pcm16Buffer);
  recording.timeline.push({
    speaker,
    label,
    timestamp: nowIso(),
    bytes: pcm16Buffer.length,
    duration_ms: Math.round((pcm16Buffer.length / 2 / 16000) * 1000),
  });
  return recording;
}

async function recordCallerAudio(session, pcm16Buffer, label = "caller-media") {
  return appendRecordingAudio(session, "caller", pcm16Buffer, label);
}

async function recordAgentAudio(session, wavBuffer, label = "agent-media") {
  if (!wavBuffer?.length) return null;
  const { pcm, sampleRate } = parseWavInfo(wavBuffer);
  return appendRecordingAudio(session, "agent", resamplePcm16(pcm, sampleRate, 16000), label);
}

async function uploadRecordingToCloudinary(filePath, callSid) {
  if (!process.env.CLOUDINARY_CLOUD_NAME || !fs.existsSync(filePath)) return null;
  try {
    const result = await cloudinary.uploader.upload(filePath, {
      resource_type: "video",
      folder: "call-recordings",
      public_id: `${callSid}/mixed`,
      overwrite: true,
    });
    return result.secure_url;
  } catch (err) {
    console.error("[cloudinary] upload failed:", err.message);
    return null;
  }
}

async function finalizeRecording(session) {
  if (!session?.recording || session.recording.finalized) {
    return session?.recordings || null;
  }
  const recording = session.recording;
  const files = {
    caller: path.join(recording.dir, "caller.wav"),
    agent: path.join(recording.dir, "agent.wav"),
    mixed: path.join(recording.dir, "mixed.wav"),
  };
  const callerPcm = fs.existsSync(recording.callerPcmPath) ? fs.readFileSync(recording.callerPcmPath) : Buffer.alloc(0);
  const agentPcm = fs.existsSync(recording.agentPcmPath) ? fs.readFileSync(recording.agentPcmPath) : Buffer.alloc(0);
  const mixedPcm = fs.existsSync(recording.mixedPcmPath) ? fs.readFileSync(recording.mixedPcmPath) : Buffer.alloc(0);
  if (callerPcm.length) writeWavFile(files.caller, callerPcm);
  if (agentPcm.length) writeWavFile(files.agent, agentPcm);
  if (mixedPcm.length) writeWavFile(files.mixed, mixedPcm);
  await fs.promises.writeFile(recording.timelinePath, JSON.stringify(recording.timeline, null, 2));
  recording.finalized = true;

  // Persist mixed WAV in Redis so it survives container restarts / redeploys.
  // Railway's filesystem is ephemeral — local file URLs break after every deploy.
  // Redis TTL: 30 days (same as call log retention).
  if (mixedPcm.length) {
    try {
      const wavBuffer = fs.readFileSync(files.mixed);
      const b64 = wavBuffer.toString("base64");
      // Only cache recordings under 10 MB to avoid Redis OOM
      if (b64.length < 10 * 1024 * 1024) {
        await redis.set(
          `recording:${session.callSid}`,
          b64,
          "EX",
          30 * 24 * 60 * 60  // 30 days
        );
        console.log(`[recording] cached to Redis callSid=${session.callSid} size=${Math.round(b64.length / 1024)}KB`);
      }
    } catch (err) {
      console.warn("[recording] Redis cache failed:", err.message);
    }
  }

  session.recordingPath = mixedPcm.length ? recordingUrl(session.callSid, "mixed.wav") : null;
  session.recordings = {
    caller_path: callerPcm.length ? files.caller : null,
    agent_path: agentPcm.length ? files.agent : null,
    mixed_path: mixedPcm.length ? files.mixed : null,
    timeline_path: recording.timelinePath,
    caller_url: callerPcm.length ? recordingUrl(session.callSid, "caller.wav") : null,
    agent_url: agentPcm.length ? recordingUrl(session.callSid, "agent.wav") : null,
    mixed_url: mixedPcm.length ? recordingUrl(session.callSid, "mixed.wav") : null,
    timeline: recording.timeline,
  };
  return session.recordings;
}

async function endCall(session, finalStatus = "completed") {
  if (session.closed) return;
  session.closed = true;
  session.status = finalStatus;
  session.endedAt = nowIso();

  // Close Deepgram streaming WebSocket for this call
  closeDeepgramStream(session);

  // Disconnect Agni LiveKit bridge if active
  if (session.agniBridge) {
    session.agniBridge.disconnect().catch(() => {});
    session.agniBridge = null;
  }

  await finalizeRecording(session);
  if (session.recordings?.mixed_path) {
    const cloudUrl = await uploadRecordingToCloudinary(session.recordings.mixed_path, session.callSid);
    if (cloudUrl) {
      session.recordingPath = cloudUrl;
      session.recordings.mixed_url = cloudUrl;
    }
  }
  const durationSec = Math.max(1, Math.round((Date.now() - session.startedTs) / 1000));
  callsTotal.labels(finalStatus).inc();
  callDuration.observe(durationSec);
  const outcome = {
    ...(session.outcome || {
    status: finalStatus,
    call_duration_sec: durationSec,
    transcript_summary: session.history.map((item) => `${item.role}: ${item.content}`).join(" | ").slice(0, 1000),
    site_visit_scheduled: false,
    callback_date: null,
    lead_temperature: "warm",
    qualification: { bhk: "", budget_range: "", purpose: "", timeline: "" },
    full_transcript: JSON.stringify(session.history),
    }),
    status: session.outcome?.status || finalStatus,
    call_duration_sec: durationSec,
    full_transcript: session.outcome?.full_transcript || JSON.stringify(session.history),
    recording_url: session.recordingPath || session.outcome?.recording_url || null,
  };
  try {
    await pushToCRM(session.lead.id, { ...outcome, call_duration_sec: durationSec });
  } catch {}
  await persistCallLog(session, { ...outcome, call_duration_sec: durationSec }, durationSec, finalStatus);
  await persistSession(session);
  sessions.delete(session.callSid);
  languageManager.clear(session.callSid);
}

// Normalize language string from dashboard ("Hindi", "English", "hi", "en", etc.) to ISO code
function normalizeLanguageToISO(lang = "") {
  const map = {
    "hindi": "hi", "english": "en", "marathi": "mr", "tamil": "ta",
    "telugu": "te", "bengali": "bn", "punjabi": "pa", "gujarati": "gu",
    "kannada": "kn", "malayalam": "ml", "auto": "auto",
  };
  const lower = String(lang || "").toLowerCase().split("-")[0];
  return map[lower] || lower || "auto";
}

function createSession(lead, campaign = {}, callSid = crypto.randomUUID()) {
  // Support both "language_preference" (CRM leads) and "language" (dashboard test calls)
  const rawLang = lead.language_preference || lead.language || campaign.language || "auto";
  const preferredLanguage = normalizeLanguageToISO(rawLang);
  languageManager.initialize(callSid, preferredLanguage);
  const session = {
    callSid,
    lead,
    campaign,
    history: [],
    status: "initiated",
    stage: "opening",
    startedAt: nowIso(),
    startedTs: Date.now(),
    closed: false,
    outcome: null,
    recordingPath: null,
    telephony: null,
    pendingGreetingAudio: null,
    dynamicVariables: null,  // set by /call/dial from dashboard KB payload
    _ttsCache: {},           // pre-warmed audio for common phrases
  };
  session.timer = setTimeout(() => endCall(session, "timeout"), config.callTimeoutMs);
  sessions.set(callSid, session);
  return session;
}

// Pre-warm TTS for the most frequent agent phrases so they play from cache instantly.
// Called after session creation — runs in background, doesn't block the dial response.
async function prewarmTTSCache(session) {
  const lang = languageManager.getBaseLanguage(session.callSid) || "hi";
  const phrases = lang === "hi" ? [
    "Ek second.",
    "Samajh gaya.",
    "Bilkul.",
    "Koi baat nahi. Aapka shukriya. Namaste.",
    "Kya aap do BHK ya teen BHK mein interested hain?",
  ] : [
    "One moment.",
    "Got it.",
    "Sure.",
    "Thank you for your time. Goodbye.",
    "Are you looking for a two BHK or three BHK?",
  ];
  for (const phrase of phrases) {
    try {
      const audio = await synthesizeSpeech(session, phrase);
      if (audio) session._ttsCache[phrase.toLowerCase().trim()] = audio;
    } catch { /* non-fatal */ }
  }
  console.log(`[tts-cache] warmed ${Object.keys(session._ttsCache).length} phrases callSid=${session.callSid}`);
}

// Wrap synthesizeSpeech to hit cache first
const _origSynthesize = synthesizeSpeech;
async function synthesizeSpeechCached(session, text) {
  const key = text.toLowerCase().trim();
  if (session._ttsCache?.[key]) {
    console.log(`[tts-cache] HIT callSid=${session.callSid}`);
    return session._ttsCache[key];
  }
  return _origSynthesize(session, text);
}

function remapSessionCallSid(session, nextCallSid) {
  if (!session || !nextCallSid || session.callSid === nextCallSid) return;
  const previousCallSid = session.callSid;
  const preferredLanguage = languageManager.getLanguage(previousCallSid);
  sessions.delete(previousCallSid);
  session.callSid = nextCallSid;
  sessions.set(nextCallSid, session);
  languageManager.initialize(nextCallSid, preferredLanguage);
  languageManager.clear(previousCallSid);
}

function muLawDecodeSample(muLawByte) {
  const MULAW_BIAS = 0x84;
  muLawByte = ~muLawByte & 0xff;
  const sign = muLawByte & 0x80;
  const exponent = (muLawByte >> 4) & 0x07;
  const mantissa = muLawByte & 0x0f;
  let sample = ((mantissa << 3) + MULAW_BIAS) << exponent;
  sample -= MULAW_BIAS;
  return sign ? -sample : sample;
}

function decodeMuLawToPcm16(muLawBuffer) {
  const pcm = Buffer.alloc(muLawBuffer.length * 2);
  for (let i = 0; i < muLawBuffer.length; i += 1) {
    pcm.writeInt16LE(muLawDecodeSample(muLawBuffer[i]), i * 2);
  }
  return pcm;
}

function upsamplePcm16To16k(pcm8kBuffer) {
  const sampleCount = Math.floor(pcm8kBuffer.length / 2);
  const pcm16k = Buffer.alloc(sampleCount * 4);
  for (let i = 0; i < sampleCount; i += 1) {
    const sample = pcm8kBuffer.readInt16LE(i * 2);
    pcm16k.writeInt16LE(sample, i * 4);
    pcm16k.writeInt16LE(sample, i * 4 + 2);
  }
  return pcm16k;
}

function parseWavToPcm16(wavBuffer) {
  return parseWavInfo(wavBuffer).pcm;
}

function parseWavInfo(wavBuffer) {
  const dataIndex = wavBuffer.indexOf(Buffer.from("data"));
  if (dataIndex === -1 || wavBuffer.length < 44) {
    return { pcm: wavBuffer, sampleRate: 16000 };
  }
  const dataLength = wavBuffer.readUInt32LE(dataIndex + 4);
  const sampleRate = wavBuffer.readUInt32LE(24) || 16000;
  return {
    pcm: wavBuffer.subarray(dataIndex + 8, dataIndex + 8 + dataLength),
    sampleRate,
  };
}

function resamplePcm16(pcmBuffer, fromRate, toRate) {
  if (!pcmBuffer?.length || fromRate === toRate) return pcmBuffer;
  const inputSamples = Math.floor(pcmBuffer.length / 2);
  const outputSamples = Math.max(1, Math.floor((inputSamples * toRate) / fromRate));
  const out = Buffer.alloc(outputSamples * 2);

  // When downsampling, apply a simple FIR low-pass anti-aliasing filter before
  // decimation. Without this, frequencies above toRate/2 alias back into the
  // audio band and produce crackling (e.g. 24kHz → 8kHz without filtering).
  if (fromRate > toRate) {
    const ratio = fromRate / toRate;
    // FIR window size: covers ~ratio samples either side for smooth rolloff
    const halfWin = Math.ceil(ratio);
    for (let i = 0; i < outputSamples; i++) {
      const center = (i * fromRate) / toRate;
      let sum = 0, weight = 0;
      for (let j = -halfWin; j <= halfWin; j++) {
        const idx = Math.round(center) + j;
        if (idx < 0 || idx >= inputSamples) continue;
        // Triangular window weight — simple, zero-artifact rolloff
        const w = 1 - Math.abs(j) / (halfWin + 1);
        sum += pcmBuffer.readInt16LE(idx * 2) * w;
        weight += w;
      }
      out.writeInt16LE(Math.round(sum / weight), i * 2);
    }
    return out;
  }

  // Upsampling: linear interpolation is fine (no aliasing risk)
  for (let i = 0; i < outputSamples; i += 1) {
    const sourceIndex = (i * fromRate) / toRate;
    const low = Math.floor(sourceIndex);
    const high = Math.min(low + 1, inputSamples - 1);
    const ratio = sourceIndex - low;
    const a = pcmBuffer.readInt16LE(low * 2);
    const b = pcmBuffer.readInt16LE(high * 2);
    out.writeInt16LE(Math.round(a + (b - a) * ratio), i * 2);
  }
  return out;
}

function downsamplePcm16To8k(pcm16kBuffer) {
  const inputSamples = Math.floor(pcm16kBuffer.length / 2);
  const outputSamples = Math.floor(inputSamples / 2);
  const out = Buffer.alloc(outputSamples * 2);
  for (let i = 0; i < outputSamples; i += 1) {
    const sample = pcm16kBuffer.readInt16LE(i * 4);
    out.writeInt16LE(sample, i * 2);
  }
  return out;
}

function muLawEncodeSample(sample) {
  const MULAW_MAX = 32635;
  const MULAW_BIAS = 0x84;
  let sign = 0;
  if (sample < 0) {
    sign = 0x80;
    sample = -sample;
  }
  sample = Math.min(sample, MULAW_MAX);
  sample += MULAW_BIAS;
  let exponent = 7;
  for (let expMask = 0x4000; (sample & expMask) === 0 && exponent > 0; exponent -= 1, expMask >>= 1) {}
  const mantissa = (sample >> Math.max(exponent + 3, 0)) & 0x0f;
  return (~(sign | (exponent << 4) | mantissa)) & 0xff;
}

function encodePcm16ToMuLaw(pcm8kBuffer) {
  const sampleCount = Math.floor(pcm8kBuffer.length / 2);
  const out = Buffer.alloc(sampleCount);
  for (let i = 0; i < sampleCount; i += 1) {
    out[i] = muLawEncodeSample(pcm8kBuffer.readInt16LE(i * 2));
  }
  return out;
}

function toEnablexMuLawChunks(ttsWavBuffer) {
  const { pcm, sampleRate } = parseWavInfo(ttsWavBuffer);
  const pcm8k = sampleRate === 16000 ? downsamplePcm16To8k(pcm) : resamplePcm16(pcm, sampleRate, 8000);
  const muLaw = encodePcm16ToMuLaw(pcm8k);
  const chunks = [];
  for (let offset = 0; offset < muLaw.length; offset += 160) {
    chunks.push(muLaw.subarray(offset, offset + 160));
  }
  return chunks;
}

function decodeEnablexInboundMedia(event) {
  const payload = Buffer.from(event.media.payload, "base64");
  const format = event.media.format || {};
  const encoding = String(format.encoding || "ulaw").toLowerCase();
  const sampleRate = Number(format.sample_rate || format.sampleRate || 8000);
  if (/linear|pcm|l16|s16/.test(encoding)) {
    return sampleRate === 16000 ? payload : resamplePcm16(payload, sampleRate, 16000);
  }
  const pcm = decodeMuLawToPcm16(payload);
  return sampleRate === 16000 ? pcm : resamplePcm16(pcm, sampleRate, 16000);
}

function sendEnablexMedia(ws, session, audioBuffer, label = "audio") {
  const streamId = session.telephony?.streamId;
  const voiceId = session.telephony?.voiceId || session.callSid;
  if (!audioBuffer || ws.readyState !== WebSocket.OPEN || session.telephony?.provider !== "enablex" || !streamId || !voiceId) {
    return false;
  }
  const chunks = toEnablexMuLawChunks(audioBuffer);
  // Each chunk = 160 bytes = 20ms of 8kHz ulaw audio (8000 samples/sec × 1 byte/sample × 0.02s).
  // * 40 was written when chunks were 320 bytes — now chunks are 160 bytes, so * 20 is correct.
  const playbackMs = chunks.length * 20;
  const generation = (session.telephony.outGeneration || 0) + 1;
  session.telephony.outGeneration = generation;
  session.telephony.agentSpeakingUntil    = Date.now() + playbackMs + 200;  // +200ms — just enough for last chunk to reach phone
  session.telephony.echoSuppressionUntil  = Date.now() + playbackMs + 200;  // +200ms — minimal tail; reduces dead zone eating "haan boliye"
  // Opening greeting protection — cap at 9s max (opening audio is ≤8.8s after word cap fix).
  // Old code: no cap → 16s audio → user blocked for 17s → 1011 Deepgram close.
  if (label && label.startsWith("opening-greeting")) {
    session.telephony.openingProtectionUntil = Date.now() + Math.min(playbackMs, 9000) + 800;
  }
  if (session.inboundAudio && !session.inboundAudio.processing) {
    session.inboundAudio.chunks = [];
    session.inboundAudio.speechFrames = 0;
    session.inboundAudio.silenceFrames = 0;
  }
  console.log(`[enablex-media] sending ${label} for ${voiceId} (${audioBuffer.length} bytes, ${chunks.length} chunks)`);
  session.telephony.lastPlaybackMs = playbackMs;
  chunks.forEach((chunk, index) => {
    setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN || session.telephony.outGeneration !== generation) return;
      const seq = session.telephony.outSeq || 0;
      ws.send(
        JSON.stringify({
          event: "media",
          voice_id: voiceId,
          stream_id: streamId,
          media: {
            seq,
            timestamp: Date.now(),
            format: {
              encoding: "ulaw",
              sample_rate: 8000,
              channels: 1,
            },
            payload: chunk.toString("base64"),
          },
        })
      );
      session.telephony.outSeq = seq + 1;
    }, index * 20);
  });
  return true;
}

function scheduleAgentSideHangup(ws, session, reason = "completed-reply") {
  if (!session || session.closed || session.telephony?.hangupScheduled) {
    return;
  }
  const delayMs = Math.max(1500, (session.telephony?.lastPlaybackMs || 0) + 1200);
  const callSidSnapshot = session.callSid;
  session.telephony = {
    ...(session.telephony || {}),
    hangupScheduled: true,
    hangupReason: reason,
  };
  setTimeout(async () => {
    // Look up by snapshot callSid — session may have already been deleted from map if caller hung up first
    const current = sessions.get(callSidSnapshot) || (session.closed ? null : session);
    if (!current || current.closed) {
      return;
    }
    const voiceId = current.telephony?.voiceId || current.callSid;
    console.log("[enablex-media] agent-side hangup firing", { callSid: callSidSnapshot, voiceId, reason, delayMs });

    // Step 1: Cancel any remaining agent audio
    if (ws.readyState === WebSocket.OPEN) {
      clearEnablexMedia(ws, current);
    }

    // Step 2: Close WebSocket from our side — primary signal to EnableX to end media/call
    if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
      ws.close(1000, "agent-ended");
    }

    // Step 3: Brief pause then call REST hangup API as belt-and-suspenders
    await new Promise((r) => setTimeout(r, 600));
    try {
      await hangupEnablexCall(voiceId);
      console.log("[enablex-media] hangup API succeeded", { callSid: callSidSnapshot, voiceId });
    } catch (error) {
      console.warn("[enablex-media] hangup API call failed", {
        callSid: callSidSnapshot,
        voiceId,
        status: error.response?.status,
        body: error.response?.data || error.message,
      });
    }

    // Step 4: Clean up our session (ws.on("close") may also call endCall, but endCall is idempotent)
    await endCall(current, "agent_completed");
  }, delayMs);
}

function clearEnablexMedia(ws, session) {
  if (ws.readyState !== WebSocket.OPEN || !session?.telephony?.streamId) return;
  session.telephony.outGeneration = (session.telephony.outGeneration || 0) + 1;
  ws.send(
    JSON.stringify({
      event: "clear_media",
      voice_id: session.telephony.voiceId || session.callSid,
      stream_id: session.telephony.streamId,
    })
  );
}

// ── Streaming mulaw queue ─────────────────────────────────────────────────────
// Accepts raw G.711 μ-law bytes at 8kHz from ElevenLabs (ulaw_8000) and sends
// to EnableX in 160-byte chunks at 40ms intervals. No conversion needed.
function createMulawStreamQueue(ws, session, label = "stream") {
  const voiceId  = session.telephony?.voiceId  || session.callSid;
  const streamId = session.telephony?.streamId;
  if (!streamId || !voiceId || ws.readyState !== WebSocket.OPEN) return null;
  if (session.telephony?.provider !== "enablex") return null;

  const generation = (session.telephony.outGeneration || 0) + 1;
  session.telephony.outGeneration = generation;
  session.telephony.agentSpeakingUntil   = Date.now() + 30000; // tentative — updated on close()
  session.telephony.echoSuppressionUntil = Date.now() + 30000;
  if (session.inboundAudio) { session.inboundAudio.chunks = []; session.inboundAudio.speechFrames = 0; }

  const queue   = [];
  let totalSent = 0;
  let running   = false;
  let isClosed  = false;
  let leftover  = Buffer.alloc(0); // partial PCM16 bytes waiting for a full pair

  function stopped() {
    return session.telephony.outGeneration !== generation || ws.readyState !== WebSocket.OPEN;
  }

  function tick() {
    if (stopped() || queue.length === 0) { running = false; return; }
    const mulaw = queue.shift();
    totalSent++;
    const seq = (session.telephony.outSeq || 0) + 1;
    session.telephony.outSeq = seq;
    try {
      ws.send(JSON.stringify({
        event: "media", stream_id: streamId, voice_id: voiceId,
        media: {
          seq,
          timestamp: Date.now(),
          format: { encoding: "ulaw", sample_rate: 8000, channels: 1 },
          payload: mulaw.toString("base64"),
        },
      }));
    } catch {}
    // 20ms interval = 160 bytes / 20ms = 8000 bytes/sec = correct G.711 8kHz rate.
    // 40ms was half the bitrate → buffer underruns at EnableX → crackling audio.
    setTimeout(tick, 20);
  }

  function kickSender() {
    if (!running && queue.length > 0 && !stopped()) { running = true; tick(); }
  }

  console.log(`[mulaw-queue] open label=${label} callSid=${session.callSid}`);

  return {
    // ElevenLabs sends raw ulaw_8000 bytes (G.711 μ-law, 8kHz, 1 byte/sample).
    // No conversion needed — split into 160-byte chunks (20ms each at 8kHz ulaw) and queue.
    // Chunks are sent every 20ms → EnableX receives at correct telephony bitrate (8000 bytes/sec).
    appendUlaw(ulawBytes) {
      if (isClosed || stopped()) return;
      const buf  = Buffer.concat([leftover, ulawBytes]);
      const step = 160; // 160 bytes = 160 samples = 20ms of 8kHz ulaw per chunk
      let   i    = 0;
      for (; i + step <= buf.length; i += step) {
        queue.push(buf.slice(i, i + step));
      }
      leftover = buf.slice(i);
      kickSender();
    },

    close() {
      // Flush any remaining partial ulaw bytes (pad to 160)
      if (leftover.length > 0) {
        const padded = Buffer.concat([leftover, Buffer.alloc(160 - (leftover.length % 160))]);
        queue.push(padded);
        leftover = Buffer.alloc(0);
        kickSender();
      }
      isClosed = true;
      // Each chunk = 160 bytes = 20ms at 8kHz ulaw. * 20 gives the true audio duration.
      // +200ms margins on both agentSpeakingUntil / echoSuppressionUntil — minimal tail.
      const pendingMs = (totalSent + queue.length) * 20;
      session.telephony.lastPlaybackMs       = pendingMs;
      session.telephony.agentSpeakingUntil   = Date.now() + pendingMs + 200;  // +200ms only
      session.telephony.echoSuppressionUntil = Date.now() + pendingMs + 200;  // +200ms — tight tail to stop swallowing "haan boliye"
      console.log(`[mulaw-queue] closed totalSent=${totalSent} pending=${queue.length} playbackMs=${pendingMs} callSid=${session.callSid}`);
    },

    isStopped() { return stopped(); },
  };
}

// ── True streaming pipeline: LLM tokens → ElevenLabs WS → mulaw queue ────────
// TTFA: ~500-800ms  vs  5-6s with sequential HTTP pipeline.
// Falls back to standard pipeline on any error (caller detects null return).
//
// Returns: reply string when done   |   null when caller should use standard pipeline
async function streamingLLMWithElevenLabs(ws, session, userText, { onFirstAudio } = {}) {
  const elevenKey = process.env.ELEVENLABS_API_KEY;
  const openaiKey = process.env.OPENAI_API_KEY;
  const ttsProvider = process.env.TTS_PROVIDER || "elevenlabs";
  if (!elevenKey || !openaiKey || ttsProvider !== "elevenlabs") return null;

  const callSid  = session.callSid;
  // Hard cap for ElevenLabs streaming — Hindi TTS is ~1.4 words/sec, 15 words ≈ 10s audio.
  // agentConfig.wordCap may be much larger (e.g. 55 set in dashboard); we apply the
  // minimum of the two so the system prompt and the audio cap agree.
  const agentWordCap = parseInt(session.agentConfig?.wordCap || "99", 10);
  const maxWords = Math.min(agentWordCap, parseInt(process.env.TTS_MAX_WORDS || "35", 10));
  const model    = process.env.ELEVENLABS_MODEL || "eleven_flash_v2_5";

  // Voice ID — same resolution as TTS service
  const gender = session.campaign?.voice_gender || session.lead?.voice_gender || "female";
  const voiceId = gender === "male"
    ? (process.env.ELEVENLABS_VOICE_MALE   || "pNInz6obpgDQGcFmaJgB")
    : (process.env.ELEVENLABS_VOICE_FEMALE || process.env.ELEVENLABS_VOICE_ID || "zmh5xhBvMzqR4ZlXgcgL");

  // Emotion → voice settings
  const emotion = emotionFromContext(userText, { stage: session.stage });
  // Voice emotion settings — warm and natural without being theatrical.
  // stability: lower = more pitch variation (conversational), higher = steady/monotone.
  // style: expressiveness 0-1. Indian real estate calls work best at 0.15-0.30 range —
  //   too high sounds fake/over-the-top on phone calls; too low sounds robotic.
  // similarity_boost: 1.0 keeps voice identity consistent.
  const ESETTINGS = {
    warm:         { stability: 0.35, similarity_boost: 1.0, style: 0.25, speed: 0.95 },  // friendly, welcoming
    excited:      { stability: 0.25, similarity_boost: 1.0, style: 0.35, speed: 1.02 },  // "वाह!", good news — not theatrical
    empathetic:   { stability: 0.55, similarity_boost: 1.0, style: 0.15, speed: 0.92 },  // budget concerns, hesitation
    professional: { stability: 0.60, similarity_boost: 1.0, style: 0.10, speed: 1.00 },  // site visit, confirmations
    neutral:      { stability: 0.45, similarity_boost: 1.0, style: 0.18, speed: 0.97 },  // default conversational
  };
  const voiceSettings = ESETTINGS[emotion] || ESETTINGS.neutral;

  // Build LLM messages — mirror getLLMResponse logic exactly
  const language = languageManager.getLanguage(callSid);
  // Push user turn to history (same as getLLMResponse line 1059)
  session.history.push({ role: "user", content: userText });
  session.history = session.history.slice(-10);
  const knowledgeContext = (
    session.dynamicVariables?.knowledge_base ||
    (await getKnowledgeContext(session.campaign?.project_id || session.lead.project_id, userText).catch(() => ""))
  ).slice(0, 3500);
  const resolvedLanguage = (language === "auto" || language === "auto-IN" || !language)
    ? (languageManager.getBaseLanguage(callSid) || "hi")
    : language;
  const systemPrompt = buildSystemPrompt(session.lead, knowledgeContext, resolvedLanguage, session.agentConfig || {});
  const historyContext = session.history.slice(-6).slice(0, -1);
  const messages = [
    { role: "system", content: systemPrompt },
    ...historyContext,
    { role: "user",   content: `[CURRENT — respond to this only]: ${userText}` },
  ];

  const t0 = Date.now();
  let fullText    = "";
  let wordCount   = 0;
  let doneSending = false;
  let mulawQueue  = null;
  let audioFired  = false;

  function fireOnFirstAudio() {
    if (!audioFired) { audioFired = true; if (onFirstAudio) onFirstAudio(); }
  }

  return new Promise((resolve, reject) => {
    const wsUrl =
      `wss://api.elevenlabs.io/v1/text-to-speech/${voiceId}/stream-input` +
      // ulaw_8000 = G.711 μ-law at 8kHz — directly compatible with EnableX, no conversion needed.
      // pcm_8000 is NOT supported by ElevenLabs WebSocket streaming (stream-input endpoint)
      // and silently falls back to MP3 → treating MP3 bytes as PCM → crackling/garbage audio.
      // optimize_streaming_latency=3: balances TTFA vs audio quality.
      // Level 4 is most aggressive (lowest latency) but causes audio artifacts / robotic
      // voice on phone calls — especially noticeable mid-sentence on ulaw_8000.
      `?model_id=${model}&output_format=ulaw_8000&optimize_streaming_latency=3`;
    let elevenWs;
    try { elevenWs = new WebSocket(wsUrl, { headers: { "xi-api-key": elevenKey } }); }
    catch (e) { return reject(e); }

    elevenWs.on("open", async () => {
      // BOS — voice settings sent before any text
      elevenWs.send(JSON.stringify({
        text: " ",
        voice_settings: voiceSettings,
        // chunk_length_schedule: controls audio chunk sizes ElevenLabs sends back.
        // [50, 100...] was too aggressive — small chunks on phone calls (ulaw_8000) cause
        // robotic/glitchy audio because the G.711 codec needs sufficient audio length to
        // maintain natural prosody. Use larger chunks for clean phone call quality.
        generation_config: { chunk_length_schedule: [120, 160, 250] },
      }));

      // LLM streaming — tokens pipe directly into ElevenLabs WS
      try {
        const llmResp = await axios.post(
          "https://api.openai.com/v1/chat/completions",
          { model: process.env.OPENAI_MODEL || "gpt-4o", messages, temperature: 0.4, max_tokens: 180, stream: true },
          { headers: { Authorization: `Bearer ${openaiKey}` }, responseType: "stream", timeout: 8000 }
        );
        let remainder = "";
        llmResp.data.on("data", (chunk) => {
          if (doneSending) return;
          const text  = remainder + chunk.toString("utf8");
          const lines = text.split("\n");
          remainder   = lines.pop() || "";
          let batch   = "";
          for (const line of lines) {
            if (!line.startsWith("data: ")) continue;
            const d = line.slice(6).trim();
            if (d === "[DONE]") {
              doneSending = true;
              if (batch) elevenWs.send(JSON.stringify({ text: batch }));
              elevenWs.send(JSON.stringify({ text: "" }));
              return;
            }
            try { const tok = JSON.parse(d).choices?.[0]?.delta?.content || ""; fullText += tok; batch += tok; wordCount = fullText.trim().split(/\s+/).length; } catch {}
          }
          if (!batch || doneSending) return;
          if (wordCount >= maxWords) {
            doneSending = true;
            elevenWs.send(JSON.stringify({ text: batch }));
            elevenWs.send(JSON.stringify({ text: "" }));   // word cap — EOS
          } else {
            elevenWs.send(JSON.stringify({ text: batch, try_trigger_generation: wordCount >= 5 }));
          }
        });
        llmResp.data.on("end", () => { if (!doneSending) { doneSending = true; elevenWs.send(JSON.stringify({ text: "" })); } });
        llmResp.data.on("error", (e) => { console.warn(`[eleven-stream] llm err callSid=${callSid}: ${e.message}`); elevenWs.close(); });
      } catch (e) { console.warn(`[eleven-stream] llm start err callSid=${callSid}: ${e.message}`); elevenWs.close(); reject(e); }
    });

    elevenWs.on("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        if (msg.audio) {
          // ElevenLabs sends ulaw_8000: raw G.711 μ-law bytes, ready for EnableX
          const ulaw = Buffer.from(msg.audio, "base64");
          if (!mulawQueue) {
            // CRITICAL ORDER: clearEnablexMedia FIRST (increments outGeneration to N+1, sends
            // clear_media to EnableX), THEN createMulawStreamQueue (increments to N+2, captures N+2).
            // Reversed order causes stopped()=true immediately → totalSent=0 (no audio plays).
            clearEnablexMedia(ws, session);
            mulawQueue = createMulawStreamQueue(ws, session, "eleven-stream");
            console.log(`[eleven-stream] TTFA=${Date.now() - t0}ms callSid=${callSid}`);
            fireOnFirstAudio();
          }
          if (mulawQueue && !mulawQueue.isStopped()) mulawQueue.appendUlaw(ulaw);
        }
        if (msg.isFinal) elevenWs.close();
      } catch {}
    });

    elevenWs.on("close", () => {
      if (mulawQueue) mulawQueue.close();
      fireOnFirstAudio(); // ensure lock released even if no audio arrived
      const reply = fullText.trim();
      const clean = reply.replace(/OUTCOME:({.*})/s, "").trim();
      // History and OUTCOME are managed by caller (getLLMResponse already pushed user msg)
      // Push assistant reply here since we bypassed getLLMResponse
      session.history.push({ role: "assistant", content: reply });
      const m = reply.match(/OUTCOME:({.*})/s);
      if (m) { try { session.outcome = JSON.parse(m[1]); } catch {} }
      console.log(`[eleven-stream] done TTFA=${Date.now()-t0}ms reply="${clean.slice(0,60)}" callSid=${callSid}`);
      resolve(clean);
    });

    elevenWs.on("error", (e) => {
      console.warn(`[eleven-stream] ws error callSid=${callSid}: ${e.message}`);
      fireOnFirstAudio();
      reject(e);
    });
  });
}

async function processCallerUtterance(ws, session, callSid, reason = "utterance") {
  const inbound = session.inboundAudio;
  if (!inbound || inbound.processing || !inbound.chunks.length || session.telephony?.hangupScheduled) return;
  inbound.processing = true;
  const utteranceAudio = Buffer.concat(inbound.chunks);
  inbound.chunks = [];
  inbound.speechFrames = 0;
  inbound.silenceFrames = 0;
  inbound.lastFlushAt = Date.now();

  // 3200 bytes = 200ms of audio — catches short acks like "haan", "ji", "ok" (was 8000 = 500ms)
  const MIN_UTTERANCE_BYTES = 3200;
  if (utteranceAudio.length < MIN_UTTERANCE_BYTES) {
    console.log(`[enablex-media] skipping short utterance callSid=${callSid} bytes=${utteranceAudio.length}`);
    inbound.processing = false;
    return;
  }

  try {
    const t0 = Date.now();
    console.log(`[enablex-media] processing utterance callSid=${callSid} reason=${reason} bytes=${utteranceAudio.length}`);

    // ── STT: use speculative result if available, otherwise fire fresh ────────
    // Speculative path: promise was fired 160ms+ ago and may already be resolved.
    // If the speculative audio was shorter (we collected more after firing),
    // check if the extra audio changes things — if > 30% more bytes, re-transcribe.
    let transcription;
    const specPromise = inbound.speculativePromise;
    const specBytes   = inbound.speculativeAudio?.length || 0;
    const extraRatio  = specBytes > 0 ? utteranceAudio.length / specBytes : 2;
    inbound.speculativePromise = null;
    inbound.speculativeAudio   = null;

    if (specPromise && extraRatio < 5.0) {
      // Audio grew less than 5× since speculative fired — speculative result is close enough
      transcription = await specPromise;
      if (!transcription?.text) {
        // Speculative failed, run full transcription now
        transcription = await transcribeAudioDirect(utteranceAudio, languageManager.getBaseLanguage(callSid) || "auto");
      }
      console.log(`[stt] SPECULATIVE callSid=${callSid} wait=${Date.now()-t0}ms text="${transcription?.text || ""}"`);
    } else {
      // Utterance grew significantly after speculative fired — full audio is more accurate
      const baseLang = languageManager.getBaseLanguage(callSid) || "auto";
      transcription = await transcribeAudioDirect(utteranceAudio, baseLang);
      console.log(`[stt] FRESH callSid=${callSid} latency=${Date.now()-t0}ms text="${transcription?.text || ""}"`);
    }
    console.log(`[stt] result: "${transcription?.text || ""}" lang=${transcription?.language || ""} elapsed=${Date.now()-t0}ms`);
    if (!transcription.text) return;

    // Fallback STT minimum word count: Deepgram handles VAD properly so 1-word responses
    // are fine there. Local STT fallback is noisier — short fragments are often agent echo
    // or background noise. Require ≥2 words UNLESS it's a known valid 1-word response.
    const wordCount = transcription.text.trim().split(/\s+/).filter(w => w.length > 0).length;
    if (wordCount < 1) {
      console.log(`[enablex-media] skipping empty transcription callSid=${callSid}`);
      return;
    }
    const VALID_ONE_WORD = /^(haan|ha|ji|nahi|nahin|theek|ok|okay|yes|no|done|bilkul|zaroor|sure|accha|achha|acha|bye|hello|हाँ|हां|जी|नहीं|नहि|ठीक|ओके|बिल्कुल|ज़रूर|अच्छा|हेलो)$/i;
    if (wordCount === 1 && !VALID_ONE_WORD.test(transcription.text.trim().replace(/[।!?.]/g, ""))) {
      console.log(`[enablex-media] skipping 1-word noise callSid=${callSid} text="${transcription.text}"`);
      return;
    }
    // Single-character noise filter (not a real word)
    if (wordCount === 1 && transcription.text.trim().length <= 1) {
      console.log(`[enablex-media] skipping single-char noise callSid=${callSid} text="${transcription.text}"`);
      return;
    }
    // Background noise filter — ElevenLabs wraps noise transcripts in parentheses e.g. "(background music)"
    // Drop these so they don't trigger LLM responses
    const cleanText = transcription.text.trim();
    if (/^\(.*\)$/.test(cleanText) || /^\[.*\]$/.test(cleanText)) {
      console.log(`[enablex-media] skipping noise transcript callSid=${callSid} text="${cleanText}"`);
      return;
    }

    // First-utterance TV/radio filter — before the lead has said anything meaningful,
    // long sentences with no greeting or question are almost certainly background audio
    // (TV, call-waiting music, ambient noise), not the lead speaking to us.
    if (!session.firstValidUtterance) {
      const wordCount2 = cleanText.split(/\s+/).filter(w => w.length > 0).length;
      const looksConversational = /\b(hello|haan|ha\b|hi\b|ji\b|namaste|theek|kaun|kya|bolo|nahi|nahin|bol|sun|suno|aap|tum|main|acha|accha|ok|haan ji|ha ji|kal|aaj|tell|what|how|where|when|price|cost|yes|no|sure|wait|who|why|want|know|about)\b/i.test(cleanText)
        || cleanText.includes("?") || wordCount2 <= 6;
      if (!looksConversational) {
        console.log(`[enablex-media] skipping first-utterance background noise callSid=${callSid} text="${cleanText.slice(0, 60)}"`);
        return;
      }
    }
    session.firstValidUtterance = true;

    const prevLang = languageManager.getBaseLanguage(callSid);
    languageManager.recordUtterance(callSid, transcription.language, transcription.text);
    const newLang = languageManager.getBaseLanguage(callSid);
    if (prevLang !== newLang) {
      console.log(`[lang-detect] language switched ${prevLang} → ${newLang} callSid=${callSid}`);
    }
    session.stage = "qualification";
    // Upgrade status so dashboard shows call is active (not stuck at stream_started)
    if (session.status === "stream_started") session.status = "active";

    const t1 = Date.now();
    const reply = await getLLMResponse(session, transcription.text);
    console.log(`[agent] callSid=${callSid} llm=${Date.now()-t1}ms total_to_llm=${Date.now()-t0}ms reply="${reply.slice(0,60)}"`);

    // Release processing lock before TTS playback waits — allows barge-in
    if (session.inboundAudio) {
      session.inboundAudio.processing  = false;
      session.inboundAudio.lastFlushAt = Date.now();
    }

    // Stream sentence-by-sentence — lead hears first word sooner
    const streamed = await synthesizeAndStreamReply(ws, session, reply);

    if (!streamed) {
      // Fallback: synthesize full reply in one shot
      const isHindi = (languageManager.getBaseLanguage(callSid) || "hi") === "hi";
      const ttsLastResort = isHindi
        ? "Ek second, main aapki baat samajh rahi hoon."
        : "One moment, I am processing your query.";
      const speech = await synthesizeSpeech(session, reply) ||
        await synthesizeSpeech(session, ttsLastResort);
      if (speech && ws.readyState === WebSocket.OPEN) {
        clearEnablexMedia(ws, session);
        await recordAgentAudio(session, speech, "agent-reply");
        sendEnablexMedia(ws, session, speech, "reply");
      }
    }

    if (isTerminalGuidedState(session)) {
      console.log(`[agent] terminal state reached, scheduling hangup callSid=${callSid} state=${session.guidedState}`);
      scheduleAgentSideHangup(ws, session, session.guidedState);
    } else {
      console.log(`[agent] continuing call callSid=${callSid} guidedState=${session.guidedState || "null"}`);
    }

    console.log(`[agent] total_latency=${Date.now()-t0}ms callSid=${callSid}`);
    await persistSession(session);
  } catch (error) {
    console.warn("[enablex-media] utterance handling failed", { callSid, message: error.message });
    const fallback = languageManager.fallback(callSid);
    const speech = await synthesizeSpeech(session, fallback);
    if (speech && ws.readyState === WebSocket.OPEN) {
      clearEnablexMedia(ws, session);
      await recordAgentAudio(session, speech, "agent-fallback");
      sendEnablexMedia(ws, session, speech, "fallback");
    }
  } finally {
    const currentInbound = session.inboundAudio;
    if (currentInbound) {
      currentInbound.processing = false;
      currentInbound.lastFlushAt = Date.now();
      const queuedBytes = currentInbound.chunks.reduce((s, c) => s + c.length, 0);
      if (queuedBytes > MIN_UTTERANCE_BYTES && ws.readyState === WebSocket.OPEN && !session.closed) {
        setImmediate(() => {
          processCallerUtterance(ws, session, callSid, "queued-after-processing").catch((error) =>
            console.warn("[enablex-media] queued utterance failed", { callSid, message: error.message })
          );
        });
      } else if (currentInbound.chunks.length) {
        // Discard tiny queued fragments — they're noise from the agent's playback period
        currentInbound.chunks = [];
        currentInbound.speechFrames = 0;
        currentInbound.silenceFrames = 0;
      }
    }
  }
}

// ── Deepgram Streaming STT ─────────────────────────────────────────────────────
// Opens a per-call WebSocket directly to Deepgram's live transcription API.
// EnableX sends μ-law 8kHz audio; Deepgram natively handles this encoding.
// With endpointing=150ms, Deepgram fires speech_final quickly when the caller
// pauses — we process it immediately without any silence-wait buffer.
// Savings vs. old pipeline: ~700ms per turn (600ms silence wait + ~100ms STT).
//
// Set DEEPGRAM_API_KEY env var to enable. Falls back to local VAD+STT if unset.
function openDeepgramStream(ws, session, callSid) {
  const dgKey = process.env.DEEPGRAM_API_KEY;
  if (!dgKey) {
    console.log(`[deepgram] DEEPGRAM_API_KEY not set — using local STT pipeline callSid=${callSid}`);
    return null;
  }
  if (session.deepgramWs?.readyState === WebSocket.OPEN) return session.deepgramWs;

  const lang = languageManager.getBaseLanguage(callSid) || "hi";
  // Deepgram language strategy:
  // • "language=multi" is NOT a valid Deepgram param — causes HTTP 400.
  // • "detect_language=true" + "language=xxx" together also cause HTTP 400.
  // • For Hindi/Hinglish: use detect_language=true ONLY (no language param).
  //   Deepgram nova-2 auto-detects Hindi, English, and code-switched Hinglish correctly.
  // • For regional Indian languages: use explicit language code (mr, ta, te, etc.)
  // • Override with DEEPGRAM_LANGUAGE env var if needed (e.g. "hi" to force Hindi).
  const forcedLang = process.env.DEEPGRAM_LANGUAGE || "";
  const baseLang   = languageManager.getBaseLanguage(callSid) || "hi";
  // null → use detect_language=true (no language param) — correct for Hinglish/auto-detect
  const langMap    = { hi: null, hinglish: null, en: "en-IN", mr: "mr", ta: "ta", te: "te", kn: "kn", ml: "ml", bn: "bn", gu: "gu", pa: "pa" };
  const dgLang     = forcedLang || langMap[baseLang]; // undefined/null = use detect_language
  const dgParams = new URLSearchParams({
    encoding:        "mulaw",
    sample_rate:     "8000",
    model:           process.env.DEEPGRAM_MODEL || "nova-2-general",
    endpointing:     process.env.DEEPGRAM_ENDPOINTING || "150",  // 150ms silence → speech_final (was 300ms — reduces latency)
    interim_results: "false",   // skip partials — only act on finals
    smart_format:    "true",    // normalises numbers/punctuation
  });
  // Add language OR detect_language — NEVER both (causes 400)
  if (dgLang) {
    dgParams.set("language", dgLang);
  } else {
    dgParams.set("detect_language", "true"); // auto-detects Hindi/English/Hinglish per utterance
  }

  let dgWs;
  try {
    dgWs = new WebSocket(`wss://api.deepgram.com/v1/listen?${dgParams}`, {
      headers: { Authorization: `Token ${dgKey}` },
    });
  } catch (err) {
    console.warn(`[deepgram] WebSocket create failed callSid=${callSid}:`, err.message);
    return null;
  }

  dgWs.on("open", () => {
    console.log(`[deepgram] stream opened callSid=${callSid} lang=${lang}`);
    session.deepgramReady = true;
  });

  dgWs.on("message", async (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type === "Error") {
      console.warn(`[deepgram] API error callSid=${callSid}:`, msg.description || msg.message);
      return;
    }
    if (msg.type !== "Results") return;

    const alt = msg.channel?.alternatives?.[0];
    const transcript = (alt?.transcript || "").trim();
    if (!transcript) return;

    // ONLY act on speech_final — the definitive end-of-utterance signal after silence.
    // is_final fires on committed audio chunks (can be partial mid-sentence) — skip those.
    // Without this, partial phrases like "How would you" reach the LLM and get wrong answers.
    if (!msg.speech_final) return;

    const conf = alt?.confidence || 0;
    console.log(`[deepgram] speech_final callSid=${callSid} text="${transcript.slice(0, 80)}" conf=${conf.toFixed(2)}`);

    // Confidence threshold — skip garbled/background-noise transcripts.
    // Phone calls in India have high background noise; low-conf short phrases are almost always noise.
    // Three-tier filter:
    //   • Any conf < 0.45 → reject always (language-switching hallucinations, e.g. Spanish at 0.40)
    //   • conf < 0.70 AND ≤3 words → reject  ("Media half food.", "Abi" echoes, random clicks)
    //   • conf < 0.60 AND ≤5 words → reject  (short ambiguous fragments)
    // ── Confidence filter — tuned for real Indian phone call speech ────────────
    // Deepgram's confidence for Hindi/Hinglish on mobile networks is typically 0.55–0.85.
    // We use a SLIDING scale: shorter phrases need higher confidence to pass
    // (short noise bursts are indistinguishable from real speech at low conf).
    //
    // CRITICAL LESSON: "Hi. Interested. Who?" at conf=0.67, 3 words is REAL USER SPEECH.
    // The old flat 0.70 threshold for ≤3 words was silently dropping genuine responses.
    //
    // Scale:
    //   1 word  → need 0.65+ (or a known conversational word at 0.55+)
    //   2 words → need 0.58+ (or a known conversational word at 0.50+)
    //   3 words → need 0.52+  ← "Hi. Interested. Who." at 0.67 PASSES now
    //   4 words → need 0.47+  (effectively the 0.45 floor)
    //   5+ words → 0.45 absolute floor (longer = harder to fake as noise)
    const MIN_CONF_ANY = parseFloat(process.env.DEEPGRAM_MIN_CONF || "0.45");
    const words = transcript.split(/\s+/).length;
    const KNOWN_CONV = /\b(hello|haan|ha|ji|nahi|nahin|theek|ok|okay|yes|no|done|bilkul|zaroor|sure|accha|achha|acha|bye|namaste|bol|bolo|sun|suno|kya|kaun|aap|tum|main|budget|bhk|price|location|project|visit|kab|kitna|kitni|details|info|batao|batayein|samjha|samjhaiye|interested|interest|dekhna|chahiye|chahie|karo|lena|dikhao)\b/i.test(transcript);
    const minConfForLength =
      words === 1 ? (KNOWN_CONV ? 0.48 : 0.65) :  // known words: 0.55→0.48 (barge-in may truncate)
      words === 2 ? (KNOWN_CONV ? 0.45 : 0.55) :  // "haan boliye", "interested hoon" → 0.45+
      words === 3 ? 0.50 :
      words <= 4  ? 0.47 :
      MIN_CONF_ANY; // 5+ words: absolute floor only
    if (conf < MIN_CONF_ANY || conf < minConfForLength) {
      console.log(`[deepgram] conf-filter skipped callSid=${callSid} conf=${conf.toFixed(2)} minNeeded=${minConfForLength} words=${words} text="${transcript}"`);
      return;
    }

    // Clear local audio buffer — Deepgram owns this utterance
    const inbound = session.inboundAudio;
    if (inbound) {
      inbound.chunks             = [];
      inbound.speechFrames       = 0;
      inbound.silenceFrames      = 0;
      inbound.speculativePromise = null;
      inbound.speculativeAudio   = null;
    }

    // Extract Deepgram's per-utterance language detection (only present when detect_language=true)
    const dgDetectedLang = msg.channel?.detected_language || msg.detected_language || null;
    if (dgDetectedLang) {
      console.log(`[deepgram] detected_language=${dgDetectedLang} callSid=${callSid}`);
    }
    await processTranscriptDirect(ws, session, callSid, transcript, "deepgram", dgDetectedLang);
  });

  dgWs.on("error", (err) => {
    console.warn(`[deepgram] error callSid=${callSid}:`, err.message);
    session.deepgramWs    = null;
    session.deepgramReady = false;
  });

  dgWs.on("close", (code) => {
    console.log(`[deepgram] closed callSid=${callSid} code=${code}`);
    session.deepgramWs    = null;
    session.deepgramReady = false;
    // Auto-reconnect on server-side errors (1011=internal error, 1006=abnormal close)
    // Don't reconnect if the call is already closed or we intentionally closed (1000)
    if (!session.closed && code !== 1000 && code !== 1001) {
      setTimeout(() => {
        if (!session.closed && !session.deepgramWs) {
          console.log(`[deepgram] reconnecting after code=${code} callSid=${callSid}`);
          openDeepgramStream(ws, session, callSid);
        }
      }, 1000);
    }
  });

  session.deepgramWs    = dgWs;
  session.deepgramReady = false;  // set true on "open"
  return dgWs;
}

function closeDeepgramStream(session) {
  const dgWs = session?.deepgramWs;
  if (!dgWs) return;
  session.deepgramWs    = null;
  session.deepgramReady = false;
  try {
    if (dgWs.readyState === WebSocket.OPEN) {
      dgWs.send(JSON.stringify({ type: "CloseStream" }));
    }
    dgWs.terminate();
  } catch {}
}

// processTranscriptDirect — fast path when Deepgram already produced the transcript.
// Same pipeline as processCallerUtterance but STT is skipped entirely.
async function processTranscriptDirect(ws, session, callSid, transcriptText, source = "deepgram", detectedLanguage = null) {
  const inbound = session.inboundAudio;
  if (!inbound || inbound.processing || session.telephony?.hangupScheduled || session.closed) return;

  // Deduplicate — Deepgram can fire speech_final twice for the same phrase
  if (
    session._lastDgTranscript === transcriptText &&
    Date.now() - (session._lastDgTranscriptAt || 0) < 1500
  ) {
    console.log(`[${source}] dedup transcript, skipping callSid=${callSid}`);
    return;
  }
  session._lastDgTranscript   = transcriptText;
  session._lastDgTranscriptAt = Date.now();

  inbound.processing = true;
  const t0 = Date.now();

  try {
    console.log(`[${source}] processing transcript callSid=${callSid} text="${transcriptText.slice(0, 80)}"`);

    const cleanText = transcriptText.trim();

    // Background noise filter — parenthetical noise markers
    if (/^\(.*\)$/.test(cleanText) || /^\[.*\]$/.test(cleanText)) {
      console.log(`[${source}] noise transcript, skipping callSid=${callSid}`);
      return;
    }

    const wordCount = cleanText.split(/\s+/).filter(w => w.length > 0).length;
    if (wordCount < 1) return;
    if (wordCount === 1 && cleanText.length <= 1) return;

    // First-utterance TV/radio background noise filter
    if (!session.firstValidUtterance) {
      const looksConversational =
        /\b(hello|haan|ha\b|hi\b|ji\b|namaste|theek|kaun|kya|bolo|nahi|nahin|bol|sun|suno|aap|tum|main|acha|accha|ok|haan ji|ha ji|kal|aaj|tell|what|how|where|when|price|cost|yes|no|sure|wait|who|why|want|know|about)\b/i.test(cleanText)
        || cleanText.includes("?") || wordCount <= 6;
      if (!looksConversational) {
        console.log(`[${source}] first-utterance background noise, skipping callSid=${callSid} text="${cleanText.slice(0, 60)}"`);
        return;
      }
    }
    session.firstValidUtterance = true;

    // Reset silence-nudge counter ONLY for substantive responses (≥4 words).
    // Single-word replies ("Hello?", "Location", "Project") don't count as real engagement —
    // resetting on those caused an infinite nudge #1 loop because the counter never grew.
    const wordCountForNudgeReset = cleanText.split(/\s+/).filter(w => w.length > 0).length;
    if (wordCountForNudgeReset >= 4) {
      session.nudgesSent = 0;
    }

    // Language tracking — prefer Deepgram's detected_language over our prior guess.
    // When detect_language=true, Deepgram tells us per-utterance what language it heard.
    // This is the ground truth for language switching (Marathi, Tamil, etc.).
    const prevLang = languageManager.getBaseLanguage(callSid);
    const langForRecord = detectedLanguage || prevLang || "hi";
    languageManager.recordUtterance(callSid, langForRecord, cleanText);
    const newLang = languageManager.getBaseLanguage(callSid);
    if (prevLang !== newLang) {
      console.log(`[lang-detect] language switched ${prevLang} → ${newLang} (deepgram detected: ${detectedLanguage || "n/a"}) callSid=${callSid}`);
    }

    session.stage = "qualification";
    if (session.status === "stream_started") session.status = "active";

    // ── Goodbye detection — intercept before LLM, close call immediately ─────
    // If user clearly signals they want to end the call, don't fire another LLM turn.
    // Pattern: matches "bye", "thank you bye", "dhanyawaad", "alvida", etc. at start
    // OR contains "bye"/"goodbye" in a short phrase (≤5 words, indicating wrap-up).
    const lcClean = cleanText.toLowerCase().replace(/[।!?.]/g, "").trim();
    const isGoodbye =
      /^(bye|goodbye|alvida|shukriya|dhanyawaad|dhanyavaad|tata|ok bye|theek hai bye|chalte hain|chal theek|chhodo|nahi chahiye|nahin chahiye|band karo|khatam|no thanks|no thank you|not interested|abhi nahi|nahi abhi)\b/i.test(lcClean) ||
      (/\b(bye|goodbye|dhanyawaad|shukriya|alvida)\b/i.test(lcClean) && wordCountForNudgeReset <= 5);
    if (isGoodbye && !isTerminalGuidedState(session)) {
      console.log(`[agent] goodbye detected callSid=${callSid} text="${cleanText}"`);
      const lang = languageManager.getBaseLanguage(callSid) || "hi";
      const goodbyeText = (lang === "hi" || lang === "hinglish")
        ? "Theek hai! Agar kabhi bhi property dekhni ho, toh hamare paas zaroor aayein. Dhanyawaad! Namaste."
        : "No problem! Feel free to reach out anytime. Thank you and goodbye!";
      session.guidedState = "closed";
      const goodbyeAudio = await synthesizeSpeech(session, goodbyeText).catch(() => null);
      if (goodbyeAudio && ws.readyState === WebSocket.OPEN && !session.closed) {
        clearEnablexMedia(ws, session);
        sendEnablexMedia(ws, session, goodbyeAudio, "goodbye");
      }
      scheduleAgentSideHangup(ws, session, "user-goodbye");
      return;
    }

    const t1 = Date.now();

    // ── Try ElevenLabs streaming pipeline first (LLM tokens → TTS → audio in ~500ms) ──
    // Falls back to sequential pipeline on any error.
    let reply = "";
    let usedStreaming = false;

    function releaseLock() {
      if (session.inboundAudio) {
        session.inboundAudio.processing  = false;
        session.inboundAudio.lastFlushAt = Date.now();
      }
    }

    try {
      const streamResult = await streamingLLMWithElevenLabs(ws, session, cleanText, {
        onFirstAudio: releaseLock,
      });
      if (streamResult !== null) {
        // Streaming handled LLM + TTS + history push — done
        reply = streamResult;
        usedStreaming = true;
        console.log(`[agent] streaming callSid=${callSid} total=${Date.now()-t0}ms reply="${reply.slice(0,60)}"`);
      }
      // streamResult === null means streaming not configured → no history push happened
    } catch (err) {
      // Streaming was attempted (history push already happened) but failed partway.
      // Roll back the user history push so getLLMResponse doesn't double-push.
      const last = session.history[session.history.length - 1];
      if (last?.role === "user" && last.content === cleanText) session.history.pop();
      console.warn(`[eleven-stream] fallback to HTTP pipeline callSid=${callSid}: ${err.message}`);
    }

    if (!usedStreaming) {
      // Standard sequential pipeline (fallback / non-ElevenLabs TTS)
      reply = await getLLMResponse(session, cleanText);
      console.log(`[agent] callSid=${callSid} llm=${Date.now()-t1}ms total=${Date.now()-t0}ms reply="${reply.slice(0,60)}"`);
      releaseLock(); // release before TTS so barge-in works during playback

      const streamed = await synthesizeAndStreamReply(ws, session, reply);
      if (!streamed) {
        const isHindiDg = (languageManager.getBaseLanguage(callSid) || "hi") === "hi";
        const ttsLastResortDg = isHindiDg
          ? "Ek second, main aapki baat samajh rahi hoon."
          : "One moment, I am processing your query.";
        const speech = await synthesizeSpeech(session, reply) ||
          await synthesizeSpeech(session, ttsLastResortDg);
        if (speech && ws.readyState === WebSocket.OPEN) {
          clearEnablexMedia(ws, session);
          await recordAgentAudio(session, speech, "agent-reply");
          sendEnablexMedia(ws, session, speech, "reply");
        }
      }
    }

    if (isTerminalGuidedState(session)) {
      console.log(`[agent] terminal state reached, scheduling hangup callSid=${callSid} state=${session.guidedState}`);
      scheduleAgentSideHangup(ws, session, session.guidedState);
    } else {
      console.log(`[agent] continuing call callSid=${callSid} guidedState=${session.guidedState || "null"}`);

      // Silence nudge — fires if lead doesn't respond after the agent finishes speaking.
      // Timer starts from echoSuppressionUntil (when user CAN actually speak), not from
      // when the agent's LLM started — otherwise the nudge fires before echo suppression
      // even ends, giving the user almost no time to respond.
      const nudgeDelay  = parseInt(process.env.SILENCE_NUDGE_MS || "15000", 10);
      // echoSuppressionUntil is updated when the streaming queue closes; grab it now with
      // a small polling delay so it reflects the final close() value.
      const scheduleNudge = () => {
        const echoEnd   = session.telephony?.echoSuppressionUntil || 0;
        const waitUntil = Math.max(echoEnd, Date.now()); // don't go back in time
        const delay     = Math.max(0, waitUntil - Date.now()) + nudgeDelay;
        const turnToken = session._lastTurnAt;
        setTimeout(async () => {
          if (session.closed || session._lastTurnAt !== turnToken || !ws || ws.readyState !== 1) return;
          const nudgeLang = languageManager.getBaseLanguage(callSid) || "hi";

          // Track nudge count per call — cap at 2, then hangup gracefully.
          // After 2 unanswered nudges the lead is clearly unavailable.
          session.nudgesSent = (session.nudgesSent || 0) + 1;
          console.log(`[agent] silence-nudge #${session.nudgesSent} callSid=${callSid}`);

          const MAX_NUDGES = parseInt(process.env.MAX_SILENCE_NUDGES || "3", 10);
          if (session.nudgesSent > MAX_NUDGES) {
            // Lead not responding — say goodbye and hang up
            const byeText = nudgeLang === "hi" || nudgeLang === "hinglish"
              ? "Main baad mein call karti hoon. Dhanyawaad! Namaste."
              : "I'll try calling you at a better time. Thank you! Goodbye.";
            console.log(`[agent] nudge-limit reached, hanging up callSid=${callSid}`);
            const byeAudio = await synthesizeSpeech(session, byeText).catch(() => null);
            if (byeAudio && ws.readyState === 1 && !session.closed) {
              clearEnablexMedia(ws, session);
              sendEnablexMedia(ws, session, byeAudio, "nudge-bye");
            }
            scheduleAgentSideHangup(ws, session, "no-response");
            return;
          }

          // Vary nudge text: first nudge is a gentle check-in, second is a final prompt
          let nudgeText;
          if (nudgeLang === "hi" || nudgeLang === "hinglish") {
            nudgeText = session.nudgesSent === 1
              ? "Haan? Koi sawaal hai toh batayein, main hoon yahan."
              : "Lagta hai aap busy hain — kab call karein aapko?";
          } else {
            nudgeText = session.nudgesSent === 1
              ? "Are you there? Feel free to ask anything."
              : "You seem busy — when would be a better time to call?";
          }

          const nudgeAudio = await synthesizeSpeech(session, nudgeText).catch(() => null);
          if (nudgeAudio && ws.readyState === 1 && !session.closed) {
            clearEnablexMedia(ws, session);
            sendEnablexMedia(ws, session, nudgeAudio, "nudge");
          }
        }, delay);
      };
      session._lastTurnAt = Date.now();
      // Schedule nudge after a brief pause to let echoSuppressionUntil settle (set by queue close())
      setTimeout(scheduleNudge, 200);
    }

    console.log(`[agent] total_latency=${Date.now() - t0}ms callSid=${callSid} source=${source}`);
    await persistSession(session);

  } catch (error) {
    console.warn(`[${source}] processing failed`, { callSid, message: error.message });
    const fallback = languageManager.fallback(callSid);
    const speech = await synthesizeSpeech(session, fallback);
    if (speech && ws.readyState === WebSocket.OPEN) {
      clearEnablexMedia(ws, session);
      await recordAgentAudio(session, speech, "agent-fallback");
      sendEnablexMedia(ws, session, speech, "fallback");
    }
  } finally {
    if (session.inboundAudio) {
      session.inboundAudio.processing    = false;
      session.inboundAudio.lastFlushAt   = Date.now();
    }
  }
}

// ── SPECULATIVE_STT_FRAMES: fire STT after this many speech frames ─────────────
// Used in the LOCAL fallback pipeline (when Deepgram is not available).
// 8 frames × 20ms = 160ms of speech → STT starts while we still collect audio.
const SPECULATIVE_STT_FRAMES = 8;

// ── handleCallerAudioFrame — accepts optional rawMulaw for Deepgram forwarding ─
// rawMulaw: the raw μ-law bytes from EnableX before PCM decoding (extracted by
// the WebSocket message handler so we avoid re-encoding on every frame).
async function handleCallerAudioFrame(ws, session, callSid, audioBuffer, rawMulaw = null) {
  if (!session.inboundAudio) {
    session.inboundAudio = {
      chunks: [], speechFrames: 0, silenceFrames: 0,
      bargeinFrames: 0,           // consecutive speech frames during agent playback
      bargeinBuffer: [],          // mulaw frames buffered during barge-in detection → flushed to Deepgram on confirm
      processing: false, lastFlushAt: Date.now(),
      speculativePromise: null,   // in-flight STT request fired early
      speculativeAudio: null,     // audio snapshot sent speculatively
    };
  }
  await recordCallerAudio(session, audioBuffer, "caller-media");

  // ── Agni mode: stream audio directly to LiveKit, skip local VAD/STT/LLM/TTS ──
  if (session.agniBridge?.connected) {
    session.agniBridge.pushCallerAudio(audioBuffer);
    return;
  }

  const inbound = session.inboundAudio;
  const hasSpeech = detectSpeech(audioBuffer); // sync — no HTTP, ~0.05ms

  // Opening protection — the greeting plays fully before we listen for anything.
  // This prevents background noise or an early "hello" from cutting off the opening.
  if (session.telephony?.openingProtectionUntil && Date.now() < session.telephony.openingProtectionUntil) {
    return; // Drop all inbound audio while opening plays
  }

  // Barge-in: caller speaks while agent is playing → cancel agent audio.
  // Threshold: 6 consecutive frames (120ms) — catches "haan" (0.15s) reliably.
  // Echo artefacts are short bursts (<80ms = 4 frames) so 6 frames filters them.
  // Was 10 frames (200ms) which was too high — "haan" at 120ms never triggered barge-in,
  // so user's speech was dropped by echo suppression and Deepgram never heard it.
  //
  // KEY FIX: buffer frames during barge-in detection so Deepgram gets the FULL word.
  // Without buffering, frames 1-5 of "haan" were dropped before Deepgram, giving
  // Deepgram only the tail of the word → garbled transcription or silence.
  if (session.telephony?.agentSpeakingUntil && Date.now() < session.telephony.agentSpeakingUntil) {
    if (hasSpeech) {
      inbound.bargeinFrames  = (inbound.bargeinFrames  || 0) + 1;
      inbound.bargeinBuffer  = inbound.bargeinBuffer  || [];
      inbound.bargeinBuffer.push(rawMulaw || null); // store mulaw frame for Deepgram replay
      if (inbound.bargeinFrames >= 6) {
        // Barge-in confirmed — stop agent, lift suppression, flush buffered frames to Deepgram
        clearEnablexMedia(ws, session);
        session.telephony.agentSpeakingUntil   = 0;
        session.telephony.echoSuppressionUntil = 0;
        // Replay buffered frames so Deepgram hears the FULL word (not just frames 6+)
        if (session.deepgramWs?.readyState === WebSocket.OPEN && session.deepgramReady) {
          for (const storedMulaw of inbound.bargeinBuffer) {
            if (storedMulaw) {
              try { session.deepgramWs.send(storedMulaw); } catch {}
            } else {
              // fallback: re-encode from PCM16 not available — send current frame only
            }
          }
        }
        inbound.bargeinFrames = 0;
        inbound.bargeinBuffer = [];
        inbound.speculativePromise = null;
        inbound.speculativeAudio   = null;
        console.log(`[enablex-media] barge-in confirmed (6 frames) callSid=${callSid}`);
      }
    } else {
      inbound.bargeinFrames = 0; // reset on silence — must be sustained speech
      inbound.bargeinBuffer = [];
    }
  } else {
    inbound.bargeinFrames = 0;
    inbound.bargeinBuffer = [];
  }

  if (session.telephony?.agentSpeakingUntil && Date.now() < session.telephony.agentSpeakingUntil) {
    return;
  }

  // ── Echo suppression dead zone ──────────────────────────────────────────────
  // The agent's voice echoes back through the phone speaker ~100-400ms after the
  // agentSpeakingUntil window closes. Without this guard, the echo gets captured as
  // user speech, STT'd as the agent's own question, and creates a reply loop.
  // During this window we drop all frames — real user speech starts slightly later.
  if (session.telephony?.echoSuppressionUntil && Date.now() < session.telephony.echoSuppressionUntil) {
    return;
  }

  // ── Deepgram streaming path (primary when DEEPGRAM_API_KEY is set) ───────────
  // Forward raw μ-law bytes directly to Deepgram WebSocket.
  // Deepgram handles VAD + endpointing (300ms) + transcription in real-time.
  // speech_final callback → processTranscriptDirect (skips all local buffering).
  if (session.deepgramWs?.readyState === WebSocket.OPEN && session.deepgramReady) {
    // Forward raw mulaw directly to Deepgram — it handles VAD+endpointing+transcription
    const mulaw = rawMulaw || encodePcm16ToMuLaw(downsamplePcm16To8k(audioBuffer));
    try {
      session.deepgramWs.send(mulaw);
    } catch (err) {
      console.warn(`[deepgram] send failed callSid=${callSid}:`, err.message);
      session.deepgramWs    = null;
      session.deepgramReady = false;
      // Fall through to local pipeline below on this frame
    }
    if (session.deepgramWs) return;  // Deepgram owns this frame
  }

  // ── Local VAD + silence detection (fallback when Deepgram is not available) ──
  const isCollecting = inbound.chunks.length > 0;
  if (hasSpeech || isCollecting) inbound.chunks.push(audioBuffer);
  if (inbound.processing) return;

  if (hasSpeech) {
    inbound.speechFrames += 1;
    inbound.silenceFrames = 0;

    // ── Speculative STT: fire early after 8 frames (160ms) ──────────────────
    // STT processes in parallel with remaining audio collection.
    // When silence triggers, the STT may already be done — saves ~200ms.
    if (inbound.speechFrames === SPECULATIVE_STT_FRAMES && !inbound.speculativePromise && !inbound.processing) {
      const earlySnap = Buffer.concat(inbound.chunks);
      const baseLang = languageManager.getBaseLanguage(callSid) || "auto";
      inbound.speculativeAudio   = earlySnap;
      inbound.speculativePromise = transcribeAudioDirect(earlySnap, baseLang)
        .catch(err => {
          console.warn(`[speculative-stt] failed callSid=${callSid}:`, err.message);
          return null;
        });
      console.log(`[speculative-stt] fired at ${inbound.speechFrames} frames callSid=${callSid}`);
    }
    return;
  }

  if (!isCollecting) return;
  inbound.silenceFrames += 1;
  const bufferedMs = inbound.chunks.length * 20;
  const enoughSpeech = inbound.speechFrames >= 10 || bufferedMs >= 1500;
  const endedBySilence = inbound.silenceFrames >= 15;  // 300ms silence (was 30×20ms=600ms) — cuts wait in half
  const tooLong = bufferedMs >= 10000;

  if ((enoughSpeech && endedBySilence) || tooLong) {
    await processCallerUtterance(ws, session, callSid, endedBySilence ? "silence" : "max-buffer");
  }
}

app.get("/metrics", async (_req, res) => {
  res.set("Content-Type", registry.contentType);
  res.end(await registry.metrics());
});

app.get("/health", async (_req, res) => {
  try {
    await redis.ping();
    res.json({ status: acceptingTraffic ? "ok" : "draining", active_sessions: sessions.size });
  } catch (error) {
    res.status(500).json({ status: "error", message: error.message });
  }
});

// ── ElevenLabs voices proxy — dashboard uses this to populate voice dropdown ─
let _elVoicesCache = null;
let _elVoicesCachedAt = 0;
app.get("/voices", async (_req, res) => {
  const apiKey = process.env.ELEVENLABS_API_KEY;
  if (!apiKey) return res.status(503).json({ error: "ELEVENLABS_API_KEY not set", voices: [] });
  // 5-minute cache
  if (_elVoicesCache && Date.now() - _elVoicesCachedAt < 300_000) {
    return res.json({ voices: _elVoicesCache });
  }
  try {
    const resp = await axios.get("https://api.elevenlabs.io/v1/voices", {
      headers: { "xi-api-key": apiKey },
      timeout: 8000,
    });
    const voices = (resp.data.voices || []).map(v => ({
      voice_id: v.voice_id,
      name:     v.name,
      gender:   v.labels?.gender || "unknown",
      language: v.labels?.language || "",
      accent:   v.labels?.accent  || "",
      preview_url: v.preview_url || null,
    }));
    _elVoicesCache = voices;
    _elVoicesCachedAt = Date.now();
    console.log(`[voices] fetched ${voices.length} voices from ElevenLabs`);
    return res.json({ voices });
  } catch (err) {
    console.error("[voices] ElevenLabs API error:", err.response?.status, err.message);
    return res.status(502).json({ error: "Failed to fetch voices", voices: _elVoicesCache || [] });
  }
});

// Session status — polled by dashboard Test Call panel
app.get("/sessions", (_req, res) => {
  const list = Array.from(sessions.values()).map((s) => ({
    call_sid: s.callSid,
    status: s.status || "active",
    state: s.guidedState || null,
    closed: s.closed,
    phone: s.lead?.phone,
    lead_name: s.lead?.name,
    language: languageManager.getLanguage(s.callSid),
    started_at: s.startedAt,
  }));
  res.json({ sessions: list, count: list.length });
});

app.get("/sessions/:callSid", (req, res) => {
  const session = sessions.get(req.params.callSid);
  if (!session) {
    return res.status(404).json({ call_sid: req.params.callSid, status: "completed", state: "not_found" });
  }
  const turnCount = Math.floor((session.history?.length || 0) / 2);
  const detectedLang = languageManager.getBaseLanguage(session.callSid);
  res.json({
    call_sid: session.callSid,
    status: session.status || "active",
    state: session.guidedState || null,
    closed: session.closed,
    phone: session.lead?.phone,
    lead_name: session.lead?.name,
    language: languageManager.getLanguage(session.callSid),
    detected_language: detectedLang,
    started_at: session.startedAt,
    turn_count: turnCount,
    kb_loaded: !!(session.dynamicVariables?.knowledge_base),
    voice_gender: session.campaign?.voice_gender || "female",
    last_agent_reply: session.history?.filter(h => h.role === "assistant").slice(-1)[0]?.content?.slice(0, 100) || null,
  });
});

app.post("/call/dial", async (req, res) => {
  if (!acceptingTraffic) {
    return res.status(503).json({ error: "Service draining" });
  }
  const lead = req.body.lead || (req.body.phone ? { id: crypto.randomUUID(), name: "Unknown Lead", phone: req.body.phone } : null);
  if (!lead || !lead.phone) {
    return res.status(400).json({ error: "lead.phone is required" });
  }
  const session = createSession(lead, req.body.campaign || {});
  // Store KB context / dynamic variables from dashboard for Agni injection
  if (req.body.dynamic_variables && typeof req.body.dynamic_variables === 'object') {
    session.dynamicVariables = req.body.dynamic_variables;
    if (req.body.dynamic_variables.knowledge_base) {
      console.log(`[dial] KB context attached (${req.body.dynamic_variables.knowledge_base.length} chars)`);
    }
  }
  // Store agent config (pitch tone, word cap, language strictness, escalation line, agent name)
  if (req.body.agent_config && typeof req.body.agent_config === 'object') {
    session.agentConfig = req.body.agent_config;
    console.log(`[dial] agent_config: tone=${session.agentConfig.pitchTone || 'balanced'} wordCap=${session.agentConfig.wordCap || 30} lang=${session.agentConfig.langStrictness || 'pure-hindi'}`);
  }
  await persistSession(session);
  const greeting = await getOpeningMessage(session);
  session.pendingGreetingAudio = await synthesizeSpeech(session, greeting);
  // Pre-warm TTS cache in background — ready before call connects
  prewarmTTSCache(session).catch(() => {});
  const provider = resolveTelephonyProvider(req.body.provider);

  if (provider === "enablex") {
    try {
      const openingLine = (
        req.body.opening_line ||
        req.body.campaign?.opening_line ||
        req.body.campaign?.openingLine ||
        greeting ||
        buildEnablexOpeningLine(lead.name || "there")
      ).trim();
      const enablexCall = await placeEnablexOutboundCall({ lead, session, openingLine });
      remapSessionCallSid(session, enablexCall.provider_call_id);
      session.telephony = {
        provider: "enablex",
        from: config.enablex.fromNumber,
        to: lead.phone,
        callSid: enablexCall.provider_call_id,
      };
      session.status = enablexCall.provider_status;
      scheduleEnablexStreamStart(session, "post-dial");
      await persistSession(session);
      return res.json({
        call_sid: enablexCall.provider_call_id,
        lead_id: lead.id,
        phone: lead.phone,
        status: enablexCall.provider_status,
        greeting: openingLine,
        provider: "enablex",
        provider_response: enablexCall.raw,
        kb_attached: !!(session.dynamicVariables?.knowledge_base),
        kb_chars: session.dynamicVariables?.knowledge_base?.length || 0,
      });
    } catch (error) {
      return res.status(502).json({
        error: "Failed to place outbound EnableX call",
        details: error.response?.data || error.message,
        call_sid: session.callSid,
        lead_id: lead.id,
        greeting,
      });
    }
  }

  res.json({
    call_sid: session.callSid,
    lead_id: lead.id,
    phone: lead.phone,
    status: "queued",
    greeting,
    provider: "simulated",
  });
});

app.post("/call/bulk-dial", async (req, res) => {
  const campaignId = req.body.campaign_id || crypto.randomUUID();
  const leads = req.body.leads || (await fetchDialableLeads(campaignId, req.body.limit || 10, req.body.filters || {}));
  const results = [];
  for (const lead of leads.slice(0, config.maxConcurrentCalls)) {
    const session = createSession(lead, req.body.campaign || {});
    await persistSession(session);
    results.push({ call_sid: session.callSid, lead_id: lead.id, phone: lead.phone, status: "queued" });
  }
  res.json({ campaign_id: campaignId, queued: results.length, results });
});

app.post("/call/inbound", async (req, res) => {
  const { phone } = req.body;
  if (!phone) {
    return res.status(400).json({ error: "phone is required" });
  }
  try {
    const lead = await fetchLeadByPhone(phone);
    const session = createSession(lead, {});
    await persistSession(session);
    res.json({ call_sid: session.callSid, lead });
  } catch {
    res.status(404).json({ error: "Lead not found" });
  }
});

app.all("/call/enablex/events", async (req, res) => {
  const payload = req.body && Object.keys(req.body).length ? req.body : req.query;
  const callSid = extractEnablexCallSid(payload);
  const callStatus = normalizeEnablexStatus(payload);
  console.log("[enablex-event] received", {
    voice_id: callSid,
    status: callStatus,
    keys: Object.keys(payload || {}),
    payload,
  });
  const session = callSid ? sessions.get(callSid) : null;

  if (session) {
    session.status = callStatus || session.status;
    session.telephony = {
      ...(session.telephony || {}),
      provider: "enablex",
      lastEvent: payload,
    };
    if (shouldStartEnablexStream(callStatus)) {
      scheduleEnablexStreamStart(session, `event-${callStatus}`, { force: callStatus === "connected" });
    }
    if (["completed", "disconnected", "failed", "busy", "no-answer", "cancelled", "canceled"].includes(callStatus)) {
      clearTimeout(session.timer);
      await endCall(session, callStatus);
    } else {
      await persistSession(session);
    }
  }

  res.json({ status: "ok" });
});

wss.on("connection", (ws, req) => {
  console.log(`[enablex-media] websocket connected url=${req.url || "/"}`);
  const wsUrl = new URL(req.url, "http://localhost");
  const pathParts = wsUrl.pathname.split("/").filter(Boolean);
  const requestedCallSid = wsUrl.searchParams.get("callSid") || pathParts[pathParts.length - 1] || crypto.randomUUID();
  let activeCallSid = requestedCallSid;
  let session = sessions.get(requestedCallSid) || null;
  const heartbeat = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.ping();
  }, 10000);

  ws.on("message", async (message, isBinary) => {
    let audioBuffer = null;
    if (isBinary) {
      if (!session) return;
      console.log(`[enablex-media] binary frame received bytes=${Buffer.byteLength(message)}`);
      audioBuffer = Buffer.from(message);
    } else {
      try {
        const event = JSON.parse(message.toString());
        if (event.event !== "media") {
          console.log(`[enablex-media] event received type=${event.event || "unknown"}`);
        }
        if (event.event === "connected") {
          console.log("[enablex-media] connected");
          return;
        }
        if (event.event === "start_media") {
          const voiceId = event.start?.voice_id || event.voice_id || activeCallSid;
          const streamId = event.stream_id || event.start?.stream_id || null;
          console.log(`[enablex-media] start_media received voiceId=${voiceId} streamId=${streamId || ""}`);
          if (!session && voiceId) {
            session = sessions.get(voiceId) || null;
            activeCallSid = voiceId;
          }
          if (!session) return;
          session.telephony = {
            ...(session.telephony || {}),
            provider: "enablex",
            streamId,
            voiceId,
            callSid: voiceId,
            outSeq: session.telephony?.outSeq || 0,
          };
          session.status = "stream_started";
          console.log(`[enablex-media] stream started for ${voiceId}`);

          // ── Deepgram streaming STT: open per-call WebSocket for real-time transcription ──
          // Opens immediately so it's ready before the first caller utterance.
          // Falls back to local VAD+STT if DEEPGRAM_API_KEY is not set.
          if (!config.agni.enabled) {
            openDeepgramStream(ws, session, voiceId);
          }

          // ── Agni mode: create LiveKit session, skip local greeting synthesis ──
          if (config.agni.enabled) {
            try {
              // Base vars + any KB context passed from the dashboard at dial time
              const agniDynamicVars = {
                lead_name:    session.lead?.name || "there",
                phone:        session.lead?.phone || "",
                project:      session.campaign?.name || session.campaign?.project_name || session.lead?.project || "",
                language:     session.lead?.language || session.lead?.language_preference || "english",
                opening_line: session.campaign?.opening_line || session.campaign?.openingLine || "",
                // Merge KB context + any other vars from the dashboard dial request
                ...(session.dynamicVariables || {}),
              };
              if (agniDynamicVars.knowledge_base) {
                console.log(`[agni-bridge] injecting KB context callSid=${voiceId} chars=${agniDynamicVars.knowledge_base.length}`);
              }
              const agniSession = await createAgniSession({
                apiKey:           config.agni.apiKey,
                agentId:          config.agni.agentId,
                callSid:          voiceId,
                dynamicVariables: agniDynamicVars,
              });
              console.log(`[agni-bridge] session created callSid=${voiceId} agni_session=${agniSession.session_id}`);
              session.agniSessionId = agniSession.session_id;

              const bridge = new AgniBridge({
                callSid: voiceId,
                livekitUrl: agniSession.url,
                token: agniSession.access_token,
                onAgentAudio: (pcm16Buffer) => {
                  // Agni speaks → encode μ-law → send to EnableX
                  if (ws.readyState === WebSocket.OPEN) {
                    sendEnablexMedia(ws, session, pcm16Buffer, "agni-reply");
                  }
                },
                onDisconnect: (reason) => {
                  console.log(`[agni-bridge] session ended callSid=${voiceId} reason=${reason}`);
                  // Agni hung up → clean up our side too
                  if (!session.closed) {
                    scheduleAgentSideHangup(ws, session, "agni_completed", 800);
                  }
                },
              });

              session.agniBridge = bridge;
              await bridge.connect();

              // Agni sends its own opening line — skip local TTS greeting
              session.pendingGreetingAudio = null;
              session.openingPlayedAt = nowIso();
            } catch (err) {
              console.error(`[agni-bridge] failed to start callSid=${voiceId}`, err.message);
              // Fall back to local STT/LLM/TTS pipeline
              session.agniBridge = null;
              if (!session.pendingGreetingAudio) {
                const greeting = await getOpeningMessage(session);
                session.pendingGreetingAudio = await synthesizeSpeech(session, greeting);
              }
              if (session.pendingGreetingAudio) {
                const pending = session.pendingGreetingAudio;
                setTimeout(() => {
                  if (sendEnablexMedia(ws, session, pending, "opening-greeting")) {
                    recordAgentAudio(session, pending, "opening-greeting").catch(() => {});
                    session.pendingGreetingAudio = null;
                    session.openingPlayedAt = nowIso();
                  }
                }, 700);
              }
            }
          } else {
            // ── Local pipeline mode (no Agni) ──────────────────────────────────
            if (!session.pendingGreetingAudio) {
              const greeting = await getOpeningMessage(session);
              session.pendingGreetingAudio = await synthesizeSpeech(session, greeting);
            }
            if (session.pendingGreetingAudio) {
              const pending = session.pendingGreetingAudio;
              // Fallback timer: plays opening if first-media path hasn't fired in 1200ms.
              // IMPORTANT: check openingPlayedAt — first inbound media packet plays the
              // opening immediately (see first-media handler below). Without this guard,
              // opening plays TWICE: once at ~200ms (first-media) and again at 1200ms,
              // which the caller hears as opening → 1s gap → opening again (the "4s delay").
              setTimeout(() => {
                if (session.closed) return;
                if (session.openingPlayedAt) return; // already played via first-media path
                if (sendEnablexMedia(ws, session, pending, "opening-greeting")) {
                  recordAgentAudio(session, pending, "opening-greeting").catch((error) =>
                    console.warn("[recording] opening capture failed", error.message)
                  );
                  session.pendingGreetingAudio = null;
                  session.openingPlayedAt = nowIso();
                  console.log(`[enablex-media] opening played via fallback-timer callSid=${session.callSid}`);
                }
              }, 1200);
            }
          }

          await persistSession(session);
          return;
        }
        if (event.event === "stop_media") {
          console.log(`[enablex-media] stop_media received callSid=${activeCallSid}`);
          if (!session) return;
          clearTimeout(session.timer);
          await endCall(session, "completed");
          return;
        }
        if (event.event !== "media" || !event.media?.payload) return;
        if (!session) return;
        const voiceId = event.voice_id || session.telephony?.voiceId || activeCallSid;
        const streamId = event.stream_id || session.telephony?.streamId || null;
        activeCallSid = voiceId || activeCallSid;
        session.telephony = {
          ...(session.telephony || {}),
          provider: "enablex",
          voiceId: activeCallSid,
          streamId,
          callSid: activeCallSid,
          lastInboundSeq: event.media.seq ?? session.telephony?.lastInboundSeq,
        };
        if (!session.telephony.inboundFormatLogged) {
          console.log("[enablex-media] inbound format", {
            callSid: activeCallSid,
            format: event.media.format || null,
          });
          session.telephony.inboundFormatLogged = true;
        }
        if (!session.pendingGreetingAudio && !session.openingPlayedAt) {
          const greeting = await getOpeningMessage(session);
          session.pendingGreetingAudio = await synthesizeSpeech(session, greeting);
        }
        if (session.pendingGreetingAudio && !session.openingPlayedAt) {
          const pending = session.pendingGreetingAudio;
          if (sendEnablexMedia(ws, session, pending, "opening-greeting-on-first-media")) {
            await recordAgentAudio(session, pending, "opening-greeting");
            session.pendingGreetingAudio = null;
            session.openingPlayedAt = nowIso();
            console.log(`[enablex-media] opening played via first-media callSid=${session.callSid}`);
          }
        }
        audioBuffer = decodeEnablexInboundMedia(event);
        // Preserve raw μ-law bytes for Deepgram (avoids re-encoding PCM→mulaw per frame)
        if (session?.deepgramReady) {
          session._rawMulawFrame = Buffer.from(event.media.payload, "base64");
        }
      } catch (error) {
        console.log("[enablex-media] failed to parse text frame", error.message);
        return;
      }
    }
    if (!audioBuffer) return;
    const rawMulawFrame = session?._rawMulawFrame || null;
    if (session) session._rawMulawFrame = null;
    await handleCallerAudioFrame(ws, session, activeCallSid, audioBuffer, rawMulawFrame);
  });

  ws.on("close", async () => {
    console.log(`[enablex-media] websocket closed callSid=${activeCallSid}`);
    clearInterval(heartbeat);
    if (session) {
      clearTimeout(session.timer);
      try {
        await stopEnablexStream(activeCallSid);
      } catch {}
      await endCall(session, "completed");
    }
  });
});

async function gracefulShutdown() {
  acceptingTraffic = false;
  for (const session of sessions.values()) {
    clearTimeout(session.timer);
    await endCall(session, "drained");
  }
  await redis.quit();
  server.close(() => process.exit(0));
}

process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown);

server.listen(config.port, () => {
  console.log(`orchestrator listening on ${config.port}`);
});
