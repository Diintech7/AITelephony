const WebSocket = require("ws");
require("dotenv").config();

// Import franc with fallback for different versions
let franc;
try {
  franc = require("franc").franc;
  if (!franc) franc = require("franc");
} catch (_) {
  franc = () => "und";
}

// Load API keys from environment variables
const API_KEYS = {
  deepgram: process.env.DEEPGRAM_API_KEY,
  sarvam: process.env.SARVAM_API_KEY,
  openai: process.env.OPENAI_API_KEY,
};

// Validate API keys
if (!API_KEYS.deepgram || !API_KEYS.sarvam || !API_KEYS.openai) {
  console.error("❌ Missing required API keys in environment variables");
  process.exit(1);
}

const fetch = globalThis.fetch || require("node-fetch");
const textDecoder = new TextDecoder();

// Performance timing helper
const createTimer = (label) => {
  const start = Date.now();
  return {
    start,
    end: () => Date.now() - start,
    checkpoint: () => Date.now() - start,
    label,
  };
};

// Enhanced language mappings with Indian locales
const LANGUAGE_MAPPING = {
  hi: "en-IN",
  en: "en-IN",
  bn: "bn-IN",
  te: "te-IN",
  ta: "ta-IN",
  mr: "mr-IN",
  gu: "gu-IN",
  kn: "kn-IN",
  ml: "ml-IN",
  pa: "pa-IN",
  or: "or-IN",
  as: "as-IN",
  ur: "ur-IN",
};

// Franc 3-letter → supported 2-letter mapping
const FRANC_TO_SUPPORTED = {
  hin: "hi",
  eng: "en",
  ben: "bn",
  tel: "te",
  tam: "ta",
  mar: "mr",
  guj: "gu",
  kan: "kn",
  mal: "ml",
  pan: "pa",
  ori: "or",
  asm: "as",
  urd: "ur",
  src: "en",
  und: "en",
  lat: "en",
  sco: "en",
  fra: "en",
  deu: "en",
  nld: "en",
  spa: "en",
  ita: "en",
  por: "en",
};

const getSarvamLanguage = (detectedLang, def = "hi") => {
  const lang = (detectedLang || def).toLowerCase();
  return LANGUAGE_MAPPING[lang] || "en-IN";
};

const getDeepgramLanguage = (detectedLang, def = "hi") => {
  const lang = (detectedLang || def).toLowerCase();
  if (lang === "hi") return "hi";
  if (lang === "en") return "en-IN";
  if (lang === "mr") return "mr";
  return lang;
};

// Valid Sarvam voice options
const VALID_SARVAM_VOICES = [
  "meera",
  "anushka",
  "arvind",
  "amol",
  "maya",
  "abhilash",
  "manisha",
  "vidya",
  "arya",
  "karun",
  "hitesh",
];

const getValidSarvamVoice = (voiceSelection = "anushka") => {
  if (VALID_SARVAM_VOICES.includes(voiceSelection)) return voiceSelection;
  const voiceMapping = {
    "male-professional": "arvind",
    "female-professional": "anushka",
    "male-friendly": "amol",
    "female-friendly": "maya",
    neutral: "anushka",
    default: "anushka",
  };
  return voiceMapping[voiceSelection] || "anushka";
};

// Enhanced language detection with better fallback logic
const detectLanguageWithFranc = (text, fallbackLanguage = "en") => {
  try {
    const cleanText = String(text || "").trim();
    if (!cleanText) return fallbackLanguage;

    if (cleanText.length < 10) {
      const hindiPatterns = /[\u0900-\u097F]/;
      const englishPatterns = /^(what|how|why|when|where|who|can|do|does|did|is|are|am|was|were|have|has|had|will|would|could|should|may|might|hello|hi|hey|yes|no|ok|okay|thank|thanks|please|sorry|our|your|my|name|help)\b/i;
      const englishWords = /^[a-zA-Z\s\?\!\.,\'\"]+$/;
      if (hindiPatterns.test(cleanText)) return "hi";
      if (englishPatterns.test(cleanText) || englishWords.test(cleanText)) return "en";
      return fallbackLanguage;
    }

    if (typeof franc !== "function") return fallbackLanguage;
    const detected = franc(cleanText);
    if (!detected || detected === "und") {
      const hindiPatterns = /[\u0900-\u097F]/;
      if (hindiPatterns.test(cleanText)) return "hi";
      const latinScript = /^[a-zA-Z\s\?\!\.,\'\"0-9\-\$]+$/;
      if (latinScript.test(cleanText)) return "en";
      return fallbackLanguage;
    }

    const mapped = FRANC_TO_SUPPORTED[detected];
    if (mapped) return mapped;

    // script-based fallback
    const hindiPatterns = /[\u0900-\u097F]/;
    const tamilScript = /[\u0B80-\u0BFF]/;
    const teluguScript = /[\u0C00-\u0C7F]/;
    const kannadaScript = /[\u0C80-\u0CFF]/;
    const malayalamScript = /[\u0D00-\u0D7F]/;
    const gujaratiScript = /[\u0A80-\u0AFF]/;
    const bengaliScript = /[\u0980-\u09FF]/;

    if (hindiPatterns.test(cleanText)) return "hi";
    if (tamilScript.test(cleanText)) return "ta";
    if (teluguScript.test(cleanText)) return "te";
    if (kannadaScript.test(cleanText)) return "kn";
    if (malayalamScript.test(cleanText)) return "ml";
    if (gujaratiScript.test(cleanText)) return "gu";
    if (bengaliScript.test(cleanText)) return "bn";

    const latinScript = /^[a-zA-Z\s\?\!\.,\'\"0-9\-\$]+$/;
    if (latinScript.test(cleanText)) return "en";
    return fallbackLanguage;
  } catch (_) {
    return fallbackLanguage;
  }
};

// ──────────────────────────────────────────────────────────────────────────────
//  Sarvam TTS Processor (WS) — supports incremental text & flush
// ──────────────────────────────────────────────────────────────────────────────
class SimplifiedSarvamTTSProcessor {
  constructor(language, ws, streamSid) {
    this.language = language;
    this.ws = ws; // Main SIP WebSocket (send audio out)
    this.streamSid = streamSid;

    this.sarvamLanguage = getSarvamLanguage(language);
    this.voice = getValidSarvamVoice(ws.sessionAgentConfig?.voiceSelection || "anushka");

    this.sarvamWs = null;
    this.sarvamWsConnected = false;

    this.audioQueue = [];
    this.isStreamingToSIP = false;
    this.totalAudioBytes = 0;
  }

  async connectSarvamWs() {
    if (this.sarvamWsConnected && this.sarvamWs?.readyState === WebSocket.OPEN) return true;

    const timer = createTimer("SARVAM_WS_CONNECT");
    try {
      const sarvamUrl = new URL("wss://api.sarvam.ai/text-to-speech/ws");
      sarvamUrl.searchParams.append("model", "bulbul:v2");

      // API key as subprotocol
      this.sarvamWs = new WebSocket(sarvamUrl.toString(), [
        `api-subscription-key.${API_KEYS.sarvam}`,
      ]);

      return await new Promise((resolve, reject) => {
        this.sarvamWs.onopen = () => {
          this.sarvamWsConnected = true;
          console.log(`🕒 [SARVAM-WS-CONNECT] ${timer.end()}ms - Sarvam TTS WebSocket connected.`);

          const configMessage = {
            type: "config",
            data: {
              target_language_code: this.sarvamLanguage,
              speaker: this.voice,
              pitch: 0.5,
              pace: 1.0,
              loudness: 1.0,
              enable_preprocessing: false,
              output_audio_codec: "linear16", // for SIP/Twilio
              output_audio_bitrate: "128k",
              speech_sample_rate: 8000,
              min_buffer_size: 50,
              max_chunk_length: 150,
            },
          };
          this.sarvamWs.send(JSON.stringify(configMessage));
          console.log("Sarvam TTS config sent.");
          resolve(true);
        };

        this.sarvamWs.onmessage = (event) => {
          try {
            const response = JSON.parse(event.data);
            if (response.type === "audio" && response.data?.audio) {
              const audioBuffer = Buffer.from(response.data.audio, "base64");
              this.audioQueue.push(audioBuffer);
              this.totalAudioBytes += audioBuffer.length;
              if (!this.isStreamingToSIP) this.startStreamingToSIP();
            } else if (response.type === "error") {
              console.error(`❌ Sarvam TTS WS Error: ${response.data.message} (Code: ${response.data.code})`);
            }
          } catch (e) {
            console.error("Error parsing Sarvam WS message:", e);
          }
        };

        this.sarvamWs.onerror = (error) => {
          this.sarvamWsConnected = false;
          console.error(`❌ [SARVAM-WS-CONNECT] ${timer.end()}ms - Sarvam TTS WebSocket error:`, error.message);
          reject(error);
        };

        this.sarvamWs.onclose = () => {
          this.sarvamWsConnected = false;
          console.log(`🕒 [SARVAM-WS-CONNECT] ${timer.end()}ms - Sarvam TTS WebSocket closed.`);
        };
      });
    } catch (error) {
      console.error(`❌ [SARVAM-WS-CONNECT] ${timer.end()}ms - Failed to connect to Sarvam TTS WebSocket: ${error.message}`);
      return false;
    }
  }

  async sendTextChunk(text) {
    if (!text) return;
    if (!(await this.connectSarvamWs())) return;
    try {
      this.sarvamWs.send(JSON.stringify({ type: "text", data: { text } }));
    } catch (e) {
      console.error("Sarvam sendTextChunk error:", e.message);
    }
  }

  async flush() {
    if (!(await this.connectSarvamWs())) return;
    try {
      this.sarvamWs.send(JSON.stringify({ type: "flush" }));
    } catch (e) {
      console.error("Sarvam flush error:", e.message);
    }
  }

  // Backward-compatible: synthesize a single full string (non-streaming)
  async synthesizeAndStream(text) {
    this.audioQueue = [];
    this.isStreamingToSIP = false;
    const timer = createTimer("TTS_SYNTHESIS_WS");
    try {
      const connected = await this.connectSarvamWs();
      if (!connected) return;
      await this.sendTextChunk(text);
      await this.flush();
      console.log(`🕒 [TTS-SYNTHESIS-WS] ${timer.end()}ms - Text and flush signal sent to Sarvam WS.`);
    } catch (error) {
      console.error(`❌ [TTS-SYNTHESIS-WS] ${timer.end()}ms - Error sending text to Sarvam WS: ${error.message}`);
    }
  }

  async startStreamingToSIP() {
    if (this.isStreamingToSIP) return;
    this.isStreamingToSIP = true;
    console.log("Starting streaming audio from Sarvam to SIP...");

    const SAMPLE_RATE = 8000;
    const BYTES_PER_SAMPLE = 2; // linear16 is 16-bit
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000;
    const OPTIMAL_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS); // ~40ms chunks

    while (this.audioQueue.length > 0) {
      const audioBuffer = this.audioQueue.shift();
      let position = 0;
      while (position < audioBuffer.length) {
        const remaining = audioBuffer.length - position;
        const chunkSize = Math.min(OPTIMAL_CHUNK_SIZE, remaining);
        const chunk = audioBuffer.slice(position, position + chunkSize);

        const mediaMessage = {
          event: "media",
          streamSid: this.streamSid,
          media: { payload: chunk.toString("base64") },
        };

        if (this.ws.readyState === WebSocket.OPEN) {
          try {
            this.ws.send(JSON.stringify(mediaMessage));
          } catch (error) {
            console.error("Error sending media to SIP WS:", error.message);
            this.isStreamingToSIP = false;
            return;
          }
        } else {
          console.log("SIP WS not open, stopping streaming.");
          this.isStreamingToSIP = false;
          return;
        }

        if (position + chunkSize < audioBuffer.length) {
          const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS);
          const delayMs = Math.max(chunkDurationMs - 2, 10);
          await new Promise((r) => setTimeout(r, delayMs));
        }
        position += chunkSize;
      }
    }
    this.isStreamingToSIP = false;
    console.log("Stopped streaming audio from Sarvam to SIP.");
  }

  getStats() {
    return { totalAudioBytes: this.totalAudioBytes };
  }
}

// ──────────────────────────────────────────────────────────────────────────────
//  OpenAI → Sarvam streaming bridge
// ──────────────────────────────────────────────────────────────────────────────
async function streamOpenAIToSarvam(userMessage, conversationHistory, agentConfig, ttsProcessor) {
  const timer = createTimer("LLM_STREAM");

  const systemPrompt = (() => {
    let sp = agentConfig?.systemPrompt || "You are a helpful AI assistant.";
    if (Buffer.byteLength(sp, "utf8") > 150) {
      while (Buffer.byteLength(sp, "utf8") > 150) sp = sp.slice(0, -1);
    }
    return sp;
  })();

  const messages = [
    { role: "system", content: systemPrompt },
    ...conversationHistory.slice(-6),
    { role: "user", content: userMessage },
  ];

  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${API_KEYS.openai}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages,
      stream: true,
      temperature: 0.3,
      max_tokens: 200,
    }),
  });

  if (!res.ok || !res.body) {
    console.error("❌ Failed to start OpenAI stream", await (async () => {
      try { return await res.text(); } catch { return "(no body)"; }
    })());
    return "";
  }

  let fullText = "";
  const reader = res.body.getReader();

  // Ensure Sarvam WS is connected before streaming deltas
  await ttsProcessor.connectSarvamWs();

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;

    const chunkStr = textDecoder.decode(value);
    const lines = chunkStr
      .split("\n")
      .map((l) => l.trim())
      .filter((l) => l.startsWith("data:"));

    for (const line of lines) {
      const payload = line.replace(/^data:\s*/, "");
      if (!payload) continue;
      if (payload === "[DONE]") {
        // flush to Sarvam
        await ttsProcessor.flush();
        console.log(`🕒 [LLM_STREAM] ${timer.end()}ms - Completed & flushed`);
        return fullText.trim();
      }
      try {
        const json = JSON.parse(payload);
        const delta = json.choices?.[0]?.delta?.content;
        if (delta) {
          fullText += delta;
          await ttsProcessor.sendTextChunk(delta);
        }
      } catch (e) {
        // ignore parse noise lines
      }
    }
  }

  await ttsProcessor.flush();
  console.log(`🕒 [LLM_STREAM] ${timer.end()}ms - Ended (implicit) & flushed`);
  return fullText.trim();
}

// ──────────────────────────────────────────────────────────────────────────────
//  Main WebSocket server setup
// ──────────────────────────────────────────────────────────────────────────────
const setupUnifiedVoiceServer = (wss) => {
  wss.on("connection", (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const urlParams = Object.fromEntries(url.searchParams.entries());

    // Session state
    let streamSid = null;
    let conversationHistory = [];
    let isProcessing = false;
    let userUtteranceBuffer = "";
    let lastProcessedText = "";
    let currentTTS = null; // persistent TTS processor instance
    let currentLanguage = "en"; // default
    let agentConfig = {
      systemPrompt: "You are a helpful AI assistant.",
      firstMessage: "Hello! How can I help you today?",
      voiceSelection: "anushka",
    };

    // Deepgram WebSocket connection
    let deepgramWs = null;
    let deepgramReady = false;
    let deepgramAudioQueue = [];
    let sttTimer = null;

    const connectToDeepgram = async () => {
      try {
        const deepgramLanguage = getDeepgramLanguage(currentLanguage);
        const deepgramUrl = new URL("wss://api.deepgram.com/v1/listen");
        deepgramUrl.searchParams.append("sample_rate", "8000");
        deepgramUrl.searchParams.append("channels", "1");
        deepgramUrl.searchParams.append("encoding", "linear16");
        deepgramUrl.searchParams.append("model", "nova-2");
        deepgramUrl.searchParams.append("language", deepgramLanguage);
        deepgramUrl.searchParams.append("interim_results", "true");
        deepgramUrl.searchParams.append("smart_format", "true");
        deepgramUrl.searchParams.append("endpointing", "300");

        deepgramWs = new WebSocket(deepgramUrl.toString(), {
          headers: { Authorization: `Token ${API_KEYS.deepgram}` },
        });

        deepgramWs.onopen = () => {
          deepgramReady = true;
          deepgramAudioQueue.forEach((buffer) => deepgramWs.send(buffer));
          deepgramAudioQueue = [];
        };

        deepgramWs.onmessage = async (event) => {
          const data = JSON.parse(event.data);
          await handleDeepgramResponse(data);
        };

        deepgramWs.onerror = () => {
          deepgramReady = false;
        };

        deepgramWs.onclose = () => {
          deepgramReady = false;
        };
      } catch (_) {
        // silent
      }
    };

    const handleDeepgramResponse = async (data) => {
      if (data.type === "Results") {
        if (!sttTimer) sttTimer = createTimer("STT_TRANSCRIPTION");

        const transcript = data.channel?.alternatives?.[0]?.transcript;
        const is_final = data.is_final;

        if (transcript?.trim()) {
          if (is_final) {
            console.log(`🕒 [STT-TRANSCRIPTION] ${sttTimer.end()}ms - Text: "${transcript.trim()}"`);
            sttTimer = null;
            userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim();
            await processUserUtterance(userUtteranceBuffer);
            userUtteranceBuffer = "";
          }
        }
      } else if (data.type === "UtteranceEnd") {
        if (sttTimer) {
          console.log(`🕒 [STT-TRANSCRIPTION] ${sttTimer.end()}ms - Text: "${userUtteranceBuffer.trim()}"`);
          sttTimer = null;
        }
        if (userUtteranceBuffer.trim()) {
          await processUserUtterance(userUtteranceBuffer);
          userUtteranceBuffer = "";
        }
      }
    };

    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText) return;
      if (isProcessing) return; // avoid overlap; simple guard

      isProcessing = true;
      lastProcessedText = text;

      try {
        const detectedLanguage = detectLanguageWithFranc(text, currentLanguage || "en");
        if (detectedLanguage !== currentLanguage) {
          currentLanguage = detectedLanguage;
          // If language changes, reset TTS processor with new language
          currentTTS = new SimplifiedSarvamTTSProcessor(currentLanguage, ws, streamSid);
        }

        if (!currentTTS) {
          currentTTS = new SimplifiedSarvamTTSProcessor(currentLanguage, ws, streamSid);
        }

        // STREAMING: OpenAI → Sarvam
        const response = await streamOpenAIToSarvam(
          text,
          conversationHistory,
          agentConfig,
          currentTTS
        );

        if (response) {
          conversationHistory.push(
            { role: "user", content: text },
            { role: "assistant", content: response }
          );
          if (conversationHistory.length > 10) {
            conversationHistory = conversationHistory.slice(-10);
          }
        }
      } catch (error) {
        console.error("Error in processUserUtterance:", error);
      } finally {
        isProcessing = false;
      }
    };

    ws.on("message", async (message) => {
      try {
        const messageStr = message.toString();
        if (messageStr === "EOS" || messageStr === "BOS" || !messageStr.startsWith("{")) return;

        const data = JSON.parse(messageStr);
        switch (data.event) {
          case "connected":
            break;

          case "start": {
            streamSid = data.streamSid || data.start?.streamSid;

            // Initialize TTS
            currentTTS = new SimplifiedSarvamTTSProcessor(currentLanguage, ws, streamSid);

            await connectToDeepgram();

            const greeting = agentConfig.firstMessage || "Hello! How can I help you today?";
            // Stream greeting via incremental TTS (no LLM)
            await currentTTS.sendTextChunk(greeting);
            await currentTTS.flush();
            break;
          }

          case "media": {
            if (data.media?.payload) {
              const audioBuffer = Buffer.from(data.media.payload, "base64");
              if (deepgramWs && deepgramReady && deepgramWs.readyState === WebSocket.OPEN) {
                deepgramWs.send(audioBuffer);
              } else {
                deepgramAudioQueue.push(audioBuffer);
              }
            }
            break;
          }

          case "stop": {
            if (deepgramWs?.readyState === WebSocket.OPEN) deepgramWs.close();
            break;
          }

          default:
            break;
        }
      } catch (_) {
        // silent
      }
    });

    ws.on("close", async () => {
      if (deepgramWs?.readyState === WebSocket.OPEN) deepgramWs.close();

      // Reset state
      streamSid = null;
      conversationHistory = [];
      isProcessing = false;
      userUtteranceBuffer = "";
      lastProcessedText = "";
      deepgramReady = false;
      deepgramAudioQueue = [];
      currentTTS = null;
      currentLanguage = "en";
      agentConfig = {
        systemPrompt: "You are a helpful AI assistant.",
        firstMessage: "Hello! How can I help you today?",
        voiceSelection: "anushka",
      };
      sttTimer = null;
    });

    ws.on("error", () => {
      // silent
    });
  });
};

module.exports = { setupUnifiedVoiceServer };
