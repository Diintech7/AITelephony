const WebSocket = require("ws")
require("dotenv").config()
const mongoose = require("mongoose")
const Agent = require("../models/Agent")
const CallLog = require("../models/CallLog")

// Import franc with fallback for different versions
let franc;
try {
  franc = require("franc").franc;
  if (!franc) {
    franc = require("franc");
  }
} catch (error) {
  franc = () => 'und';
}

// Load API keys from environment variables
const API_KEYS = {
  deepgram: process.env.DEEPGRAM_API_KEY,
  sarvam: process.env.SARVAM_API_KEY,
  openai: process.env.OPENAI_API_KEY,
}

// Validate API keys
if (!API_KEYS.deepgram || !API_KEYS.sarvam || !API_KEYS.openai) {
  console.error("❌ Missing required API keys in environment variables")
  process.exit(1)
}

const fetch = globalThis.fetch || require("node-fetch")

// Performance timing helper
const createTimer = (label) => {
  const start = Date.now()
  return {
    start,
    end: () => Date.now() - start,
    checkpoint: (checkpointName) => Date.now() - start,
  }
}

// Enhanced language mappings with Marathi support
const LANGUAGE_MAPPING = {
  hi: "hi-IN",
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
}

// Enhanced Franc language code mapping to our supported languages
const FRANC_TO_SUPPORTED = {
  'hin': 'hi',
  'eng': 'en',
  'ben': 'bn',
  'tel': 'te',
  'tam': 'ta',
  'mar': 'mr',
  'guj': 'gu',
  'kan': 'kn',
  'mal': 'ml',
  'pan': 'pa',
  'ori': 'or',
  'asm': 'as',
  'urd': 'ur',
  'src': 'en',
  'und': 'en',
  'lat': 'en',
  'sco': 'en',
  'fra': 'en',
  'deu': 'en',
  'nld': 'en',
  'spa': 'en',
  'ita': 'en',
  'por': 'en',
}

const getSarvamLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang
  return LANGUAGE_MAPPING[lang] || "hi-IN"
}

const getDeepgramLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang
  if (lang === "hi") return "hi"
  if (lang === "en") return "en-IN"
  if (lang === "mr") return "mr"
  return lang
}

// Valid Sarvam voice options
const VALID_SARVAM_VOICES = ["meera", "anushka", "arvind", "amol", "maya", "anushka", "abhilash", "manisha", "vidya", "arya", "karun", "hitesh"]

const getValidSarvamVoice = (voiceSelection = "anushka") => {
  if (VALID_SARVAM_VOICES.includes(voiceSelection)) {
    return voiceSelection
  }

  const voiceMapping = {
    "male-professional": "arvind",
    "female-professional": "anushka",
    "male-friendly": "amol",
    "female-friendly": "maya",
    neutral: "anushka",
    default: "anushka",
  }

  return voiceMapping[voiceSelection] || "anushka"
}

// Utility function to decode base64 extra data
const decodeExtraData = (extraBase64) => {
  try {
    if (!extraBase64) return null

    const decodedString = Buffer.from(extraBase64, "base64").toString("utf-8")
    const fixedString = decodedString
      .replace(/="([^"]*?)"/g, ':"$1"')
      .replace(/=([^",}\s]+)/g, ':"$1"')
      .replace(/,\s*}/g, "}")
      .replace(/,\s*]/g, "]")

    const parsedData = JSON.parse(fixedString)
    return parsedData
  } catch (error) {
    return null
  }
}

// Enhanced language detection with better fallback logic
const detectLanguageWithFranc = (text, fallbackLanguage = "en") => {
  try {
    const cleanText = text.trim()
    
    if (cleanText.length < 10) {
      const englishPatterns = /^(what|how|why|when|where|who|can|do|does|did|is|are|am|was|were|have|has|had|will|would|could|should|may|might|hello|hi|hey|yes|no|ok|okay|thank|thanks|please|sorry|our|your|my|name|help)\b/i
      const hindiPatterns = /[\u0900-\u097F]/
      const englishWords = /^[a-zA-Z\s\?\!\.\,\'\"]+$/
      
      if (hindiPatterns.test(cleanText)) {
        return "hi"
      } else if (englishPatterns.test(cleanText) || englishWords.test(cleanText)) {
        return "en"
      } else {
        return fallbackLanguage
      }
    }

    if (typeof franc !== 'function') {
      return fallbackLanguage
    }

    const detected = franc(cleanText)

    if (detected === 'und' || !detected) {
      const hindiPatterns = /[\u0900-\u097F]/
      if (hindiPatterns.test(cleanText)) {
        return "hi"
      }
      
      const latinScript = /^[a-zA-Z\s\?\!\.\,\'\"0-9\-$$$$]+$/
      if (latinScript.test(cleanText)) {
        return "en"
      }
      
      return fallbackLanguage
    }

    const mappedLang = FRANC_TO_SUPPORTED[detected]
    
    if (mappedLang) {
      return mappedLang
    } else {
      const hindiPatterns = /[\u0900-\u097F]/
      if (hindiPatterns.test(cleanText)) {
        return "hi"
      }
      
      const tamilScript = /[\u0B80-\u0BFF]/
      const teluguScript = /[\u0C00-\u0C7F]/
      const kannadaScript = /[\u0C80-\u0CFF]/
      const malayalamScript = /[\u0D00-\u0D7F]/
      const gujaratiScript = /[\u0A80-\u0AFF]/
      const bengaliScript = /[\u0980-\u09FF]/
      
      if (tamilScript.test(cleanText)) return "ta"
      if (teluguScript.test(cleanText)) return "te"
      if (kannadaScript.test(cleanText)) return "kn"
      if (malayalamScript.test(cleanText)) return "ml"
      if (gujaratiScript.test(cleanText)) return "gu"
      if (bengaliScript.test(cleanText)) return "bn"
      
      const latinScript = /^[a-zA-Z\s\?\!\.\,\'\"0-9\-$$$$]+$/
      if (latinScript.test(cleanText)) {
        return "en"
      }
      
      return fallbackLanguage
    }
    
  } catch (error) {
    return fallbackLanguage
  }
}

// Fallback to OpenAI for uncertain cases
const detectLanguageWithOpenAI = async (text) => {
  const timer = createTimer("LLM_LANGUAGE_DETECTION")
  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEYS.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content: `You are a language detection expert. Analyze the given text and return ONLY the 2-letter language code (hi, en, bn, te, ta, mr, gu, kn, ml, pa, or, as, ur). 

Examples:
- "Hello, how are you?" → en
- "What's our name?" → en
- "नमस्ते, आप कैसे हैं?" → hi
- "আপনি কেমন আছেন?" → bn
- "நீங்கள் எப்படி இருக்கிறீர்கள்?" → ta
- "तुम्ही कसे आहात?" → mr
- "તમે કેમ છો?" → gu

Return only the language code, nothing else.`,
          },
          {
            role: "user",
            content: text,
          },
        ],
        max_tokens: 10,
        temperature: 0.1,
      }),
    })

    if (!response.ok) {
      throw new Error(`Language detection failed: ${response.status}`)
    }

    const data = await response.json()
    const detectedLang = data.choices[0]?.message?.content?.trim().toLowerCase()

    const validLanguages = Object.keys(LANGUAGE_MAPPING)
    if (validLanguages.includes(detectedLang)) {
      console.log(`🕒 [LLM-LANG-DETECT] ${timer.end()}ms - Detected: ${detectedLang}`)
      return detectedLang
    }

    return "en"
  } catch (error) {
    console.log(`❌ [LLM-LANG-DETECT] ${timer.end()}ms - Error: ${error.message}`)
    return "en"
  }
}

// Enhanced hybrid language detection
const detectLanguageHybrid = async (text, useOpenAIFallback = false) => {
  const francResult = detectLanguageWithFranc(text)
  
  if (text.trim().length < 20) {
    const englishPatterns = /^(what|how|why|when|where|who|can|do|does|did|is|are|am|was|were|have|has|had|will|would|could|should|may|might|hello|hi|hey|yes|no|ok|okay|thank|thanks|please|sorry|our|your|my|name|help)\b/i
    const hindiPatterns = /[\u0900-\u097F]/
    
    if (hindiPatterns.test(text)) {
      return "hi"
    } else if (englishPatterns.test(text)) {
      return "en"
    }
  }
  
  if (francResult === 'hi' || francResult === 'en') {
    return francResult
  }
  
  if (useOpenAIFallback && !['hi', 'en'].includes(francResult)) {
    return await detectLanguageWithOpenAI(text)
  }
  
  return francResult
}

// Call logging utility class
class CallLogger {
  constructor(clientId, mobile = null, callDirection = "inbound") {
    this.clientId = clientId
    this.mobile = mobile
    this.callDirection = callDirection
    this.callStartTime = new Date()
    this.transcripts = []
    this.responses = []
    this.totalDuration = 0
  }

  logUserTranscript(transcript, language, timestamp = new Date()) {
    const entry = {
      type: "user",
      text: transcript,
      language: language,
      timestamp: timestamp,
      source: "deepgram",
    }

    this.transcripts.push(entry)
  }

  logAIResponse(response, language, timestamp = new Date()) {
    const entry = {
      type: "ai",
      text: response,
      language: language,
      timestamp: timestamp,
      source: "sarvam",
    }

    this.responses.push(entry)
  }

  generateFullTranscript() {
    const allEntries = [...this.transcripts, ...this.responses].sort(
      (a, b) => new Date(a.timestamp) - new Date(b.timestamp),
    )

    return allEntries
      .map((entry) => {
        const speaker = entry.type === "user" ? "User" : "AI"
        const time = entry.timestamp.toISOString()
        return `[${time}] ${speaker} (${entry.language}): ${entry.text}`
      })
      .join("\n")
  }

  async saveToDatabase(leadStatus = "medium") {
    const timer = createTimer("MONGODB_SAVE")
    try {
      const callEndTime = new Date()
      this.totalDuration = Math.round((callEndTime - this.callStartTime) / 1000)

      const callLogData = {
        clientId: this.clientId,
        mobile: this.mobile,
        time: this.callStartTime,
        transcript: this.generateFullTranscript(),
        duration: this.totalDuration,
        leadStatus: leadStatus,
        metadata: {
          userTranscriptCount: this.transcripts.length,
          aiResponseCount: this.responses.length,
          languages: [...new Set([...this.transcripts, ...this.responses].map((entry) => entry.language))],
          callEndTime: callEndTime,
          callDirection: this.callDirection,
        },
      }

      const callLog = new CallLog(callLogData)
      const savedLog = await callLog.save()

      console.log(`🕒 [MONGODB-SAVE] ${timer.end()}ms - CallLog saved: ${savedLog._id}`)
      return savedLog
    } catch (error) {
      console.log(`❌ [MONGODB-SAVE] ${timer.end()}ms - Error: ${error.message}`)
      throw error
    }
  }

  getStats() {
    return {
      duration: this.totalDuration,
      userMessages: this.transcripts.length,
      aiResponses: this.responses.length,
      languages: [...new Set([...this.transcripts, ...this.responses].map((entry) => entry.language))],
      startTime: this.callStartTime,
      callDirection: this.callDirection,
    }
  }
}

// Simplified OpenAI processing
const processWithOpenAI = async (
  userMessage,
  conversationHistory,
  detectedLanguage,
  callLogger,
  agentConfig,
) => {
  const timer = createTimer("LLM_PROCESSING")

  try {
    let systemPrompt = agentConfig.systemPrompt || "You are a helpful AI assistant."

    if (Buffer.byteLength(systemPrompt, "utf8") > 150) {
      let truncated = systemPrompt
      while (Buffer.byteLength(truncated, "utf8") > 150) {
        truncated = truncated.slice(0, -1)
      }
      systemPrompt = truncated
    }

    const messages = [
      { role: "system", content: systemPrompt },
      ...conversationHistory.slice(-6),
      { role: "user", content: userMessage },
    ]

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEYS.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages,
        max_tokens: 50,
        temperature: 0.3,
      }),
    })

    if (!response.ok) {
      console.log(`❌ [LLM-PROCESSING] ${timer.end()}ms - Error: ${response.status}`)
      return null
    }

    const data = await response.json()
    const fullResponse = data.choices[0]?.message?.content?.trim()

    console.log(`🕒 [LLM-PROCESSING] ${timer.end()}ms - Response generated`)

    // CallLogger will now be handled by the TTS processor after text is sent to Sarvam WS
    // if (callLogger && fullResponse) {
    //   callLogger.logAIResponse(fullResponse, detectedLanguage)
    // }

    return fullResponse
  } catch (error) {
    console.log(`❌ [LLM-PROCESSING] ${timer.end()}ms - Error: ${error.message}`)
    return null
  }
}

// Simplified TTS processor using Sarvam WebSocket
class SimplifiedSarvamTTSProcessor {
  constructor(language, ws, streamSid, callLogger = null) {
    this.language = language
    this.ws = ws // Main SIP WebSocket (for sending audio out)
    this.streamSid = streamSid
    this.callLogger = callLogger
    this.sarvamLanguage = getSarvamLanguage(language)
    this.voice = getValidSarvamVoice(ws.sessionAgentConfig?.voiceSelection || "anushka")
    this.isInterrupted = false
    this.sarvamWs = null // Sarvam TTS WebSocket
    this.sarvamWsConnected = false
    this.audioQueue = [] // Queue for audio buffers received from Sarvam
    this.isStreamingToSIP = false // Flag to prevent multiple streaming loops
    this.totalAudioBytes = 0
    this.currentSarvamRequestId = 0 // To manage multiple TTS requests and interruptions
  }

  interrupt() {
    this.isInterrupted = true
    if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.OPEN) {
      this.sarvamWs.close() // Close Sarvam TTS WS to stop generation
    }
    this.sarvamWsConnected = false
    this.audioQueue = [] // Clear any pending audio
    this.isStreamingToSIP = false
    console.log("TTS interrupted and Sarvam WS closed.")
  }

  reset(newLanguage) {
    this.interrupt() // Interrupt current process
    if (newLanguage) {
      this.language = newLanguage
      this.sarvamLanguage = getSarvamLanguage(newLanguage)
    }
    this.isInterrupted = false
    this.totalAudioBytes = 0
    this.currentSarvamRequestId = 0
    // Connection will be re-established on next synthesizeAndStream call if needed
  }

  async connectSarvamWs(requestId) {
    if (this.sarvamWsConnected && this.sarvamWs?.readyState === WebSocket.OPEN) {
      return true // Already connected
    }

    if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.CONNECTING) {
      // Wait for existing connection attempt to complete
      return new Promise((resolve, reject) => {
        const checkInterval = setInterval(() => {
          if (this.sarvamWsConnected) {
            clearInterval(checkInterval);
            resolve(true);
          } else if (this.sarvamWs?.readyState === WebSocket.CLOSED) {
            clearInterval(checkInterval);
            reject(new Error("Sarvam WS connection failed during wait."));
          }
        }, 100);
      });
    }

    const timer = createTimer("SARVAM_WS_CONNECT");
    try {
      const sarvamUrl = new URL("wss://api.sarvam.ai/text-to-speech/ws");
      sarvamUrl.searchParams.append("model", "bulbul:v2"); 

      // Corrected: Pass API key as a subprotocol
      this.sarvamWs = new WebSocket(sarvamUrl.toString(), [`api-subscription-key.${API_KEYS.sarvam}`]);

      return new Promise((resolve, reject) => {
        this.sarvamWs.onopen = () => {
          if (this.isInterrupted || this.currentSarvamRequestId !== requestId) {
            this.sarvamWs.close();
            return reject(new Error("Connection opened but interrupted or outdated request."));
          }
          this.sarvamWsConnected = true;
          console.log(`🕒 [SARVAM-WS-CONNECT] ${timer.end()}ms - Sarvam TTS WebSocket connected.`);

          // Send initial config message
          const configMessage = {
            type: "config",
            data: {
              target_language_code: "hi-IN",
              speaker: "anushka",
              pitch: 0.5,
              pace: 1.0,
              loudness: 1.0, 
              enable_preprocessing: false,
              output_audio_codec: "linear16", // Crucial for SIP/Twilio
              output_audio_bitrate: "128k", // For 8000 Hz linear16
              speech_sample_rate: 8000, // Crucial for SIP/Twilio
              min_buffer_size: 50, // As per HTML example
              max_chunk_length: 150, // As per HTML example
            },
          };
          this.sarvamWs.send(JSON.stringify(configMessage));
          console.log("Sarvam TTS config sent.");
          resolve(true);
        };

        this.sarvamWs.onmessage = async (event) => {
          if (this.isInterrupted || this.currentSarvamRequestId !== requestId) {
            return; // Ignore messages if interrupted or outdated
          }
          try {
            const response = JSON.parse(event.data);
            if (response.type === "audio" && response.data?.audio) {
              const audioBuffer = Buffer.from(response.data.audio, "base64");
              this.audioQueue.push(audioBuffer);
              this.totalAudioBytes += audioBuffer.length;
              if (!this.isStreamingToSIP) {
                this.startStreamingToSIP(requestId);
              }
            } else if (response.type === "error") {
              console.error(`❌ Sarvam TTS WS Error: ${response.data.message} (Code: ${response.data.code})`);
              // Handle specific errors if needed
            }
          } catch (parseError) {
            console.error("Error parsing Sarvam WS message:", parseError);
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
          // If closed unexpectedly, it will attempt to reconnect on next synthesizeAndStream
        };
      });
    } catch (error) {
      console.error(`❌ [SARVAM-WS-CONNECT] ${timer.end()}ms - Failed to connect to Sarvam TTS WebSocket: ${error.message}`);
      return false;
    }
  }

  async synthesizeAndStream(text) {
    if (this.isInterrupted) return;

    const requestId = ++this.currentSarvamRequestId; // Increment request ID for new synthesis
    this.audioQueue = []; // Clear queue for new synthesis
    this.isStreamingToSIP = false; // Reset streaming flag

    const timer = createTimer("TTS_SYNTHESIS_WS");

    try {
      const connected = await this.connectSarvamWs(requestId);
      if (!connected || this.isInterrupted || this.currentSarvamRequestId !== requestId) {
        console.log("Sarvam WS not connected or interrupted/outdated request, aborting synthesis.");
        return;
      }

      const textMessage = {
        type: "text",
        data: { text: text },
      };
      this.sarvamWs.send(JSON.stringify(textMessage));
      
      // Send a flush message to signal end of utterance to Sarvam TTS
      const flushMessage = { type: "flush" };
      this.sarvamWs.send(JSON.stringify(flushMessage));
      
      console.log(`🕒 [TTS-SYNTHESIS-WS] ${timer.end()}ms - Text and flush signal sent to Sarvam WS.`);

      if (this.callLogger && text) {
        // Log AI response after sending to TTS, assuming it will be spoken
        this.callLogger.logAIResponse(text, this.language);
      }

      // The actual streaming to SIP will be handled by startStreamingToSIP
      // which is triggered by onmessage from sarvamWs
    } catch (error) {
      if (!this.isInterrupted) {
        console.error(`❌ [TTS-SYNTHESIS-WS] ${timer.end()}ms - Error sending text to Sarvam WS: ${error.message}`);
        throw error;
      }
    }
  }

  async startStreamingToSIP(requestId) {
    if (this.isStreamingToSIP || this.isInterrupted || this.currentSarvamRequestId !== requestId) {
      return; // Already streaming or interrupted/outdated request
    }
    this.isStreamingToSIP = true;
    console.log("Starting streaming audio from Sarvam to SIP...");

    const SAMPLE_RATE = 8000;
    const BYTES_PER_SAMPLE = 2; // linear16 is 16-bit, so 2 bytes
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000;
    const OPTIMAL_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS); // 40ms chunks for SIP

    while (!this.isInterrupted && this.currentSarvamRequestId === requestId) {
      if (this.audioQueue.length > 0) {
        const audioBuffer = this.audioQueue.shift(); // Get the next audio chunk

        let position = 0;
        while (position < audioBuffer.length && !this.isInterrupted && this.currentSarvamRequestId === requestId) {
          const remaining = audioBuffer.length - position;
          const chunkSize = Math.min(OPTIMAL_CHUNK_SIZE, remaining);
          const chunk = audioBuffer.slice(position, position + chunkSize);

          const mediaMessage = {
            event: "media",
            streamSid: this.streamSid,
            media: {
              payload: chunk.toString("base64"),
            },
          };

          if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted && this.currentSarvamRequestId === requestId) {
            try {
              this.ws.send(JSON.stringify(mediaMessage));
            } catch (error) {
              console.error("Error sending media to SIP WS:", error.message);
              this.isInterrupted = true; // Stop streaming on error
              break;
            }
          } else {
            console.log("SIP WS not open or interrupted, stopping streaming.");
            this.isInterrupted = true;
            break;
          }

          if (position + chunkSize < audioBuffer.length && !this.isInterrupted && this.currentSarvamRequestId === requestId) {
            const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS);
            const delayMs = Math.max(chunkDurationMs - 2, 10); // Small delay to prevent buffer underrun
            await new Promise((resolve) => setTimeout(resolve, delayMs));
          }

          position += chunkSize;
        }
      } else {
        // No audio in queue, wait for a short period or until new audio arrives
        await new Promise((resolve) => setTimeout(resolve, 50)); // Wait 50ms
      }
    }
    this.isStreamingToSIP = false;
    console.log("Stopped streaming audio from Sarvam to SIP.");
  }

  getStats() {
    return {
      totalAudioBytes: this.totalAudioBytes,
    };
  }
}

// Enhanced agent lookup function
const findAgentForCall = async (callData) => {
  const timer = createTimer("MONGODB_AGENT_LOOKUP")
  try {
    const { accountSid, callDirection, extraData } = callData

    let agent = null

    if (callDirection === "inbound") {
      if (!accountSid) {
        throw new Error("Missing accountSid for inbound call")
      }

      agent = await Agent.findOne({ accountSid }).lean()
      if (!agent) {
        throw new Error(`No agent found for accountSid: ${accountSid}`)
      }
    } else if (callDirection === "outbound") {
      if (!extraData) {
        throw new Error("Missing extraData for outbound call")
      }

      if (!extraData.CallVaId) {
        throw new Error("Missing CallVaId in extraData for outbound call")
      }

      const callVaId = extraData.CallVaId
      agent = await Agent.findOne({ callerId: callVaId }).lean()
      if (!agent) {
        throw new Error(`No agent found for callerId: ${callVaId}`)
      }
    } else {
      throw new Error(`Unknown call direction: ${callDirection}`)
    }

    console.log(`🕒 [MONGODB-AGENT-LOOKUP] ${timer.end()}ms - Agent found: ${agent.agentName}`)
    return agent
  } catch (error) {
    console.log(`❌ [MONGODB-AGENT-LOOKUP] ${timer.end()}ms - Error: ${error.message}`)
    throw error
  }
}

// Main WebSocket server setup
const setupUnifiedVoiceServer = (wss) => {
  wss.on("connection", (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`)
    const urlParams = Object.fromEntries(url.searchParams.entries())

    // Session state
    let streamSid = null
    let conversationHistory = []
    let isProcessing = false
    let userUtteranceBuffer = ""
    let lastProcessedText = ""
    let currentTTS = null // This will be the persistent TTS processor instance
    let currentLanguage = undefined
    let processingRequestId = 0
    let callLogger = null
    let callDirection = "inbound"
    let agentConfig = null

    // Deepgram WebSocket connection
    let deepgramWs = null
    let deepgramReady = false
    let deepgramAudioQueue = []
    let sttTimer = null

    const connectToDeepgram = async () => {
      try {
        const deepgramLanguage = getDeepgramLanguage(currentLanguage)

        const deepgramUrl = new URL("wss://api.deepgram.com/v1/listen")
        deepgramUrl.searchParams.append("sample_rate", "8000")
        deepgramUrl.searchParams.append("channels", "1")
        deepgramUrl.searchParams.append("encoding", "linear16")
        deepgramUrl.searchParams.append("model", "nova-2")
        deepgramUrl.searchParams.append("language", deepgramLanguage)
        deepgramUrl.searchParams.append("interim_results", "true")
        deepgramUrl.searchParams.append("smart_format", "true")
        deepgramUrl.searchParams.append("endpointing", "300")

        deepgramWs = new WebSocket(deepgramUrl.toString(), {
          headers: { Authorization: `Token ${API_KEYS.deepgram}` },
        })

        deepgramWs.onopen = () => {
          deepgramReady = true
          deepgramAudioQueue.forEach((buffer) => deepgramWs.send(buffer))
          deepgramAudioQueue = []
        }

        deepgramWs.onmessage = async (event) => {
          const data = JSON.parse(event.data)
          await handleDeepgramResponse(data)
        }

        deepgramWs.onerror = (error) => {
          deepgramReady = false
        }

        deepgramWs.onclose = () => {
          deepgramReady = false
        }
      } catch (error) {
        // Silent error handling
      }
    }

    const handleDeepgramResponse = async (data) => {
      if (data.type === "Results") {
        if (!sttTimer) {
          sttTimer = createTimer("STT_TRANSCRIPTION")
        }

        const transcript = data.channel?.alternatives?.[0]?.transcript
        const is_final = data.is_final

        if (transcript?.trim()) {
          if (currentTTS && isProcessing) {
            currentTTS.interrupt() // Interrupt ongoing TTS if user speaks
            isProcessing = false
            processingRequestId++ // Invalidate current processing request
          }

          if (is_final) {
            console.log(`🕒 [STT-TRANSCRIPTION] ${sttTimer.end()}ms - Text: "${transcript.trim()}"`)
            sttTimer = null

            userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim()

            if (callLogger && transcript.trim()) {
              const detectedLang = detectLanguageWithFranc(transcript.trim(), currentLanguage || "en")
              callLogger.logUserTranscript(transcript.trim(), detectedLang)
            }

            await processUserUtterance(userUtteranceBuffer)
            userUtteranceBuffer = ""
          }
        }
      } else if (data.type === "UtteranceEnd") {
        if (sttTimer) {
          console.log(`🕒 [STT-TRANSCRIPTION] ${sttTimer.end()}ms - Text: "${userUtteranceBuffer.trim()}"`)
          sttTimer = null
        }

        if (userUtteranceBuffer.trim()) {
          if (callLogger && userUtteranceBuffer.trim()) {
            const detectedLang = detectLanguageWithFranc(userUtteranceBuffer.trim(), currentLanguage || "en")
            callLogger.logUserTranscript(userUtteranceBuffer.trim(), detectedLang)
          }

          await processUserUtterance(userUtteranceBuffer)
          userUtteranceBuffer = ""
        }
      }
    }

    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText) return

      // If there's an ongoing TTS, interrupt it for the new user utterance
      if (currentTTS) {
        currentTTS.interrupt() // This will close the Sarvam WS and clear its queue
      }

      isProcessing = true
      lastProcessedText = text
      const currentRequestId = ++processingRequestId

      try {
        const detectedLanguage = detectLanguageWithFranc(text, currentLanguage || "en")

        if (detectedLanguage !== currentLanguage) {
          currentLanguage = detectedLanguage
          // If language changes, reset TTS processor with new language
          if (currentTTS) {
            currentTTS.reset(currentLanguage)
          }
        }

        const response = await processWithOpenAI(
          text,
          conversationHistory,
          detectedLanguage,
          callLogger,
          agentConfig,
        )

        if (processingRequestId === currentRequestId && response) {
          // Reuse the existing currentTTS instance
          // The synthesizeAndStream method will handle connecting/reconnecting Sarvam WS
          await currentTTS.synthesizeAndStream(response)

          conversationHistory.push(
            { role: "user", content: text },
            { role: "assistant", content: response }
          )

          if (conversationHistory.length > 10) {
            conversationHistory = conversationHistory.slice(-10)
          }
        }
      } catch (error) {
        console.error("Error in processUserUtterance:", error)
      } finally {
        if (processingRequestId === currentRequestId) {
          isProcessing = false
        }
      }
    }

    ws.on("message", async (message) => {
      try {
        const messageStr = message.toString()

        if (messageStr === "EOS" || messageStr === "BOS" || !messageStr.startsWith("{")) {
          return
        }

        const data = JSON.parse(messageStr)

        switch (data.event) {
          case "connected":
            break

          case "start": {
            streamSid = data.streamSid || data.start?.streamSid
            const accountSid = data.start?.accountSid

            let mobile = null
            if (data.start?.from) {
              mobile = data.start.from
            } else if (urlParams.caller_id) {
              mobile = urlParams.caller_id
            } else if (data.start?.extraData?.CallCli) {
              mobile = data.start.extraData.CallCli
            }

            let to = null
            if (data.start?.to) {
              to = data.start.to
            } else if (urlParams.did) {
              to = urlParams.did
            } else if (data.start?.extraData?.DID) {
              to = data.start.extraData.DID
            }

            let extraData = null

            if (data.start?.extraData) {
              extraData = decodeExtraData(data.start.extraData)
            } else if (urlParams.extra) {
              extraData = decodeExtraData(urlParams.extra)
            }

            if (extraData?.CallCli && !mobile) {
              mobile = extraData.CallCli
            }

            if (extraData && extraData.CallDirection === "OutDial") {
              callDirection = "outbound"
            } else if (urlParams.direction === "OutDial") {
              callDirection = "outbound"
              if (!extraData && urlParams.extra) {
                extraData = decodeExtraData(urlParams.extra)
              }
            } else {
              callDirection = "inbound"
            }

            try {
              agentConfig = await findAgentForCall({
                accountSid,
                callDirection,
                extraData,
              })

              if (!agentConfig) {
                ws.send(
                  JSON.stringify({
                    event: "error",
                    message: `No agent found for ${callDirection} call`,
                  }),
                )
                ws.close()
                return
              }
            } catch (err) {
              ws.send(
                JSON.stringify({
                  event: "error",
                  message: err.message,
                }),
              )
              ws.close()
              return
            }

            ws.sessionAgentConfig = agentConfig
            currentLanguage = agentConfig.language || "en"

            callLogger = new CallLogger(agentConfig.clientId || accountSid, mobile, callDirection)
            currentTTS = new SimplifiedSarvamTTSProcessor(currentLanguage, ws, streamSid, callLogger) // Initialize TTS processor here

            await connectToDeepgram()

            const greeting = agentConfig.firstMessage || "Hello! How can I help you today?"

            // Log AI response and synthesize via the new TTS processor
            await currentTTS.synthesizeAndStream(greeting)
            break
          }

          case "media":
            if (data.media?.payload) {
              const audioBuffer = Buffer.from(data.media.payload, "base64")

              if (deepgramWs && deepgramReady && deepgramWs.readyState === WebSocket.OPEN) {
                deepgramWs.send(audioBuffer)
              } else {
                deepgramAudioQueue.push(audioBuffer)
              }
            }
            break

          case "stop":
            if (callLogger) {
              try {
                const savedLog = await callLogger.saveToDatabase("medium")
              } catch (error) {
                // Silent error handling
              }
            }

            if (deepgramWs?.readyState === WebSocket.OPEN) {
              deepgramWs.close()
            }
            if (currentTTS) {
              currentTTS.interrupt() // Ensure Sarvam TTS WS is closed
            }
            break

          default:
            break
        }
      } catch (error) {
        // Silent error handling
      }
    })

    ws.on("close", async () => {
      if (callLogger) {
        try {
          const savedLog = await callLogger.saveToDatabase("not_connected")
        } catch (error) {
          // Silent error handling
        }
      }

      if (deepgramWs?.readyState === WebSocket.OPEN) {
        deepgramWs.close()
      }
      if (currentTTS) {
        currentTTS.interrupt() // Ensure Sarvam TTS WS is closed
      }

      // Reset state
      streamSid = null
      conversationHistory = []
      isProcessing = false
      userUtteranceBuffer = ""
      lastProcessedText = ""
      deepgramReady = false
      deepgramAudioQueue = []
      currentTTS = null
      currentLanguage = undefined
      processingRequestId = 0
      callLogger = null
      callDirection = "inbound"
      agentConfig = null
      sttTimer = null
    })

    ws.on("error", (error) => {
      // Silent error handling
    })
  })
}

module.exports = { setupUnifiedVoiceServer }
