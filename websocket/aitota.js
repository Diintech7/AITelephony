const WebSocket = require("ws")
require("dotenv").config()
const mongoose = require("mongoose")
const Agent = require("../models/Agent")
const CallLog = require("../models/CallLog")
const Credit = require("../models/Credit")

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
  whatsapp: process.env.WHATSAPP_TOKEN,
}

// Validate API keys
if (!API_KEYS.deepgram || !API_KEYS.sarvam || !API_KEYS.openai) {
  console.error("❌ Missing required API keys in environment variables")
  process.exit(1)
}

const fetch = globalThis.fetch || require("node-fetch")

// WhatsApp send-info API config (will be retrieved from agent config)
let WHATSAPP_API_URL = null

// Normalize Indian mobile to 91XXXXXXXXXX format (no +)
const normalizeIndianMobile = (raw) => {
  try {
    if (!raw) return null
    const digits = String(raw).replace(/\D+/g, "")
    if (!digits) return null
    // Remove leading country/long trunk prefixes; keep last 10 digits for India
    const last10 = digits.slice(-10)
    if (last10.length !== 10) return null
    return `91${last10}`
  } catch (_) {
    return null
  }
}

// Send WhatsApp info via external endpoint (fire-and-forget safe)
const sendWhatsAppTemplateMessage = async (toNumber, link = null, whatsappUrl = null) => {
  const body = link ? { to: toNumber, link } : { to: toNumber }
  const apiUrl = whatsappUrl || WHATSAPP_API_URL

  if (!apiUrl) {
    console.log("❌ [WHATSAPP] No WhatsApp API URL configured")
    return { ok: false, error: "No WhatsApp API URL configured" }
  }

  try {
    console.log("📨 [WHATSAPP] POST", apiUrl)
    console.log("📨 [WHATSAPP] Payload:", JSON.stringify(body))
    const res = await fetch(apiUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(API_KEYS.whatsapp ? { Authorization: `Bearer ${API_KEYS.whatsapp}` } : {}),
      },
      body: JSON.stringify(body),
    })
    const text = await res.text()
    const isOk = res.ok
    console.log(`📨 [WHATSAPP] Status: ${res.status} ${res.statusText}`)
    console.log("📨 [WHATSAPP] Response:", text)
    return { ok: isOk, status: res.status, body: text }
  } catch (err) {
    console.log("❌ [WHATSAPP] Error:", err.message)
    return { ok: false, error: err.message }
  }
}

// Resolve WhatsApp link from agent config
const getAgentWhatsappLink = (agent) => {
  try {
    if (!agent) return null
    if (agent.whatsapplink && typeof agent.whatsapplink === "string" && agent.whatsapplink.trim()) {
      return agent.whatsapplink.trim()
    }
    if (Array.isArray(agent.whatsapp) && agent.whatsapp.length > 0) {
      const first = agent.whatsapp.find((w) => w && typeof w.link === "string" && w.link.trim())
      if (first) return first.link.trim()
    }
    return null
  } catch (_) {
    return null
  }
}

// Performance timing helper
const createTimer = (label) => {
  const start = Date.now()
  return {
    start,
    end: () => Date.now() - start,
    checkpoint: (checkpointName) => Date.now() - start,
  }
}

// Format timestamp with milliseconds for detailed latency tracking
const formatTimestamp = (timestamp = Date.now()) => {
  const date = new Date(timestamp)
  const hours = date.getHours().toString().padStart(2, '0')
  const minutes = date.getMinutes().toString().padStart(2, '0')
  const seconds = date.getSeconds().toString().padStart(2, '0')
  const milliseconds = date.getMilliseconds().toString().padStart(3, '0')
  return `${hours}:${minutes}:${seconds}:${milliseconds}`
}

// Enhanced logging with timestamp
const logWithTimestamp = (prefix, message, timestamp = Date.now()) => {
  const timeStr = formatTimestamp(timestamp)
  console.log(`[${timeStr}] ${prefix} ${message}`)
}

// Performance timing logger with detailed metrics
const logPerformanceTiming = (stage, duration, details = {}) => {
  const timeStr = formatTimestamp()
  const durationStr = duration ? `${duration}ms` : 'N/A'
  const detailsStr = Object.keys(details).length > 0 ? ` | ${JSON.stringify(details)}` : ''
  console.log(`[${timeStr}] ⏱️ [PERF-${stage.toUpperCase()}] ${durationStr}${detailsStr}`)
}

// Pipeline timing tracker
class PipelineTimingTracker {
  constructor() {
    this.startTime = Date.now()
    this.checkpoints = {}
    this.stageDurations = {}
  }

  checkpoint(stage, details = {}) {
    const now = Date.now()
    const duration = now - this.startTime
    this.checkpoints[stage] = { time: now, duration, details }
    logPerformanceTiming(stage, duration, details)
    return duration
  }

  stageComplete(stage, startTime, details = {}) {
    const duration = Date.now() - startTime
    this.stageDurations[stage] = duration
    logPerformanceTiming(stage, duration, details)
    return duration
  }

  getSummary() {
    const totalDuration = Date.now() - this.startTime
    const summary = {
      totalDuration,
      checkpoints: this.checkpoints,
      stageDurations: this.stageDurations
    }
    logPerformanceTiming('SUMMARY', totalDuration, summary)
    return summary
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
const VALID_SARVAM_VOICES = new Set([
  "abhilash",
  "anushka",
  "meera",
  "pavithra",
  "maitreyi",
  "arvind",
  "amol",
  "amartya",
  "diya",
  "neel",
  "misha",
  "vian",
  "arjun",
  "maya",
  "manisha",
  "vidya",
  "arya",
  "karun",
  "hitesh",
])

const getValidSarvamVoice = (voiceSelection = "pavithra") => {
  const normalized = (voiceSelection || "").toString().trim().toLowerCase()
  if (VALID_SARVAM_VOICES.has(normalized)) {
    return normalized
  }

  const voiceMapping = {
    "male-professional": "arvind",
    "female-professional": "pavithra",
    "male-friendly": "amol",
    "female-friendly": "maya",
    neutral: "pavithra",
    default: "pavithra",
    male: "arvind",
    female: "pavithra",
  }

  return voiceMapping[normalized] || "pavithra"
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

// Utility function to decode czdata (base64 JSON)
const decodeCzdata = (czdataBase64) => {
  try {
    if (!czdataBase64) return null;
    const decoded = Buffer.from(czdataBase64, "base64").toString("utf-8");
    return JSON.parse(decoded);
  } catch (e) {
    return null;
  }
};

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
      
      const latinScript = /^[a-zA-Z\s\?\!\.\,\'\"0-9\-\(\)]+$/
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
      
      const latinScript = /^[a-zA-Z\s\?\!\.\,\'\"0-9\-\(\)]+$/
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

// Allowed lead statuses based on CallLog model
const ALLOWED_LEAD_STATUSES = new Set([
  'vvi', 'maybe', 'enrolled',
  'junk_lead', 'not_required', 'enrolled_other', 'decline', 'not_eligible', 'wrong_number',
  'hot_followup', 'cold_followup', 'schedule',
  'not_connected'
]);

const normalizeLeadStatus = (value, fallback = 'maybe') => {
  if (!value || typeof value !== 'string') return fallback;
  const normalized = value.trim().toLowerCase();
  return ALLOWED_LEAD_STATUSES.has(normalized) ? normalized : fallback;
};

// Enhanced Call logging utility class with live transcript saving
class EnhancedCallLogger {
  constructor(clientId, mobile = null, callDirection = "inbound") {
    this.clientId = clientId
    this.mobile = mobile
    this.callDirection = callDirection
    this.callStartTime = new Date()
    this.transcripts = []
    this.responses = []
    this.totalDuration = 0
    this.callLogId = null
    this.isCallLogCreated = false
    this.pendingTranscripts = []
    this.batchTimer = null
    this.batchSize = 5 // Save every 5 transcript entries
    this.batchTimeout = 3000 // Or save every 3 seconds
    this.customParams = {}
    this.callerId = null
    this.streamSid = null
    this.callSid = null
    this.accountSid = null
    this.ws = null // Store WebSocket reference for disconnection
    this.uniqueid = null // Store uniqueid for outbound calls
    this.currentLeadStatus = 'not_connected' // Track current lead status
    this.whatsappSent = false // Track if WhatsApp was already sent
    this.whatsappRequested = false // Track if user requested WhatsApp
  }

  // Create initial call log entry immediately when call starts
  async createInitialCallLog(agentId = null, leadStatusInput = 'not_connected') {
    const timer = createTimer("INITIAL_CALL_LOG_CREATE")
    try {
      const initialCallLogData = {
        clientId: this.clientId,
        agentId: agentId,
        mobile: this.mobile,
        time: this.callStartTime,
        transcript: "",
        duration: 0,
        leadStatus: normalizeLeadStatus(leadStatusInput, 'not_connected'),
        streamSid: this.streamSid,
        callSid: this.callSid,
        metadata: {
          userTranscriptCount: 0,
          aiResponseCount: 0,
          languages: [],
          callDirection: this.callDirection,
          isActive: true,
          lastUpdated: new Date(),
          sttProvider: 'deepgram',
          ttsProvider: 'sarvam',
          llmProvider: 'openai',
          customParams: this.customParams || {},
          callerId: this.callerId || undefined,
        },
      }

      const callLog = new CallLog(initialCallLogData)
      const savedLog = await callLog.save()
      this.callLogId = savedLog._id
      this.isCallLogCreated = true

      // Add to active call loggers map for manual termination
      if (this.streamSid) {
        activeCallLoggers.set(this.streamSid, this)
        console.log(`📋 [ACTIVE-CALL-LOGGERS] Added call logger for streamSid: ${this.streamSid}`)
      }

      console.log(`🕒 [INITIAL-CALL-LOG] ${timer.end()}ms - Created: ${savedLog._id}`)
      return savedLog
    } catch (error) {
      console.log(`❌ [INITIAL-CALL-LOG] ${timer.end()}ms - Error: ${error.message}`)
      throw error
    }
  }

  // Method to disconnect the call - OPTIMIZED FOR PARALLEL EXECUTION
  async disconnectCall(reason = 'user_disconnected') {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.log("⚠️ [CALL-DISCONNECT] WebSocket not available for disconnection")
      return false
    }

    try {
      console.log(`🛑 [CALL-DISCONNECT] Disconnecting call: ${reason}`)
      
      // Send stop event to terminate the call with proper structure
      const stopMessage = {
        event: "stop",
        sequenceNumber: stopEventSequence++,
        stop: {
          accountSid: this.accountSid || "ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
          callSid: this.callSid || "CAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        },
        streamSid: this.streamSid
      }
      
      console.log(`🛑 [CALL-DISCONNECT] Sending stop event:`, JSON.stringify(stopMessage, null, 2))
      
      // Execute all disconnection operations in parallel for minimal latency
      const disconnectionPromises = []
      
      // 1. Send stop event immediately (non-blocking)
      if (this.ws.readyState === WebSocket.OPEN) {
        try {
          this.ws.send(JSON.stringify(stopMessage))
          console.log(`🛑 [CALL-DISCONNECT] Stop event sent successfully`)
        } catch (error) {
          console.log(`⚠️ [CALL-DISCONNECT] Error sending stop event: ${error.message}`)
        }
      }
      
      // 2. Send fallback close event after short delay (non-blocking)
      const fallbackClosePromise = new Promise((resolve) => {
        setTimeout(() => {
          if (this.ws.readyState === WebSocket.OPEN) {
            const closeMessage = {
              event: "close",
              streamSid: this.streamSid,
              reason: reason
            }
            console.log(`🛑 [CALL-DISCONNECT] Sending fallback close event:`, JSON.stringify(closeMessage, null, 2))
            
            try {
              this.ws.send(JSON.stringify(closeMessage))
              console.log(`🛑 [CALL-DISCONNECT] Fallback close event sent`)
            } catch (error) {
              console.log(`⚠️ [CALL-DISCONNECT] Error sending fallback close: ${error.message}`)
            }
          }
          resolve()
        }, 500) // Reduced from 1000ms to 500ms for faster disconnection
      })
      disconnectionPromises.push(fallbackClosePromise)
      
      // 3. Force close WebSocket after delay (non-blocking)
      const forceClosePromise = new Promise((resolve) => {
        setTimeout(() => {
          if (this.ws.readyState === WebSocket.OPEN) {
            console.log(`🛑 [CALL-DISCONNECT] Force closing WebSocket connection`)
            this.ws.close(1000, `Call terminated: ${reason}`)
          }
          resolve()
        }, 1500) // Reduced from 2000ms to 1500ms for faster disconnection
      })
      disconnectionPromises.push(forceClosePromise)
      
      // 4. Update call log to mark as inactive (non-blocking)
      const callLogUpdatePromise = CallLog.findByIdAndUpdate(this.callLogId, {
        'metadata.isActive': false,
        'metadata.callEndTime': new Date(),
        'metadata.lastUpdated': new Date(),
        'metadata.terminationReason': reason,
        'metadata.terminatedAt': new Date(),
        'metadata.terminationMethod': 'manual_api'
      }).catch(err => console.log(`⚠️ [CALL-DISCONNECT] Call log update error: ${err.message}`))
      disconnectionPromises.push(callLogUpdatePromise)
      
      // Wait for all disconnection operations to complete
      await Promise.allSettled(disconnectionPromises)
      
      console.log("✅ [CALL-DISCONNECT] Call disconnected successfully")
      return true
    } catch (error) {
      console.log(`❌ [CALL-DISCONNECT] Error disconnecting call: ${error.message}`)
      return false
    }
  }

  // Method to get call information for external disconnection
  getCallInfo() {
    return {
      streamSid: this.streamSid,
      callSid: this.callSid,
      accountSid: this.accountSid,
      callLogId: this.callLogId,
      clientId: this.clientId,
      mobile: this.mobile,
      isActive: this.isCallLogCreated && this.callLogId
    }
  }

  // Method to gracefully end call with goodbye message - PARALLEL EXECUTION
  async gracefulCallEnd(goodbyeMessage = "Thank you for your time. Have a great day!", language = "en") {
    try {
      console.log("👋 [GRACEFUL-END] Ending call gracefully with goodbye message")
      
      // Log the goodbye message
      this.logAIResponse(goodbyeMessage, language)
      
      // Update call log immediately (non-blocking)
      const callLogUpdate = CallLog.findByIdAndUpdate(this.callLogId, {
        'metadata.lastUpdated': new Date()
      }).catch(err => console.log(`⚠️ [GRACEFUL-END] Call log update error: ${err.message}`))
      
      // Start TTS synthesis for goodbye message (non-blocking)
      const ttsPromise = this.synthesizeGoodbyeMessage(goodbyeMessage, language)
      
      // Start disconnection process in parallel (non-blocking)
      const disconnectPromise = this.disconnectCall('graceful_termination')
      
      // Execute all operations in parallel for minimal latency
      await Promise.allSettled([
        callLogUpdate,
        ttsPromise,
        disconnectPromise
      ])
      
      console.log("✅ [GRACEFUL-END] All operations completed in parallel")
      return true
    } catch (error) {
      console.log(`❌ [GRACEFUL-END] Error in graceful call end: ${error.message}`)
      return false
    }
  }

  // Synthesize goodbye message without waiting for completion
  async synthesizeGoodbyeMessage(message, language) {
    try {
      console.log("🎤 [GRACEFUL-END] Starting goodbye message TTS...")
      
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const tts = new SimplifiedSarvamTTSProcessor(language, this.ws, this.streamSid, this.callLogger)
        
        // Start TTS synthesis but don't wait for completion
        tts.synthesizeAndStream(message).catch(err => 
          console.log(`⚠️ [GRACEFUL-END] TTS error: ${err.message}`)
        )
        
        console.log("✅ [GRACEFUL-END] Goodbye message TTS started")
      } else {
        console.log("⚠️ [GRACEFUL-END] WebSocket not available for TTS")
      }
    } catch (error) {
      console.log(`❌ [GRACEFUL-END] TTS synthesis error: ${error.message}`)
    }
  }

  // Fast parallel call termination for minimal latency
  async fastTerminateCall(reason = 'fast_termination') {
    try {
      console.log(`⚡ [FAST-TERMINATE] Fast terminating call: ${reason}`)
      
      // Execute all termination operations in parallel for minimal latency
      const terminationPromises = []
      
      // 1. Send stop event immediately (non-blocking)
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const stopMessage = {
          event: "stop",
          sequenceNumber: stopEventSequence++,
          stop: {
            accountSid: this.accountSid || "ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            callSid: this.callSid || "CAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
          },
          streamSid: this.streamSid
        }
        
        try {
          this.ws.send(JSON.stringify(stopMessage))
          console.log(`⚡ [FAST-TERMINATE] Stop event sent immediately`)
        } catch (error) {
          console.log(`⚠️ [FAST-TERMINATE] Error sending stop event: ${error.message}`)
        }
      }
      
      // 2. Update call log (non-blocking)
      if (this.callLogId) {
        const callLogUpdate = CallLog.findByIdAndUpdate(this.callLogId, {
          'metadata.isActive': false,
          'metadata.callEndTime': new Date(),
          'metadata.lastUpdated': new Date(),
          'metadata.terminationReason': reason,
          'metadata.terminatedAt': new Date(),
          'metadata.terminationMethod': 'fast_termination'
        }).catch(err => console.log(`⚠️ [FAST-TERMINATE] Call log update error: ${err.message}`))
        
        terminationPromises.push(callLogUpdate)
      }
      
      // 3. Force close WebSocket after minimal delay (non-blocking)
      const forceClosePromise = new Promise((resolve) => {
        setTimeout(() => {
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            console.log(`⚡ [FAST-TERMINATE] Force closing WebSocket connection`)
            this.ws.close(1000, `Call terminated: ${reason}`)
          }
          resolve()
        }, 300) // Reduced to 300ms for faster termination
      })
      terminationPromises.push(forceClosePromise)
      
      // Wait for all operations to complete
      await Promise.allSettled(terminationPromises)
      
      console.log("✅ [FAST-TERMINATE] Call terminated with minimal latency")
      return true
    } catch (error) {
      console.log(`❌ [FAST-TERMINATE] Error in fast termination: ${error.message}`)
      return false
    }
  }

  // Ultra-fast termination with goodbye message - minimal latency approach
  async ultraFastTerminateWithMessage(goodbyeMessage = "Thank you, goodbye!", language = "en", reason = 'ultra_fast_termination') {
    try {
      console.log(`🚀 [ULTRA-FAST-TERMINATE] Ultra-fast termination with message: ${reason}`)
      
      // Execute all operations in parallel for absolute minimal latency
      const allPromises = []
      
      // 1. Log the goodbye message (non-blocking)
      this.logAIResponse(goodbyeMessage, language)
      
      // 2. Start TTS synthesis first to ensure message is sent (non-blocking, but wait for start)
      let ttsStarted = false
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const tts = new SimplifiedSarvamTTSProcessor(language, this.ws, this.streamSid, this.callLogger)
        
        // Start TTS and wait for it to begin
        try {
          await tts.synthesizeAndStream(goodbyeMessage)
          ttsStarted = true
          console.log(`🚀 [ULTRA-FAST-TERMINATE] Goodbye message TTS completed`)
        } catch (err) {
          console.log(`⚠️ [ULTRA-FAST-TERMINATE] TTS error: ${err.message}`)
        }
      }
      
      // 3. Send stop event after TTS starts (non-blocking)
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const stopMessage = {
          event: "stop",
          sequenceNumber: stopEventSequence++,
          stop: {
            accountSid: this.accountSid || "ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            callSid: this.callSid || "CAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
          },
          streamSid: this.streamSid
        }
        
        try {
          this.ws.send(JSON.stringify(stopMessage))
          console.log(`🚀 [ULTRA-FAST-TERMINATE] Stop event sent after TTS`)
        } catch (error) {
          console.log(`⚠️ [ULTRA-FAST-TERMINATE] Error sending stop event: ${error.message}`)
        }
      }
      
      // 4. Update call log (non-blocking)
      if (this.callLogId) {
        const callLogUpdate = CallLog.findByIdAndUpdate(this.callLogId, {
          'metadata.isActive': false,
          'metadata.callEndTime': new Date(),
          'metadata.lastUpdated': new Date(),
          'metadata.terminationReason': reason,
          'metadata.terminatedAt': new Date(),
          'metadata.terminationMethod': 'ultra_fast_termination'
        }).catch(err => console.log(`⚠️ [ULTRA-FAST-TERMINATE] Call log update error: ${err.message}`))
        
        allPromises.push(callLogUpdate)
      }
      
      // 5. Force close WebSocket after ensuring TTS is sent (non-blocking)
      const forceClosePromise = new Promise((resolve) => {
        setTimeout(() => {
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            console.log(`🚀 [ULTRA-FAST-TERMINATE] Force closing WebSocket connection`)
            this.ws.close(1000, `Call terminated: ${reason}`)
          }
          resolve()
        }, 500) // Increased delay to ensure TTS is sent
      })
      allPromises.push(forceClosePromise)
      
      // Wait for all operations to complete
      await Promise.allSettled(allPromises)
      
      console.log("✅ [ULTRA-FAST-TERMINATE] Call terminated with ultra-minimal latency")
      return true
    } catch (error) {
      console.log(`❌ [ULTRA-FAST-TERMINATE] Error in ultra-fast termination: ${error.message}`)
      return false
    }
  }

  // Controlled termination with proper timing - ensures message is sent before disconnection
  async controlledTerminateWithMessage(goodbyeMessage = "Thank you, goodbye!", language = "en", reason = 'controlled_termination', delayMs = 2000) {
    try {
      console.log(`⏱️ [CONTROLLED-TERMINATE] Controlled termination with message: ${reason}, delay: ${delayMs}ms`)
      
      // 1. Log the goodbye message
      this.logAIResponse(goodbyeMessage, language)
      
      // 2. Start TTS synthesis and wait for completion
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const tts = new SimplifiedSarvamTTSProcessor(language, this.ws, this.streamSid, this.callLogger)
        
        try {
          console.log(`⏱️ [CONTROLLED-TERMINATE] Starting TTS synthesis...`)
          await tts.synthesizeAndStream(goodbyeMessage)
          console.log(`⏱️ [CONTROLLED-TERMINATE] TTS synthesis completed`)
        } catch (err) {
          console.log(`⚠️ [CONTROLLED-TERMINATE] TTS error: ${err.message}`)
        }
      }
      
      // 3. Wait for specified delay to ensure message is processed
      console.log(`⏱️ [CONTROLLED-TERMINATE] Waiting ${delayMs}ms before disconnection...`)
      await new Promise(resolve => setTimeout(resolve, delayMs))
      
      // 4. Now terminate the call
      console.log(`⏱️ [CONTROLLED-TERMINATE] Delay completed, now terminating call...`)
      return await this.fastTerminateCall(reason)
      
    } catch (error) {
      console.log(`❌ [CONTROLLED-TERMINATE] Error in controlled termination: ${error.message}`)
      return false
    }
  }

  // Add transcript with batched live saving
  logUserTranscript(transcript, language, timestamp = new Date()) {
    const entry = {
      type: "user",
      text: transcript,
      language: language,
      timestamp: timestamp,
      source: "deepgram",
    }

    this.transcripts.push(entry)
    this.pendingTranscripts.push(entry)
    
    // Trigger batch save
    this.scheduleBatchSave()
  }

  // Add AI response with batched live saving
  logAIResponse(response, language, timestamp = new Date()) {
    const entry = {
      type: "ai",
      text: response,
      language: language,
      timestamp: timestamp,
      source: "sarvam",
    }

    this.responses.push(entry)
    this.pendingTranscripts.push(entry)
    
    // Trigger batch save
    this.scheduleBatchSave()
  }

  // Schedule batched saving to reduce DB calls
  scheduleBatchSave() {
    // Clear existing timer
    if (this.batchTimer) {
      clearTimeout(this.batchTimer)
    }

    // Save immediately if batch size reached
    if (this.pendingTranscripts.length >= this.batchSize) {
      this.savePendingTranscripts()
      return
    }

    // Otherwise schedule save after timeout
    this.batchTimer = setTimeout(() => {
      this.savePendingTranscripts()
    }, this.batchTimeout)
  }

  // Save pending transcripts in background (non-blocking)
  async savePendingTranscripts() {
    if (!this.isCallLogCreated || this.pendingTranscripts.length === 0) {
      return
    }

    // Create a copy and clear pending immediately to avoid blocking
    const transcriptsToSave = [...this.pendingTranscripts]
    this.pendingTranscripts = []
    
    // Clear timer
    if (this.batchTimer) {
      clearTimeout(this.batchTimer)
      this.batchTimer = null
    }

    // Save asynchronously without awaiting (fire and forget)
    setImmediate(async () => {
      const timer = createTimer("LIVE_TRANSCRIPT_BATCH_SAVE")
      try {
        const currentTranscript = this.generateFullTranscript()
        const currentDuration = Math.round((new Date() - this.callStartTime) / 1000)
        
        const updateData = {
          transcript: currentTranscript,
          duration: currentDuration,
          'metadata.userTranscriptCount': this.transcripts.length,
          'metadata.aiResponseCount': this.responses.length,
          'metadata.languages': [...new Set([...this.transcripts, ...this.responses].map(e => e.language))],
          'metadata.lastUpdated': new Date()
        }

        await CallLog.findByIdAndUpdate(this.callLogId, updateData, { 
          new: false, // Don't return updated doc to save bandwidth
          runValidators: false // Skip validation for performance
        })

        console.log(`🕒 [LIVE-TRANSCRIPT-SAVE] ${timer.end()}ms - Saved ${transcriptsToSave.length} entries`)
      } catch (error) {
        console.log(`❌ [LIVE-TRANSCRIPT-SAVE] ${timer.end()}ms - Error: ${error.message}`)
        // On error, add back to pending for retry
        this.pendingTranscripts.unshift(...transcriptsToSave)
      }
    })
  }

  // Generate full transcript
  generateFullTranscript() {
    const allEntries = [...this.transcripts, ...this.responses].sort(
      (a, b) => new Date(a.timestamp) - new Date(b.timestamp)
    )

    return allEntries
      .map((entry) => {
        const speaker = entry.type === "user" ? "User" : "AI"
        const time = entry.timestamp.toISOString()
        return `[${time}] ${speaker} (${entry.language}): ${entry.text}`
      })
      .join("\n")
  }

  // Final save with complete call data
  async saveToDatabase(leadStatusInput = 'maybe') {
    const timer = createTimer("FINAL_CALL_LOG_SAVE")
    try {
      const callEndTime = new Date()
      this.totalDuration = Math.round((callEndTime - this.callStartTime) / 1000)

      // Save any remaining pending transcripts first
      if (this.pendingTranscripts.length > 0) {
        await this.savePendingTranscripts()
        // Small delay to ensure batch save completes
        await new Promise(resolve => setTimeout(resolve, 100))
      }

      const leadStatus = normalizeLeadStatus(leadStatusInput, 'maybe')

      if (this.isCallLogCreated && this.callLogId) {
        // Update existing call log with final data
        const finalUpdateData = {
          transcript: this.generateFullTranscript(),
          duration: this.totalDuration,
          leadStatus: leadStatus,
          streamSid: this.streamSid,
          callSid: this.callSid,
          'metadata.userTranscriptCount': this.transcripts.length,
          'metadata.aiResponseCount': this.responses.length,
          'metadata.languages': [...new Set([...this.transcripts, ...this.responses].map(e => e.language))],
          'metadata.callEndTime': callEndTime,
          'metadata.isActive': false,
          'metadata.lastUpdated': callEndTime,
          'metadata.customParams': this.customParams || {},
          'metadata.callerId': this.callerId || undefined,
        }

        const updatedLog = await CallLog.findByIdAndUpdate(
          this.callLogId, 
          finalUpdateData, 
          { new: true }
        )

        console.log(`🕒 [FINAL-CALL-LOG-SAVE] ${timer.end()}ms - Updated: ${updatedLog._id}`)
        return updatedLog
      } else {
        // Fallback: create new call log if initial creation failed
        const callLogData = {
          clientId: this.clientId,
          mobile: this.mobile,
          time: this.callStartTime,
          transcript: this.generateFullTranscript(),
          duration: this.totalDuration,
          leadStatus: leadStatus,
          streamSid: this.streamSid,
          callSid: this.callSid,
          metadata: {
            userTranscriptCount: this.transcripts.length,
            aiResponseCount: this.responses.length,
            languages: [...new Set([...this.transcripts, ...this.responses].map(e => e.language))],
            callEndTime: callEndTime,
            callDirection: this.callDirection,
            isActive: false,
            customParams: this.customParams || {},
            callerId: this.callerId || undefined,
          },
        }

        const callLog = new CallLog(callLogData)
        const savedLog = await callLog.save()
        console.log(`🕒 [FINAL-CALL-LOG-SAVE] ${timer.end()}ms - Created: ${savedLog._id}`)
        return savedLog
      }
    } catch (error) {
      console.log(`❌ [FINAL-CALL-LOG-SAVE] ${timer.end()}ms - Error: ${error.message}`)
      throw error
    }
  }

  // Cleanup method
  cleanup() {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer)
      this.batchTimer = null
    }
    
    // Remove from active call loggers map
    if (this.streamSid) {
      activeCallLoggers.delete(this.streamSid)
      console.log(`📋 [ACTIVE-CALL-LOGGERS] Removed call logger for streamSid: ${this.streamSid}`)
    }
  }

  // Update lead status
  updateLeadStatus(newStatus) {
    this.currentLeadStatus = newStatus
    console.log(`📊 [LEAD-STATUS] Updated to: ${newStatus}`)
  }

  // Mark WhatsApp as sent
  markWhatsAppSent() {
    this.whatsappSent = true
    console.log(`📨 [WHATSAPP-TRACKING] Marked as sent`)
  }

  // Mark WhatsApp as requested
  markWhatsAppRequested() {
    this.whatsappRequested = true
    console.log(`📨 [WHATSAPP-TRACKING] Marked as requested by user`)
  }

  // Check if WhatsApp should be sent based on lead status and user request
  shouldSendWhatsApp() {
    // Don't send if already sent
    if (this.whatsappSent) {
      console.log(`📨 [WHATSAPP-LOGIC] Skipping - already sent`)
      return false
    }

    // Send if user is VVI (very very interested)
    if (this.currentLeadStatus === 'vvi') {
      console.log(`📨 [WHATSAPP-LOGIC] Sending - user is VVI`)
      return true
    }

    // Send if user explicitly requested WhatsApp
    if (this.whatsappRequested) {
      console.log(`📨 [WHATSAPP-LOGIC] Sending - user requested WhatsApp`)
      return true
    }

    console.log(`📨 [WHATSAPP-LOGIC] Skipping - not VVI and no request`)
    return false
  }

  getStats() {
    return {
      duration: this.totalDuration,
      userMessages: this.transcripts.length,
      aiResponses: this.responses.length,
      languages: [...new Set([...this.transcripts, ...this.responses].map(e => e.language))],
      startTime: this.callStartTime,
      callDirection: this.callDirection,
      callLogId: this.callLogId,
      pendingTranscripts: this.pendingTranscripts.length,
      currentLeadStatus: this.currentLeadStatus,
      whatsappSent: this.whatsappSent,
      whatsappRequested: this.whatsappRequested
    }
  }
}

// Simplified OpenAI processing (non-streaming; kept for fallback)
const processWithOpenAI = async (
  userMessage,
  conversationHistory,
  detectedLanguage,
  callLogger,
  agentConfig,
  userName = null,
) => {
  const timer = createTimer("LLM_PROCESSING")

  try {
    // Build a stricter system prompt that embeds firstMessage and sets answering policy
    const basePrompt = agentConfig.systemPrompt || "You are a helpful AI assistant."
    const firstMessage = (agentConfig.firstMessage || "").trim()
    const knowledgeBlock = firstMessage
      ? `FirstGreeting: "${firstMessage}"\n`
      : ""

    const policyBlock = [
      "Answer strictly using the information provided above.",
      "If the user asks for address, phone, timings, or other specifics, check the System Prompt or FirstGreeting.",
      "If the information is not present, reply briefly that you don't have that information.",
      "Always end your answer with a short, relevant follow-up question to keep the conversation going.",
      "Keep the entire reply under 100 tokens.",
    ].join(" ")

    const systemPrompt = `System Prompt:\n${basePrompt}\n\n${knowledgeBlock}${policyBlock}`

    const personalizationMessage = userName && userName.trim()
      ? { role: "system", content: `The user's name is ${userName.trim()}. Address them by name naturally when appropriate.` }
      : null

    const messages = [
      { role: "system", content: systemPrompt },
      ...(personalizationMessage ? [personalizationMessage] : []),
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
        max_tokens: 120,
        temperature: 0.3,
      }),
    })

    if (!response.ok) {
      console.log(`❌ [LLM-PROCESSING] ${timer.end()}ms - Error: ${response.status}`)
      return null
    }

    const data = await response.json()
    let fullResponse = data.choices[0]?.message?.content?.trim()

    console.log(`🕒 [LLM-PROCESSING] ${timer.end()}ms - Response generated`)

    // Ensure a follow-up question is present at the end
    if (fullResponse) {
      const needsFollowUp = !/[?]\s*$/.test(fullResponse)
      if (needsFollowUp) {
        const followUps = {
          hi: "क्या मैं और किसी बात में आपकी मदद कर सकता/सकती हूँ?",
          en: "Is there anything else I can help you with?",
          bn: "আর কিছু কি আপনাকে সাহায্য করতে পারি?",
          ta: "வேறு எதற்காவது உதவி வேண்டுமா?",
          te: "ఇంకేమైనా సహాయం కావాలా?",
          mr: "आणखी काही मदत हवी आहे का?",
          gu: "શું બીજી કોઈ મદદ કરી શકું?",
        }
        const fu = followUps[detectedLanguage] || followUps.en
        fullResponse = `${fullResponse} ${fu}`.trim()
      }
    }

    if (callLogger && fullResponse) {
      callLogger.logAIResponse(fullResponse, detectedLanguage)
    }

    return fullResponse
  } catch (error) {
    console.log(`❌ [LLM-PROCESSING] ${timer.end()}ms - Error: ${error.message}`)
    return null
  }
}

// OpenAI streaming with immediate sentence-by-sentence processing and early return
const processWithOpenAIStreaming = async (
  userMessage,
  conversationHistory,
  detectedLanguage,
  callLogger,
  agentConfig,
  ws,
  streamSid,
  userName = null,
  callbacks = {}
) => {
  const timer = createTimer("LLM_STREAMING")
  try {
    const basePrompt = agentConfig.systemPrompt || "You are a helpful AI assistant."
    const firstMessage = (agentConfig.firstMessage || "").trim()
    const knowledgeBlock = firstMessage ? `FirstGreeting: "${firstMessage}"\n` : ""
    const policyBlock = [
      "Answer strictly using the information provided above.",
      "If specifics are missing, say you don't have that info.",
      "Keep replies concise.",
    ].join(" ")

    const systemPrompt = `System Prompt:\n${basePrompt}\n\n${knowledgeBlock}${policyBlock}`
    const personalizationMessage = userName && userName.trim()
      ? { role: "system", content: `The user's name is ${userName.trim()}.` }
      : null

    const messages = [
      { role: "system", content: systemPrompt },
      ...(personalizationMessage ? [personalizationMessage] : []),
      ...conversationHistory.slice(-6),
      { role: "user", content: userMessage },
    ]

    const llmStart = Date.now()
    const resp = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEYS.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages,
        temperature: 0.3,
        stream: true,
      }),
    })

    if (!resp.ok || !resp.body) {
      console.log(`❌ [LLM-STREAMING] ${timer.end()}ms - Error: ${resp.status}`)
      return null
    }

    const firstTokenTimer = createTimer("LLM_FIRST_TOKEN")
    const reader = resp.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ""
    let fullText = ""
    let sentenceBuffer = ""
    let lastSentenceTime = Date.now()

    let firstEnqueue = true
    let firstTokenReceived = false
    let sentencesProcessed = 0
    let hasReturnedEarly = false  // Prevent multiple early returns
    const maxSentencesBeforeReturn = 1  // Return after first sentence for minimal latency
    const enableEarlyReturn = true  // Enable early return for minimal latency
    const earlyReturnTimeout = 2000  // Max time to wait before forcing early return
    
    // Enhanced sentence processing with immediate callbacks
    const processSentence = async (sentence) => {
      if (!sentence || !sentence.trim()) return
      
      const sentenceText = sentence.trim()
      fullText += (fullText ? " " : "") + sentenceText
      
      // Log to call logger
      if (callLogger) {
        callLogger.logAIResponse(sentenceText, detectedLanguage)
      }
      
      // Trigger first enqueue callback
      if (firstEnqueue) {
        firstEnqueue = false
        firstTokenReceived = true
        if (callbacks?.onFirstEnqueue) {
          try { 
            callbacks.onFirstEnqueue(Date.now()) 
            console.log(`[STREAM] First token received at ${Date.now()}`)
          } catch (e) {
            console.log(`[STREAM] Error in onFirstEnqueue: ${e.message}`)
          }
        }
      }
      
      // Trigger sentence ready callback for immediate processing
      if (callbacks?.onSentenceReady) {
        try {
          await callbacks.onSentenceReady(sentenceText)
          console.log(`[STREAM] Sentence processed immediately: "${sentenceText}"`)
        } catch (e) {
          console.log(`[STREAM] Error in onSentenceReady: ${e.message}`)
        }
      }
      
      // Enqueue to TTS for immediate playback
      if (ws?.__enqueueSpeak) {
        try {
          await ws.__enqueueSpeak(sentenceText, detectedLanguage)
          console.log(`[STREAM] Sentence enqueued to TTS: "${sentenceText}"`)
        } catch (e) {
          console.log(`[STREAM] Error enqueuing to TTS: ${e.message}`)
        }
      }
    }

    // Enhanced sentence flushing with better punctuation detection
    const flushSentences = async () => {
      // Split on sentence boundaries (period, exclamation, question mark, and some Indian punctuation)
      const sentenceRegex = /(?<=[\.!?\u0964\u0965])\s+/
      const parts = sentenceBuffer.split(sentenceRegex)
      
      // Keep last partial sentence in buffer
      sentenceBuffer = parts.pop() || ""
      
      // Process complete sentences immediately
      for (const part of parts) {
        const text = part.trim()
        if (!text) continue
        
        await processSentence(text)
        lastSentenceTime = Date.now()
        sentencesProcessed++
        
        // Return early after processing first sentence for minimal latency
        if (enableEarlyReturn && sentencesProcessed >= maxSentencesBeforeReturn && !hasReturnedEarly) {
          hasReturnedEarly = true
          console.log(`[STREAM] Returning early after ${sentencesProcessed} sentence(s) for minimal latency`)
          return fullText
        }
      }
    }

    // Process partial sentences if they're getting too long (fallback)
    const processLongBuffer = async () => {
      if (sentenceBuffer.length > 100) { // If buffer gets too long, process it
        const words = sentenceBuffer.split(/\s+/)
        if (words.length > 10) { // If more than 10 words, process first half
          const midPoint = Math.floor(words.length / 2)
          const firstHalf = words.slice(0, midPoint).join(' ')
          const secondHalf = words.slice(midPoint).join(' ')
          
          await processSentence(firstHalf)
          sentenceBuffer = secondHalf
          lastSentenceTime = Date.now()
          sentencesProcessed++
          
          // Return early after processing first chunk
          if (enableEarlyReturn && sentencesProcessed >= maxSentencesBeforeReturn && !hasReturnedEarly) {
            hasReturnedEarly = true
            console.log(`[STREAM] Returning early after ${sentencesProcessed} chunk(s) for minimal latency`)
            return fullText
          }
        }
      }
    }

    // Process word chunks for very fast responses (optional)
    const processWordChunks = async () => {
      if (sentenceBuffer.length > 50 && sentenceBuffer.includes(' ')) {
        const words = sentenceBuffer.split(/\s+/)
        if (words.length >= 5) { // Process first 5 words
          const chunk = words.slice(0, 5).join(' ')
          const remaining = words.slice(5).join(' ')
          
          await processSentence(chunk)
          sentenceBuffer = remaining
          lastSentenceTime = Date.now()
          sentencesProcessed++
          
          // Return early after processing first chunk
          if (enableEarlyReturn && sentencesProcessed >= maxSentencesBeforeReturn && !hasReturnedEarly) {
            hasReturnedEarly = true
            console.log(`[STREAM] Returning early after ${sentencesProcessed} word chunk(s) for minimal latency`)
            return fullText
          }
        }
      }
    }

    let sawFirstDelta = false
    const streamStartTime = Date.now()
    
    // Set up timeout-based early return as fallback
    const timeoutEarlyReturn = setTimeout(() => {
      if (!hasReturnedEarly && enableEarlyReturn) {
        hasReturnedEarly = true
        console.log(`[STREAM] Timeout-based early return after ${earlyReturnTimeout}ms`)
        return fullText
      }
    }, earlyReturnTimeout)
    
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      
      const chunk = decoder.decode(value, { stream: true })
      const lines = chunk.split(/\n/)
      
      for (const line of lines) {
        const trimmed = line.trim()
        if (!trimmed.startsWith("data:")) continue
        
        const dataStr = trimmed.slice(5).trim()
        if (dataStr === "[DONE]") {
          await flushSentences()
          break
        }
        
        try {
          const obj = JSON.parse(dataStr)
          const delta = obj?.choices?.[0]?.delta?.content || ""
          
          if (delta) {
            if (!sawFirstDelta) { 
              sawFirstDelta = true
              const firstTokenTime = firstTokenTimer.end()
              console.log(`⚡ [LLM-FIRST-TOKEN] ${firstTokenTime}ms`)
            }
            
            buffer += delta
            sentenceBuffer += delta
            
            // Immediate processing on sentence completion
            if (/[\.!?\u0964\u0965]\s*$/.test(sentenceBuffer)) {
              await flushSentences()
              
              // Return early after first sentence for minimal latency
              if (enableEarlyReturn && sentencesProcessed >= maxSentencesBeforeReturn && !hasReturnedEarly) {
                hasReturnedEarly = true
                console.log(`[STREAM] Returning early after ${sentencesProcessed} sentence(s) for minimal latency`)
                return fullText
              }
            }
            
            // Also process on commas and semicolons for faster response (optional)
            if (/[,;]\s*$/.test(sentenceBuffer) && sentenceBuffer.length > 20) {
              await flushSentences()
              
              // Return early after first chunk
              if (enableEarlyReturn && sentencesProcessed >= maxSentencesBeforeReturn && !hasReturnedEarly) {
                hasReturnedEarly = true
                console.log(`[STREAM] Returning early after ${sentencesProcessed} chunk(s) for minimal latency`)
                return fullText
              }
            }
            
            // Fallback processing for long buffers
            if (Date.now() - lastSentenceTime > 2000) { // 2 seconds timeout
              await processLongBuffer()
              
              // Return early after processing
              if (enableEarlyReturn && sentencesProcessed >= maxSentencesBeforeReturn && !hasReturnedEarly) {
                hasReturnedEarly = true
                console.log(`[STREAM] Returning early after ${sentencesProcessed} chunk(s) for minimal latency`)
                return fullText
              }
            }
            
            // Process word chunks for faster response (1 second timeout)
            if (Date.now() - lastSentenceTime > 1000) { // 1 second timeout for word chunks
              await processWordChunks()
              
              // Return early after processing
              if (enableEarlyReturn && sentencesProcessed >= maxSentencesBeforeReturn && !hasReturnedEarly) {
                hasReturnedEarly = true
                console.log(`[STREAM] Returning early after ${sentencesProcessed} chunk(s) for minimal latency`)
                return fullText
              }
            }
            
            // Process very short responses immediately (like "Yes", "No", "OK")
            if (sentenceBuffer.trim().length > 0 && 
                /^(yes|no|ok|okay|sure|alright|thanks?|thank you|bye|goodbye)$/i.test(sentenceBuffer.trim())) {
              await processSentence(sentenceBuffer.trim())
              sentenceBuffer = ""
              sentencesProcessed++
              
              // Return immediately for short responses
              if (!hasReturnedEarly) {
                hasReturnedEarly = true
                console.log(`[STREAM] Returning immediately for short response: "${sentenceBuffer.trim()}"`)
                return fullText
              }
            }
          }
        } catch (e) {
          // Silent error handling for malformed JSON
        }
      }
    }

    // Clear timeout
    clearTimeout(timeoutEarlyReturn)
    
    // Flush any remaining content
    if (sentenceBuffer.trim()) {
      await processSentence(sentenceBuffer.trim())
    }

    const totalTime = Date.now() - llmStart
    console.log(`🕒 [LLM-STREAMING] ${timer.end()}ms - Completed (total=${totalTime}ms)`) 
    console.log(`[STREAM] Full response: "${fullText}"`)
    
    return fullText || null
  } catch (error) {
    console.log(`❌ [LLM-STREAMING] ${timer.end()}ms - Error: ${error.message}`)
    return null
  }
}

// Intelligent lead status detection using OpenAI
const detectLeadStatusWithOpenAI = async (userMessage, conversationHistory, detectedLanguage) => {
  const timer = createTimer("LEAD_STATUS_DETECTION")
  try {
    const leadStatusPrompt = `Analyze the user's interest level and conversation context to determine the appropriate lead status.

Available statuses:
- 'vvi' (very very interested): User shows high enthusiasm, asks detailed questions, wants to proceed immediately
- 'maybe': User shows some interest but is hesitant or needs more information
- 'enrolled': User has agreed to enroll, sign up, or take action
- 'junk_lead': User is clearly not interested, rude, or spam
- 'not_required': User says they don't need the service
- 'enrolled_other': User mentions they're already enrolled elsewhere
- 'decline': User explicitly declines the offer
- 'not_eligible': User doesn't meet requirements
- 'wrong_number': Wrong number or person
- 'hot_followup': User wants to be called back later with high interest
- 'cold_followup': User wants to be called back later with low interest
- 'schedule': User wants to schedule something
- 'not_connected': Call didn't connect or was very short

User message: "${userMessage}"
Conversation context: ${conversationHistory.slice(-3).map(msg => `${msg.role}: ${msg.content}`).join(' | ')}

Return ONLY the status code (e.g., "vvi", "maybe", "enrolled", etc.) based on the user's current interest level and intent.`

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEYS.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: leadStatusPrompt },
        ],
        max_tokens: 15,
        temperature: 0.1,
      }),
    })

    if (!response.ok) {
      console.log(`❌ [LEAD-STATUS-DETECTION] ${timer.end()}ms - Error: ${response.status}`)
      return "maybe" // Default to maybe on error
    }

    const data = await response.json()
    const detectedStatus = data.choices[0]?.message?.content?.trim().toLowerCase()

    // Validate the detected status
    const validStatuses = ['vvi', 'maybe', 'enrolled', 'junk_lead', 'not_required', 'enrolled_other', 'decline', 'not_eligible', 'wrong_number', 'hot_followup', 'cold_followup', 'schedule', 'not_connected']
    
    if (validStatuses.includes(detectedStatus)) {
      console.log(`🕒 [LEAD-STATUS-DETECTION] ${timer.end()}ms - Detected: ${detectedStatus}`)
      return detectedStatus
    } else {
      console.log(`⚠️ [LEAD-STATUS-DETECTION] ${timer.end()}ms - Invalid status detected: ${detectedStatus}, defaulting to maybe`)
      return "maybe"
    }
  } catch (error) {
    console.log(`❌ [LEAD-STATUS-DETECTION] ${timer.end()}ms - Error: ${error.message}`)
    return "maybe" // Default to maybe on error
  }
}

// Intelligent call disconnection detection using OpenAI
const detectCallDisconnectionIntent = async (userMessage, conversationHistory, detectedLanguage) => {
  const timer = createTimer("DISCONNECTION_DETECTION")
  try {
    const disconnectionPrompt = `Analyze if the user wants to end/disconnect the call. Look for:
- "thank you", "thanks", "bye", "goodbye", "end call", "hang up"
- "hold on", "wait", "not available", "busy", "call back later"
- "not interested", "no thanks", "stop calling"
- Any indication they want to end the conversation

User message: "${userMessage}"

Return ONLY: "DISCONNECT" if they want to end the call, or "CONTINUE" if they want to continue.`

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEYS.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: disconnectionPrompt },
        ],
        max_tokens: 10,
        temperature: 0.1,
      }),
    })

    if (!response.ok) {
      console.log(`❌ [DISCONNECTION-DETECTION] ${timer.end()}ms - Error: ${response.status}`)
      return "CONTINUE" // Default to continue on error
    }

    const data = await response.json()
    const result = data.choices[0]?.message?.content?.trim().toUpperCase()

    if (result === "DISCONNECT") {
      console.log(`🕒 [DISCONNECTION-DETECTION] ${timer.end()}ms - User wants to disconnect`)
      return "DISCONNECT"
    } else {
      console.log(`🕒 [DISCONNECTION-DETECTION] ${timer.end()}ms - User wants to continue`)
      return "CONTINUE"
    }
  } catch (error) {
    console.log(`❌ [DISCONNECTION-DETECTION] ${timer.end()}ms - Error: ${error.message}`)
    return "CONTINUE" // Default to continue on error
  }
}

// Intelligent WhatsApp request detection using OpenAI
const detectWhatsAppRequest = async (userMessage, conversationHistory, detectedLanguage) => {
  const timer = createTimer("WHATSAPP_REQUEST_DETECTION")
  try {
    const whatsappPrompt = `Analyze if the user is asking for WhatsApp information, link, or contact details. Look for:
- "WhatsApp", "whatsapp", "WA", "wa"
- "send me", "share", "link", "contact", "number"
- "message me", "text me", "connect on WhatsApp"
- "send details", "share information"
- Any request for digital communication or messaging

User message: "${userMessage}"

Return ONLY: "WHATSAPP_REQUEST" if they want WhatsApp info, or "NO_REQUEST" if not.`

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEYS.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: whatsappPrompt },
        ],
        max_tokens: 15,
        temperature: 0.1,
      }),
    })

    if (!response.ok) {
      console.log(`❌ [WHATSAPP-REQUEST-DETECTION] ${timer.end()}ms - Error: ${response.status}`)
      return "NO_REQUEST" // Default to no request on error
    }

    const data = await response.json()
    const result = data.choices[0]?.message?.content?.trim().toUpperCase()

    if (result === "WHATSAPP_REQUEST") {
      console.log(`🕒 [WHATSAPP-REQUEST-DETECTION] ${timer.end()}ms - User wants WhatsApp info`)
      return "WHATSAPP_REQUEST"
    } else {
      console.log(`🕒 [WHATSAPP-REQUEST-DETECTION] ${timer.end()}ms - No WhatsApp request`)
      return "NO_REQUEST"
    }
  } catch (error) {
    console.log(`❌ [WHATSAPP-REQUEST-DETECTION] ${timer.end()}ms - Error: ${error.message}`)
    return "NO_REQUEST" // Default to no request on error
  }
}

// Simplified TTS processor
class SimplifiedSarvamTTSProcessor {
  constructor(language, ws, streamSid, callLogger = null) {
    this.language = language
    this.ws = ws
    this.streamSid = streamSid
    this.callLogger = callLogger
    this.sarvamLanguage = getSarvamLanguage(language)
    this.voice = getValidSarvamVoice(ws.sessionAgentConfig?.voiceSelection || "manisha")
    this.isInterrupted = false
    this.sarvamWs = null
    this.sarvamWsConnected = false
    this.audioQueue = []
    this.isStreamingToSIP = false
    this.totalAudioBytes = 0
    this.currentSarvamRequestId = 0
    this.configAcked = false
    this.firstAudioPending = false
    this.firstAudioSentAt = 0
    // Prewarm connection to reduce first-audio latency
    setImmediate(() => {
      if (!this.isInterrupted) {
        this.connectSarvamWs(0).catch(() => {})
      }
    })
  }

  interrupt() {
    this.isInterrupted = true
    try { if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.OPEN) this.sarvamWs.close() } catch (_) {}
    this.sarvamWsConnected = false
    this.audioQueue = []
    this.isStreamingToSIP = false
  }

  reset(newLanguage) {
    this.interrupt()
    if (newLanguage) {
      this.language = newLanguage
      this.sarvamLanguage = getSarvamLanguage(newLanguage)
    }
    this.isInterrupted = false
    this.totalAudioBytes = 0
    this.currentSarvamRequestId = 0
  }

  async connectSarvamWs(requestId) {
    if (this.sarvamWsConnected && this.sarvamWs?.readyState === WebSocket.OPEN) return true
    if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.CONNECTING) {
      return new Promise((resolve, reject) => {
        const iv = setInterval(() => {
          if (this.sarvamWsConnected) { clearInterval(iv); resolve(true) }
          else if (this.sarvamWs?.readyState === WebSocket.CLOSED) { clearInterval(iv); reject(new Error('Sarvam WS failed')) }
        }, 100)
      })
    }

    const timer = createTimer("SARVAM_WS_CONNECT")
    const sarvamUrl = new URL("wss://api.sarvam.ai/text-to-speech/ws")
    sarvamUrl.searchParams.append("model", "bulbul:v2")
    this.sarvamWs = new WebSocket(sarvamUrl.toString(), [`api-subscription-key.${API_KEYS.sarvam}`])

    return new Promise((resolve, reject) => {
      this.sarvamWs.onopen = () => {
        // Allow prewarm (requestId === 0) even if currentSarvamRequestId differs
        if (this.isInterrupted || (requestId !== 0 && this.currentSarvamRequestId !== requestId)) {
          try { this.sarvamWs.close() } catch (_) {}
          return reject(new Error("Sarvam WS opened for outdated request"))
        }
        this.sarvamWsConnected = true
        console.log(`🎙️ [SARVAM-WS] Connected (lang=${this.sarvamLanguage}, voice=${this.voice})`)
        // Fix misconfiguration: target_language_code must be a BCP-47 code, speaker must be a voice name
        let resolvedVoice = this.voice
        const hindiVoices = new Set(["manisha","meera","vidya","arya","anushka","abhilash","karun","hitesh","arvind","amol"])
        if ((this.sarvamLanguage || "").toLowerCase().startsWith("hi") && !hindiVoices.has((resolvedVoice||"").toLowerCase())) {
          resolvedVoice = "manisha"
          console.log(`ℹ️ [SARVAM-WS] Adjusted voice to '${resolvedVoice}' for language ${this.sarvamLanguage}`)
        }
        const configMessage = {
          type: 'config',
          data: {
            target_language_code: this.sarvamLanguage,
            speaker: resolvedVoice,
            pitch: 0.0,
            pace: 1.0,
            loudness: 1.0,
            output_audio_codec: 'linear16',
            output_audio_bitrate: '128k',
            speech_sample_rate: 8000,
            min_buffer_size: 50,
            max_chunk_length: 150,
            enable_preprocessing: false,
          }
        }
        try {
          console.log("🧩 [SARVAM-WS] Sending config:", JSON.stringify(configMessage))
          this.sarvamWs.send(JSON.stringify(configMessage))
        } catch (_) {}
        resolve(true)
      }

      this.sarvamWs.onmessage = (event) => {
        if (this.isInterrupted || this.currentSarvamRequestId !== requestId) return
        try {
          const msg = JSON.parse(event.data)
          if (msg.type === 'config_ack') {
            this.configAcked = true
            console.log(`🎙️ [SARVAM-WS] Config acknowledged in ${timer.end()}ms`)
          }
          if (msg.type === 'audio' && msg.data?.audio) {
            const audioBuffer = Buffer.from(msg.data.audio, 'base64')
            const audioReceivedTime = Date.now()
            if (this.firstAudioPending && this.firstAudioSentAt) {
              const firstAudioLatency = audioReceivedTime - this.firstAudioSentAt
              logWithTimestamp("⚡ [SARVAM-FIRST-AUDIO]", `${firstAudioLatency}ms from text-send`, audioReceivedTime)
              this.firstAudioPending = false
              this.firstAudioSentAt = 0
            }
            this.audioQueue.push(audioBuffer)
            this.totalAudioBytes += audioBuffer.length
            logWithTimestamp("🎵 [SARVAM-AUDIO]", `Received audio chunk: ${audioBuffer.length} bytes`, audioReceivedTime)
            if (!this.isStreamingToSIP) this.startStreamingToSIP(requestId)
          } else if (msg.type === 'error') {
            logWithTimestamp("❌ [SARVAM-WS]", `Error from TTS: ${msg?.data?.message || 'unknown'}`)
          }
        } catch (_) {}
      }

      this.sarvamWs.onerror = (err) => {
        this.sarvamWsConnected = false
        console.log(`❌ [SARVAM-WS] Socket error: ${err?.message || err}`)
        reject(err)
      }
      this.sarvamWs.onclose = () => {
        this.sarvamWsConnected = false
        console.log('🔌 [SARVAM-WS] Closed')
      }
    })
  }

  async synthesizeAndStream(text) {
    if (this.isInterrupted) return
    if (!text || !text.trim()) { logWithTimestamp('⚠️ [SARVAM-WS]', 'Skipping empty text'); return }
    
    const requestId = ++this.currentSarvamRequestId
    this.audioQueue = []
    this.isStreamingToSIP = false
    const synthesisStart = Date.now()
    const sarvamTracker = new PipelineTimingTracker()
    
    logWithTimestamp("🎙️ [SARVAM-START]", `Starting synthesis: "${text}" (${text.length} chars)`, synthesisStart)
    sarvamTracker.checkpoint('SARVAM_START', { textLength: text.length, language: this.language })
    
    const connected = await this.connectSarvamWs(requestId).catch(() => false)
    if (!connected || this.isInterrupted || this.currentSarvamRequestId !== requestId) {
      sarvamTracker.checkpoint('SARVAM_CONNECTION_FAILED', { requestId, isInterrupted: this.isInterrupted })
      return
    }
    sarvamTracker.checkpoint('SARVAM_CONNECTED', { requestId })

    // Wait for config ack with longer timeout to avoid reconnects
    const configWaitStart = Date.now()
    while (!this.configAcked && Date.now() - configWaitStart < 500) {
      await new Promise(r => setTimeout(r, 20))
    }
    const configWaitTime = Date.now() - configWaitStart
    sarvamTracker.checkpoint('SARVAM_CONFIG_ACK', { waitTime: configWaitTime, acked: this.configAcked })
    
    const textMessage = { type: 'text', data: { text } }
    try { 
      this.firstAudioPending = true
      this.firstAudioSentAt = Date.now()
      logWithTimestamp("📤 [SARVAM-SEND]", `Sending text to Sarvam: "${text}"`, this.firstAudioSentAt)
      this.sarvamWs.send(JSON.stringify(textMessage)) 
      sarvamTracker.checkpoint('SARVAM_TEXT_SENT', { textLength: text.length })
    } catch (error) {
      sarvamTracker.checkpoint('SARVAM_SEND_ERROR', { error: error.message })
    }
    
    try { 
      this.sarvamWs.send(JSON.stringify({ type: 'flush' })) 
      sarvamTracker.checkpoint('SARVAM_FLUSH_SENT')
    } catch (error) {
      sarvamTracker.checkpoint('SARVAM_FLUSH_ERROR', { error: error.message })
    }
    
    logWithTimestamp("📝 [SARVAM-WS]", `Sent text (${text.length} chars) and flush`)

    // Warn if no audio arrives shortly - increased timeout to reduce reconnects
    const audioWarnTimer = setTimeout(async () => {
      if (!this.isStreamingToSIP && this.audioQueue.length === 0 && this.currentSarvamRequestId === requestId && !this.isInterrupted) {
        sarvamTracker.checkpoint('SARVAM_AUDIO_TIMEOUT', { timeout: 1000 })
        console.log('⚠️ [SARVAM-WS] No audio within 1s after text; reconnect+resend')
        try {
          // One-shot reconnect and resend
          try { if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.OPEN) this.sarvamWs.close() } catch (_) {}
          this.sarvamWsConnected = false
          this.configAcked = false
          const reconnected = await this.connectSarvamWs(requestId).catch(() => false)
          if (reconnected && !this.isInterrupted && this.currentSarvamRequestId === requestId) {
            const startWait2 = Date.now()
            while (!this.configAcked && Date.now() - startWait2 < 500) {
              await new Promise(r => setTimeout(r, 20))
            }
            try { 
              this.firstAudioPending = true
              this.firstAudioSentAt = Date.now()
              this.sarvamWs.send(JSON.stringify({ type: 'text', data: { text } })) 
            } catch (_) {}
            try { this.sarvamWs.send(JSON.stringify({ type: 'flush' })) } catch (_) {}
            sarvamTracker.checkpoint('SARVAM_RECONNECT_RESEND', { textLength: text.length })
            console.log('🔁 [SARVAM-WS] Resent text after reconnect')
          }
        } catch (error) {
          sarvamTracker.checkpoint('SARVAM_RECONNECT_ERROR', { error: error.message })
        }
      }
    }, 1000)

    if (this.callLogger && text) {
      this.callLogger.logAIResponse(text, this.language)
    }
    
    // Clear timer later when streaming starts
    const clearTimerInterval = setInterval(() => {
      if (this.isStreamingToSIP || this.isInterrupted || this.currentSarvamRequestId !== requestId) {
        clearTimeout(audioWarnTimer)
        clearInterval(clearTimerInterval)
        // Log final summary when synthesis completes
        sarvamTracker.getSummary()
      }
    }, 100)
  }

  async startStreamingToSIP(requestId) {
    if (this.isStreamingToSIP || this.isInterrupted || this.currentSarvamRequestId !== requestId) return
    this.isStreamingToSIP = true
    const streamStartTime = Date.now()
    logWithTimestamp('🚀 [SARVAM→SIP]', 'Start streaming audio to SIP', streamStartTime)

    const SAMPLE_RATE = 8000
    const BYTES_PER_SAMPLE = 2
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000
    const OPTIMAL_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS)

    let sentChunks = 0
    let sentBytes = 0
    while (!this.isInterrupted && this.currentSarvamRequestId === requestId) {
      if (this.audioQueue.length === 0) { await new Promise(r => setTimeout(r, 50)); continue }
      const audioBuffer = this.audioQueue.shift()
      let position = 0
      while (position < audioBuffer.length && !this.isInterrupted && this.currentSarvamRequestId === requestId) {
        const remaining = audioBuffer.length - position
        const chunkSize = Math.min(OPTIMAL_CHUNK_SIZE, remaining)
        const chunk = audioBuffer.slice(position, position + chunkSize)
        const mediaMessage = { event: 'media', streamSid: this.streamSid, media: { payload: chunk.toString('base64') } }
        if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted) {
          try { this.ws.send(JSON.stringify(mediaMessage)); sentChunks++; sentBytes += chunk.length } catch (e) { this.isInterrupted = true; break }
        } else { this.isInterrupted = true; break }
        if (position + chunkSize < audioBuffer.length && !this.isInterrupted) {
          const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS)
          const delayMs = Math.max(chunkDurationMs - 2, 10)
          await new Promise(res => setTimeout(res, delayMs))
        }
        position += chunkSize
      }
      if (sentChunks % 50 === 0 && sentChunks > 0) {
        console.log(`🎧 [SARVAM→SIP] Sent ${sentChunks} chunks, ${(sentBytes/1024).toFixed(1)} KB`) 
      }
    }
    this.isStreamingToSIP = false
    console.log('🛑 [SARVAM→SIP] Streaming stopped')
  }

  getStats() {
    return { totalAudioBytes: this.totalAudioBytes }
  }
}

// Deepgram TTS processor (streaming WS → SIP)
class SimplifiedDeepgramTTSProcessor {
  constructor(language, ws, streamSid, callLogger = null) {
    this.language = language
    this.ws = ws
    this.streamSid = streamSid
    this.callLogger = callLogger
    this.dgWs = null
    this.dgWsConnected = false
    this.isInterrupted = false
    this.audioQueue = []
    this.isStreamingToSIP = false
    this.currentRequestId = 0
  }

  interrupt() {
    this.isInterrupted = true
    try { if (this.dgWs && this.dgWs.readyState === WebSocket.OPEN) this.dgWs.close() } catch (_) {}
    this.dgWsConnected = false
    this.audioQueue = []
    this.isStreamingToSIP = false
  }

  reset(newLanguage) {
    this.interrupt()
    if (newLanguage) this.language = newLanguage
    this.isInterrupted = false
  }

  getDeepgramModelForLanguage(lang) {
    const l = (lang || 'en').toLowerCase()
    if (l.startsWith('hi')) return 'aura-hera-hi'
    if (l.startsWith('en')) return 'aura-asteria-en'
    return 'aura-asteria-en'
  }

  async connectDgWs(requestId) {
    if (this.dgWsConnected && this.dgWs?.readyState === WebSocket.OPEN) return true
    const model = this.getDeepgramModelForLanguage(this.language)
    const url = new URL('wss://api.deepgram.com/v1/speak')
    url.searchParams.append('model', model)
    url.searchParams.append('encoding', 'linear16')
    url.searchParams.append('sample_rate', '8000')

    return new Promise((resolve, reject) => {
      this.dgWs = new WebSocket(url.toString(), {
        headers: { Authorization: `Token ${API_KEYS.deepgram}` },
      })

      this.dgWs.onopen = () => {
        if (this.isInterrupted || this.currentRequestId !== requestId) {
          try { this.dgWs.close() } catch (_) {}
          return reject(new Error('DG WS opened for outdated request'))
        }
        this.dgWsConnected = true
        console.log(`🎙️ [DG-WS] Connected (model=${model}, lang=${this.language})`)
        resolve(true)
      }

      this.dgWs.onmessage = (event) => {
        if (this.isInterrupted || this.currentRequestId !== requestId) return
        try {
          if (Buffer.isBuffer(event.data)) {
            const audioBuffer = event.data
            this.audioQueue.push(audioBuffer)
            if (!this.isStreamingToSIP) this.startStreamingToSIP(requestId)
          } else if (typeof event.data === 'string') {
            try {
              const msg = JSON.parse(event.data)
              if (msg?.type === 'error') {
                logWithTimestamp('❌ [DG-WS]', `Error from TTS: ${msg?.message || 'unknown'}`)
              }
            } catch (_) {}
          }
        } catch (_) {}
      }

      this.dgWs.onerror = (err) => {
        this.dgWsConnected = false
        console.log(`❌ [DG-WS] Socket error: ${err?.message || err}`)
        reject(err)
      }

      this.dgWs.onclose = () => {
        this.dgWsConnected = false
        console.log('🔌 [DG-WS] Closed')
      }
    })
  }

  async synthesizeAndStream(text) {
    if (this.isInterrupted) return
    if (!text || !text.trim()) { logWithTimestamp('⚠️ [DG-WS]', 'Skipping empty text'); return }
    const requestId = ++this.currentRequestId
    this.audioQueue = []
    this.isStreamingToSIP = false

    const connected = await this.connectDgWs(requestId).catch(() => false)
    if (!connected || this.isInterrupted || this.currentRequestId !== requestId) return

    const speakMsg = { type: 'Speak', text }
    try { this.dgWs.send(JSON.stringify(speakMsg)) } catch (_) {}
    try { this.dgWs.send(JSON.stringify({ type: 'Flush' })) } catch (_) {}

    if (this.callLogger && text) {
      this.callLogger.logAIResponse(text, this.language)
    }
  }

  async startStreamingToSIP(requestId) {
    if (this.isStreamingToSIP || this.isInterrupted || this.currentRequestId !== requestId) return
    this.isStreamingToSIP = true

    const SAMPLE_RATE = 8000
    const BYTES_PER_SAMPLE = 2
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000
    const OPTIMAL_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS)

    let sentChunks = 0
    let sentBytes = 0
    while (!this.isInterrupted && this.currentRequestId === requestId) {
      if (this.audioQueue.length === 0) { await new Promise(r => setTimeout(r, 30)); continue }
      const audioBuffer = this.audioQueue.shift()
      let position = 0
      while (position < audioBuffer.length && !this.isInterrupted && this.currentRequestId === requestId) {
        const remaining = audioBuffer.length - position
        const chunkSize = Math.min(OPTIMAL_CHUNK_SIZE, remaining)
        const chunk = audioBuffer.slice(position, position + chunkSize)
        const mediaMessage = { event: 'media', streamSid: this.streamSid, media: { payload: chunk.toString('base64') } }
        if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted) {
          try { this.ws.send(JSON.stringify(mediaMessage)); sentChunks++; sentBytes += chunk.length } catch (e) { this.isInterrupted = true; break }
        } else { this.isInterrupted = true; break }
        if (position + chunkSize < audioBuffer.length && !this.isInterrupted) {
          const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS)
          const delayMs = Math.max(chunkDurationMs - 2, 10)
          await new Promise(res => setTimeout(res, delayMs))
        }
        position += chunkSize
      }
      if (sentChunks % 50 === 0 && sentChunks > 0) {
        console.log(`🎧 [DG→SIP] Sent ${sentChunks} chunks, ${(sentBytes/1024).toFixed(1)} KB`)
      }
    }
    this.isStreamingToSIP = false
    console.log('🛑 [DG→SIP] Streaming stopped')
  }
}

// Enhanced agent lookup function with isActive check
const findAgentForCall = async (callData) => {
  const timer = createTimer("MONGODB_AGENT_LOOKUP")
  try {
    const { accountSid, callDirection, extraData } = callData

    let agent = null

    if (callDirection === "inbound") {
      if (!accountSid) {
        throw new Error("Missing accountSid for inbound call")
      }

      // Only find active agents for inbound calls
      agent = await Agent.findOne({ 
        accountSid, 
        isActive: true 
      }).lean()
      
      if (!agent) {
        throw new Error(`No active agent found for accountSid: ${accountSid}`)
      }
    } else if (callDirection === "outbound") {
      if (!extraData) {
        throw new Error("Missing extraData for outbound call")
      }

      if (!extraData.CallVaId) {
        throw new Error("Missing CallVaId in extraData for outbound call")
      }

      const callVaId = extraData.CallVaId
      
      // Only find active agents for outbound calls
      agent = await Agent.findOne({ 
        callerId: callVaId, 
        isActive: true 
      }).lean()
      
      if (!agent) {
        throw new Error(`No active agent found for callerId: ${callVaId}`)
      }
    } else {
      throw new Error(`Unknown call direction: ${callDirection}`)
    }

    console.log(`🕒 [MONGODB-AGENT-LOOKUP] ${timer.end()}ms - Active agent found: ${agent.agentName}`)
    console.log(`✅ [MONGODB-AGENT-LOOKUP] Agent Status: Active (${agent.isActive})`)
    return agent
  } catch (error) {
    console.log(`❌ [MONGODB-AGENT-LOOKUP] ${timer.end()}ms - Error: ${error.message}`)
    throw error
  }
}

// Utility function to handle external call disconnection
const handleExternalCallDisconnection = async (streamSid, reason = 'external_disconnection') => {
  try {
    const activeCall = await CallLog.findActiveCallByStreamSid(streamSid)
    if (activeCall) {
      console.log(`🛑 [EXTERNAL-DISCONNECT] Disconnecting call ${streamSid}: ${reason}`)
      
      // Update call log to mark as inactive
      await CallLog.findByIdAndUpdate(activeCall._id, {
        'metadata.isActive': false,
        'metadata.callEndTime': new Date(),
        'metadata.lastUpdated': new Date()
      })
      
      console.log(`✅ [EXTERNAL-DISCONNECT] Call ${streamSid} marked as disconnected`)
      return true
    } else {
      console.log(`⚠️ [EXTERNAL-DISCONNECT] No active call found for streamSid: ${streamSid}`)
      return false
    }
  } catch (error) {
    console.log(`❌ [EXTERNAL-DISCONNECT] Error handling external disconnection: ${error.message}`)
    return false
  }
}

// Main WebSocket server setup with enhanced live transcript functionality
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
    let currentTTS = null
    let currentLanguage = undefined
    let processingRequestId = 0
    let callLogger = null
    let callDirection = "inbound"
    let agentConfig = null
    let userName = null

    // Deepgram WebSocket connection
    let deepgramWs = null
    let deepgramReady = false
    let deepgramAudioQueue = []
    let sttTimer = null
    let lastInterimProcessAt = 0
    let speechStartTime = null
    let firstInterimTime = null
    let firstFinalTime = null

    const connectToDeepgram = async () => {
      try {
        const deepgramLanguage = getDeepgramLanguage(currentLanguage)
        const dgConnectStart = Date.now()

        const deepgramUrl = new URL("wss://api.deepgram.com/v1/listen")
        deepgramUrl.searchParams.append("sample_rate", "8000")
        deepgramUrl.searchParams.append("channels", "1")
        deepgramUrl.searchParams.append("encoding", "linear16")
        deepgramUrl.searchParams.append("model", "nova-2")
        deepgramUrl.searchParams.append("language", deepgramLanguage)
        deepgramUrl.searchParams.append("interim_results", "true")
        deepgramUrl.searchParams.append("smart_format", "true")
        deepgramUrl.searchParams.append("endpointing", "25")
        deepgramUrl.searchParams.append("vad_events", "true")
        deepgramUrl.searchParams.append("utterance_end_ms", "1000")

        deepgramWs = new WebSocket(deepgramUrl.toString(), {
          headers: { Authorization: `Token ${API_KEYS.deepgram}` },
        })

        deepgramWs.onopen = () => {
          const connectTime = Date.now()
          const connectLatency = connectTime - dgConnectStart
          logWithTimestamp("🎤 [DEEPGRAM]", "Connection established", connectTime)
          logWithTimestamp("🕒 [DEEPGRAM-CONNECT]", `${connectLatency}ms`, connectTime)
          deepgramReady = true
          logWithTimestamp("🎤 [DEEPGRAM]", `Processing queued audio packets: ${deepgramAudioQueue.length}`, connectTime)
          deepgramAudioQueue.forEach((buffer) => deepgramWs.send(buffer))
          deepgramAudioQueue = []
        }

        deepgramWs.onmessage = async (event) => {
          const data = JSON.parse(event.data)
          await handleDeepgramResponse(data)
        }

        deepgramWs.onerror = (error) => {
          console.log("❌ [DEEPGRAM] Connection error:", error.message)
          deepgramReady = false
        }

        deepgramWs.onclose = () => {
          console.log("🔌 [DEEPGRAM] Connection closed")
          deepgramReady = false
          // Attempt a quick reconnect if call still active
          try {
            if (ws && ws.readyState === WebSocket.OPEN) {
              console.log("🔄 [DEEPGRAM] Attempting quick reconnect in 500ms...")
              setTimeout(() => {
                if (!deepgramReady) {
                  connectToDeepgram().catch(() => {})
                }
              }, 500)
            }
          } catch (_) {}
        }
      } catch (error) {
        // Silent error handling
      }
    }

    const handleDeepgramResponse = async (data) => {
      // Handle VAD (Voice Activity Detection) events for faster processing
      if (data.type === "SpeechStarted") {
        speechStartTime = Date.now()
        logWithTimestamp("🎤 [VAD]", "Speech started - user is speaking", speechStartTime)
      } else if (data.type === "SpeechEnded") {
        const speechEndTime = Date.now()
        const speechDuration = speechStartTime ? speechEndTime - speechStartTime : 'unknown'
        logWithTimestamp("🎤 [VAD]", `Speech ended - processing any pending audio (duration: ${speechDuration}ms)`, speechEndTime)
        
        // Force process any pending utterance buffer immediately
        if (userUtteranceBuffer.trim()) {
          logWithTimestamp("⚡ [VAD-TRIGGERED]", `Processing pending utterance: "${userUtteranceBuffer.trim()}"`, speechEndTime)
          try { 
            await processUserUtterance(userUtteranceBuffer.trim()) 
          } catch (_) {}
          userUtteranceBuffer = ""
        }
      } else if (data.type === "Results") {
        if (!sttTimer) {
          sttTimer = createTimer("STT_TRANSCRIPTION")
          logWithTimestamp("🎤 [DEEPGRAM-RESULT]", "Started STT transcription timer")
        }

        const transcript = data.channel?.alternatives?.[0]?.transcript
        const is_final = data.is_final
        const confidence = data.channel?.alternatives?.[0]?.confidence || 0

        if (transcript?.trim()) {
          // Opportunistic early processing on strong interim hypotheses
          if (!is_final) {
            const text = transcript.trim()
            const endsWithPunct = /[\.\!\?\u0964]$/.test(text)
            const longPhrase = text.length >= 10
            const nowTs = Date.now()
            
            logWithTimestamp("🔄 [DEEPGRAM-INTERIM]", `Received interim: "${text}" (${text.length} chars, confidence: ${(confidence * 100).toFixed(1)}%)`)
            
            // More aggressive early processing for faster response
            const shouldProcessEarly = (
              endsWithPunct || 
              longPhrase || 
              (text.length >= 3 && confidence > 0.8) || // High confidence short phrases
              (text.length >= 5 && confidence > 0.6) || // Medium confidence medium phrases
              nowTs - lastInterimProcessAt > 300 // Reduced from 500ms
            ) && nowTs - lastInterimProcessAt > 100 // Reduced from 250ms
            
            if (shouldProcessEarly) {
              lastInterimProcessAt = nowTs
              if (!firstInterimTime) firstInterimTime = nowTs
              const timeFromSpeechStart = speechStartTime ? nowTs - speechStartTime : 'unknown'
              logWithTimestamp("⚡ [EARLY-PROCESSING]", `Processing interim: "${text}" (confidence: ${(confidence * 100).toFixed(1)}%, ${timeFromSpeechStart}ms from speech start)`)
              try { await processUserUtterance(text) } catch (_) {}
            }
          }

          if (is_final) {
            const finalTime = Date.now()
            const sttDuration = sttTimer.end()
            if (!firstFinalTime) firstFinalTime = finalTime
            const timeFromSpeechStart = speechStartTime ? finalTime - speechStartTime : 'unknown'
            const timeFromFirstInterim = firstInterimTime ? finalTime - firstInterimTime : 'unknown'
            logWithTimestamp("✅ [DEEPGRAM-FINAL]", `Final transcript: "${transcript.trim()}" (${sttDuration}ms, confidence: ${(confidence * 100).toFixed(1)}%)`)
            logWithTimestamp("📊 [PERFORMANCE]", `Final result: ${timeFromSpeechStart}ms from speech start, ${timeFromFirstInterim}ms from first interim`)
            sttTimer = null

            userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim()

            if (callLogger && transcript.trim()) {
              const fixedLang = currentLanguage || "en"
              callLogger.logUserTranscript(transcript.trim(), fixedLang)
            }

            await processUserUtterance(userUtteranceBuffer)
            userUtteranceBuffer = ""
          }
        }
      } else if (data.type === "UtteranceEnd") {
        if (sttTimer) {
          const sttDuration = sttTimer.end()
          logWithTimestamp("🕒 [STT-TRANSCRIPTION]", `${sttDuration}ms - Text: "${userUtteranceBuffer.trim()}"`)
          sttTimer = null
        }

        if (userUtteranceBuffer.trim()) {
          if (callLogger && userUtteranceBuffer.trim()) {
            const fixedLang = currentLanguage || "en"
            callLogger.logUserTranscript(userUtteranceBuffer.trim(), fixedLang)
          }

          await processUserUtterance(userUtteranceBuffer)
          userUtteranceBuffer = ""
        }
      }
    }

    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText) return

      const processStartTime = Date.now()
      const pipelineTracker = new PipelineTimingTracker()
      
      logWithTimestamp("🗣️ [USER-UTTERANCE]", "========== USER SPEECH ==========", processStartTime)
      logWithTimestamp("🗣️ [USER-UTTERANCE]", `Text: "${text.trim()}" (${text.length} chars)`, processStartTime)
      logWithTimestamp("🗣️ [USER-UTTERANCE]", `Current Language: ${currentLanguage}`, processStartTime)

      // Do not interrupt TTS; enforce ordered playback via queue
      isProcessing = true
      lastProcessedText = text
      const currentRequestId = ++processingRequestId

      try {
        // Language detection disabled; stick to agent-configured language
        const detectedLanguage = currentLanguage || "en"
        pipelineTracker.checkpoint('LANGUAGE_DETECTION', { language: detectedLanguage, method: 'fixed' })
        
        // Run all AI detections in parallel for efficiency
        pipelineTracker.checkpoint('AI_DETECTIONS_START', { textLength: text.length })
        
        // Disable high-latency detections; only stream OpenAI response
        const llmStartTime = Date.now()
        pipelineTracker.checkpoint('LLM_START', { model: 'gpt-4o-mini', streaming: true })
        
        const aiResponse = await processWithOpenAIStreaming(
          text,
          conversationHistory,
          detectedLanguage,
          callLogger,
          agentConfig,
          ws,
          streamSid,
          userName,
          {
            onFirstEnqueue: (enqueueTs) => {
              const latency = enqueueTs - llmStartTime
              pipelineTracker.checkpoint('FIRST_TTS_ENQUEUE', { 
                latency, 
                textLength: text.length,
                timeFromUtterance: latency 
              })
              console.log(`[STREAM] First token received at ${enqueueTs}`)
            },
            onSentenceReady: async (sentence) => {
              // Immediately process this sentence for TTS
              console.log(`[STREAM] Processing sentence immediately: "${sentence}"`)
              
              // The sentence is already being processed in the streaming function
              // This callback allows for additional custom processing if needed
              try {
                // Add any custom processing here if needed
                // For example, you could add custom logging, analytics, etc.
                console.log(`[STREAM] Custom processing for sentence: "${sentence}"`)
              } catch (error) {
                console.log(`[STREAM] Error in custom sentence processing: ${error.message}`)
              }
            }
          }
        )

        // Lead/disconnect/WhatsApp intent detection disabled to reduce latency
        const llmDuration = Date.now() - llmStartTime
        pipelineTracker.stageComplete('LLM_PROCESSING', llmStartTime, { 
          responseLength: aiResponse?.length || 0,
          streaming: true,
          earlyReturn: true // Mark that we returned early for minimal latency
        })

        if (processingRequestId === currentRequestId) {
          pipelineTracker.checkpoint('PROCESSING_COMPLETE', { 
            totalDuration: Date.now() - processStartTime,
            requestId: currentRequestId 
          })
        } else {
          pipelineTracker.checkpoint('PROCESSING_SKIPPED', { 
            reason: 'newer_request_in_progress',
            currentRequestId,
            originalRequestId: currentRequestId 
          })
        }
      } catch (error) {
        pipelineTracker.checkpoint('PROCESSING_ERROR', { 
          error: error.message,
          duration: Date.now() - processStartTime 
        })
      } finally {
        if (processingRequestId === currentRequestId) {
          isProcessing = false
        }
        
        // Log final pipeline summary
        const summary = pipelineTracker.getSummary()
        logWithTimestamp("🗣️ [USER-UTTERANCE]", "======================================")
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
            console.log("🔗 [SIP-CONNECTION] WebSocket connected")
            break

          case "start": {
            const callStartTime = Date.now()
            streamSid = data.streamSid || data.start?.streamSid
            const accountSid = data.start?.accountSid

            // Log all incoming SIP data
            logWithTimestamp("📞 [SIP-START]", "========== CALL START DATA ==========", callStartTime)
            logWithTimestamp("📞 [SIP-START]", `StreamSID: ${streamSid}`, callStartTime)
            logWithTimestamp("📞 [SIP-START]", `AccountSID: ${accountSid}`, callStartTime)

            let mobile = null;
            let callerId = null;
            let customParams = {};
            let czdataDecoded = null;
            let uniqueid = null;
            if (urlParams.czdata) {
              czdataDecoded = decodeCzdata(urlParams.czdata);
                              if (czdataDecoded) {
                  customParams = czdataDecoded;
                  userName = (
                    czdataDecoded.name ||
                    czdataDecoded.Name ||
                    czdataDecoded.full_name ||
                    czdataDecoded.fullName ||
                    czdataDecoded.customer_name ||
                    czdataDecoded.customerName ||
                    czdataDecoded.CustomerName ||
                    czdataDecoded.candidate_name ||
                    czdataDecoded.contactName ||
                    null
                  );
                  uniqueid = czdataDecoded.uniqueid || czdataDecoded.uniqueId || null;
                  console.log("[SIP-START] Decoded czdata customParams:", customParams);
                  if (userName) {
                    console.log("[SIP-START] User Name (czdata):", userName);
                  }
                  if (uniqueid) {
                    console.log("[SIP-START] Unique ID (czdata):", uniqueid);
                  }
                }
            }

            if (data.start?.from) {
              mobile = data.start.from;
            } else if (urlParams.caller_id) {
              mobile = urlParams.caller_id;
            } else if (data.start?.extraData?.CallCli) {
              mobile = data.start.extraData.CallCli;
            }

            let to = null
            if (data.start?.to) {
              to = data.start.to
            } else if (urlParams.did) {
              to = urlParams.did
            } else if (data.start?.extraData?.DID) {
              to = data.start.extraData.DID
            }

            let extraData = null;

            if (data.start?.extraData) {
              extraData = decodeExtraData(data.start.extraData);
            } else if (urlParams.extra) {
              extraData = decodeExtraData(urlParams.extra);
            }

            if (extraData?.CallCli) {
              mobile = extraData.CallCli;
            }
            if (extraData?.CallVaId) {
              callerId = extraData.CallVaId;
            }
            if (!userName && extraData) {
              userName = (
                extraData.name ||
                extraData.Name ||
                extraData.full_name ||
                extraData.fullName ||
                extraData.customer_name ||
                extraData.customerName ||
                extraData.CustomerName ||
                extraData.candidate_name ||
                extraData.candidateName ||
                null
              );
              if (userName) {
                console.log("[SIP-START] User Name (extraData):", userName);
              }
            }
            
            // Capture uniqueid from extraData if not already captured
            if (!uniqueid && extraData) {
              uniqueid = extraData.uniqueid || extraData.uniqueId || null;
              if (uniqueid) {
                console.log("[SIP-START] Unique ID (extraData):", uniqueid);
              }
            }

            if (!userName && urlParams.name) {
              userName = urlParams.name;
              console.log("[SIP-START] User Name (url param):", userName);
            }

            if (extraData && extraData.CallDirection === "OutDial") {
              callDirection = "outbound";
            } else if (urlParams.direction === "OutDial") {
              callDirection = "outbound";
              if (!extraData && urlParams.extra) {
                extraData = decodeExtraData(urlParams.extra);
              }
            } else {
              callDirection = "inbound";
            }

            // Log parsed call information
            console.log("📞 [SIP-START] ========== PARSED CALL INFO ==========")
            console.log("📞 [SIP-START] Call Direction:", callDirection)
            console.log("📞 [SIP-START] From/Mobile:", mobile)
            console.log("📞 [SIP-START] To/DID:", to)
            console.log("📞 [SIP-START] Unique ID:", uniqueid)
            console.log("📞 [SIP-START] Extra Data:", JSON.stringify(extraData, null, 2))
            console.log("📞 [SIP-START] ======================================")

            // Note: WhatsApp message will be sent at call end if enabled in agent

            try {
              console.log("🔍 [SIP-AGENT-LOOKUP] ========== AGENT LOOKUP ==========")
              console.log("🔍 [SIP-AGENT-LOOKUP] AccountSID:", accountSid)
              console.log("🔍 [SIP-AGENT-LOOKUP] Call Direction:", callDirection)
              console.log("🔍 [SIP-AGENT-LOOKUP] Extra Data:", JSON.stringify(extraData, null, 2))
              
              agentConfig = await findAgentForCall({
                accountSid,
                callDirection,
                extraData,
              })

              console.log("✅ [SIP-AGENT-LOOKUP] Agent found successfully")
              console.log("✅ [SIP-AGENT-LOOKUP] Agent Name:", agentConfig.agentName)
              console.log("✅ [SIP-AGENT-LOOKUP] Client ID:", agentConfig.clientId)
              console.log("✅ [SIP-AGENT-LOOKUP] Language:", agentConfig.language)
              console.log("✅ [SIP-AGENT-LOOKUP] Voice Selection:", agentConfig.voiceSelection)
              console.log("✅ [SIP-AGENT-LOOKUP] First Message:", agentConfig.firstMessage)
              console.log("✅ [SIP-AGENT-LOOKUP] WhatsApp Enabled:", agentConfig.whatsappEnabled)
              console.log("✅ [SIP-AGENT-LOOKUP] WhatsApp API URL:", agentConfig.whatsapplink)
              console.log("✅ [SIP-AGENT-LOOKUP] ======================================")

              if (!agentConfig) {
                console.log("❌ [SIP-AGENT-LOOKUP] No agent found for call")
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
              console.log("❌ [SIP-AGENT-LOOKUP] Error finding agent:", err.message)
              ws.send(
                JSON.stringify({
                  event: "error",
                  message: err.message,
                }),
              )
              ws.close()
              return
            }

            // Block call if the client has no credits
            try {
              const creditRecord = await Credit.getOrCreateCreditRecord(agentConfig.clientId)
              const currentBalance = Number(creditRecord?.currentBalance || 0)
              if (currentBalance <= 0) {
                console.log("🛑 [SIP-CREDIT-CHECK] Insufficient credits. Blocking call connection.")
                ws.send(
                  JSON.stringify({
                    event: "error",
                    code: "insufficient_credits",
                    message: "Call blocked: insufficient credits. Please recharge to place or receive calls.",
                  }),
                )
                try { ws.close() } catch (_) {}
                return
              }
            } catch (creditErr) {
              console.log("⚠️ [SIP-CREDIT-CHECK] Credit check failed:", creditErr.message)
              // Fail safe: if we cannot verify credits, prevent connection to avoid free calls
              ws.send(
                JSON.stringify({
                  event: "error",
                  code: "credit_check_failed",
                  message: "Unable to verify credits. Call cannot be connected at this time.",
                }),
              )
              try { ws.close() } catch (_) {}
              return
            }

            ws.sessionAgentConfig = agentConfig
            currentLanguage = agentConfig.language || "en"

            console.log("🎯 [SIP-CALL-SETUP] ========== CALL SETUP ==========")
            console.log("🎯 [SIP-CALL-SETUP] Current Language:", currentLanguage)
            console.log("🎯 [SIP-CALL-SETUP] Mobile Number:", mobile)
            console.log("🎯 [SIP-CALL-SETUP] Call Direction:", callDirection)
            console.log("🎯 [SIP-CALL-SETUP] Client ID:", agentConfig.clientId || accountSid)
            console.log("🎯 [SIP-CALL-SETUP] StreamSID:", streamSid)
            console.log("🎯 [SIP-CALL-SETUP] CallSID:", data.start?.callSid || data.start?.CallSid || data.callSid || data.CallSid)

            // Create enhanced call logger with live transcript capability
            callLogger = new EnhancedCallLogger(
              agentConfig.clientId || accountSid,
              mobile,
              callDirection
            );
            callLogger.customParams = customParams;
            callLogger.callerId = callerId || undefined;
            callLogger.streamSid = streamSid;
            callLogger.callSid = data.start?.callSid || data.start?.CallSid || data.callSid || data.CallSid;
            callLogger.accountSid = accountSid;
            callLogger.ws = ws; // Store WebSocket reference
            callLogger.uniqueid = uniqueid; // Store uniqueid for outbound calls

            // Create initial call log entry immediately
            try {
              await callLogger.createInitialCallLog(agentConfig._id, 'not_connected');
              console.log("✅ [SIP-CALL-SETUP] Initial call log created successfully")
              console.log("✅ [SIP-CALL-SETUP] Call Log ID:", callLogger.callLogId)
            } catch (error) {
              console.log("❌ [SIP-CALL-SETUP] Failed to create initial call log:", error.message)
              // Continue anyway - fallback will create log at end
            }

            console.log("🎯 [SIP-CALL-SETUP] Call Logger initialized")
            console.log("🎯 [SIP-CALL-SETUP] Connecting to Deepgram...")

            await connectToDeepgram()

            let greeting = agentConfig.firstMessage || "Hello! How can I help you today?"
            if (userName && userName.trim()) {
              const base = agentConfig.firstMessage || "How can I help you today?"
              greeting = `Hello ${userName.trim()}! ${base}`
            }

            console.log("🎯 [SIP-CALL-SETUP] Greeting Message:", greeting)
            console.log("🎯 [SIP-CALL-SETUP] ======================================")

            if (callLogger) {
              callLogger.logAIResponse(greeting, currentLanguage)
            }

            console.log("🎤 [SIP-TTS] Starting greeting TTS...")
            // Initialize persistent Deepgram WS-based TTS processor and FIFO queue
            const tts = new SimplifiedDeepgramTTSProcessor(currentLanguage, ws, streamSid, callLogger)
            ws.__sarvamTts = tts
            if (!ws.__ttsQueue) ws.__ttsQueue = []
            ws.__ttsRunning = ws.__ttsRunning || false
            const enqueueSpeak = async (text, language) => {
              if (!text || !text.trim()) return
              const enqueueTime = Date.now()
              const ttsTracker = new PipelineTimingTracker()
              
              logWithTimestamp("📝 [TTS-ENQUEUE]", `Enqueuing: "${text.trim()}" (${text.trim().length} chars)`, enqueueTime)
              ttsTracker.checkpoint('TTS_ENQUEUE', { textLength: text.trim().length, language })
              
              ws.__ttsQueue.push({ text: text.trim(), language })
              if (ws.__ttsRunning) {
                ttsTracker.checkpoint('TTS_QUEUE_BUSY', { queueLength: ws.__ttsQueue.length })
                return
              }
              
              ws.__ttsRunning = true
              const drainStart = Date.now()
              ttsTracker.checkpoint('TTS_QUEUE_START', { queueLength: ws.__ttsQueue.length })
              
              let totalSynthesisTime = 0
              let totalChars = 0
              let utteranceCount = 0
              
              while (ws.__ttsQueue.length > 0 && ws.readyState === WebSocket.OPEN) {
                const next = ws.__ttsQueue.shift()
                utteranceCount++
                
                if (!ws.__sarvamTts) ws.__sarvamTts = new SimplifiedSarvamTTSProcessor(language || currentLanguage || "en", ws, streamSid, callLogger)
                if (ws.__sarvamTts.language !== (language || currentLanguage || "en")) {
                  ws.__sarvamTts.reset(language || currentLanguage || "en")
                }
                
                const t0 = Date.now()
                ttsTracker.checkpoint(`TTS_SYNTHESIS_${utteranceCount}`, { 
                  text: next.text, 
                  length: next.text.length,
                  language: next.language 
                })
                
                try { 
                  await ws.__sarvamTts.synthesizeAndStream(next.text) 
                } catch (error) { 
                  ttsTracker.checkpoint(`TTS_SYNTHESIS_ERROR_${utteranceCount}`, { error: error.message })
                  continue 
                }
                
                const synthesisTime = Date.now() - t0
                totalSynthesisTime += synthesisTime
                totalChars += next.text.length
                
                ttsTracker.stageComplete(`TTS_UTTERANCE_${utteranceCount}`, t0, { 
                  duration: synthesisTime, 
                  chars: next.text.length,
                  charsPerMs: (next.text.length / synthesisTime).toFixed(2)
                })
              }
              
              const totalDrainTime = Date.now() - drainStart
              ttsTracker.stageComplete('TTS_QUEUE_DRAIN', drainStart, {
                totalDuration: totalDrainTime,
                utteranceCount,
                totalSynthesisTime,
                totalChars,
                avgSynthesisTime: utteranceCount > 0 ? (totalSynthesisTime / utteranceCount).toFixed(1) : 0,
                avgCharsPerMs: totalSynthesisTime > 0 ? (totalChars / totalSynthesisTime).toFixed(2) : 0
              })
              
              ws.__ttsRunning = false
            }
            ws.__enqueueSpeak = enqueueSpeak
            await enqueueSpeak(greeting, currentLanguage)
            console.log("✅ [SIP-TTS] Greeting TTS enqueued")
            break
          }

          case "media":
            if (data.media?.payload) {
              const audioBuffer = Buffer.from(data.media.payload, "base64")
              const mediaReceivedTime = Date.now()
              
              // Log media stats periodically (every 1000 packets to avoid spam)
              if (!ws.mediaPacketCount) ws.mediaPacketCount = 0
              ws.mediaPacketCount++
              
              if (ws.mediaPacketCount % 1000 === 0) {
                logWithTimestamp("🎵 [SIP-MEDIA]", `Audio packets received: ${ws.mediaPacketCount}`, mediaReceivedTime)
              }

              if (deepgramWs && deepgramReady && deepgramWs.readyState === WebSocket.OPEN) {
                deepgramWs.send(audioBuffer)
                if (ws.mediaPacketCount % 1000 === 0) {
                  logWithTimestamp("📤 [DEEPGRAM-SEND]", `Sent audio to Deepgram: ${audioBuffer.length} bytes`, mediaReceivedTime)
                }
              } else {
                deepgramAudioQueue.push(audioBuffer)
                // Cap queue to avoid memory/latency buildup
                if (deepgramAudioQueue.length > 2000) {
                  deepgramAudioQueue.splice(0, deepgramAudioQueue.length - 2000)
                }
                if (deepgramAudioQueue.length % 200 === 0) {
                  logWithTimestamp("⏳ [SIP-MEDIA]", `Audio queued for Deepgram: ${deepgramAudioQueue.length}`, mediaReceivedTime)
                }
              }
            }
            break

          case "stop":
            const callStopTime = Date.now()
            logWithTimestamp("🛑 [SIP-STOP]", "========== CALL END ==========", callStopTime)
            logWithTimestamp("🛑 [SIP-STOP]", `StreamSID: ${streamSid}`, callStopTime)
            logWithTimestamp("🛑 [SIP-STOP]", `Call Direction: ${callDirection}`, callStopTime)
            logWithTimestamp("🛑 [SIP-STOP]", `Mobile: ${mobile}`, callStopTime)

            // Intelligent WhatsApp send based on lead status and user requests
            try {
              if (callLogger && agentConfig?.whatsappEnabled && callLogger.shouldSendWhatsApp()) {
                const waLink = getAgentWhatsappLink(agentConfig)
                const waNumber = normalizeIndianMobile(mobile)
                const waApiUrl = agentConfig?.whatsapplink
                console.log("📨 [WHATSAPP] stop-event check → enabled=", agentConfig.whatsappEnabled, ", link=", waLink, ", apiUrl=", waApiUrl, ", normalized=", waNumber, ", leadStatus=", callLogger.currentLeadStatus, ", requested=", callLogger.whatsappRequested)
                if (waLink && waNumber && waApiUrl) {
                  sendWhatsAppTemplateMessage(waNumber, waLink, waApiUrl)
                    .then(async (r) => {
                      console.log("📨 [WHATSAPP] stop-event result:", r?.ok ? "OK" : "FAIL", r?.status || r?.reason || r?.error || "")
                      if (r?.ok) {
                        await billWhatsAppCredit({
                          clientId: agentConfig.clientId || accountSid,
                          mobile,
                          link: waLink,
                          callLogId: callLogger?.callLogId,
                          streamSid,
                        })
                        callLogger.markWhatsAppSent()
                      }
                    })
                    .catch((e) => console.log("❌ [WHATSAPP] stop-event error:", e.message))
                } else {
                  console.log("📨 [WHATSAPP] stop-event skipped → missing:", !waLink ? "link" : "", !waNumber ? "number" : "", !waApiUrl ? "apiUrl" : "")
                }
              } else {
                console.log("📨 [WHATSAPP] stop-event skipped → conditions not met:", {
                  hasCallLogger: !!callLogger,
                  whatsappEnabled: agentConfig?.whatsappEnabled,
                  shouldSend: callLogger?.shouldSendWhatsApp(),
                  leadStatus: callLogger?.currentLeadStatus,
                  alreadySent: callLogger?.whatsappSent,
                  requested: callLogger?.whatsappRequested
                })
              }
            } catch (waErr) {
              console.log("❌ [WHATSAPP] stop-event unexpected:", waErr.message)
            }
            
            // Handle external call disconnection
            if (streamSid) {
              await handleExternalCallDisconnection(streamSid, 'sip_stop_event')
            }
            
            if (callLogger) {
              const stats = callLogger.getStats()
              console.log("🛑 [SIP-STOP] Call Stats:", JSON.stringify(stats, null, 2))
              // Bill credits at end of call (decimal precision)
              const durationSeconds = Math.round((new Date() - callLogger.callStartTime) / 1000)
              await billCallCredits({
                clientId: callLogger.clientId,
                durationSeconds,
                callDirection,
                mobile,
                callLogId: callLogger.callLogId,
                streamSid,
                uniqueid: callLogger.uniqueid || agentConfig?.uniqueid || null
              })
              
              try {
                console.log("💾 [SIP-STOP] Saving final call log to database...")
                const finalLeadStatus = callLogger.currentLeadStatus || "maybe"
                console.log("📊 [SIP-STOP] Final lead status:", finalLeadStatus)
                const savedLog = await callLogger.saveToDatabase(finalLeadStatus)
                console.log("✅ [SIP-STOP] Final call log saved with ID:", savedLog._id)
              } catch (error) {
                console.log("❌ [SIP-STOP] Error saving final call log:", error.message)
              } finally {
                callLogger.cleanup()
              }
            }

            if (deepgramWs?.readyState === WebSocket.OPEN) {
              console.log("🛑 [SIP-STOP] Closing Deepgram connection...")
              deepgramWs.close()
            }
            // Close persistent Sarvam WS TTS if present
            try { if (ws.__sarvamTts) { ws.__sarvamTts.interrupt(); ws.__sarvamTts = null } } catch (_) {}
            
            console.log("🛑 [SIP-STOP] ======================================")
            break

          default:
            break
        }
      } catch (error) {
        // Silent error handling
      }
    })

    ws.on("close", async () => {
      console.log("🔌 [SIP-CLOSE] ========== WEBSOCKET CLOSED ==========")
      console.log("🔌 [SIP-CLOSE] StreamSID:", streamSid)
      console.log("🔌 [SIP-CLOSE] Call Direction:", callDirection)
      
      // Safety: Intelligent WhatsApp send on close if conditions are met
      try {
        if (callLogger && agentConfig?.whatsappEnabled && callLogger.shouldSendWhatsApp()) {
          const waLink = getAgentWhatsappLink(agentConfig)
          const waNumber = normalizeIndianMobile(callLogger?.mobile || null)
          const waApiUrl = agentConfig?.whatsapplink
          console.log("📨 [WHATSAPP] close-event check → enabled=", agentConfig.whatsappEnabled, ", link=", waLink, ", apiUrl=", waApiUrl, ", normalized=", waNumber, ", leadStatus=", callLogger.currentLeadStatus, ", requested=", callLogger.whatsappRequested)
          if (waLink && waNumber && waApiUrl) {
            sendWhatsAppTemplateMessage(waNumber, waLink, waApiUrl)
              .then(async (r) => {
                console.log("📨 [WHATSAPP] close-event result:", r?.ok ? "OK" : "FAIL", r?.status || r?.reason || r?.error || "")
                if (r?.ok) {
                  await billWhatsAppCredit({
                    clientId: agentConfig.clientId || callLogger?.clientId,
                    mobile: callLogger?.mobile || null,
                    link: waLink,
                    callLogId: callLogger?.callLogId,
                    streamSid,
                  })
                  callLogger.markWhatsAppSent()
                }
              })
              .catch((e) => console.log("❌ [WHATSAPP] close-event error:", e.message))
          } else {
            console.log("📨 [WHATSAPP] close-event skipped → missing:", !waLink ? "link" : "", !waNumber ? "number" : "", !waApiUrl ? "apiUrl" : "")
          }
        } else {
          console.log("📨 [WHATSAPP] close-event skipped → conditions not met:", {
            hasCallLogger: !!callLogger,
            whatsappEnabled: agentConfig?.whatsappEnabled,
            shouldSend: callLogger?.shouldSendWhatsApp(),
            leadStatus: callLogger?.currentLeadStatus,
            alreadySent: callLogger?.whatsappSent,
            requested: callLogger?.whatsappRequested
          })
        }
      } catch (waErr) {
        console.log("❌ [WHATSAPP] close-event unexpected:", waErr.message)
      }
      
      if (callLogger) {
        const stats = callLogger.getStats()
        console.log("🔌 [SIP-CLOSE] Final Call Stats:", JSON.stringify(stats, null, 2))
        // Bill credits on close as safety (guarded by billedStreamSids)
        const durationSeconds = Math.round((new Date() - callLogger.callStartTime) / 1000)
        await billCallCredits({
          clientId: callLogger.clientId,
          durationSeconds,
          callDirection,
          mobile: callLogger.mobile,
          callLogId: callLogger.callLogId,
          streamSid,
          uniqueid: callLogger.uniqueid || agentConfig?.uniqueid || null
        })
        
        try {
          console.log("💾 [SIP-CLOSE] Saving call log due to connection close...")
          const finalLeadStatus = callLogger.currentLeadStatus || "maybe"
          console.log("📊 [SIP-CLOSE] Final lead status:", finalLeadStatus)
          const savedLog = await callLogger.saveToDatabase(finalLeadStatus)
          console.log("✅ [SIP-CLOSE] Call log saved with ID:", savedLog._id)
        } catch (error) {
          console.log("❌ [SIP-CLOSE] Error saving call log:", error.message)
        } finally {
          callLogger.cleanup()
        }
      }

      if (deepgramWs?.readyState === WebSocket.OPEN) {
        console.log("🔌 [SIP-CLOSE] Closing Deepgram connection...")
        deepgramWs.close()
      }
      // Ensure Sarvam WS TTS is closed
      try { if (ws.__sarvamTts) { ws.__sarvamTts.interrupt(); ws.__sarvamTts = null } } catch (_) {}

      console.log("🔌 [SIP-CLOSE] Resetting session state...")
      
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
      speechStartTime = null
      firstInterimTime = null
      firstFinalTime = null
      
      console.log("🔌 [SIP-CLOSE] ======================================")
    })

    ws.on("error", (error) => {
      console.log("❌ [SIP-ERROR] WebSocket error:", error.message)
      console.log("❌ [SIP-ERROR] StreamSID:", streamSid)
      console.log("❌ [SIP-ERROR] Call Direction:", callDirection)
    })
  })
}

// Global map to store active call loggers by streamSid
const activeCallLoggers = new Map()

// Global sequence counter for stop events
let stopEventSequence = 1

// Track billed streams to avoid double-charging on both stop and close
const billedStreamSids = new Set()

// Helper to bill call credits with decimal precision (1/30 credit per second)
const billCallCredits = async ({ clientId, durationSeconds, callDirection, mobile, callLogId, streamSid, uniqueid }) => {
  try {
    if (!clientId || !streamSid) return
    if (billedStreamSids.has(streamSid)) return

    const creditRecord = await Credit.getOrCreateCreditRecord(clientId)
    const currentSeconds = Math.max(0, Number(durationSeconds) || 0)
    const balanceBefore = Number(creditRecord.currentBalance || 0)

    // Use new decimal billing method
    const billingResult = creditRecord.billCallCredits(
      currentSeconds, 
      mobile || 'unknown', 
      callDirection || 'inbound', 
      callLogId, 
      streamSid,
      uniqueid
    )

    // Save the updated credit record
    await creditRecord.save()

    if (callLogId) {
      await CallLog.findByIdAndUpdate(callLogId, {
        'metadata.billing': {
          creditsUsed: billingResult.creditsUsed,
          durationFormatted: billingResult.durationFormatted,
          durationSeconds: currentSeconds,
          balanceBefore: balanceBefore,
          balanceAfter: billingResult.balanceAfter,
          billingMethod: 'decimal_precision',
          creditsPerSecond: 1/30,
          uniqueid: uniqueid || null,
          billedAt: new Date(),
        },
        'metadata.lastUpdated': new Date(),
      }).catch(() => {})
    }

    billedStreamSids.add(streamSid)
    console.log(`💰 [CALL-BILLING] Call: ${billingResult.durationFormatted} (${currentSeconds}s). Charged: ${billingResult.creditsUsed} credits. Balance: ${balanceBefore} → ${billingResult.balanceAfter}`)
  } catch (e) {
    console.log(`❌ [CALL-BILLING] Error: ${e.message}`)
    // Swallow billing errors to not affect call flow
  }
}

// Helper to deduct 1 credit for successful WhatsApp sends
const billWhatsAppCredit = async ({ clientId, mobile, link, callLogId, streamSid }) => {
  try {
    if (!clientId) return
    const creditRecord = await Credit.getOrCreateCreditRecord(clientId)
    const balanceBefore = Number(creditRecord?.currentBalance || 0)
    if (balanceBefore < 1) {
      console.log("⚠️ [WHATSAPP-BILLING] Insufficient credits to deduct for WhatsApp message")
      return
    }
    await creditRecord.useCredits(1, 'whatsapp', `WhatsApp message sent to ${mobile || 'unknown'} with link: ${link || 'none'}`, {
      mobile: mobile || null,
      link: link || null,
      callLogId: callLogId || null,
      streamSid: streamSid || null,
    })
    console.log(`💰 [WHATSAPP-BILLING] Deducted 1.00 credit for WhatsApp message to ${mobile}`)
  } catch (e) {
    console.log("❌ [WHATSAPP-BILLING] Error deducting credit:", e.message)
  }
}

/**
 * Terminate a call by streamSid
 * @param {string} streamSid - The stream SID to terminate
 * @param {string} reason - Reason for termination
 * @returns {Object} Result of termination attempt
 */
const terminateCallByStreamSid = async (streamSid, reason = 'manual_termination') => {
  try {
    console.log(`🛑 [MANUAL-TERMINATION] Attempting to terminate call with streamSid: ${streamSid}`)
    
    // Check if we have an active call logger for this streamSid
    const callLogger = activeCallLoggers.get(streamSid)
    
    if (callLogger) {
      console.log(`🛑 [MANUAL-TERMINATION] Found active call logger, terminating gracefully...`)
      console.log(`🛑 [MANUAL-TERMINATION] Call Logger Info:`, callLogger.getCallInfo())
      
      // Check WebSocket state
      if (callLogger.ws) {
        console.log(`🛑 [MANUAL-TERMINATION] WebSocket State: ${callLogger.ws.readyState} (0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED)`)
      }
      
      await callLogger.disconnectCall(reason)
      return {
        success: true,
        message: 'Call terminated successfully',
        streamSid,
        reason,
        method: 'graceful_termination'
      }
    } else {
      console.log(`🛑 [MANUAL-TERMINATION] No active call logger found, updating database directly...`)
      
      // Fallback: Update the call log directly in the database
      try {
        const CallLog = require("../models/CallLog")
        const result = await CallLog.updateMany(
          { streamSid, 'metadata.isActive': true },
          { 
            'metadata.isActive': false,
            'metadata.terminationReason': reason,
            'metadata.terminatedAt': new Date(),
            'metadata.terminationMethod': 'api_manual',
            leadStatus: 'disconnected_api'
          }
        )
        
        if (result.modifiedCount > 0) {
          return {
            success: true,
            message: 'Call marked as terminated in database',
            streamSid,
            reason,
            method: 'database_update',
            modifiedCount: result.modifiedCount
          }
        } else {
          return {
            success: false,
            message: 'No active calls found with this streamSid',
            streamSid,
            reason,
            method: 'database_update'
          }
        }
      } catch (dbError) {
        console.error(`❌ [MANUAL-TERMINATION] Database update error:`, dbError.message)
        return {
          success: false,
          message: 'Failed to update database',
          streamSid,
          reason,
          method: 'database_update',
          error: dbError.message
        }
      }
    }
  } catch (error) {
    console.error(`❌ [MANUAL-TERMINATION] Error terminating call:`, error.message)
    return {
      success: false,
      message: 'Failed to terminate call',
      streamSid,
      reason,
      method: 'error',
      error: error.message
    }
  }
}

module.exports = { 
  setupUnifiedVoiceServer, 
  terminateCallByStreamSid,
  // Export termination methods for external use
  terminationMethods: {
    graceful: (callLogger, message, language) => callLogger?.gracefulCallEnd(message, language),
    fast: (callLogger, reason) => callLogger?.fastTerminateCall(reason),
    ultraFast: (callLogger, message, language, reason) => callLogger?.ultraFastTerminateWithMessage(message, language, reason)
  }
}