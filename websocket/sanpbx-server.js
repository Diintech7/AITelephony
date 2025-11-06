const WebSocket = require("ws")
require("dotenv").config()
const mongoose = require("mongoose")
const Agent = require("../models/Agent")
const CallLog = require("../models/CallLog")
const Credit = require("../models/Credit")
const { getSystemPromptWithCache } = require("../utils/system-prompt-helper")

// Load API keys from environment variables
const API_KEYS = {
  deepgram: process.env.DEEPGRAM_API_KEY,
  sarvam: process.env.SARVAM_API_KEY,
  openai: process.env.OPENAI_API_KEY,
  whatsapp: process.env.WHATSAPP_TOKEN,
  smallest: process.env.SMALLEST_API_KEY,
}

// Optional base URL to build full WAV URLs from SanPBX rec_path values
const RECORDING_BASE_URL = process.env.SANPBX_RECORDING_BASE_URL || null

// SanPBX API configuration
const SANPBX_API_CONFIG = {
  baseUrl: "https://clouduat28.sansoftwares.com/pbxadmin/sanpbxapi",
  accessToken: "e4b197411fd53012607649f23a6d28f9",
  genTokenEndpoint: "/gentoken",
  disconnectEndpoint: "/calldisconnect"
}

// Validate API keys (require Deepgram + OpenAI, and at least one TTS: Sarvam or Smallest)
if (!API_KEYS.deepgram || !API_KEYS.openai || (!API_KEYS.sarvam && !API_KEYS.smallest)) {
  console.error("❌ Missing required API keys: need DEEPGRAM_API_KEY, OPENAI_API_KEY, and either SARVAM_API_KEY or SMALLEST_API_KEY")
  process.exit(1)
}

const fetch = globalThis.fetch || require("node-fetch")
const fs = require('fs')
const path = require('path')
const { uploadBufferToS3, buildRecordingKey } = require('../utils/s3')

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

// Resolve a full recording URL from a relative rec_path if possible
const resolveRecordingUrl = (recPath) => {
  try {
    if (!recPath || typeof recPath !== 'string') return null
    const trimmed = recPath.trim()
    if (!trimmed) return null
    if (/^https?:\/\//i.test(trimmed)) return trimmed
    if (!RECORDING_BASE_URL) return trimmed
    const base = RECORDING_BASE_URL
    const sep = base.endsWith('/') ? '' : '/'
    const path = trimmed.replace(/^\/+/, '')
    return `${base}${sep}${path}`
  } catch (_) {
    return recPath
  }
}

// Download a recording into Buffer from either HTTP(S) URL or local path
const fetchRecordingBuffer = async (recordingPathOrUrl) => {
  try {
    if (!recordingPathOrUrl) return null
    const value = String(recordingPathOrUrl).trim()
    if (!value) return null
    if (/^https?:\/\//i.test(value)) {
      const res = await fetch(value)
      if (!res.ok) return null
      const arrBuf = await res.arrayBuffer()
      return Buffer.from(arrBuf)
    }
    // Try reading from local filesystem (relative to process cwd)
    const abs = path.isAbsolute(value) ? value : path.join(process.cwd(), value)
    if (fs.existsSync(abs)) {
      return fs.readFileSync(abs)
    }
    return null
  } catch (_) {
    return null
  }
}

// Upload SanPBX recording to S3 and return its URL
const uploadRecordingToS3 = async (recordingPath) => {
  try {
    const bucket = process.env.AWS_S3_BUCKET || process.env.AWS_BUCKET_NAME
    if (!bucket) return null
    const resolved = resolveRecordingUrl(recordingPath)
    const buffer = await fetchRecordingBuffer(resolved || recordingPath)
    if (!buffer) return null
    const key = buildRecordingKey(recordingPath)
    const url = await uploadBufferToS3(buffer, bucket, key, 'audio/wav')
    return url
  } catch (_) {
    return null
  }
}

// Precompiled responses for common queries (instant responses)
const QUICK_RESPONSES = {
  hello: "Hello! How can I help you?",
  hi: "Hi there! What can I do for you?",
  "how are you": "I'm doing great! How about you?",
  "thank you": "You're welcome! Is there anything else I can help with?",
  thanks: "My pleasure! What else can I assist you with?",
  yes: "Great! What would you like to know more about?",
  no: "No problem! Is there something else I can help you with?",
  okay: "Perfect! What's next?",
  "good morning": "Good morning! How can I assist you today?",
  "good afternoon": "Good afternoon! What can I help you with?",
  "good evening": "Good evening! How may I help you?",
  "bye": "Goodbye! Have a great day!",
  "goodbye": "Goodbye! Take care!",
  "see you": "See you later!",
  "that's all": "Alright! Is there anything else you need?",
  "nothing else": "Perfect! Have a wonderful day!",
  "that's it": "Great! Feel free to call back if you need anything else.",
}

// Sentence utilities
const splitIntoSentences = (text) => {
  try {
    if (!text) return []
    // Split on ., !, ? while keeping Unicode and avoiding empty fragments
    const parts = String(text)
      .replace(/\s+/g, ' ')
      .split(/(?<=[.!?])\s+/)
      .map(s => s.trim())
      .filter(Boolean)
    return parts
  } catch (_) {
    return [String(text || '').trim()].filter(Boolean)
  }
}

// Replace placeholders like [name] using session/context values
const resolvePlaceholders = (text, { name } = {}) => {
  try {
    const src = String(text || '')
    if (!src) return ''
    let out = src
    if (name && typeof name === 'string' && name.trim()) {
      out = out.replace(/\[name\]/gi, name.trim())
    }
    return out
  } catch (_) {
    return String(text || '')
  }
}

const limitSentences = (text, maxSentences = 3) => {
  try {
    const sentences = splitIntoSentences(text)
    return sentences.slice(0, Math.max(1, Math.min(maxSentences, sentences.length))).join(' ')
  } catch (_) {
    return String(text || '').trim()
  }
}

// Fast user-intent checks to avoid latency
const shouldTerminateFast = (text) => {
  try {
    const t = String(text || '').toLowerCase().trim()
    if (!t) return false
    const noPhrases = [
      'not interested', 'no thanks', 'no thank you', 'stop calling', 'don\'t call', 'don\'t want', 'not needed',
      'never call', 'remove my number', 'wrong number', 'disconnect', 'hang up', 'bye', 'goodbye', 'stop', 'no'
    ]
    return noPhrases.some(p => t === p || t.includes(p))
  } catch (_) { return false }
}

const indicatesWaitOrThinking = (text) => {
  try {
    const t = String(text || '').toLowerCase().trim()
    if (!t) return false
    const waitPhrases = [
      'hold on', 'please wait', 'wait', 'one minute', 'just a minute', 'give me a minute',
      'thinking', 'let me think', 'call back later', 'later', 'busy', 'in a meeting'
    ]
    return waitPhrases.some(p => t.includes(p))
  } catch (_) { return false }
}

const smoothGreeting = (text, maxSentences = 2) => {
  try {
    const normalized = String(text || '').replace(/\s+/g, ' ').trim()
    if (!normalized) return 'Hello! How can I help you today?'
    return limitSentences(normalized, maxSentences)
  } catch (_) {
    return 'Hello! How can I help you today?'
  }
}

// Helpers
function extractDigits(value) {
  if (!value) return ""
  return String(value).replace(/\D+/g, "")
}
function last10Digits(value) {
  const digits = extractDigits(value)
  return digits.slice(-10)
}

// Language mapping helpers (agent-configured language only)
const LANGUAGE_MAPPING = { hi: "hi-IN", en: "en-IN", bn: "bn-IN", te: "te-IN", ta: "ta-IN", mr: "mr-IN", gu: "gu-IN", kn: "kn-IN", ml: "ml-IN", pa: "pa-IN", or: "or-IN", as: "as-IN", ur: "ur-IN" }
const getSarvamLanguage = (language = "hi") => LANGUAGE_MAPPING[(language || "hi").toLowerCase()] || "hi-IN"
const getDeepgramLanguage = (language = "hi") => {
  const lang = (language || "hi").toLowerCase()
  if (lang === "hi") return "hi"
  if (lang === "en") return "en-IN"
  if (lang === "mr") return "mr"
  return lang
}

// Latency/interim thresholds for interruption control (aligned with reference)
const LATENCY_CONFIG = {
  INTERIM_MIN_WORDS: 1,
  INTERIM_MIN_LENGTH: 10,
  INTERIM_DEBOUNCE_MS: 800,
  CONFIDENCE_THRESHOLD: 0.85,
}

// Conversation history manager with strict interim handling
class ConversationHistoryManager {
  constructor() {
    this.entries = []
    this.pendingTranscript = ""
    this.lastTranscriptTime = 0
    this.transcriptMergeTimer = null
    this.lastInterimText = ""
    this.lastInterimTime = 0
  }

  addUserTranscript(text, timestamp = Date.now()) {
    const clean = String(text || "").trim()
    if (!clean || clean.split(/\s+/).length < 2) return
    if (this.transcriptMergeTimer) { clearTimeout(this.transcriptMergeTimer); this.transcriptMergeTimer = null }
    this.entries.push({ role: "user", content: clean, timestamp })
    this.lastTranscriptTime = timestamp
    this.trim()
  }

  addAssistantResponse(text, timestamp = Date.now()) {
    const clean = String(text || "").trim()
    if (!clean) return
    this.entries.push({ role: "assistant", content: clean, timestamp })
    this.trim()
  }

  handleInterimTranscript(text, timestamp = Date.now()) {
    const clean = String(text || "").trim()
    if (!clean) return false
    
    // Filter out filler words and noise
    const fillerWords = ["um", "uh", "ah", "er", "mm", "hmm", "like", "you know", "so", "well"]
    const words = clean.toLowerCase().split(/\s+/)
    const meaningfulWords = words.filter(w => w.length > 1 && !fillerWords.includes(w))
    
    if (meaningfulWords.length < LATENCY_CONFIG.INTERIM_MIN_WORDS || 
        clean.length < LATENCY_CONFIG.INTERIM_MIN_LENGTH) {
      return false
    }
    
    // Debounce rapid interim updates
    const timeSinceLastInterim = timestamp - this.lastInterimTime
    if (timeSinceLastInterim < LATENCY_CONFIG.INTERIM_DEBOUNCE_MS) {
      return false
    }
    
    this.lastInterimText = clean
    this.lastInterimTime = timestamp
    return true
  }

  getConversationHistory() {
    return this.entries.map(e => ({ role: e.role, content: e.content }))
  }

  trim(max = 20) {
    if (this.entries.length > max) {
      this.entries = this.entries.slice(-max)
    }
  }

  clear() {
    this.entries = []
    this.pendingTranscript = ""
    this.lastTranscriptTime = 0
    if (this.transcriptMergeTimer) { clearTimeout(this.transcriptMergeTimer); this.transcriptMergeTimer = null }
    this.lastInterimText = ""
    this.lastInterimTime = 0
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

const VALID_SARVAM_VOICES = new Set([
  "abhilash","anushka","meera","pavithra","maitreyi","arvind","amol","amartya","diya","neel","misha","vian","arjun","maya","manisha","vidya","arya","karun","hitesh"
])

const getValidSarvamVoice = (voiceSelection = "pavithra") => {
  const normalized = (voiceSelection || "").toString().trim().toLowerCase()
  if (VALID_SARVAM_VOICES.has(normalized)) return normalized
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

// Enhanced auto disposition detection using OpenAI
const detectAutoDisposition = async (userMessage, conversationHistory, detectedLanguage) => {
  const timer = createTimer("AUTO_DISPOSITION_DETECTION")
  try {
    const dispositionPrompt = `You must decide whether to continue or terminate the call based on engagement.

TERMINATE only if the user clearly wants to end or block the call:
- Explicit rejections: "not interested", "no thanks", "don't call", "stop calling", "not needed"
- End-of-call intent: "bye", "goodbye", "hang up", "end call"
- Wrong party: "wrong number", "not the right person"
- Unavailable with request to end the call now

IMPORTANT: Do not treat polite phrases like "thank you" by themselves as termination unless accompanied by an end-of-call intent (e.g., "thank you, bye").

CONTINUE when the user is engaged or progressing the conversation:
- Asking questions, requesting information, or saying they don't know specifics but are willing to proceed
- Providing details (name, age, condition) or answering questions
- Agreeing to next steps such as booking/appointment/consultation
- Expressing interest ("yes", "okay", "please proceed")

User message: "${userMessage}"
Recent conversation: ${conversationHistory.slice(-3).map(msg => `${msg.role}: ${msg.content}`).join(' | ')}

Return ONLY: "TERMINATE" if the call should be ended now, or "CONTINUE" if the conversation should continue.`

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEYS.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: dispositionPrompt },
        ],
        max_tokens: 10,
        temperature: 0.1,
      }),
    })

    if (!response.ok) {
      console.log(`❌ [AUTO-DISPOSITION] ${timer.end()}ms - Error: ${response.status}`)
      return "CONTINUE" // Default to continue on error
    }

    const data = await response.json()
    const result = data.choices[0]?.message?.content?.trim().toUpperCase()

    if (result === "TERMINATE") {
      console.log(`🛑 [AUTO-DISPOSITION] ${timer.end()}ms - User wants to terminate call`)
      return "TERMINATE"
    } else {
      console.log(`✅ [AUTO-DISPOSITION] ${timer.end()}ms - User wants to continue`)
      return "CONTINUE"
    }
  } catch (error) {
    console.log(`❌ [AUTO-DISPOSITION] ${timer.end()}ms - Error: ${error.message}`)
    return "CONTINUE" // Default to continue on error
  }
}

// Intelligent call disconnection detection using OpenAI (legacy function)
const detectCallDisconnectionIntent = async (userMessage, conversationHistory, detectedLanguage) => {
  return await detectAutoDisposition(userMessage, conversationHistory, detectedLanguage)
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

// Simplified OpenAI processing
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
    const basePromptRaw = agentConfig.systemPrompt || "You are a helpful AI assistant."
    const firstMessageRaw = (agentConfig.firstMessage || "").trim()
    const callerName = (userName || '').trim()
    const basePrompt = resolvePlaceholders(basePromptRaw, { name: callerName })
    const firstMessage = resolvePlaceholders(firstMessageRaw, { name: callerName })
    const knowledgeBlock = firstMessage
      ? `FirstGreeting: "${firstMessage}"\n`
      : ""
    const detailsTextRaw = (agentConfig.details || "").trim()
    const detailsText = resolvePlaceholders(detailsTextRaw, { name: callerName })
    const detailsBlock = detailsText ? `Details:\n${detailsText}\n\n` : ""
    const qaItems = Array.isArray(agentConfig.qa) ? agentConfig.qa : []
    const qaBlock = qaItems.length > 0
      ? `QnA:\n${qaItems.map((item, idx) => {
          const q = resolvePlaceholders((item?.question || "").toString().trim(), { name: callerName })
          const a = resolvePlaceholders((item?.answer || "").toString().trim(), { name: callerName })
          return q && a ? `${idx + 1}. Q: ${q}\n   A: ${a}` : null
        }).filter(Boolean).join("\n")}\n\n`
      : ""
    console.log(qaBlock)
    // Get policy block from SystemPrompt database (with fallback)
    const policyBlock = await getSystemPromptWithCache()
    console.log(qaBlock)

    const systemPrompt = `System Prompt:\n${basePrompt}\n\n${detailsBlock}${qaBlock}${knowledgeBlock}${policyBlock}\n\nAnswer strictly using the Details and QnA above. If information is missing, say you don't have that info.`
    console.log(systemPrompt)
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
          whatsappRequested: false,
          whatsappMessageSent: false,
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
          'metadata.lastUpdated': new Date(),
          'metadata.whatsappRequested': this.whatsappRequested,
          'metadata.whatsappMessageSent': this.whatsappSent,
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
  async saveToDatabase(leadStatusInput = 'maybe', agentConfig = null) {
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

      // Detect disposition if agent has depositions configured
      let disposition = null
      let subDisposition = null
      let dispositionId = null
      let subDispositionId = null
      try {
        const agentDepositions = agentConfig?.depositions
        if (Array.isArray(agentDepositions) && agentDepositions.length > 0) {
          console.log("🔍 [DISPOSITION-DETECTION] Analyzing conversation for disposition...")
          const conversation = this.generateFullTranscript()
          const history = [...this.transcripts, ...this.responses]
            .sort((a,b)=>new Date(a.timestamp)-new Date(b.timestamp))
            .map(e=>({ role: e.type === 'user' ? 'user' : 'assistant', content: e.text }))
          const result = await detectDispositionWithOpenAI(history, agentDepositions)
          disposition = result.disposition
          subDisposition = result.subDisposition
          dispositionId = result.dispositionId
          subDispositionId = result.subDispositionId
          if (disposition) {
            console.log(`📊 [DISPOSITION-DETECTION] Detected disposition: ${disposition} (ID: ${dispositionId}) | ${subDisposition || 'N/A'} (ID: ${subDispositionId || 'N/A'})`)
          }
        } else {
          console.log("⚠️ [DISPOSITION-DETECTION] No agent depositions configured")
        }
      } catch (e) {
        console.log(`⚠️ [DISPOSITION-DETECTION] Error: ${e.message}`)
      }

      if (this.isCallLogCreated && this.callLogId) {
        // Update existing call log with final data
        const finalUpdateData = {
          transcript: this.generateFullTranscript(),
          duration: this.totalDuration,
          leadStatus: leadStatus,
          disposition: disposition,
          subDisposition: subDisposition,
          dispositionId: dispositionId,
          subDispositionId: subDispositionId,
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
          'metadata.whatsappRequested': this.whatsappRequested,
          'metadata.whatsappMessageSent': this.whatsappSent,
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
          disposition: disposition,
          subDisposition: subDisposition,
          dispositionId: dispositionId,
          subDispositionId: subDispositionId,
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
            whatsappRequested: this.whatsappRequested,
            whatsappMessageSent: this.whatsappSent,
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

// Global map to store active call loggers by streamSid
const activeCallLoggers = new Map()

// Global map to store active WebSocket connections by streamSid
const activeWebSockets = new Map()

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

// Helper to get SanPBX API token
const getSanPBXToken = async () => {
  try {
    const tokenUrl = `${SANPBX_API_CONFIG.baseUrl}${SANPBX_API_CONFIG.genTokenEndpoint}`
    const requestBody = {
      access_key: "mob"
    }

    console.log(`🔑 [SANPBX-TOKEN] Getting API token from: ${tokenUrl}`)

    const response = await fetch(tokenUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accesstoken": SANPBX_API_CONFIG.accessToken,
      },
      body: JSON.stringify(requestBody),
    })

    const responseData = await response.json()
    const isOk = response.ok

    console.log(`🔑 [SANPBX-TOKEN] Response Status: ${response.status}`)
    console.log(`🔑 [SANPBX-TOKEN] Response:`, JSON.stringify(responseData))

    if (isOk && responseData.status === "success" && responseData.Apitoken) {
      console.log(`✅ [SANPBX-TOKEN] Successfully obtained API token`)
      return responseData.Apitoken
    } else {
      console.log(`❌ [SANPBX-TOKEN] Failed to get API token: ${responseData.msg || 'Unknown error'}`)
      return null
    }
  } catch (error) {
    console.log(`❌ [SANPBX-TOKEN] Error getting API token:`, error.message)
    return null
  }
}

// Helper to disconnect call via SanPBX API
const disconnectCallViaAPI = async (callId, reason = 'manual_disconnect') => {
  try {
    if (!callId) {
      console.log("❌ [SANPBX-DISCONNECT] No callId provided for disconnect")
      return { success: false, error: "No callId provided" }
    }

    // Get fresh API token
    const apiToken = await getSanPBXToken()
    if (!apiToken) {
      console.log("❌ [SANPBX-DISCONNECT] Failed to get API token")
      return { success: false, error: "Failed to get API token" }
    }

    const disconnectUrl = `${SANPBX_API_CONFIG.baseUrl}${SANPBX_API_CONFIG.disconnectEndpoint}`
    const requestBody = {
      callid: callId
    }

    console.log(`🛑 [SANPBX-DISCONNECT] Attempting to disconnect call: ${callId}`)
    console.log(`🛑 [SANPBX-DISCONNECT] API URL: ${disconnectUrl}`)
    console.log(`🛑 [SANPBX-DISCONNECT] Request Body:`, JSON.stringify(requestBody))

    const response = await fetch(disconnectUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Apitoken": apiToken,
      },
      body: JSON.stringify(requestBody),
    })

    const responseText = await response.text()
    const isOk = response.ok

    console.log(`🛑 [SANPBX-DISCONNECT] Response Status: ${response.status} ${response.statusText}`)
    console.log(`🛑 [SANPBX-DISCONNECT] Response Body: ${responseText}`)

    if (isOk) {
      console.log(`✅ [SANPBX-DISCONNECT] Successfully disconnected call: ${callId}`)
      return { 
        success: true, 
        callId, 
        reason,
        status: response.status,
        response: responseText 
      }
    } else {
      console.log(`❌ [SANPBX-DISCONNECT] Failed to disconnect call: ${callId} - Status: ${response.status}`)
      return { 
        success: false, 
        callId, 
        reason,
        status: response.status,
        error: responseText 
      }
    }
  } catch (error) {
    console.log(`❌ [SANPBX-DISCONNECT] Error disconnecting call ${callId}:`, error.message)
    return { 
      success: false, 
      callId, 
      reason,
      error: error.message 
    }
  }
}

/**
 * Setup unified voice server for SanIPPBX integration
 * @param {WebSocket} ws - The WebSocket connection from SanIPPBX
 */
// Check if call is already active to prevent multiple WebSocket connections
const isCallActive = (streamSid) => {
  return activeWebSockets.has(streamSid)
}

// Force disconnect existing WebSocket for a streamSid
const forceDisconnectWebSocket = (streamSid) => {
  const existingWs = activeWebSockets.get(streamSid)
  if (existingWs && existingWs.readyState === WebSocket.OPEN) {
    console.log(`🔗 [SANPBX-WS-FORCE-DISCONNECT] Force closing existing WebSocket for streamSid: ${streamSid}`)
    existingWs.close()
    activeWebSockets.delete(streamSid)
    return true
  }
  return false
}

const setupSanPbxWebSocketServer = (ws) => {
  console.log("🔗 [SANPBX] Setting up SanIPPBX voice server connection")
  
  // Track this WebSocket connection
  let currentStreamSid = null

  // Session state for this connection
  let streamId = null
  let callId = null
  let channelId = null
  let inputSampleRateHz = 8000
  let inputChannels = 1
  let inputEncoding = "linear16"
  let callerIdValue = ""
  let callDirectionValue = ""
  let didValue = ""
  // Conversation history manager for low-latency interim handling
  const history = new ConversationHistoryManager()
  let deepgramWs = null
  let isProcessing = false
  let userUtteranceBuffer = ""
  let silenceTimer = null
  let sttFailed = false
  let chunkCounter = 0
  // Always use JSON base64 media; binary mode disabled
  
  // Add duplicate prevention tracking
  let lastProcessedTranscript = ""
  let lastProcessedTime = 0
  let activeResponseId = null
  // Additional session state for logging and DB
  let sessionCustomParams = {}
  let sessionUserName = null
  let sessionUniqueId = null
  let callLogId = null
  let callStartTime = new Date()
  let userTranscripts = []
  let aiResponses = []
  let whatsappRequested = false
  let whatsappSent = false
  
  // Enhanced session state
  let currentLanguage = undefined
  let processingRequestId = 0
  let callLogger = null
  let callDirection = "inbound"
  let agentConfig = null
  let userName = null
  let currentTTS = null
  let deepgramReady = false
  let deepgramAudioQueue = []
  let sttTimer = null
  let lastUserActivity = Date.now()
  let silenceTimeout = null
  let autoDispositionEnabled = true
  let sessionRecordingPath = null

  // Closing/termination state for Cancer Healer Center flows
  let closingState = {
    isClosing: false,
    finalMessageSent: false,
    lastIntent: null, // Interested_Now | Interested_Later | Not_Interested | null
    closeTimer: null,
    closeSilenceTimer: null,
    ignoreFurtherInputs: false
  }

  // Closing triggers and ignore keywords
  const CLOSING_TRIGGER_PATTERNS = [
    /धन्यवाद\s+sir\/?ma'?am[,!\s]*\s*cancer\s+healer\s+center\s+चुनने के लिए/i,
    /consulting\s+team\s+आपसे\s+जल्द\s+ही\s+संपर्क करेगी/i,
    /धन्यवाद\s+आपका\s+समय\s+देने\s+के\s+लिए/i,
    /मैं\s+आपको\s+details\s+whatsapp\/?\s*sms\s+कर देती हूँ/i,
    /आपका\s+दिन\s+शुभ\s+हो/i,
    /thank you[,!\s]*\s*have a good day/i,
    /धन्यवाद\s+sir\/?ma'?am/i,
  ]

  const POST_GOODBYE_IGNORE_REGEX = /^(ok(ay)?|bye|th(i|ee)k\s*hai|thanks|thank\s*you|done|hmm|acha)\b/i

  // Map lead status to intents for forced-end logic
  const mapLeadStatusToIntent = (status) => {
    const s = (status || '').toLowerCase()
    if (s === 'vvi' || s === 'enrolled') return 'Interested_Now'
    if (['hot_followup','cold_followup','schedule'].includes(s)) return 'Interested_Later'
    if (['decline','not_required','junk_lead','not_eligible','wrong_number','not_connected'].includes(s)) return 'Not_Interested'
    return null
  }

  const performCallEnd = async (reason = 'close_call') => {
    try {
      // Ensure any pending reverse-media finishes quickly, but do not alter chunking
      await waitForSipQueueDrain(800)
      if (callLogger) {
        await callLogger.saveToDatabase(callLogger.currentLeadStatus || 'maybe', agentConfig)
        callLogger.cleanup()
      }
      if (callId) {
        await disconnectCallViaAPI(callId, reason)
      }
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close()
      }
      if (currentStreamSid) {
        activeWebSockets.delete(currentStreamSid)
        console.log(`🔗 [SANPBX-WS-TRACKING] Removed WebSocket for streamSid: ${currentStreamSid}`)
      }
    } catch (_) {}
  }

  // const startClosingFlow = async ({ reason = 'close_call', politeDelayMs = 2000, finalTone = "कॉल समाप्त की जा रही है। धन्यवाद।" } = {}) => {
  //   if (closingState.isClosing) return
  //   closingState.isClosing = true
  //   closingState.ignoreFurtherInputs = true
  //   console.log(`🛑 [CLOSING] Initiated closing flow: ${reason}`)

  //   // Schedule polite closing tone/message just before hangup
  //   try {
  //     // Send the polite tone/message without changing SIP chunk sizes
  //     await enqueueTts(finalTone, (ws.sessionAgentConfig?.language || 'en').toLowerCase())
  //     closingState.finalMessageSent = true
  //   } catch (_) {}

  //   // After final message, enforce 4s silence fallback end
  //   if (closingState.closeSilenceTimer) { clearTimeout(closingState.closeSilenceTimer) }
  //   closingState.closeSilenceTimer = setTimeout(async () => {
  //     console.log('⏰ [CLOSING] 4s silence after final message → ending call')
  //     await performCallEnd('silence_after_closing')
  //   }, 4000)

  //   // Forced end after confirmation intents: 2.5s; otherwise 2s
  //   const waitMs = ['Interested_Now','Interested_Later','Not_Interested'].includes(closingState.lastIntent) ? 2500 : politeDelayMs
  //   if (closingState.closeTimer) { clearTimeout(closingState.closeTimer) }
  //   closingState.closeTimer = setTimeout(async () => {
  //     console.log(`🛑 [CLOSING] Finalizing call end after ${waitMs}ms`)
  //     await performCallEnd(reason)
  //   }, waitMs)
  // }

  const textTriggersClosing = (text) => {
    if (!text) return false
    const t = String(text).toLowerCase()
    return CLOSING_TRIGGER_PATTERNS.some((re) => re.test(t))
  }

  const buildFullTranscript = () => {
    try {
      const all = [...userTranscripts, ...aiResponses].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp))
      return all.map((e) => {
        const speaker = e.type === 'user' ? 'User' : 'AI'
        const time = e.timestamp instanceof Date ? e.timestamp.toISOString() : new Date(e.timestamp).toISOString()
        return `[${time}] ${speaker} (${e.language}): ${e.text}`
      }).join("\n")
    } catch (_) {
      return ''
    }
  }

  // Central SIP audio FIFO queue to serialize all reverse-media sends
  const sipAudioQueue = []
  let isSipStreaming = false
  let sipQueueInterrupted = false
  
  const enqueueSipAudio = async (pcmBase64) => {
    try {
      if (sipQueueInterrupted) return // Drop audio while interrupted
      if (!pcmBase64 || !pcmBase64.trim()) return
      sipAudioQueue.push(pcmBase64)
      if (!isSipStreaming) {
        processSipAudioQueue().catch(() => {})
      }
    } catch (_) {}
  }
  
  const processSipAudioQueue = async () => {
    if (isSipStreaming) return
    isSipStreaming = true
    try {
      while (sipAudioQueue.length > 0 && !sipQueueInterrupted) {
        const audioItem = sipAudioQueue.shift()
        if (sipQueueInterrupted) break
        try { await streamAudioToSanIPPBX(audioItem) } catch (_) {}
        if (sipQueueInterrupted) break
        // tiny gap between items to avoid boundary artifacts
        await new Promise(r => setTimeout(r, 40))
      }
    } finally {
      isSipStreaming = false
    }
  }
  
  const interruptSipQueue = () => {
    sipQueueInterrupted = true
    sipAudioQueue.length = 0 // Clear the queue
    console.log("🛑 [SIP-QUEUE] Interrupted and cleared")
  }
  
  const resetSipQueue = () => {
    sipQueueInterrupted = false
  }

  // Wait until SIP queue has fully drained (all reverse-media sent) or timeout
  const waitForSipQueueDrain = async (timeoutMs = 2000) => {
    try {
      const start = Date.now()
      while ((Date.now() - start) < timeoutMs) {
        if (sipAudioQueue.length === 0 && !isSipStreaming) {
          return true
        }
        await new Promise(r => setTimeout(r, 50))
      }
      return sipAudioQueue.length === 0 && !isSipStreaming
    } catch (_) {
      return false
    }
  }

  // Ensure current streaming halts before allowing new audio; then reset interrupt
  const waitForSipHaltAndReset = async (timeoutMs = 1000) => {
    try {
      const start = Date.now()
      while (isSipStreaming && (Date.now() - start) < timeoutMs) {
        await new Promise(r => setTimeout(r, 20))
      }
      resetSipQueue()
    } catch (_) { resetSipQueue() }
  }

  // Auto disposition and silence timeout handlers
  const resetSilenceTimeout = () => {
    if (silenceTimeout) {
      clearTimeout(silenceTimeout)
      silenceTimeout = null
    }
    lastUserActivity = Date.now()
    
    // Set 30-second silence timeout
    silenceTimeout = setTimeout(async () => {
      console.log("⏰ [SILENCE-TIMEOUT] 30 seconds of silence detected - terminating call")
      await terminateCallForSilence()
    }, 100000)
  }

  const terminateCallForSilence = async () => {
    try {
      console.log("🛑 [AUTO-TERMINATION] Terminating call due to silence timeout")
      // Ensure last TTS/audio is delivered before termination
      await waitForSipQueueDrain(2500)
      
      if (callLogger) {
        callLogger.updateLeadStatus('not_connected')
        await callLogger.saveToDatabase('not_connected', agentConfig)
        callLogger.cleanup()
      }
      
      if (callId) {
        await disconnectCallViaAPI(callId, 'silence_timeout')
      }
      
      // Close WebSocket and cleanup tracking
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close()
      }
      if (currentStreamSid) {
        activeWebSockets.delete(currentStreamSid)
        console.log(`🔗 [SANPBX-WS-TRACKING] Removed WebSocket for streamSid: ${currentStreamSid}`)
      }
    } catch (error) {
      console.log("❌ [AUTO-TERMINATION] Error terminating call:", error.message)
    }
  }

  const terminateCallForDisposition = async (reason = 'auto_disposition') => {
    try {
      console.log(`🛑 [AUTO-TERMINATION] Terminating call due to disposition: ${reason}`)
      // Ensure last TTS/audio is delivered before termination
      await waitForSipQueueDrain(2500)
      
      if (callLogger) {
        callLogger.updateLeadStatus('not_required')
        await callLogger.saveToDatabase('not_required', agentConfig)
        callLogger.cleanup()
      }
      
      if (callId) {
        await disconnectCallViaAPI(callId, reason)
      }
      
      // Close WebSocket and cleanup tracking
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close()
      }
      if (currentStreamSid) {
        activeWebSockets.delete(currentStreamSid)
        console.log(`🔗 [SANPBX-WS-TRACKING] Removed WebSocket for streamSid: ${currentStreamSid}`)
      }
    } catch (error) {
      console.log("❌ [AUTO-TERMINATION] Error terminating call:", error.message)
    }
  }

  // Simple TTS queue to serialize chunk playback and avoid overlaps
  let ttsQueue = []
  let ttsBusy = false
  // Shared Smallest TTS per call to avoid multiple WS connections / 429
  let sharedSmallestTTS = null
  const enqueueTts = async (text, language = "en") => {
    if (!text || !text.trim()) return
    ttsQueue.push({ text: text.trim(), language })
    if (!ttsBusy) {
      processTtsQueue().catch(() => {})
    }
  }
  const processTtsQueue = async () => {
    if (ttsBusy) return
    ttsBusy = true
    try {
      while (ttsQueue.length > 0) {
        const item = ttsQueue.shift()
        try {
          const provider = (ws.sessionAgentConfig?.voiceServiceProvider || "sarvam").toLowerCase()
          if (provider === "smallest") {
            if (!sharedSmallestTTS || sharedSmallestTTS.isInterrupted) {
              sharedSmallestTTS = new SimplifiedSmallestWSTTSProcessor(ws, streamId, callLogger)
            }
            currentTTS = sharedSmallestTTS
            await sharedSmallestTTS.synthesizeAndStream(item.text)
          } else {
          const tts = createTtsProcessor(ws, streamId, callLogger)
          currentTTS = tts
          await tts.synthesizeAndStream(item.text)
          }
        } catch (_) {}
      }
    } finally {
      ttsBusy = false
    }
  }

  // Streaming OpenAI completion that emits partials via callback
  const processWithOpenAIStream = async (
    userMessage,
    conversationHistory,
    agentConfig,
    userName = null,
    onPartial = null,
  ) => {
    const timer = createTimer("LLM_STREAMING")
    let accumulated = ""
    try {
      if (!API_KEYS.openai) {
        console.warn("⚠️ [LLM-STREAM] OPENAI_API_KEY not set; skipping generation")
        return null
      }

      const callerName = (userName || '').trim()
      const basePrompt = resolvePlaceholders((agentConfig?.systemPrompt || "You are a helpful AI assistant. Answer concisely.").trim(), { name: callerName })
      const firstMessage = resolvePlaceholders((agentConfig?.firstMessage || "").trim(), { name: callerName })
      const knowledgeBlock = firstMessage ? `FirstGreeting: "${firstMessage}"\n` : ""
      const detailsText = resolvePlaceholders((agentConfig?.details || "").trim(), { name: callerName })
      const detailsBlock = detailsText ? `Details:\n${detailsText}\n\n` : ""
      const qaItems = Array.isArray(agentConfig?.qa) ? agentConfig.qa : []
      const qaBlock = qaItems.length > 0
        ? `QnA:\n${qaItems.map((item, idx) => {
            const q = resolvePlaceholders((item?.question || "").toString().trim(), { name: callerName })
            const a = resolvePlaceholders((item?.answer || "").toString().trim(), { name: callerName })
            return q && a ? `${idx + 1}. Q: ${q}\n   A: ${a}` : null
          }).filter(Boolean).join("\n")}\n\n`
        : ""
      const policyBlock = [
        "Answer strictly using the information provided above.",
        "If specifics (address/phone/timings) are missing, say you don't have that info.",
        "End with a brief follow-up question.",
        "Keep reply under 100 tokens.",
        "dont give any fornts or styles in it or symbols in it",
        "in which language you get the transcript in same language give response in same language"
      ].join(" ")
      const systemPrompt = `System Prompt:\n${basePrompt}\n\n${detailsBlock}${qaBlock}${knowledgeBlock}${policyBlock}\n\nAnswer strictly using the Details and QnA above. If information is missing, say you don't have that info.`
      console.log(systemPrompt)
      const personalizationMessage = userName && userName.trim()
        ? { role: "system", content: `The user's name is ${userName.trim()}. Address them naturally when appropriate.` }
        : null

      const messages = [
        { role: "system", content: systemPrompt },
        ...(personalizationMessage ? [personalizationMessage] : []),
        ...conversationHistory.slice(-12),
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
          stream: true,
        }),
      })

      if (!response.ok || !response.body) {
        console.error(`❌ [LLM-STREAM] ${timer.end()}ms - HTTP ${response.status}`)
        return null
      }

      const reader = response.body.getReader()
      const decoder = new TextDecoder("utf-8")
      let buffer = ""

      while (true) {
        const { value, done } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split(/\r?\n/)
        buffer = lines.pop() || ""
        for (const line of lines) {
          const trimmed = line.trim()
          if (!trimmed) continue
          if (trimmed === "data: [DONE]") {
            break
          }
          if (trimmed.startsWith("data:")) {
            const jsonStr = trimmed.slice(5).trim()
            try {
              const chunk = JSON.parse(jsonStr)
              const delta = chunk.choices?.[0]?.delta?.content || ""
              if (delta) {
                accumulated += delta
                if (typeof onPartial === "function") {
                  try { await onPartial(accumulated) } catch (_) {}
                }
              }
            } catch (_) {}
          }
        }
      }

      console.log(`🕒 [LLM-STREAM] ${timer.end()}ms - Streaming completed (${accumulated.length} chars)`) 
      return accumulated || null
    } catch (error) {
      console.error(`❌ [LLM-STREAM] ${timer.end()}ms - Error: ${error.message}`)
      return accumulated || null
    }
  }

  const updateLiveCallLog = async () => {
    try {
      if (!callLogId) return
      const transcript = buildFullTranscript()
      const languages = Array.from(new Set([...userTranscripts, ...aiResponses].map(e => e.language).filter(Boolean)))
      await CallLog.findByIdAndUpdate(callLogId, {
        transcript,
        duration: Math.round((new Date() - callStartTime) / 1000),
        'metadata.userTranscriptCount': userTranscripts.length,
        'metadata.aiResponseCount': aiResponses.length,
        'metadata.languages': languages,
        'metadata.lastUpdated': new Date(),
        'metadata.whatsappRequested': !!whatsappRequested,
        'metadata.whatsappMessageSent': !!whatsappSent,
      }).catch(() => {})
    } catch (_) {}
  }

  /**
   * Track response to prevent multiple responses to same input
   */
  const trackResponse = () => {
    const responseId = Date.now() + Math.random()
    activeResponseId = responseId
    return responseId
  }

  /**
   * Check if response is still active
   */
  const isResponseActive = (responseId) => {
    return activeResponseId === responseId
  }

  /**
   * Check for quick responses first (0ms latency)
   */
  const getQuickResponse = (text) => {
    const normalized = text.toLowerCase().trim()
    
    // Direct match
    if (QUICK_RESPONSES[normalized]) {
      return QUICK_RESPONSES[normalized]
    }
    
    // Partial match for common variations
    for (const [key, response] of Object.entries(QUICK_RESPONSES)) {
      if (normalized.includes(key) || key.includes(normalized)) {
        return response
      }
    }
    
    // Handle common variations
    if (normalized.includes("hello") || normalized.includes("hi")) {
      return QUICK_RESPONSES.hello
    }
    
    if (normalized.includes("thank")) {
      return QUICK_RESPONSES["thank you"]
    }
    
    if (normalized.includes("bye") || normalized.includes("goodbye")) {
      return QUICK_RESPONSES.bye
    }
    
    return null
  }

  /**
   * Stream raw PCM (16-bit, mono, 8kHz) audio to SanIPPBX using reverse-media
   * Chunks: 20ms (320 bytes) to match PBX expectations
   */
  const streamAudioToSanIPPBX = async (pcmBase64) => {
    if (!streamId || !callId || !channelId) {
      console.error("[SANPBX] Missing required IDs for streaming")
      return
    }

    try {
      const audioBuffer = Buffer.from(pcmBase64, "base64")
      
      // SanIPPBX format: 8kHz, 16-bit PCM, mono, 20ms chunks
      // 8000 samples/sec * 0.02 sec * 2 bytes = 320 bytes per chunk
      const CHUNK_SIZE = 320 // 20ms chunks for 8kHz 16-bit mono
      const CHUNK_DURATION_MS = 20
      const BYTES_PER_SAMPLE = 2
      const CHANNELS = 1
      const ENCODING = "LINEAR16"
      const SAMPLE_RATE_HZ = 8000
      
      let position = 0
      let currentChunk = 1
      const streamStart = Date.now()
      const streamStartTime = new Date().toISOString()

      console.log(
        `[SANPBX-STREAM] ${streamStartTime} - Starting stream: ${audioBuffer.length} bytes in ${Math.ceil(audioBuffer.length / CHUNK_SIZE)} chunks`,
      )
      console.log(`[SANPBX-STREAM-MODE] json_base64=true`)
      console.log(
        `[SANPBX-FORMAT] Sending audio -> encoding=${ENCODING}, sample_rate_hz=${SAMPLE_RATE_HZ}, channels=${CHANNELS}, bytes_per_sample=${BYTES_PER_SAMPLE}, chunk_duration_ms=${CHUNK_DURATION_MS}, chunk_size_bytes=${CHUNK_SIZE}`,
      )
      console.log(`[SANPBX] StreamID: ${streamId}, CallID: ${callId}, ChannelID: ${channelId}`)

      // Spec conformance pre-check (one-time per stream)
      console.log(
        `[SPEC-CHECK:PRE] event=reverse-media, sample_rate=8000, channels=1, encoding=LINEAR16, chunk_bytes=320, chunk_durn_ms=20`,
      )

      let chunksSuccessfullySent = 0
      let firstChunkSpecChecked = false

      while (position < audioBuffer.length && ws.readyState === WebSocket.OPEN && !sipQueueInterrupted) {
        const chunk = audioBuffer.slice(position, position + CHUNK_SIZE)

        // Pad smaller chunks with silence if needed
        const paddedChunk = chunk.length < CHUNK_SIZE 
          ? Buffer.concat([chunk, Buffer.alloc(CHUNK_SIZE - chunk.length)]) 
          : chunk

        // Prepare payload
        const payloadBase64 = paddedChunk.toString("base64")

        try {
          // JSON reverse-media mode only
          const mediaMessage = {
            event: "reverse-media",
            payload: payloadBase64,
            streamId: streamId,
            channelId: channelId,
            callId: callId,
          }
          if (!firstChunkSpecChecked) {
            const usedEventName = mediaMessage.event
            const sizeOk = paddedChunk.length === CHUNK_SIZE
            const durOk = CHUNK_DURATION_MS === 20
            const fmtOk = ENCODING === "LINEAR16" && SAMPLE_RATE_HZ === 8000 && CHANNELS === 1
            console.log(
              `[SPEC-CHECK:CHUNK#${currentChunk}] event_ok=${usedEventName === 'reverse-media'}, size_ok=${sizeOk} (bytes=${paddedChunk.length}), duration_ok=${durOk} (ms=${CHUNK_DURATION_MS}), format_ok=${fmtOk}`,
            )
            firstChunkSpecChecked = true
          }
          ws.send(JSON.stringify(mediaMessage))
          chunksSuccessfullySent++
          currentChunk++
          if (chunksSuccessfullySent % 20 === 0) {
            console.log(`[SANPBX-STREAM] Sent ${chunksSuccessfullySent} chunks`)
          }
        } catch (error) {
          console.error(`[SANPBX-STREAM] Failed to send chunk ${chunksSuccessfullySent + 1}:`, error.message)
          break
        }

        position += CHUNK_SIZE

        // Wait for chunk duration before sending next chunk
        if (position < audioBuffer.length) {
          // If interrupted, break immediately so next chunk is not sent
          if (sipQueueInterrupted) break
          await new Promise((resolve) => setTimeout(resolve, CHUNK_DURATION_MS))
        }
      }

      // Add silence buffer at the end to ensure clean audio termination (only if not interrupted)
      if (!sipQueueInterrupted) {
        try {
          for (let i = 0; i < 3 && !sipQueueInterrupted; i++) {
            const silenceChunk = Buffer.alloc(CHUNK_SIZE)
            const silenceMessage = {
              event: "reverse-media",
              payload: silenceChunk.toString("base64"),
              streamId: streamId,
              channelId: channelId,
              callId: callId,
            }
            console.log(`[SANPBX-SEND] reverse-media silence chunk #${currentChunk}`)
            ws.send(JSON.stringify(silenceMessage))
            currentChunk++
            await new Promise(r => setTimeout(r, CHUNK_DURATION_MS))
          }
        } catch (error) {
          console.error("[SANPBX-STREAM] Failed to send end silence:", error.message)
        }
      }

      const streamDuration = Date.now() - streamStart
      const completionTime = new Date().toISOString()
      if (sipQueueInterrupted) {
        console.log(
          `[SANPBX-STREAM-INTERRUPTED] ${completionTime} - Interrupted after ${streamDuration}ms, sent ${chunksSuccessfullySent} chunks`,
        )
      } else {
        console.log(
          `[SANPBX-STREAM-COMPLETE] ${completionTime} - Completed in ${streamDuration}ms, sent ${chunksSuccessfullySent} chunks successfully`,
        )
      }
    } catch (error) {
      console.error("[SANPBX-STREAM] Error:", error.message)
    }
  }

  /**
   * Optimized text-to-speech with Sarvam API for 8kHz output
   */
  const synthesizeAndStreamAudio = async (text, language = "en") => {
    try {
      const ttsStartTime = new Date().toISOString()
      console.log(`[TTS-START] ${ttsStartTime} - Starting TTS for: "${text}"`)

      const startTime = Date.now()

      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 3500)

      const response = await fetch("https://api.sarvam.ai/text-to-speech", {
        method: "POST",
      headers: {
          "Content-Type": "application/json",
          "API-Subscription-Key": API_KEYS.sarvam,
          Connection: "keep-alive",
        },
        body: JSON.stringify({
          inputs: [text],
          target_language_code: getSarvamLanguage(language),
          speaker: getValidSarvamVoice(ws.sessionAgentConfig?.voiceSelection || "pavithra"),
          pitch: 0,
          pace: 1.1,
          loudness: 1.0,
          speech_sample_rate: 8000, // FIXED: 8kHz to match SanIPPBX format
          enable_preprocessing: true,
          model: "bulbul:v2",
        }),
        signal: controller.signal,
      })

      clearTimeout(timeoutId)

      if (!response.ok) {
        const errorText = await response.text()
        throw new Error(`Sarvam API error: ${response.status} - ${errorText}`)
      }

      const data = await response.json()
      const audioBase64 = data.audios?.[0]

      if (!audioBase64) {
        throw new Error("No audio data received from Sarvam")
      }

      const ttsTime = Date.now() - startTime
      console.log(`[TTS] Audio generated in ${ttsTime}ms, size: ${audioBase64.length} chars`)

      const streamStartTime = new Date().toISOString()
      console.log(`[SANPBX-STREAM-START] ${streamStartTime} - Starting streaming to SanIPPBX`)

      // Convert WAV (if provided) to raw PCM 16-bit mono 8kHz before streaming
      const pcmBase64 = extractPcmLinear16Mono8kBase64(audioBase64)

      // Enqueue audio to central SIP queue to ensure one-by-one playback
      await enqueueSipAudio(pcmBase64)
      
    } catch (error) {
      console.error("[TTS] Error:", error.message)

      // Send simple silence as fallback
      const fallbackAudio = Buffer.alloc(8000).toString("base64") // 1 second of silence
      await enqueueSipAudio(fallbackAudio)
    }
  }

  /**
   * Ensure base64 audio is raw PCM 16-bit mono @ 8kHz.
   * If it's a WAV (RIFF/WAVE), strip header and return the data chunk.
   */
  const extractPcmLinear16Mono8kBase64 = (audioBase64) => {
    try {
      const buf = Buffer.from(audioBase64, 'base64')
      if (buf.length >= 12 && buf.toString('ascii', 0, 4) === 'RIFF' && buf.toString('ascii', 8, 12) === 'WAVE') {
        // Parse chunks to find 'fmt ' and 'data'
        let offset = 12
        let fmt = null
        let dataOffset = null
        let dataSize = null
        while (offset + 8 <= buf.length) {
          const chunkId = buf.toString('ascii', offset, offset + 4)
          const chunkSize = buf.readUInt32LE(offset + 4)
          const next = offset + 8 + chunkSize
          if (chunkId === 'fmt ') {
            fmt = {
              audioFormat: buf.readUInt16LE(offset + 8),
              numChannels: buf.readUInt16LE(offset + 10),
              sampleRate: buf.readUInt32LE(offset + 12),
              bitsPerSample: buf.readUInt16LE(offset + 22),
            }
          } else if (chunkId === 'data') {
            dataOffset = offset + 8
            dataSize = chunkSize
            break
          }
          offset = next
        }
        if (dataOffset != null && dataSize != null) {
          // Optional: validate fmt, but still proceed to avoid blocking audio
          const dataBuf = buf.slice(dataOffset, dataOffset + dataSize)
          return dataBuf.toString('base64')
        }
      }
      // Assume it's already raw PCM
      return audioBase64
    } catch (e) {
      return audioBase64
    }
  }

  /**
   * Connect to Deepgram with enhanced language detection and processing
   */
  const connectToDeepgram = async () => {
    try {
      const deepgramLanguage = getDeepgramLanguage(currentLanguage)

      const deepgramUrl = new URL("wss://api.deepgram.com/v1/listen")
        deepgramUrl.searchParams.append("sample_rate", "44100")
        deepgramUrl.searchParams.append("channels", "1")
        deepgramUrl.searchParams.append("encoding", "linear16")
        deepgramUrl.searchParams.append("model", "nova-2")
        deepgramUrl.searchParams.append("language", deepgramLanguage)
        deepgramUrl.searchParams.append("interim_results", "true")

      deepgramWs = new WebSocket(deepgramUrl.toString(), {
        headers: { Authorization: `Token ${API_KEYS.deepgram}` },
      })

      deepgramWs.onopen = () => {
        console.log("🎤 [DEEPGRAM] Connection established")
        deepgramReady = true
        console.log("🎤 [DEEPGRAM] Processing queued audio packets:", deepgramAudioQueue.length)
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
        // Reset silence timer on any Deepgram transcript activity (interim or final)
        try { resetSilenceTimeout() } catch (_) {}
        if (is_final) {
          // Immediate interruption on final user utterance
          const interruptStartTime = Date.now()
          if (currentTTS && isProcessing) {
            // Stop SIP queue first to drop any pending audio instantly
            interruptSipQueue()
            try { currentTTS.interrupt() } catch (_) {}
            isProcessing = false
            processingRequestId++
            // Wait until current 20ms send finishes, then allow new audio
            await waitForSipHaltAndReset(500)
          }

          console.log(`🕒 [STT-TRANSCRIPTION] ${sttTimer.end()}ms - Text: "${transcript.trim()}"`)
          sttTimer = null

          // Add to conversation history and process
          history.addUserTranscript(transcript.trim())
          
          // Log to database for persistence
          if (callLogger && transcript.trim()) {
            callLogger.logUserTranscript(transcript.trim(), (agentConfig?.language || 'en').toLowerCase())
          }
          
          await processUserUtterance(transcript.trim())
        } else {
          // Immediate interrupt on ANY meaningful interim to stop current response
          try { history.handleInterimTranscript(transcript.trim()) } catch (_) {}
          if (currentTTS && isProcessing) {
            console.log(`⚡ [INTERIM-INTERRUPT] Immediate stop on interim: "${transcript.trim()}"`)
            // Stop SIP queue first to drop any pending audio instantly
            interruptSipQueue()
            try { currentTTS.interrupt() } catch (_) {}
            isProcessing = false
            processingRequestId++
            // Wait until current 20ms send finishes, then allow new audio
            await waitForSipHaltAndReset(500)
          }
        }
      }
    } else if (data.type === "UtteranceEnd") {
      console.log("🔄 [DEEPGRAM] Utterance ended")
    }
  }

  const processUserUtterance = async (text) => {
      // If we already triggered closing, ignore any further inputs. If it's just an ACK, end immediately.
      // if (closingState.ignoreFurtherInputs) {
      //   if (POST_GOODBYE_IGNORE_REGEX.test(text || '')) {
      //     console.log('🛑 [POST-CLOSING] Ack/bye detected → immediate hangup')
      //     await performCallEnd('post_goodbye_ack')
      //   } else {
      //     console.log('🛑 [POST-CLOSING] Ignoring user input after closing trigger')
      //   }
      //   return
      // }
    if (!text.trim() || text === lastProcessedTranscript) return

    console.log("🗣️ [USER-UTTERANCE] ========== USER SPEECH ==========")
    console.log("🗣️ [USER-UTTERANCE] Text:", text.trim())
    console.log("🗣️ [USER-UTTERANCE] Current Language:", currentLanguage)

    if (currentTTS) {
      console.log("🛑 [USER-UTTERANCE] Interrupting current TTS...")
      currentTTS.interrupt()
      interruptSipQueue() // Immediately stop SIP audio queue
    }

    isProcessing = true
    lastProcessedTranscript = text
    const currentRequestId = ++processingRequestId
    
    // Reset SIP queue for new processing
    resetSipQueue()

    try {
      const detectedLanguage = "en"
      console.log("🌐 [USER-UTTERANCE] Detected Language:", detectedLanguage)


      // Reset silence timeout on user activity
      resetSilenceTimeout()
      
      // Immediate intent handling (zero-latency)
      if (shouldTerminateFast(text)) {
        console.log("🛑 [USER-UTTERANCE] Fast intent detected: strict NO → terminate")
        try { currentTTS?.interrupt?.() } catch (_) {}
        interruptSipQueue() // Immediately stop SIP audio queue
        await terminateCallForDisposition('user_not_interested')
        return
      }
      if (indicatesWaitOrThinking(text)) {
        console.log("⏳ [USER-UTTERANCE] Wait/Thinking detected → continue without termination")
      }
      
      // Auto disposition detection (non-blocking, no added latency)
      if (autoDispositionEnabled) {
        ;(async () => {
          try {
            console.log("🔍 [USER-UTTERANCE] Running auto disposition detection (async)...")
            const dispositionResult = await detectAutoDisposition(text, history.getConversationHistory(), detectedLanguage)
            if (dispositionResult === "TERMINATE") {
              console.log("🛑 [USER-UTTERANCE] Auto disposition detected (async) - terminating call")
              try { currentTTS?.interrupt?.() } catch (_) {}
              await terminateCallForDisposition('user_not_interested')
            }
          } catch (_) {}
        })()
      }
      
      // Use streaming path immediately (like testing2) so partials can play
      let aiResponse = null
      const tts = createTtsProcessor(ws, streamId, callLogger)
      currentTTS = tts
      // Sentence-based streaming (enqueue each complete sentence as it forms)
      let lastLen = 0
      let carry = ""
      let completeSentences = []
      const takeBatch = () => {
        if (completeSentences.length < 2) return null
        const batch = []
        batch.push(completeSentences.shift())
        batch.push(completeSentences.shift())
        if (completeSentences.length > 0) {
          const currentLen = batch.reduce((s, t) => s + t.length, 0)
          if (currentLen + completeSentences[0].length < 150) {
            batch.push(completeSentences.shift())
          }
        }
        return batch
      }
      aiResponse = await processWithOpenAIStream(
        text,
        history.getConversationHistory(),
        agentConfig,
        userName,
        async (partial) => {
          if (processingRequestId !== currentRequestId) return
          if (!partial || partial.length <= lastLen) return
          const delta = carry + partial.slice(lastLen)
          const sentences = splitIntoSentences(delta)
          // If delta doesn't end with terminator, keep last as carry
          const endsWithTerminator = /[.!?]\s*$/.test(delta)
          const full = endsWithTerminator ? sentences : sentences.slice(0, -1)
          for (const s of full) {
            const trimmed = s.trim()
            if (trimmed) completeSentences.push(trimmed)
          }
          carry = endsWithTerminator ? "" : (sentences[sentences.length - 1] || "")
          // Only send batches of 2–3 sentences; do not send singles during streaming
          let batch
          while ((batch = takeBatch())) {
            const combined = batch.join(' ')
            try { await tts.enqueueText(combined) } catch (_) {}
          }
          lastLen = partial.length
        }
      )

      // Final flush: include carry and any remaining sentences as up to 3 combined
      if (processingRequestId === currentRequestId) {
        if (carry && carry.trim()) completeSentences.push(carry.trim())
        if (completeSentences.length > 0) {
          const batch = []
          while (batch.length < 3 && completeSentences.length > 0) batch.push(completeSentences.shift())
          const combined = batch.join(' ')
          try { await currentTTS.enqueueText(combined) } catch (_) {}
        }
      }
      
      
      // if (disconnectionIntent === "DISCONNECT") {
      //   console.log("🛑 [USER-UTTERANCE] User wants to disconnect - waiting 2 seconds then ending call")
        
      //   // Wait 2 seconds to ensure last message is processed, then terminate
      //   setTimeout(async () => {
      //     if (callLogger) {
      //       try {
      //         await callLogger.saveToDatabase(callLogger.currentLeadStatus || "maybe")
      //         console.log("✅ [USER-UTTERANCE] Call terminated after 2 second delay")
      //       } catch (err) {
      //         console.log(`⚠️ [USER-UTTERANCE] Termination error: ${err.message}`)
      //       }
      //     }
      //   }, 2000)
        
      //   return
      // }

      if (processingRequestId === currentRequestId && aiResponse) {
        console.log("🤖 [USER-UTTERANCE] AI Response (streamed):", aiResponse)
        // Do NOT TTS the full response here – partials already queued
        
        // Save the complete AI response as a single entry
        try {
          if (callLogger && aiResponse && aiResponse.trim()) {
            callLogger.logAIResponse(aiResponse.trim(), (agentConfig?.language || 'en').toLowerCase())
          }
        } catch (_) {}

        // Add to conversation history
        history.addAssistantResponse(aiResponse)
        
        console.log("✅ [USER-UTTERANCE] Processing completed")
      } else {
        console.log("⏭️ [USER-UTTERANCE] Processing skipped (newer request in progress)")
      }
    } catch (error) {
      console.log("❌ [USER-UTTERANCE] Error processing utterance:", error.message)
    } finally {
      if (processingRequestId === currentRequestId) {
        isProcessing = false
      }
      console.log("🗣️ [USER-UTTERANCE] ======================================")
    }
  }

  // Simplified TTS processor (aligned with testing2.js behavior + SanPBX transport)
  class SimplifiedSarvamTTSProcessor {
    constructor(ws, streamSid, callLogger = null) {
      this.ws = ws
      this.streamSid = streamSid
      this.callLogger = callLogger
      this.sarvamLanguage = getSarvamLanguage((ws.sessionAgentConfig?.language || 'en').toLowerCase())
      this.voice = getValidSarvamVoice(ws.sessionAgentConfig?.voiceSelection || "pavithra")
      this.isInterrupted = false
      this.currentAudioStreaming = null
      this.totalAudioBytes = 0
      this.pendingQueue = [] // { text, audioBase64, preparing }
      this.isProcessingQueue = false
    }

    interrupt() {
      this.isInterrupted = true
      if (this.currentAudioStreaming) {
        this.currentAudioStreaming.interrupt = true
      }
      // Clear FIFO queue on interruption for proper interpretation
      try { this.pendingQueue = [] } catch (_) {}
    }

    reset() {
      this.interrupt()
      this.isInterrupted = false
      this.totalAudioBytes = 0
    }

    async synthesizeAndStream(text) {
      if (this.isInterrupted) return

      const timer = createTimer("TTS_SYNTHESIS")
      try {
        const response = await fetch("https://api.sarvam.ai/text-to-speech", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "API-Subscription-Key": API_KEYS.sarvam,
          },
          body: JSON.stringify({
            inputs: [text],
            target_language_code: this.sarvamLanguage,
            speaker: this.voice,
            pitch: 0,
            pace: 1.0,
            loudness: 1.0,
            speech_sample_rate: 8000,
            enable_preprocessing: true,
            model: "bulbul:v2",
          }),
        })

        if (!response.ok || this.isInterrupted) {
          if (!this.isInterrupted) {
            console.log(`❌ [TTS-SYNTHESIS] ${timer.end()}ms - Error: ${response.status}`)
            throw new Error(`Sarvam API error: ${response.status}`)
          }
          return
        }

        const responseData = await response.json()
        const audioBase64 = responseData.audios?.[0]
        if (!audioBase64 || this.isInterrupted) {
          if (!this.isInterrupted) {
            console.log(`❌ [TTS-SYNTHESIS] ${timer.end()}ms - No audio data received`)
            throw new Error("No audio data received from Sarvam API")
          }
          return
        }

        console.log(`🕒 [TTS-SYNTHESIS] ${timer.end()}ms - Audio generated`)
        if (!this.isInterrupted) {
          // Strip WAV header if present; send raw PCM 16-bit mono @ 8kHz
          const pcmBase64 = extractPcmLinear16Mono8kBase64(audioBase64)
          await this.streamAudioOptimizedForSIP(pcmBase64)
          const audioBuffer = Buffer.from(pcmBase64, "base64")
          this.totalAudioBytes += audioBuffer.length
        }
      } catch (error) {
        if (!this.isInterrupted) {
          console.log(`❌ [TTS-SYNTHESIS] ${timer.end()}ms - Error: ${error.message}`)
          throw error
        }
      }
    }

    async synthesizeToBuffer(text) {
      const timer = createTimer("TTS_PREPARE")
      const response = await fetch("https://api.sarvam.ai/text-to-speech", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "API-Subscription-Key": API_KEYS.sarvam,
        },
        body: JSON.stringify({
          inputs: [text],
          target_language_code: this.sarvamLanguage,
          speaker: this.voice,
          pitch: 0,
          pace: 1.0,
          loudness: 1.0,
          speech_sample_rate: 8000,
          enable_preprocessing: true,
          model: "bulbul:v2",
        }),
      })
      if (!response.ok) {
        console.log(`❌ [TTS-PREPARE] ${timer.end()}ms - Error: ${response.status}`)
        throw new Error(`Sarvam API error: ${response.status}`)
      }
      const responseData = await response.json()
      const audioBase64 = responseData.audios?.[0]
      if (!audioBase64) {
        console.log(`❌ [TTS-PREPARE] ${timer.end()}ms - No audio data received`)
        throw new Error("No audio data received from Sarvam API")
      }
      console.log(`🕒 [TTS-PREPARE] ${timer.end()}ms - Audio prepared`)
      return audioBase64
    }

    async enqueueText(text) {
      if (this.isInterrupted) return
      const item = { text, audioBase64: null, preparing: true }
      this.pendingQueue.push(item)
      ;(async () => {
        try {
          item.audioBase64 = await this.synthesizeToBuffer(text)
        } catch (_) {
          item.audioBase64 = null
        } finally {
          item.preparing = false
        }
      })()
      if (!this.isProcessingQueue) {
        this.processQueue().catch(() => {})
      }
    }

    async processQueue() {
      if (this.isProcessingQueue) return
      this.isProcessingQueue = true
      try {
        while (!this.isInterrupted && this.pendingQueue.length > 0) {
          const item = this.pendingQueue[0]
          if (!item.audioBase64) {
            let waited = 0
            while (!this.isInterrupted && item.preparing && waited < 3000) {
              await new Promise(r => setTimeout(r, 20))
              waited += 20
            }
          }
          if (this.isInterrupted) break
          const audioBase64 = item.audioBase64
          this.pendingQueue.shift()
          if (audioBase64) {
            const pcmBase64 = extractPcmLinear16Mono8kBase64(audioBase64)
            await this.streamAudioOptimizedForSIP(pcmBase64)
            // Small gap to avoid chunk boundary artifacts between enqueued items
            await new Promise(r => setTimeout(r, 60))
          }
        }
      } finally {
        this.isProcessingQueue = false
      }
    }

    async streamAudioOptimizedForSIP(audioBase64) {
      if (this.isInterrupted) return
      // Route through central SIP queue to serialize playback
      try {
        await enqueueSipAudio(audioBase64)
      } finally {
        this.currentAudioStreaming = null
      }
    }

    getStats() {
      return { totalAudioBytes: this.totalAudioBytes }
    }
  }

  // WebSocket-based Smallest.ai TTS processor (mirrors aitota.js behavior)
  class SimplifiedSmallestWSTTSProcessor {
    constructor(ws, streamSid, callLogger = null) {
      this.ws = ws
      this.streamSid = streamSid
      this.callLogger = callLogger
      this.isInterrupted = false
      this.currentAudioStreaming = null
      this.smallestWs = null
      this.smallestReady = false
      this.keepAliveTimer = null
      this.connectionRetryCount = 0
      this.maxRetries = 3
      this.lastActivity = Date.now()
      this.requestId = 0
      this.pendingRequests = new Map() // id -> { resolve, reject, audioChunks: [] }
      this.isProcessingQueue = false
      this.pendingQueue = [] // text FIFO
  	  this.recentTexts = new Set()
  	  this.lastQueuedText = null
  	  this.generation = 0
      this.lastRequestedText = null
      this.lastRequestTime = 0
      this.unknownChunkSeen = false
      this.unknownCompleteFallbackCount = 0
    }

    interrupt() {
      this.isInterrupted = true
  	  this.generation++
      if (this.currentAudioStreaming) {
        this.currentAudioStreaming.interrupt = true
      }
      if (this.keepAliveTimer) {
        clearInterval(this.keepAliveTimer)
        this.keepAliveTimer = null
      }
      try { if (this.smallestWs && this.smallestWs.readyState === WebSocket.OPEN) this.smallestWs.close(1000, "interrupted") } catch (_) {}
      this.smallestWs = null
      this.smallestReady = false
      this.pendingRequests.clear()
      this.pendingQueue = []
    }

    startKeepAlive() {
      if (this.keepAliveTimer) clearInterval(this.keepAliveTimer)
      this.keepAliveTimer = setInterval(() => {
        if (!this.smallestWs || this.smallestWs.readyState !== WebSocket.OPEN || this.isInterrupted) return
        try {
          if (typeof this.smallestWs.ping === 'function') this.smallestWs.ping()
          else this.smallestWs.send(JSON.stringify({ type: "ping" }))
        } catch (_) {}
      }, 30000)
    }

    async connectToSmallest() {
      if (this.smallestWs && this.smallestWs.readyState === WebSocket.OPEN) return
      if (!API_KEYS.smallest) throw new Error("Smallest API key not configured")

      // Try a few times with short backoff to mitigate transient timeouts
      let attempt = 0
      const maxAttempts = Math.max(this.maxRetries, 3)
      while (attempt < maxAttempts) {
        attempt++
        try {
      await new Promise((resolve, reject) => {
        try {
          const wsConn = new WebSocket("wss://waves-api.smallest.ai/api/v1/lightning-v2/get_speech/stream", {
            headers: { Authorization: `Bearer ${API_KEYS.smallest}` },
          })
          this.smallestWs = wsConn
              const timeout = setTimeout(() => reject(new Error("Smallest WS connect timeout")), 10000)
          wsConn.onopen = () => {
            clearTimeout(timeout)
            this.smallestReady = true
            this.connectionRetryCount = 0
            this.startKeepAlive()
            resolve()
          }
          wsConn.onmessage = (evt) => {
            try { this.handleSmallestMessage(JSON.parse(evt.data)) } catch (_) {}
          }
          wsConn.onclose = (event) => {
            this.smallestReady = false
            if (this.keepAliveTimer) { clearInterval(this.keepAliveTimer); this.keepAliveTimer = null }
            this.smallestWs = null
            // Auto-reconnect within retry limit
            if (!this.isInterrupted && event.code !== 1000 && this.connectionRetryCount < this.maxRetries) {
              this.connectionRetryCount++
              setTimeout(() => {
                if (!this.isInterrupted) {
                  this.connectToSmallest().catch(() => {})
                }
              }, 2000)
            }
          }
          wsConn.onerror = () => {}
        } catch (e) { reject(e) }
      })
          // Connected successfully, exit loop
          return
        } catch (e) {
          if (attempt >= maxAttempts) {
            throw e
          }
          // Backoff before retrying
          await new Promise(r => setTimeout(r, 600 * attempt))
        }
      }
    }

    handleSmallestMessage(msg) {
      const rid = msg?.request_id
      const status = msg?.status || msg?.type
      try { if (status) console.log(`[SMALLEST-TTS] onmessage status=${status} rid=${rid || 'unknown'}`) } catch (_) {}

      const streamIncomingAudio = (base64) => {
        try {
          const pcmChunk = extractPcmLinear16Mono8kBase64(base64)
          enqueueSipAudio(pcmChunk).catch(() => {})
        } catch (_) {}
      }

      if (!rid || !this.pendingRequests.has(Number(rid))) {
        // Unknown request_id: still stream chunks/success audio
        if (status === 'audio_chunk' || status === 'chunk') {
          const incomingAudio = (typeof msg.audio === 'string') ? msg.audio : (typeof msg.data?.audio === 'string' ? msg.data.audio : null)
          if (incomingAudio) {
            this.unknownChunkSeen = true
            streamIncomingAudio(incomingAudio)
            try { console.log(`⚠️ [SMALLEST-TTS] Streamed CHUNK for unknown request_id: ${rid || 'unknown'}`) } catch (_) {}
          }
          return
        }
        if ((status === 'completed' || status === 'complete' || status === 'done' || status === 'success')) {
          const successAudio = (typeof msg.audio === 'string') ? msg.audio : (typeof msg.data?.audio === 'string' ? msg.data.audio : null)
          if (successAudio) {
            streamIncomingAudio(successAudio)
            try { console.log(`⚠️ [SMALLEST-TTS] Streamed SUCCESS for unknown request_id: ${rid || 'unknown'}`) } catch (_) {}
            return
          }
          // No audio present: optionally retry last text once within window
          try {
            const withinWindow = (Date.now() - this.lastRequestTime) < 4000
            if (withinWindow && this.lastRequestedText && this.unknownCompleteFallbackCount < 1 && !this.isInterrupted && !this.unknownChunkSeen) {
              this.unknownCompleteFallbackCount++
              console.log(`🔄 [SMALLEST-TTS] Unknown complete without audio → retry last text once: "${String(this.lastRequestedText).slice(0, 60)}..."`)
              this.enqueueText(this.lastRequestedText)
            } else {
              console.log(`⚠️ [SMALLEST-TTS] Unknown ${status} without audio; skipping retry (window/met conditions false)`) 
            }
          } catch (_) {}
          return
        }
        if (status === 'error') {
          try { console.log(`⚠️ [SMALLEST-TTS] Error for unknown request_id: ${rid || 'unknown'} - ${msg?.message || 'Unknown error'}`) } catch (_) {}
        }
        return
      }

      // Known request_id flow
      const req = this.pendingRequests.get(Number(rid))
      if (!req) return

      if (status === 'audio_chunk' || status === 'chunk') {
        const incomingAudio = (typeof msg.audio === 'string') ? msg.audio : (typeof msg.data?.audio === 'string' ? msg.data.audio : null)
        if (incomingAudio) {
          if (!req.audioChunks) req.audioChunks = []
          req.audioChunks.push(incomingAudio)
          streamIncomingAudio(incomingAudio)
        }
	  } else if (status === 'completed' || status === 'complete' || status === 'done' || status === 'success') {
	    try { req.resolve('ALREADY_STREAMED') } catch (_) {}
        this.pendingRequests.delete(Number(rid))
      } else if (status === 'error') {
        this.retryRequest(Number(rid), req)
      }
    }

    async processSingle(text) {
      // Ensure connection; on failure, fall back to brief silence to keep flow
      try {
      await this.connectToSmallest()
      } catch (connectError) {
        const id = ++this.requestId
        return new Promise((resolve, reject) => {
          const request = { requestId: id, resolve, reject, audioChunks: [], text }
          this.pendingRequests.set(id, request)
          console.log(`[SMALLEST-TTS] WS synth error: ${connectError.message}`)
          this.createFallbackAudio(text || " ", request)
        })
      }

      const id = ++this.requestId
      return new Promise((resolve, reject) => {
        this.pendingRequests.set(id, { resolve, reject, audioChunks: [], text })

        // Build payload mirroring aitota.js
        const agentCfg = this.ws?.sessionAgentConfig || {}
        let voiceId = agentCfg.voiceId || 'ryan'
        if (!voiceId && agentCfg.voiceSelection) {
          const voiceMap = { 'male-professional': 'ryan', 'female-professional': 'sarah', 'male-friendly': 'ryan', 'female-friendly': 'sarah', 'neutral': 'ryan' }
          voiceId = voiceMap[String(agentCfg.voiceSelection).toLowerCase()] || 'ryan'
        }
        const lang2 = (agentCfg.language || 'en').toLowerCase()
        const language = lang2 === 'hi' ? 'hi' : 'en'

        const payload = {
          request_id: String(id),
          voice_id: voiceId,
          text: text,
          max_buffer_flush_ms: 0,
          continue: false,
          flush: false,
          language,
          sample_rate: 8000,
          speed: 1,
          consistency: 0.5,
          enhancement: 1,
          similarity: 0,
        }
        try { console.log(`[SMALLEST-TTS] send request ${payload.request_id}, voice_id=${payload.voice_id}, lang=${payload.language}`) } catch (_) {}
        try { this.smallestWs.send(JSON.stringify(payload)) } catch (e) { reject(e) }

        // Track last text/time for unknown-id fallback
        try { this.lastRequestedText = text; this.lastRequestTime = Date.now() } catch (_) {}

        // Adaptive timeout: if we streamed any chunks (known or unknown), treat as completed
        setTimeout(() => {
          const req = this.pendingRequests.get(id)
          if (!req) return
          const hadChunks = Array.isArray(req.audioChunks) && req.audioChunks.length > 0
          const unknownSeen = this.unknownChunkSeen === true
          try { console.log(`⏰ [SMALLEST-TTS] Request ${id} timed out after 15000ms (hadChunks=${hadChunks}, unknownChunksSeen=${unknownSeen}, chunks=${(req.audioChunks||[]).length})`) } catch (_) {}
          this.pendingRequests.delete(id)
          if (hadChunks || unknownSeen) {
            try { resolve('ALREADY_STREAMED') } catch (_) {}
          } else {
            reject(new Error('TTS request timeout'))
          }
        }, 15000)
      })
    }

    async enqueueText(text) {
      if (this.isInterrupted) return
  	  try {
  	    const normalized = String(text || '').trim()
  	    if (!normalized) return
  	    if (this.lastQueuedText && this.lastQueuedText === normalized) return
  	    if (this.recentTexts.has(normalized)) return
  	    this.lastQueuedText = normalized
  	    this.recentTexts.add(normalized)
  	    // keep recentTexts small
  	    if (this.recentTexts.size > 8) {
  	      const first = this.recentTexts.values().next().value
  	      this.recentTexts.delete(first)
  	    }
  	  } catch (_) {}
      this.pendingQueue.push(text)
      if (!this.isProcessingQueue) this.processQueue().catch(() => {})
    }

    async synthesizeAndStream(text) {
      return this.enqueueText(text)
    }

    async processQueue() {
      if (this.isProcessingQueue) return
      this.isProcessingQueue = true
  	  const startedGeneration = this.generation
      try {
  	    while (!this.isInterrupted && this.pendingQueue.length > 0) {
  	      if (startedGeneration !== this.generation) break
          const text = this.pendingQueue.shift()
          let audioBase64 = null
          try {
            audioBase64 = await this.processSingle(text)
          } catch (e) {
            if (this.isInterrupted) break
            console.log(`[SMALLEST-TTS] WS synth error: ${e.message}`)
            audioBase64 = null
          }
          if (this.isInterrupted) break
  	      // Skip streaming if chunks were already streamed live
  	      if (audioBase64 && audioBase64 !== 'ALREADY_STREAMED') {
            const pcm = extractPcmLinear16Mono8kBase64(audioBase64)
            await this.streamAudioOptimizedForSIP(pcm)
            await new Promise(r => setTimeout(r, 60))
          }
        }
      } finally {
        this.isProcessingQueue = false
      }
    }

    async streamAudioOptimizedForSIP(audioBase64) {
      if (this.isInterrupted) return
      const audioBuffer = Buffer.from(audioBase64, "base64")
      const streamingSession = { interrupt: false }
      this.currentAudioStreaming = streamingSession
  	  const currentGeneration = this.generation
      const CHUNK_SIZE = 320
      let position = 0
      // Pre-roll one silence chunk to avoid first-chunk clipping on SIP
      try {
        const silence = Buffer.alloc(CHUNK_SIZE)
        const silenceMsg = { event: "reverse-media", payload: silence.toString("base64"), streamId: this.streamSid, channelId: channelId, callId: callId }
        if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted) {
          this.ws.send(JSON.stringify(silenceMsg))
          await new Promise(r => setTimeout(r, 20))
        }
      } catch (_) {}
  	  while (position < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
  	    if (currentGeneration !== this.generation) break
        const chunk = audioBuffer.slice(position, position + CHUNK_SIZE)
        const padded = chunk.length < CHUNK_SIZE ? Buffer.concat([chunk, Buffer.alloc(CHUNK_SIZE - chunk.length)]) : chunk
        // Route through central SIP queue
        try { await enqueueSipAudio(padded.toString("base64")) } catch (_) { break }
        position += CHUNK_SIZE
        if (position < audioBuffer.length && !this.isInterrupted) await new Promise(r => setTimeout(r, 20))
      }
      this.currentAudioStreaming = null
    }

    async retryRequest(requestId, request) {
      if (this.connectionRetryCount >= this.maxRetries) {
        // Fallback audio to keep flow
        this.createFallbackAudio(" ", request)
        return
      }
      this.connectionRetryCount++
      try {
        await new Promise(r => setTimeout(r, 800))
        if (!this.smallestReady || !this.smallestWs || this.smallestWs.readyState !== WebSocket.OPEN) {
          await this.connectToSmallest()
        }
        // Re-send with same text if available in queue context is lost; resolve with fallback
        this.createFallbackAudio(" ", request)
      } catch (_) {
        this.createFallbackAudio(" ", request)
      }
    }

    createFallbackAudio(text, request) {
      try {
        const fallbackAudio = Buffer.alloc(1600).toString('base64') // ~100ms silence
        this.streamAudioOptimizedForSIP(fallbackAudio)
          .then(() => { try { request.resolve("FALLBACK_COMPLETED") } catch (_) {} })
          .catch(() => { try { request.reject(new Error('fallback_failed')) } catch (_) {} })
          .finally(() => { this.pendingRequests.delete(request.requestId || 0) })
      } catch (_) {
        try { request.reject(new Error('fallback_error')) } catch (_) {}
      }
    }
  }

  // TTS factory to choose provider per agent
  const createTtsProcessor = (ws, streamSid, callLogger) => {
    const provider = (ws.sessionAgentConfig?.voiceServiceProvider || "sarvam").toLowerCase()
    if (provider === "smallest") {
      return new SimplifiedSmallestWSTTSProcessor(ws, streamSid, callLogger)
    }
    return new SimplifiedSarvamTTSProcessor(ws, streamSid, callLogger)
  }

  /**
   * Optimized AI response with parallel processing
   */
  const getAIResponse = async (userMessage) => {
    try {
      console.log(`[LLM] Processing: "${userMessage}"`)
      const startTime = Date.now()

      // Check for quick responses first
      const quickResponse = getQuickResponse(userMessage)
      if (quickResponse) {
        console.log(`[LLM] Quick response: "${quickResponse}" (0ms)`)
        return quickResponse
      }

      // Mirror aitota.js prompt structure
      const callerName = (sessionUserName || '').trim()
      const basePrompt = resolvePlaceholders((ws.sessionAgentConfig?.systemPrompt || "You are a helpful AI assistant.").trim(), { name: callerName })
      const firstMessage = resolvePlaceholders((ws.sessionAgentConfig?.firstMessage || "").trim(), { name: callerName })
      const knowledgeBlock = firstMessage ? `FirstGreeting: "${firstMessage}"\n` : ""
      
      // Get policy block from SystemPrompt database (with fallback)
      const policyBlock = await getSystemPromptWithCache()
      
      const systemPrompt = `System Prompt:\n${basePrompt}\n\n${knowledgeBlock}${policyBlock}`
      const personalizationMessage = callerName
        ? { role: "system", content: `The user's name is ${callerName}. Address them by name naturally when appropriate.` }
        : null

      const messages = [
        { role: "system", content: systemPrompt },
        ...(personalizationMessage ? [personalizationMessage] : []),
        ...conversationHistory.slice(-6),
        { role: "user", content: userMessage },
      ]

      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 2500)

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
          stream: false,
        }),
        signal: controller.signal,
      })

      clearTimeout(timeoutId)

      if (!response.ok) {
        throw new Error(`OpenAI API error: ${response.status}`)
      }

      const data = await response.json()
      let aiResponse = data.choices[0]?.message?.content?.trim()

      // Ensure a follow-up question exists (mirrors aitota.js)
      if (aiResponse && !/[?]\s*$/.test(aiResponse)) {
        const lang = (ws.sessionAgentConfig?.language || "en").toLowerCase()
        const followUps = {
          hi: "क्या मैं और किसी बात में आपकी मदद कर सकता/सकती हूँ?",
          en: "Is there anything else I can help you with?",
          bn: "আর কিছু কি আপনাকে সাহায্য করতে পারি?",
          ta: "வேறு எதற்காவது உதவி வேண்டுமா?",
          te: "ఇంకేమైనా సహాయం కావాలా?",
          mr: "आणखी काही मदत हवी आहे का?",
          gu: "શું બીજી કોઈ મદદ કરી શકું?",
        }
        aiResponse = `${aiResponse} ${followUps[lang] || followUps.en}`.trim()
      }

      const llmTime = Date.now() - startTime
      console.log(`[LLM] Response: "${aiResponse}" (${llmTime}ms)`)
      return aiResponse
    } catch (error) {
      console.error("[LLM] Error:", error.message)
      return "I apologize, but I encountered an issue. Could you please try again?"
    }
  }

  /**
   * Process user speech input with duplicate prevention and response tracking
   */
  const processUserInput = async (transcript) => {
    const responseId = trackResponse()
    
    if (!transcript.trim()) return
    if (isProcessing) {
      console.log(`[PROCESS] Busy. Skipping new transcript while speaking: "${transcript}"`)
      return
    }

    // Prevent duplicate processing of same transcript within 1.2 seconds
    const now = Date.now()
    if (transcript === lastProcessedTranscript && (now - lastProcessedTime) < 1200) {
      console.log(`[PROCESS] Skipping duplicate transcript: "${transcript}"`)
      return
    }

    lastProcessedTranscript = transcript
    lastProcessedTime = now
    
    isProcessing = true
    const totalStart = Date.now()
    console.log(`[PROCESS] Starting processing for: "${transcript}" (ID: ${responseId})`)

    try {
      if (!isResponseActive(responseId)) {
        console.log(`[PROCESS] Response ${responseId} cancelled - newer request in progress`)
        return
      }

      const quickResponse = getQuickResponse(transcript)

      if (quickResponse && isResponseActive(responseId)) {
        console.log(`[PROCESS] Quick response found: "${quickResponse}"`)
        history.addUserTranscript(transcript)
        history.addAssistantResponse(quickResponse)
        
        // Log to database for persistence
        if (callLogger && transcript.trim()) {
          callLogger.logUserTranscript(transcript.trim(), (ws.sessionAgentConfig?.language || 'en').toLowerCase())
        }
        try {
          aiResponses.push({
            type: 'ai',
            text: quickResponse,
            language: (ws.sessionAgentConfig?.language || 'en').toLowerCase(),
            timestamp: new Date(),
          })
        } catch (_) {}
        // Live update after AI response
        await updateLiveCallLog()
        try {
          const tts = createTtsProcessor(ws, streamId, callLogger)
          currentTTS = tts
          await tts.synthesizeAndStream(quickResponse)
        } catch (_) {}
        console.log(`[PROCESS] TTS finished for quick response.`)
      } else if (isResponseActive(responseId)) {
        console.log(`[PROCESS] Getting AI response (streaming) for: "${transcript}"`)

        let lastLen = 0
        // Accumulate complete sentences and send in batches of 2-3 like aitota.js
        let carry = ""
        let completeSentences = []
        const takeBatch = () => {
          if (completeSentences.length < 2) return null
          const batch = []
          // Always take 2
          batch.push(completeSentences.shift())
          batch.push(completeSentences.shift())
          // Optionally take a 3rd if total would be short
          if (completeSentences.length > 0) {
            const currentLen = batch.reduce((s, t) => s + t.length, 0)
            if (currentLen + completeSentences[0].length < 150) {
              batch.push(completeSentences.shift())
            }
          }
          return batch
        }
        const tts = createTtsProcessor(ws, streamSid, callLogger)
        currentTTS = tts

        const finalResponse = await processWithOpenAIStream(
          transcript,
          history.getConversationHistory(),
          ws.sessionAgentConfig || {},
          sessionUserName,
          async (partial) => {
            if (!isResponseActive(responseId)) return
            if (!partial || partial.length <= lastLen) return
            const delta = (carry + partial.slice(lastLen)).trim()
            lastLen = partial.length
            if (!delta) return
            const sentences = splitIntoSentences(delta)
            const endsWithTerminator = /[.!?]\s*$/.test(delta)
            const full = endsWithTerminator ? sentences : sentences.slice(0, -1)
            for (const s of full) {
              const trimmed = s.trim()
              if (trimmed) completeSentences.push(trimmed)
            }
            carry = endsWithTerminator ? "" : (sentences[sentences.length - 1] || "")
            // Send batches of 2-3 sentences
            let batch
            while ((batch = takeBatch())) {
              if (closingState.isClosing) return
              const combined = batch.join(' ')
              // If combined contains closing triggers, start closing flow in parallel
              if (textTriggersClosing(combined)) {
                console.log('🛑 [CLOSING-DETECT] Trigger phrase detected in AI response batch')
                startClosingFlow({ reason: 'close_call_detected' }).catch(() => {})
              }
              try { await tts.enqueueText(combined) } catch (_) {}
            }
            // If only one sentence is ready and it is sufficiently long, allow sending it alone
            if (completeSentences.length === 1) {
              const lone = completeSentences[0]
              if (lone && lone.length >= 80) {
                try { await tts.enqueueText(lone) } catch (_) {}
                completeSentences.shift()
              }
            }
          }
        )

        if (finalResponse && isResponseActive(responseId)) {
          // Flush remaining carry and sentences in up to 3-sentence batch
          if (carry && carry.trim()) completeSentences.push(carry.trim())
          if (completeSentences.length > 0) {
            const batch = []
            while (batch.length < 3 && completeSentences.length > 0) batch.push(completeSentences.shift())
            const combined = batch.join(' ')
            if (textTriggersClosing(combined)) {
              console.log('🛑 [CLOSING-DETECT] Trigger phrase detected in final AI batch')
              startClosingFlow({ reason: 'close_call_detected' }).catch(() => {})
            }
            try { await currentTTS.enqueueText(combined) } catch (_) {}
          }
          history.addUserTranscript(transcript)
          history.addAssistantResponse(finalResponse)
          
          // Log to database for persistence
          if (callLogger && transcript.trim()) {
            callLogger.logUserTranscript(transcript.trim(), (ws.sessionAgentConfig?.language || 'en').toLowerCase())
          }
          try {
            aiResponses.push({
              type: 'ai',
              text: finalResponse,
              language: (ws.sessionAgentConfig?.language || 'en').toLowerCase(),
              timestamp: new Date(),
            })
          } catch (_) {}
          try {
            if (callLogger) {
              callLogger.logAIResponse(finalResponse, (ws.sessionAgentConfig?.language || 'en').toLowerCase())
            }
          } catch (_) {}
          await updateLiveCallLog()

          // Non-blocking lead status detection to set lastIntent for forced end timing
          ;(async () => {
            try {
              const lead = await detectLeadStatusWithOpenAI(transcript, history.getConversationHistory(), (ws.sessionAgentConfig?.language || 'en').toLowerCase())
              const intent = mapLeadStatusToIntent(lead)
              if (intent) {
                closingState.lastIntent = intent
                console.log(`📊 [INTENT] Mapped lead status "${lead}" → ${intent}`)
              }
            } catch (_) {}
          })()
        }
      }

      const totalTime = Date.now() - totalStart
      console.log(`[PROCESS] Processing completed in ${totalTime}ms for response ${responseId}`)
    } catch (error) {
      console.error("[PROCESS] Error processing user input:", error.message)
    } finally {
      isProcessing = false
      console.log(`[PROCESS] isProcessing reset. Ready for next input.`)
    }
  }

  // Handle incoming messages from SanIPPBX
  ws.on("message", async (message) => {
    try {
      // Normalize to string and parse JSON; PBX must send JSON base64 media
      const messageStr = Buffer.isBuffer(message) ? message.toString() : String(message)
      const data = JSON.parse(messageStr)

      switch (data.event) {
        case "connected":
          console.log("🔗 [SANPBX] Connected")
          
          // Log ALL data received from SIP team during connection
          console.log("=".repeat(80))
          console.log("[SANPBX-CONNECTED] COMPLETE DATA RECEIVED FROM SIP TEAM:")
          console.log("=".repeat(80))
          console.log("[SANPBX-CONNECTED] Raw data object:", JSON.stringify(data, null, 2))
          console.log("[SANPBX-CONNECTED] Event type:", data.event)
          console.log("[SANPBX-CONNECTED] ChannelID:", data.channelId)
          console.log("[SANPBX-CONNECTED] CallID:", data.callId) 
          console.log("[SANPBX-CONNECTED] StreamID:", data.streamId)
          console.log("[SANPBX-CONNECTED] CallerID:", data.callerId)
          console.log("[SANPBX-CONNECTED] Call Direction:", data.callDirection)
          console.log("[SANPBX-CONNECTED] DID:", data.did)
          console.log("[SANPBX-CONNECTED] From Number:", data.from)
          console.log("[SANPBX-CONNECTED] To Number:", data.to)
          console.log("[SANPBX-CONNECTED] Additional Properties:")
          
          // Log any additional properties not explicitly handled
          const connectedKnownProps = ['event', 'channelId', 'callId', 'streamId', 'callerId', 'callDirection', 'did', 'from', 'to']
          Object.keys(data).forEach(key => {
            if (!connectedKnownProps.includes(key)) {
              console.log(`[SANPBX-CONNECTED] ${key}:`, data[key])
            }
          })
          console.log("=".repeat(80))
          
          // Cache identifiers if provided
          callerIdValue = data.callerId || callerIdValue
          callDirectionValue = data.callDirection || callDirectionValue
          didValue = data.did || didValue

          // Capture SanPBX-style extraParams for persistence
          try {
            if (data.extraParams && typeof data.extraParams === 'object') {
              sessionCustomParams = { ...sessionCustomParams, ...data.extraParams }
            } else if (data.extraParams && typeof data.extraParams === 'string') {
              try {
                const decoded = Buffer.from(String(data.extraParams).trim(), 'base64').toString('utf8')
                const parsed = JSON.parse(decoded)
                sessionCustomParams = { ...sessionCustomParams, ...parsed }
              } catch (_) {}
            }
            if (sessionCustomParams && typeof sessionCustomParams === 'object') {
              if (sessionCustomParams.name && !sessionCustomParams.contact_name) {
                sessionCustomParams.contact_name = sessionCustomParams.contact_name || sessionCustomParams.name
              }
              if (!sessionUserName && (sessionCustomParams.name || sessionCustomParams.contact_name)) {
                sessionUserName = sessionCustomParams.name || sessionCustomParams.contact_name
              }
              if (!sessionUniqueId && (sessionCustomParams.uniqueid || sessionCustomParams.uniqueId)) {
                sessionUniqueId = sessionCustomParams.uniqueid || sessionCustomParams.uniqueId
              }
              if (!sessionRecordingPath && typeof sessionCustomParams.rec_path === 'string') {
                sessionRecordingPath = sessionCustomParams.rec_path
                const resolved = resolveRecordingUrl(sessionRecordingPath)
                try {
                  console.log('🎙️ [SANPBX-RECORDING] rec_path detected (connected):', sessionRecordingPath)
                  if (resolved) console.log('🎙️ [SANPBX-RECORDING] Resolved URL:', resolved)
                } catch (_) {}
              }
            }
          } catch (_) {}
          try {
            if (data.extraParams && typeof data.extraParams === 'object') {
              sessionCustomParams = { ...sessionCustomParams, ...data.extraParams }
            } else if (data.extraParams && typeof data.extraParams === 'string') {
              try {
                const decoded = Buffer.from(String(data.extraParams).trim(), 'base64').toString('utf8')
                const parsed = JSON.parse(decoded)
                sessionCustomParams = { ...sessionCustomParams, ...parsed }
              } catch (_) {}
            }
            if (sessionCustomParams && typeof sessionCustomParams === 'object') {
              if (sessionCustomParams.name && !sessionCustomParams.contact_name) {
                sessionCustomParams.contact_name = sessionCustomParams.contact_name || sessionCustomParams.name
              }
              if (!sessionUserName && (sessionCustomParams.name || sessionCustomParams.contact_name)) {
                sessionUserName = sessionCustomParams.name || sessionCustomParams.contact_name
              }
              if (!sessionUniqueId && (sessionCustomParams.uniqueid || sessionCustomParams.uniqueId)) {
                sessionUniqueId = sessionCustomParams.uniqueid || sessionCustomParams.uniqueId
              }
              if (!sessionRecordingPath && typeof sessionCustomParams.rec_path === 'string') {
                sessionRecordingPath = sessionCustomParams.rec_path
                const resolved = resolveRecordingUrl(sessionRecordingPath)
                try {
                  console.log('🎙️ [SANPBX-RECORDING] rec_path detected (start):', sessionRecordingPath)
                  if (resolved) console.log('🎙️ [SANPBX-RECORDING] Resolved URL:', resolved)
                } catch (_) {}
              }
            }
          } catch (_) {}
          break

        case "start": {
          console.log("📞 [SANPBX] Call started")
          
          // Log ALL data received from SIP team at start
          console.log("=".repeat(80))
          console.log("[SANPBX-START] COMPLETE DATA RECEIVED FROM SIP TEAM:")
          console.log("=".repeat(80))
          console.log("[SANPBX-START] Raw data object:", JSON.stringify(data, null, 2))
          console.log("[SANPBX-START] Event type:", data.event)
          console.log("[SANPBX-START] StreamID:", data.streamId)
          console.log("[SANPBX-START] CallID:", data.callId)
          console.log("[SANPBX-START] ChannelID:", data.channelId)
          console.log("[SANPBX-START] CallerID:", data.callerId)
          console.log("[SANPBX-START] Call Direction:", data.callDirection)
          console.log("[SANPBX-START] DID:", data.did)
          console.log("[SANPBX-START] From Number:", data.from)
          console.log("[SANPBX-START] To Number:", data.to)
          console.log("[SANPBX-START] Media Format:", JSON.stringify(data.mediaFormat, null, 2))
          console.log("[SANPBX-START] Start Object:", JSON.stringify(data.start, null, 2))
          console.log("[SANPBX-START] Additional Properties:")
          
          // Log any additional properties not explicitly handled
          const knownProps = ['event', 'streamId', 'callId', 'channelId', 'callerId', 'callDirection', 'did', 'from', 'to', 'mediaFormat', 'start']
          Object.keys(data).forEach(key => {
            if (!knownProps.includes(key)) {
              console.log(`[SANPBX-START] ${key}:`, data[key])
            }
          })
          console.log("=".repeat(80))
          
          streamId = data.streamId
          callId = data.callId
          channelId = data.channelId
          callStartTime = new Date()
          userTranscripts = []
          aiResponses = []
          whatsappRequested = false
          whatsappSent = false
          
          // Check if call is already active and handle accordingly
          if (isCallActive(streamId)) {
            console.log(`⚠️ [SANPBX-WS-CONFLICT] Call already active for streamSid: ${streamId}, force disconnecting existing connection`)
            forceDisconnectWebSocket(streamId)
          }
          
          // Track this WebSocket connection
          currentStreamSid = streamId
          activeWebSockets.set(streamId, ws)
          console.log(`🔗 [SANPBX-WS-TRACKING] Registered WebSocket for streamSid: ${streamId}`)

          // Cache identifiers if provided (prefer start values if present)
          callerIdValue = data.callerId || callerIdValue
          callDirectionValue = data.callDirection || callDirectionValue
          didValue = data.did || didValue

          // Determine call direction
          callDirection = (callDirectionValue || '').toLowerCase() === 'outgoing' ? 'outbound' : 'inbound'

          // Apply media format to Deepgram params when available
          try {
            const mf = data.mediaFormat || {}
            // Normalize encoding to Deepgram expected value
            const enc = (mf.encoding || '').toString().toLowerCase()
            if (enc.includes('pcm') || enc.includes('linear16') || enc === '') {
              inputEncoding = 'linear16'
            } else {
              inputEncoding = enc
            }
            const sr = Number(mf.sampleRate)
            inputSampleRateHz = Number.isFinite(sr) && sr > 0 ? sr : 8000
            const ch = Number(mf.channels)
            inputChannels = Number.isFinite(ch) && ch > 0 ? ch : 1
            console.log(`[STT] Using media format -> encoding=${inputEncoding}, sample_rate=${inputSampleRateHz}, channels=${inputChannels}`)
            const conforms = inputEncoding === 'linear16' && inputSampleRateHz === 8000 && inputChannels === 1
            console.log(`[SPEC-CHECK:INCOMING-FORMAT] conforms=${conforms} (expected: encoding=linear16, sample_rate=8000, channels=1)`) 
            if (!conforms) {
              console.warn(`[SPEC-WARN] Incoming media format differs from spec: encoding=${inputEncoding}, sample_rate=${inputSampleRateHz}, channels=${inputChannels}`)
            }
          } catch (e) {
            console.log('[STT] Using default media format due to parse error:', e.message)
            inputEncoding = 'linear16'
            inputSampleRateHz = 8000
            inputChannels = 1
          }

          // Enhanced agent lookup with SanPBX-specific matching
          try {
            console.log("🔍 [SANPBX-AGENT-LOOKUP] ========== AGENT LOOKUP ==========")
            console.log("🔍 [SANPBX-AGENT-LOOKUP] Call Direction:", callDirection)
            console.log("🔍 [SANPBX-AGENT-LOOKUP] DID:", didValue)
            console.log("🔍 [SANPBX-AGENT-LOOKUP] CallerID:", callerIdValue)
            
            const fromNumber = (data.start && data.start.from) || data.from || callerIdValue
            const toNumber = (data.start && data.start.to) || data.to || didValue
            const fromLast = last10Digits(fromNumber)
            const toLast = last10Digits(toNumber)

            let agent = null
            let matchReason = "none"

            // Priority 1: Match by DID (for inbound calls)
            if (!agent && didValue) {
              agent = await Agent.findOne({ isActive: true, callerId: String(didValue) })
                .select("_id clientId agentName callingNumber sttSelection ttsSelection llmSelection systemPrompt firstMessage voiceSelection voiceServiceProvider voiceId language callerId whatsappEnabled whatsapplink depositions details qa")
                .lean()
              if (agent) matchReason = "callerId==DID"
            }

            // Priority 2: Match by CallerID (for outbound calls)
            if (!agent && callerIdValue) {
              agent = await Agent.findOne({ isActive: true, callerId: String(callerIdValue) })
                .select("_id clientId agentName callingNumber sttSelection ttsSelection llmSelection systemPrompt firstMessage voiceSelection voiceServiceProvider voiceId language callerId whatsappEnabled whatsapplink depositions details qa")
                .lean()
              if (agent) matchReason = "callerId==CallerID"
            }

            // Priority 3: Match by calling number (last 10 digits)
            if (!agent) {
              try {
                const candidates = await Agent.find({ isActive: true, callingNumber: { $exists: true } })
                  .select("_id clientId agentName callingNumber sttSelection ttsSelection llmSelection systemPrompt firstMessage voiceSelection voiceServiceProvider voiceId language callerId whatsappEnabled whatsapplink depositions details qa")
                  .lean()
                agent = candidates.find((a) => last10Digits(a.callingNumber) === toLast || last10Digits(a.callingNumber) === fromLast) || null
                if (agent) matchReason = "callingNumber(last10)==to/from"
              } catch (_) {}
            }

            if (agent) {
              console.log("✅ [SANPBX-AGENT-LOOKUP] Agent found successfully")
              console.log("✅ [SANPBX-AGENT-LOOKUP] Agent Name:", agent.agentName)
              console.log("✅ [SANPBX-AGENT-LOOKUP] Client ID:", agent.clientId)
              console.log("✅ [SANPBX-AGENT-LOOKUP] Language:", agent.language)
              console.log("✅ [SANPBX-AGENT-LOOKUP] Voice Selection:", agent.voiceSelection)
              console.log("✅ [SANPBX-AGENT-LOOKUP] First Message:", agent.firstMessage)
              console.log("✅ [SANPBX-AGENT-LOOKUP] WhatsApp Enabled:", agent.whatsappEnabled)
              console.log("✅ [SANPBX-AGENT-LOOKUP] WhatsApp API URL:", agent.whatsapplink)
              console.log("✅ [SANPBX-AGENT-LOOKUP] Match Reason:", matchReason)
              console.log("✅ [SANPBX-AGENT-LOOKUP] ======================================")
              
              // Bind into session for downstream use (TTS, prompts, etc.)
              ws.sessionAgentConfig = agent
              agentConfig = agent
              currentLanguage = agent.language || "en"
            } else {
              console.log("❌ [SANPBX-AGENT-LOOKUP] No agent found for call")
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
            console.log("❌ [SANPBX-AGENT-LOOKUP] Error finding agent:", err.message)
            ws.send(
              JSON.stringify({
                event: "error",
                message: err.message,
              }),
            )
            ws.close()
            return
          }

          // Block call if the client has no credits (fast, lean query)
          try {
            const t0 = Date.now()
            const creditDoc = await Credit.findOne({ clientId: agentConfig.clientId })
              .select('clientId currentBalance')
              .lean()
            const currentBalance = Number(creditDoc?.currentBalance ?? 0)
            console.log(`🪙 [SANPBX-CREDIT-CHECK] ${Date.now() - t0}ms - clientId=${agentConfig.clientId}, balance=${currentBalance}`)
            if (!(currentBalance > 0)) {
              console.log("🛑 [SANPBX-CREDIT-CHECK] Insufficient credits. Blocking call connection.")
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
            console.log("⚠️ [SANPBX-CREDIT-CHECK] Credit check failed:", creditErr.message)
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

          // Create enhanced call logger with live transcript capability
          callLogger = new EnhancedCallLogger(
            agentConfig.clientId,
            data.from || data.start?.from || callerIdValue || undefined,
            callDirection
          );
          callLogger.customParams = sessionCustomParams;
          callLogger.callerId = callerIdValue || undefined;
          callLogger.streamSid = streamId;
          callLogger.callSid = callId;
          callLogger.accountSid = agentConfig.clientId;
          callLogger.ws = ws; // Store WebSocket reference
          callLogger.uniqueid = sessionUniqueId; // Store uniqueid for outbound calls

          // Create initial call log entry immediately
          try {
            await callLogger.createInitialCallLog(agentConfig._id, 'not_connected');
            // Persist initial SanPBX start-event fields into the call log (non-blocking)
            try {
              const sanMeta = {
                event: 'start',
                channelId: data.channelId,
                callId: data.callId,
                streamId: data.streamId,
                callerId: data.callerId,
                callDirection: data.callDirection,
                extraParams: data.extraParams || {},
                cid: data.cid,
                did: data.did,
                timestamp: data.timestamp,
                mediaFormat: data.mediaFormat || {},
                from: (data.start && data.start.from) || data.from || null,
                to: (data.start && data.start.to) || data.to || null,
                rec_path: sessionRecordingPath || null,
              }
              if (callLogger.callLogId) {
                await CallLog.findByIdAndUpdate(callLogger.callLogId, {
                  $set: {
                    'metadata.sanpbx': sanMeta,
                    'metadata.customParams': { ...(callLogger.customParams || {}), ...(data.extraParams || {}) },
                    'metadata.callerId': data.callerId || callLogger.callerId || undefined,
                    'metadata.lastUpdated': new Date(),
                    ...(sessionRecordingPath ? { 'metadata.recording.rec_path': sessionRecordingPath } : {}),
                    ...(sessionRecordingPath && resolveRecordingUrl(sessionRecordingPath) ? { audioUrl: resolveRecordingUrl(sessionRecordingPath) } : {}),
                  },
                })
              }
            } catch (_) {}
            console.log("✅ [SANPBX-CALL-SETUP] Initial call log created successfully")
            console.log("✅ [SANPBX-CALL-SETUP] Call Log ID:", callLogger.callLogId)
          } catch (error) {
            console.log("❌ [SANPBX-CALL-SETUP] Failed to create initial call log:", error.message)
            // Continue anyway - fallback will create log at end
          }

          console.log("🎯 [SANPBX-CALL-SETUP] ========== CALL SETUP ==========")
          console.log("🎯 [SANPBX-CALL-SETUP] Current Language:", currentLanguage)
          console.log("🎯 [SANPBX-CALL-SETUP] Mobile Number:", data.from || data.start?.from || callerIdValue)
          console.log("🎯 [SANPBX-CALL-SETUP] Call Direction:", callDirection)
          console.log("🎯 [SANPBX-CALL-SETUP] Client ID:", agentConfig.clientId)
          console.log("🎯 [SANPBX-CALL-SETUP] StreamSID:", streamId)
          console.log("🎯 [SANPBX-CALL-SETUP] CallSID:", callId)

          // Send greeting first to avoid STT echo/disturbance, then connect STT
          // For first message: play full greeting at once (no sentence limiting)
          let greeting = String(agentConfig.firstMessage || "Hello! How can I help you today?").replace(/\s+/g, ' ').trim()
          if (sessionUserName && sessionUserName.trim()) {
            const base = String(agentConfig.firstMessage || "How can I help you today?").replace(/\s+/g, ' ').trim()
            greeting = `Hello ${sessionUserName.trim()}! ${base}`.replace(/\s+/g, ' ').trim()
          }

          console.log("🎯 [SANPBX-CALL-SETUP] Greeting Message:", greeting)
          console.log("🎯 [SANPBX-CALL-SETUP] ======================================")

          if (callLogger) {
            callLogger.logAIResponse(greeting, currentLanguage)
          }

          console.log("🎤 [SANPBX-TTS] Starting greeting TTS...")
          if ((ws.sessionAgentConfig?.voiceServiceProvider || "sarvam").toLowerCase() === "smallest") {
            if (!sharedSmallestTTS || sharedSmallestTTS.isInterrupted) {
              sharedSmallestTTS = new SimplifiedSmallestWSTTSProcessor(ws, streamId, callLogger)
            }
            currentTTS = sharedSmallestTTS
            await sharedSmallestTTS.synthesizeAndStream(greeting)
          } else {
          currentTTS = createTtsProcessor(ws, streamId, callLogger)
          await currentTTS.synthesizeAndStream(greeting)
          }
          console.log("✅ [SANPBX-TTS] Greeting TTS completed")

          // Now connect to Deepgram for speech recognition after greeting
          await connectToDeepgram()
          
          // Initialize silence timeout after call setup
          resetSilenceTimeout()
          break
        }

        case "answer":
          console.log("[SANPBX] Call answered - ready for media streaming")
          
          // Log ALL data received from SIP team during answer
          console.log("=".repeat(80))
          console.log("[SANPBX-ANSWER] COMPLETE DATA RECEIVED FROM SIP TEAM:")
          console.log("=".repeat(80))
          console.log("[SANPBX-ANSWER] Raw data object:", JSON.stringify(data, null, 2))
          console.log("[SANPBX-ANSWER] Event type:", data.event)
          console.log("[SANPBX-ANSWER] Additional Properties:")
          
          // Log any additional properties not explicitly handled
          const answerKnownProps = ['event']
          Object.keys(data).forEach(key => {
            if (!answerKnownProps.includes(key)) {
              console.log(`[SANPBX-ANSWER] ${key}:`, data[key])
            }
          })
          console.log("=".repeat(80))
          break

        case "media":
          // Expect base64 payload; forward decoded PCM to Deepgram
          if (data.payload) {
            const audioBuffer = Buffer.from(data.payload, "base64")
            
            // Log media stats periodically (every 1000 packets to avoid spam)
            if (!ws.mediaPacketCount) ws.mediaPacketCount = 0
            ws.mediaPacketCount++
            
            if (ws.mediaPacketCount % 1000 === 0) {
              console.log("🎵 [SANPBX-MEDIA] Audio packets received:", ws.mediaPacketCount)
            }

            if (deepgramWs && deepgramReady && deepgramWs.readyState === WebSocket.OPEN) {
              deepgramWs.send(audioBuffer)
            } else {
              deepgramAudioQueue.push(audioBuffer)
              if (deepgramAudioQueue.length % 100 === 0) {
                console.log("⏳ [SANPBX-MEDIA] Audio queued for Deepgram:", deepgramAudioQueue.length)
              }
            }
          } else if (sttFailed) {
            console.log("[STT] Audio received but STT unavailable - consider implementing DTMF fallback")
          }
          break

          case "stop":
            console.log("🛑 [SANPBX] Call ended")
          
            // Ensure last TTS/audio is delivered before termination
            await waitForSipQueueDrain(2500)
          
            if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
              deepgramWs.close()
            }
          
            if (silenceTimer) {
              clearTimeout(silenceTimer)
            }
            
            // Close shared Smallest WS if exists
            if (sharedSmallestTTS && !sharedSmallestTTS.isInterrupted) {
              try { sharedSmallestTTS.interrupt() } catch (_) {}
              sharedSmallestTTS = null
            }
            
            // Cleanup WebSocket tracking
            if (currentStreamSid) {
              activeWebSockets.delete(currentStreamSid)
              console.log(`🔗 [SANPBX-WS-TRACKING] Removed WebSocket for streamSid: ${currentStreamSid}`)
            }
          
            // Intelligent WhatsApp send based on lead status and user requests
            try {
              // Extract recording path from stop event payload or fall back to session variable
              const recordingPath = data.rec_file || sessionRecordingPath
              
              // If we have a recording path, surface a resolved URL in logs and into the call log
              try {
                if (recordingPath) {
                  const resolved = resolveRecordingUrl(recordingPath)
                  console.log('🎙️ [SANPBX-RECORDING] stop-event rec_file:', data.rec_file)
                  console.log('🎙️ [SANPBX-RECORDING] stop-event session rec_path:', sessionRecordingPath)
                  console.log('🎙️ [SANPBX-RECORDING] stop-event using:', recordingPath)
                  if (resolved) console.log('🎙️ [SANPBX-RECORDING] stop-event resolved URL:', resolved)

                  // Attempt S3 upload if configured
                  let s3Url = null
                  try { s3Url = await uploadRecordingToS3(recordingPath) } catch (_) {}
                  const audioUrlToSave = s3Url || resolved || recordingPath
                  const recordingUpdate = {
                    $set: { 
                      audioUrl: audioUrlToSave,
                      'metadata.recording.rec_path': recordingPath,
                      'metadata.recording.rec_file': data.rec_file,
                      'metadata.lastUpdated': new Date() 
                    }
                  }
                  if (callLogger?.callLogId && resolved) {
                    try {
                      await CallLog.findByIdAndUpdate(callLogger.callLogId, recordingUpdate)
                      console.log('✅ [SANPBX-RECORDING] Updated call log with recording URL')
                    } catch (updateErr) {
                      console.log('❌ [SANPBX-RECORDING] Failed to update call log:', updateErr.message)
                    }
                  }
                } else {
                  console.log('⚠️ [SANPBX-RECORDING] No recording path available in stop event')
                }
              } catch (recErr) {
                console.log('❌ [SANPBX-RECORDING] Error processing recording:', recErr.message)
              }
              
              // Recompute WhatsApp request at end-of-call using full history
              if (callLogger && agentConfig?.whatsappEnabled) {
                try { await detectWhatsAppRequestedAtEnd() } catch (_) {}
              }
              
              if (callLogger && agentConfig?.whatsappEnabled && callLogger.shouldSendWhatsApp()) {
                const waLink = getAgentWhatsappLink(agentConfig)
                const waNumber = normalizeIndianMobile(callLogger?.mobile || null)
                const waApiUrl = agentConfig?.whatsapplink
                console.log("📨 [WHATSAPP] stop-event check → enabled=", agentConfig.whatsappEnabled, ", link=", waLink, ", apiUrl=", waApiUrl, ", normalized=", waNumber, ", leadStatus=", callLogger.currentLeadStatus, ", requested=", callLogger.whatsappRequested)
                if (waLink && waNumber && waApiUrl) {
                  sendWhatsAppTemplateMessage(waNumber, waLink, waApiUrl)
                    .then(async (r) => {
                      console.log("📨 [WHATSAPP] stop-event result:", r?.ok ? "OK" : "FAIL", r?.status || r?.reason || r?.error || "")
                      if (r?.ok) {
                        await billWhatsAppCredit({
                          clientId: agentConfig.clientId,
                          mobile: callLogger?.mobile || null,
                          link: waLink,
                          callLogId: callLogger?.callLogId,
                          streamSid: streamId,
                        })
                        callLogger.markWhatsAppSent()
                        try {
                          if (callLogger?.callLogId) {
                            await CallLog.findByIdAndUpdate(callLogger.callLogId, {
                              'metadata.whatsappMessageSent': true,
                              'metadata.whatsappRequested': !!callLogger.whatsappRequested,
                              'metadata.lastUpdated': new Date(),
                            })
                          }
                        } catch (_) {}
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
            
            if (callLogger) {
              const stats = callLogger.getStats()
              console.log("🛑 [SANPBX-STOP] Call Stats:", JSON.stringify(stats, null, 2))
              // Bill credits at end of call (decimal precision)
              const durationSeconds = Math.round((new Date() - callLogger.callStartTime) / 1000)
              await billCallCredits({
                clientId: callLogger.clientId,
                durationSeconds,
                callDirection,
                mobile: callLogger.mobile,
                callLogId: callLogger.callLogId,
                streamSid: streamId,
                uniqueid: callLogger.uniqueid || agentConfig?.uniqueid || null
              })
              
              try {
                console.log("💾 [SANPBX-STOP] Saving final call log to database...")
                const finalLeadStatus = callLogger.currentLeadStatus || "maybe"
                console.log("📊 [SANPBX-STOP] Final lead status:", finalLeadStatus)
                const savedLog = await callLogger.saveToDatabase(finalLeadStatus, agentConfig)
                console.log("✅ [SANPBX-STOP] Final call log saved with ID:", savedLog._id)
              } catch (error) {
                console.log("❌ [SANPBX-STOP] Error saving final call log:", error.message)
              } finally {
                callLogger.cleanup()
              }
            }
            break
        case "dtmf":
          console.log("[SANPBX] DTMF received:", data.digit)
          
          // Log ALL data received from SIP team during DTMF
          console.log("=".repeat(60))
          console.log("[SANPBX-DTMF] COMPLETE DATA RECEIVED FROM SIP TEAM:")
          console.log("=".repeat(60))
          console.log("[SANPBX-DTMF] Raw data object:", JSON.stringify(data, null, 2))
          console.log("[SANPBX-DTMF] Event type:", data.event)
          console.log("[SANPBX-DTMF] DTMF Digit:", data.digit)
          console.log("[SANPBX-DTMF] Additional Properties:")
          
          // Log any additional properties not explicitly handled
          const dtmfKnownProps = ['event', 'digit']
          Object.keys(data).forEach(key => {
            if (!dtmfKnownProps.includes(key)) {
              console.log(`[SANPBX-DTMF] ${key}:`, data[key])
            }
          })
          console.log("=".repeat(60))
          // Handle DTMF input if needed
          break

        case "transfer-call-response":
          console.log("[SANPBX] Transfer response:", data.message)
          
          // Log ALL data received from SIP team during transfer response
          console.log("=".repeat(60))
          console.log("[SANPBX-TRANSFER] COMPLETE DATA RECEIVED FROM SIP TEAM:")
          console.log("=".repeat(60))
          console.log("[SANPBX-TRANSFER] Raw data object:", JSON.stringify(data, null, 2))
          console.log("[SANPBX-TRANSFER] Event type:", data.event)
          console.log("[SANPBX-TRANSFER] Message:", data.message)
          console.log("[SANPBX-TRANSFER] Additional Properties:")
          
          // Log any additional properties not explicitly handled
          const transferKnownProps = ['event', 'message']
          Object.keys(data).forEach(key => {
            if (!transferKnownProps.includes(key)) {
              console.log(`[SANPBX-TRANSFER] ${key}:`, data[key])
            }
          })
          console.log("=".repeat(60))
          break

        case "hangup-call-response":
          console.log("[SANPBX] Hangup response:", data.message)
          
          // Log ALL data received from SIP team during hangup response
          console.log("=".repeat(60))
          console.log("[SANPBX-HANGUP] COMPLETE DATA RECEIVED FROM SIP TEAM:")
          console.log("=".repeat(60))
          console.log("[SANPBX-HANGUP] Raw data object:", JSON.stringify(data, null, 2))
          console.log("[SANPBX-HANGUP] Event type:", data.event)
          console.log("[SANPBX-HANGUP] Message:", data.message)
          console.log("[SANPBX-HANGUP] Additional Properties:")
          
          // Log any additional properties not explicitly handled
          const hangupKnownProps = ['event', 'message']
          Object.keys(data).forEach(key => {
            if (!hangupKnownProps.includes(key)) {
              console.log(`[SANPBX-HANGUP] ${key}:`, data[key])
            }
          })
          console.log("=".repeat(60))
          break

        case "recording":
        case "call-recording":
        case "audio-recording":
        case "recording-complete":
          console.log("🎙️ [SANPBX-RECORDING] ========== CALL RECORDING RECEIVED ==========")
          console.log("=".repeat(80))
          console.log("🎙️ [SANPBX-RECORDING] Event type:", data.event)
          console.log("🎙️ [SANPBX-RECORDING] Call ID:", data.callId || callId || "N/A")
          console.log("🎙️ [SANPBX-RECORDING] Stream ID:", data.streamId || streamId || "N/A")
          console.log("🎙️ [SANPBX-RECORDING] Channel ID:", data.channelId || channelId || "N/A")
          
          // Log recording-specific fields
          if (data.recordingUrl) {
            console.log("🎙️ [SANPBX-RECORDING] Recording URL:", data.recordingUrl)
          }
          if (data.recordingPath) {
            console.log("🎙️ [SANPBX-RECORDING] Recording Path:", data.recordingPath)
            const resolved = resolveRecordingUrl(data.recordingPath)
            if (resolved) console.log("🎙️ [SANPBX-RECORDING] Resolved URL:", resolved)
          }
          if (data.audioUrl) {
            console.log("🎙️ [SANPBX-RECORDING] Audio URL:", data.audioUrl)
          }
          if (!data.audioUrl && !data.recordingUrl && (data.rec_path || sessionRecordingPath)) {
            const fromPath = data.rec_path || sessionRecordingPath
            const resolved = resolveRecordingUrl(fromPath)
            if (fromPath) console.log("🎙️ [SANPBX-RECORDING] rec_path:", fromPath)
            if (resolved) console.log("🎙️ [SANPBX-RECORDING] Resolved URL:", resolved)
          }
          if (data.audioBase64) {
            const base64Length = data.audioBase64.length
            const estimatedSizeKB = Math.round((base64Length * 3) / 4 / 1024)
            console.log("🎙️ [SANPBX-RECORDING] Audio Base64 Length:", base64Length, "characters")
            console.log("🎙️ [SANPBX-RECORDING] Estimated WAV Size:", estimatedSizeKB, "KB (~", Math.round(estimatedSizeKB / 1024 * 10) / 10, "MB)")
            console.log("🎙️ [SANPBX-RECORDING] Audio Base64 Preview (first 100 chars):", data.audioBase64.substring(0, 100) + "...")
          }
          if (data.audioData) {
            const audioDataLength = typeof data.audioData === 'string' ? data.audioData.length : JSON.stringify(data.audioData).length
            console.log("🎙️ [SANPBX-RECORDING] Audio Data Length:", audioDataLength, "characters")
            console.log("🎙️ [SANPBX-RECORDING] Audio Data Type:", typeof data.audioData)
            if (typeof data.audioData === 'string') {
              console.log("🎙️ [SANPBX-RECORDING] Audio Data Preview (first 100 chars):", data.audioData.substring(0, 100) + "...")
            }
          }
          if (data.wavFile) {
            const wavLength = typeof data.wavFile === 'string' ? data.wavFile.length : JSON.stringify(data.wavFile).length
            console.log("🎙️ [SANPBX-RECORDING] WAV File Length:", wavLength, "characters")
            console.log("🎙️ [SANPBX-RECORDING] WAV File Type:", typeof data.wavFile)
            if (typeof data.wavFile === 'string') {
              const estimatedSizeKB = Math.round((wavLength * 3) / 4 / 1024)
              console.log("🎙️ [SANPBX-RECORDING] Estimated WAV Size:", estimatedSizeKB, "KB (~", Math.round(estimatedSizeKB / 1024 * 10) / 10, "MB)")
              console.log("🎙️ [SANPBX-RECORDING] WAV File Preview (first 100 chars):", data.wavFile.substring(0, 100) + "...")
            }
          }
          if (data.fileName) {
            console.log("🎙️ [SANPBX-RECORDING] File Name:", data.fileName)
          }
          if (data.fileSize) {
            console.log("🎙️ [SANPBX-RECORDING] File Size:", data.fileSize, "bytes")
          }
          if (data.duration) {
            console.log("🎙️ [SANPBX-RECORDING] Recording Duration:", data.duration, "seconds")
          }
          if (data.format) {
            console.log("🎙️ [SANPBX-RECORDING] Audio Format:", data.format)
          }
          if (data.sampleRate) {
            console.log("🎙️ [SANPBX-RECORDING] Sample Rate:", data.sampleRate, "Hz")
          }
          if (data.channels) {
            console.log("🎙️ [SANPBX-RECORDING] Channels:", data.channels)
          }
          if (data.timestamp) {
            console.log("🎙️ [SANPBX-RECORDING] Timestamp:", data.timestamp)
          }
          if (data.status) {
            console.log("🎙️ [SANPBX-RECORDING] Status:", data.status)
          }
          if (data.message) {
            console.log("🎙️ [SANPBX-RECORDING] Message:", data.message)
          }
          
          // Log all additional properties
          console.log("🎙️ [SANPBX-RECORDING] All Properties:")
          const recordingKnownProps = ['event', 'callId', 'streamId', 'channelId', 'recordingUrl', 'recordingPath', 'audioUrl', 'audioBase64', 'audioData', 'wavFile', 'fileName', 'fileSize', 'duration', 'format', 'sampleRate', 'channels', 'timestamp', 'status', 'message']
          Object.keys(data).forEach(key => {
            if (!recordingKnownProps.includes(key)) {
              const value = data[key]
              const valueType = typeof value
              if (valueType === 'string' && value.length > 200) {
                console.log(`🎙️ [SANPBX-RECORDING] ${key}:`, value.substring(0, 200) + "... (truncated, total length: " + value.length + ")")
              } else {
                console.log(`🎙️ [SANPBX-RECORDING] ${key}:`, value)
              }
            }
          })
          
          // Log complete raw data object (truncate large base64 strings)
          const rawDataCopy = { ...data }
          if (rawDataCopy.audioBase64 && rawDataCopy.audioBase64.length > 500) {
            rawDataCopy.audioBase64 = rawDataCopy.audioBase64.substring(0, 500) + "... (truncated, total length: " + rawDataCopy.audioBase64.length + ")"
          }
          if (rawDataCopy.audioData && typeof rawDataCopy.audioData === 'string' && rawDataCopy.audioData.length > 500) {
            rawDataCopy.audioData = rawDataCopy.audioData.substring(0, 500) + "... (truncated, total length: " + rawDataCopy.audioData.length + ")"
          }
          if (rawDataCopy.wavFile && typeof rawDataCopy.wavFile === 'string' && rawDataCopy.wavFile.length > 500) {
            rawDataCopy.wavFile = rawDataCopy.wavFile.substring(0, 500) + "... (truncated, total length: " + rawDataCopy.wavFile.length + ")"
          }
          console.log("🎙️ [SANPBX-RECORDING] Raw data object (truncated for large fields):", JSON.stringify(rawDataCopy, null, 2))
          
          // Try to update call log with recording information if available
          if (callLogger && callLogger.callLogId) {
            try {
              const CallLog = require("../models/CallLog")
              const updateData = {
                'metadata.lastUpdated': new Date(),
              }
              
              if (data.recordingUrl || data.audioUrl) {
                updateData.audioUrl = data.recordingUrl || data.audioUrl
              }
              if (!updateData.audioUrl && (data.recordingPath || data.rec_path || sessionRecordingPath)) {
                const rp = data.recordingPath || data.rec_path || sessionRecordingPath
                const resolved = resolveRecordingUrl(rp)
                if (resolved) updateData.audioUrl = resolved
              }
              
              if (data.audioBase64 || data.wavFile || data.audioData) {
                updateData['metadata.recording'] = {
                  received: true,
                  receivedAt: new Date(),
                  hasAudioData: !!(data.audioBase64 || data.wavFile || data.audioData),
                  format: data.format || 'wav',
                  fileSize: data.fileSize || null,
                  duration: data.duration || null,
                }
              }
              
              await CallLog.findByIdAndUpdate(callLogger.callLogId, updateData)
              console.log("✅ [SANPBX-RECORDING] Updated call log with recording information")
            } catch (updateError) {
              console.log("⚠️ [SANPBX-RECORDING] Failed to update call log:", updateError.message)
            }
          }
          
          console.log("=".repeat(80))
          console.log("🎙️ [SANPBX-RECORDING] =============================================")
          break
        default:
          // Check if this unknown event contains recording data
          const hasRecordingData = !!(data.audioBase64 || data.wavFile || data.audioData || data.recordingUrl || data.audioUrl || data.recordingPath)
          
          if (hasRecordingData) {
            console.log(`🎙️ [SANPBX-RECORDING] Received recording data in event: ${data.event}`)
            console.log("🎙️ [SANPBX-RECORDING] ========== CALL RECORDING RECEIVED ==========")
            console.log("=".repeat(80))
            console.log("🎙️ [SANPBX-RECORDING] Event type:", data.event)
            console.log("🎙️ [SANPBX-RECORDING] Call ID:", data.callId || callId || "N/A")
            console.log("🎙️ [SANPBX-RECORDING] Stream ID:", data.streamId || streamId || "N/A")
            console.log("🎙️ [SANPBX-RECORDING] Channel ID:", data.channelId || channelId || "N/A")
            
            // Log recording-specific fields
            if (data.recordingUrl) {
              console.log("🎙️ [SANPBX-RECORDING] Recording URL:", data.recordingUrl)
            }
            if (data.recordingPath) {
              console.log("🎙️ [SANPBX-RECORDING] Recording Path:", data.recordingPath)
            }
            if (data.audioUrl) {
              console.log("🎙️ [SANPBX-RECORDING] Audio URL:", data.audioUrl)
            }
            if (data.audioBase64) {
              const base64Length = data.audioBase64.length
              const estimatedSizeKB = Math.round((base64Length * 3) / 4 / 1024)
              console.log("🎙️ [SANPBX-RECORDING] Audio Base64 Length:", base64Length, "characters")
              console.log("🎙️ [SANPBX-RECORDING] Estimated WAV Size:", estimatedSizeKB, "KB (~", Math.round(estimatedSizeKB / 1024 * 10) / 10, "MB)")
              console.log("🎙️ [SANPBX-RECORDING] Audio Base64 Preview (first 100 chars):", data.audioBase64.substring(0, 100) + "...")
            }
            if (data.audioData) {
              const audioDataLength = typeof data.audioData === 'string' ? data.audioData.length : JSON.stringify(data.audioData).length
              console.log("🎙️ [SANPBX-RECORDING] Audio Data Length:", audioDataLength, "characters")
              console.log("🎙️ [SANPBX-RECORDING] Audio Data Type:", typeof data.audioData)
              if (typeof data.audioData === 'string') {
                console.log("🎙️ [SANPBX-RECORDING] Audio Data Preview (first 100 chars):", data.audioData.substring(0, 100) + "...")
              }
            }
            if (data.wavFile) {
              const wavLength = typeof data.wavFile === 'string' ? data.wavFile.length : JSON.stringify(data.wavFile).length
              console.log("🎙️ [SANPBX-RECORDING] WAV File Length:", wavLength, "characters")
              console.log("🎙️ [SANPBX-RECORDING] WAV File Type:", typeof data.wavFile)
              if (typeof data.wavFile === 'string') {
                const estimatedSizeKB = Math.round((wavLength * 3) / 4 / 1024)
                console.log("🎙️ [SANPBX-RECORDING] Estimated WAV Size:", estimatedSizeKB, "KB (~", Math.round(estimatedSizeKB / 1024 * 10) / 10, "MB)")
                console.log("🎙️ [SANPBX-RECORDING] WAV File Preview (first 100 chars):", data.wavFile.substring(0, 100) + "...")
              }
            }
            if (data.fileName) {
              console.log("🎙️ [SANPBX-RECORDING] File Name:", data.fileName)
            }
            if (data.fileSize) {
              console.log("🎙️ [SANPBX-RECORDING] File Size:", data.fileSize, "bytes")
            }
            if (data.duration) {
              console.log("🎙️ [SANPBX-RECORDING] Recording Duration:", data.duration, "seconds")
            }
            if (data.format) {
              console.log("🎙️ [SANPBX-RECORDING] Audio Format:", data.format)
            }
            if (data.sampleRate) {
              console.log("🎙️ [SANPBX-RECORDING] Sample Rate:", data.sampleRate, "Hz")
            }
            if (data.channels) {
              console.log("🎙️ [SANPBX-RECORDING] Channels:", data.channels)
            }
            if (data.timestamp) {
              console.log("🎙️ [SANPBX-RECORDING] Timestamp:", data.timestamp)
            }
            if (data.status) {
              console.log("🎙️ [SANPBX-RECORDING] Status:", data.status)
            }
            if (data.message) {
              console.log("🎙️ [SANPBX-RECORDING] Message:", data.message)
            }
            
            // Log all additional properties
            console.log("🎙️ [SANPBX-RECORDING] All Properties:")
            const recordingKnownProps = ['event', 'callId', 'streamId', 'channelId', 'recordingUrl', 'recordingPath', 'audioUrl', 'audioBase64', 'audioData', 'wavFile', 'fileName', 'fileSize', 'duration', 'format', 'sampleRate', 'channels', 'timestamp', 'status', 'message']
            Object.keys(data).forEach(key => {
              if (!recordingKnownProps.includes(key)) {
                const value = data[key]
                const valueType = typeof value
                if (valueType === 'string' && value.length > 200) {
                  console.log(`🎙️ [SANPBX-RECORDING] ${key}:`, value.substring(0, 200) + "... (truncated, total length: " + value.length + ")")
                } else {
                  console.log(`🎙️ [SANPBX-RECORDING] ${key}:`, value)
                }
              }
            })
            
            // Log complete raw data object (truncate large base64 strings)
            const rawDataCopy = { ...data }
            if (rawDataCopy.audioBase64 && rawDataCopy.audioBase64.length > 500) {
              rawDataCopy.audioBase64 = rawDataCopy.audioBase64.substring(0, 500) + "... (truncated, total length: " + rawDataCopy.audioBase64.length + ")"
            }
            if (rawDataCopy.audioData && typeof rawDataCopy.audioData === 'string' && rawDataCopy.audioData.length > 500) {
              rawDataCopy.audioData = rawDataCopy.audioData.substring(0, 500) + "... (truncated, total length: " + rawDataCopy.audioData.length + ")"
            }
            if (rawDataCopy.wavFile && typeof rawDataCopy.wavFile === 'string' && rawDataCopy.wavFile.length > 500) {
              rawDataCopy.wavFile = rawDataCopy.wavFile.substring(0, 500) + "... (truncated, total length: " + rawDataCopy.wavFile.length + ")"
            }
            console.log("🎙️ [SANPBX-RECORDING] Raw data object (truncated for large fields):", JSON.stringify(rawDataCopy, null, 2))
            
            // Try to update call log with recording information if available
            if (callLogger && callLogger.callLogId) {
              try {
                const CallLog = require("../models/CallLog")
                const updateData = {
                  'metadata.lastUpdated': new Date(),
                }
                
                if (data.recordingUrl || data.audioUrl) {
                  updateData.audioUrl = data.recordingUrl || data.audioUrl
                }
                
                if (data.audioBase64 || data.wavFile || data.audioData) {
                  updateData['metadata.recording'] = {
                    received: true,
                    receivedAt: new Date(),
                    hasAudioData: !!(data.audioBase64 || data.wavFile || data.audioData),
                    format: data.format || 'wav',
                    fileSize: data.fileSize || null,
                    duration: data.duration || null,
                  }
                }
                
                await CallLog.findByIdAndUpdate(callLogger.callLogId, updateData)
                console.log("✅ [SANPBX-RECORDING] Updated call log with recording information")
              } catch (updateError) {
                console.log("⚠️ [SANPBX-RECORDING] Failed to update call log:", updateError.message)
              }
            }
            
            console.log("=".repeat(80))
            console.log("🎙️ [SANPBX-RECORDING] =============================================")
          } else {
            console.log(`[SANPBX] Unknown event: ${data.event}`)
            
            // Log ALL data received from SIP team for unknown events
            console.log("=".repeat(60))
            console.log(`[SANPBX-UNKNOWN-${data.event?.toUpperCase() || 'EVENT'}] COMPLETE DATA RECEIVED FROM SIP TEAM:`)
            console.log("=".repeat(60))
            console.log(`[SANPBX-UNKNOWN-${data.event?.toUpperCase() || 'EVENT'}] Raw data object:`, JSON.stringify(data, null, 2))
            console.log(`[SANPBX-UNKNOWN-${data.event?.toUpperCase() || 'EVENT'}] Event type:`, data.event)
            console.log(`[SANPBX-UNKNOWN-${data.event?.toUpperCase() || 'EVENT'}] All Properties:`)
            
            // Log all properties for unknown events
            Object.keys(data).forEach(key => {
              console.log(`[SANPBX-UNKNOWN-${data.event?.toUpperCase() || 'EVENT'}] ${key}:`, data[key])
            })
            console.log("=".repeat(60))
          }
          break
      }
    } catch (error) {
      console.error("[SANPBX] Error processing message:", error.message)
    }
  })

  // Handle connection close
  ws.on("close", async () => {
    console.log("🔌 [SANPBX] WebSocket connection closed")

    // Close shared Smallest WS if exists
    if (sharedSmallestTTS && !sharedSmallestTTS.isInterrupted) {
      try { sharedSmallestTTS.interrupt() } catch (_) {}
      sharedSmallestTTS = null
    }

    // Safety: Intelligent WhatsApp send on close if conditions are met
    try {
      // Recompute at end-of-call using full history
      if (callLogger && agentConfig?.whatsappEnabled) {
        try { await detectWhatsAppRequestedAtEnd() } catch (_) {}
      }
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
                  streamSid: streamId,
                })
                callLogger.markWhatsAppSent()
                try {
                  if (callLogger?.callLogId) {
                    await CallLog.findByIdAndUpdate(callLogger.callLogId, {
                      'metadata.whatsappMessageSent': true,
                      'metadata.whatsappRequested': !!callLogger.whatsappRequested,
                      'metadata.lastUpdated': new Date(),
                    })
                  }
                } catch (_) {}
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
      console.log("🔌 [SANPBX-CLOSE] Final Call Stats:", JSON.stringify(stats, null, 2))
      // Surface session recording path (if any) on close
      try {
        if (sessionRecordingPath) {
          const resolved = resolveRecordingUrl(sessionRecordingPath)
          console.log('🎙️ [SANPBX-RECORDING] close-event session rec_path:', sessionRecordingPath)
          if (resolved) console.log('🎙️ [SANPBX-RECORDING] close-event resolved URL:', resolved)
          if (resolved && callLogger?.callLogId) {
            try {
              await CallLog.findByIdAndUpdate(callLogger.callLogId, {
                $set: { audioUrl: resolved, 'metadata.recording.rec_path': sessionRecordingPath, 'metadata.lastUpdated': new Date() }
              })
            } catch (_) {}
          }
        }
      } catch (_) {}
      // Bill credits on close as safety (guarded by billedStreamSids)
      const durationSeconds = Math.round((new Date() - callLogger.callStartTime) / 1000)
      await billCallCredits({
        clientId: callLogger.clientId,
        durationSeconds,
        callDirection,
        mobile: callLogger.mobile,
        callLogId: callLogger.callLogId,
        streamSid: streamId,
        uniqueid: callLogger.uniqueid || agentConfig?.uniqueid || null
      })
      
      try {
        console.log("💾 [SANPBX-CLOSE] Saving call log due to connection close...")
        const finalLeadStatus = callLogger.currentLeadStatus || "maybe"
        console.log("📊 [SANPBX-CLOSE] Final lead status:", finalLeadStatus)
        const savedLog = await callLogger.saveToDatabase(finalLeadStatus, agentConfig)
        console.log("✅ [SANPBX-CLOSE] Call log saved with ID:", savedLog._id)
      } catch (error) {
        console.log("❌ [SANPBX-CLOSE] Error saving call log:", error.message)
      } finally {
        callLogger.cleanup()
      }
    }

    // Cleanup
    if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
      deepgramWs.close()
    }

    if (silenceTimer) {
      clearTimeout(silenceTimer)
    }

    // Clear silence timeout
    if (silenceTimeout) {
      clearTimeout(silenceTimeout)
      silenceTimeout = null
    }

    // Cleanup WebSocket tracking
    if (currentStreamSid) {
      activeWebSockets.delete(currentStreamSid)
      console.log(`🔗 [SANPBX-WS-TRACKING] Removed WebSocket for streamSid: ${currentStreamSid}`)
    }

    // Reset session state
    streamId = null
    callId = null
    channelId = null
    history.clear()
    isProcessing = false
    userUtteranceBuffer = ""
    sttFailed = false
    chunkCounter = 0
    lastProcessedTranscript = ""
    lastProcessedTime = 0
    activeResponseId = null
    deepgramReady = false
    deepgramAudioQueue = []
    currentTTS = null
    currentLanguage = undefined
    processingRequestId = 0
    callLogger = null
    callDirection = "inbound"
    agentConfig = null
    sttTimer = null
    lastUserActivity = Date.now()
    autoDispositionEnabled = true
    currentStreamSid = null
  })

  // Handle errors
  ws.on("error", (error) => {
    console.error("[SANPBX] WebSocket error:", error.message)
  })

  // Determine at end of call if WhatsApp was requested by scanning the conversation
  const detectWhatsAppRequestedAtEnd = async () => {
    try {
      // If already flagged during call, respect it
      if (whatsappRequested) return true
      const history = [...userTranscripts, ...aiResponses]
        .sort((a,b)=>new Date(a.timestamp)-new Date(b.timestamp))
        .map(e=>({ role: e.type === 'user' ? 'user' : 'assistant', content: e.text }))
      const combinedUserText = userTranscripts.map(u => u.text).join(' \n ')
      const result = await detectWhatsAppRequest(combinedUserText || ' ', history, (ws.sessionAgentConfig?.language || 'en').toLowerCase())
      if (result === 'WHATSAPP_REQUEST') {
        whatsappRequested = true
        if (callLogger) callLogger.markWhatsAppRequested()
        return true
      }
      return false
    } catch (_) {
      return whatsappRequested === true
    }
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
    let callId = null
    
    if (callLogger) {
      console.log(`🛑 [MANUAL-TERMINATION] Found active call logger, terminating gracefully...`)
      console.log(`🛑 [MANUAL-TERMINATION] Call Logger Info:`, callLogger.getStats())
      
      // Get callId from call logger
      callId = callLogger.callSid
      
      // Check WebSocket state
      if (callLogger.ws) {
        console.log(`🛑 [MANUAL-TERMINATION] WebSocket State: ${callLogger.ws.readyState} (0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED)`)
      }
      
      // Try to disconnect via SanPBX API first if we have callId
      if (callId) {
        console.log(`🛑 [MANUAL-TERMINATION] Attempting to disconnect call via SanPBX API: ${callId}`)
        const disconnectResult = await disconnectCallViaAPI(callId, reason)
        
        if (disconnectResult.success) {
          console.log(`✅ [MANUAL-TERMINATION] Successfully disconnected call via API: ${callId}`)
        } else {
          console.log(`⚠️ [MANUAL-TERMINATION] API disconnect failed, continuing with graceful termination: ${disconnectResult.error}`)
        }
      }
      
      await callLogger.saveToDatabase(callLogger.currentLeadStatus || "maybe", agentConfig)
      return {
        success: true,
        message: 'Call terminated successfully',
        streamSid,
        callId,
        reason,
        method: 'graceful_termination_with_api',
        apiDisconnectResult: callId ? await disconnectCallViaAPI(callId, reason) : null
      }
    } else {
      console.log(`🛑 [MANUAL-TERMINATION] No active call logger found, trying to find callId from database...`)
      
      // Try to find callId from database first
      try {
        const CallLog = require("../models/CallLog")
        const activeCall = await CallLog.findOne({ streamSid, 'metadata.isActive': true })
        
        if (activeCall && activeCall.callSid) {
          callId = activeCall.callSid
          console.log(`🛑 [MANUAL-TERMINATION] Found callId from database: ${callId}`)
          
          // Try to disconnect via SanPBX API
          const disconnectResult = await disconnectCallViaAPI(callId, reason)
          
          if (disconnectResult.success) {
            console.log(`✅ [MANUAL-TERMINATION] Successfully disconnected call via API: ${callId}`)
          } else {
            console.log(`⚠️ [MANUAL-TERMINATION] API disconnect failed: ${disconnectResult.error}`)
          }
        }
      } catch (dbError) {
        console.log(`⚠️ [MANUAL-TERMINATION] Could not find callId from database: ${dbError.message}`)
      }
      
      // Update the call log directly in the database
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
            callId,
            reason,
            method: 'database_update_with_api',
            modifiedCount: result.modifiedCount,
            apiDisconnectResult: callId ? await disconnectCallViaAPI(callId, reason) : null
          }
        } else {
          return {
            success: false,
            message: 'No active calls found with this streamSid',
            streamSid,
            callId,
            reason,
            method: 'database_update',
            apiDisconnectResult: callId ? await disconnectCallViaAPI(callId, reason) : null
          }
        }
      } catch (dbError) {
        console.error(`❌ [MANUAL-TERMINATION] Database update error:`, dbError.message)
        return {
          success: false,
          message: 'Failed to update database',
          streamSid,
          callId,
          reason,
          method: 'database_update',
          error: dbError.message,
          apiDisconnectResult: callId ? await disconnectCallViaAPI(callId, reason) : null
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

/**
 * Disconnect a call by callId using SanPBX API
 * @param {string} callId - The call ID to disconnect
 * @param {string} reason - Reason for disconnection
 * @returns {Object} Result of disconnection attempt
 */
const disconnectCallByCallId = async (callId, reason = 'manual_disconnect') => {
  try {
    console.log(`🛑 [CALL-DISCONNECT] Attempting to disconnect call: ${callId}`)
    
    const result = await disconnectCallViaAPI(callId, reason)
    
    if (result.success) {
      console.log(`✅ [CALL-DISCONNECT] Successfully disconnected call: ${callId}`)
    } else {
      console.log(`❌ [CALL-DISCONNECT] Failed to disconnect call: ${callId} - ${result.error}`)
    }
    
    return result
  } catch (error) {
    console.error(`❌ [CALL-DISCONNECT] Error disconnecting call ${callId}:`, error.message)
    return {
      success: false,
      callId,
      reason,
      error: error.message
    }
  }
}

// Disposition detection using OpenAI based on agent's depositions
const detectDispositionWithOpenAI = async (conversationHistory, agentDepositions) => {
  const timer = createTimer("DISPOSITION_DETECTION")
  try {
    if (!agentDepositions || !Array.isArray(agentDepositions) || agentDepositions.length === 0) {
      console.log(`⚠️ [DISPOSITION-DETECTION] ${timer.end()}ms - No depositions configured for agent`)
      return { disposition: null, subDisposition: null, dispositionId: null, subDispositionId: null }
    }

    const depositionsList = agentDepositions.map((dep, index) => {
      const subDeps = dep.sub && Array.isArray(dep.sub) && dep.sub.length > 0 
        ? dep.sub.map((sub, subIndex) => `${subIndex + 1}. ${sub}`).join('\n        ')
        : 'No sub-dispositions'
      return `${index + 1}. ${dep.title}:
        Sub-dispositions:
        ${subDeps}`
    }).join('\n\n')

    const conversationText = conversationHistory
      .slice(-10)
      .map(msg => `${msg.role === 'user' ? 'User' : 'AI'}: ${msg.content}`)
      .join('\n')

    const dispositionPrompt = `Analyze the conversation history and determine the most appropriate disposition and sub-disposition based on the user's responses and conversation outcome.

Available Dispositions:
${depositionsList}

Conversation History:
${conversationText}

Instructions:
1. Analyze the user's interest level, responses, and conversation outcome
2. Select the most appropriate disposition from the list above
3. If the selected disposition has sub-dispositions, choose the most relevant one
4. If no sub-dispositions are available, return "N/A" for sub-disposition
5. If the conversation doesn't clearly fit any disposition, return "General Inquiry" as disposition and "N/A" as sub-disposition

Return your response in this exact format:
DISPOSITION: [exact title from the list]
SUB_DISPOSITION: [exact sub-disposition or "N/A"]`

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${API_KEYS.openai}` },
      body: JSON.stringify({ model: "gpt-4o-mini", messages: [{ role: "system", content: dispositionPrompt }], max_tokens: 100, temperature: 0.1 })
    })

    if (!response.ok) {
      console.log(`❌ [DISPOSITION-DETECTION] ${timer.end()}ms - Error: ${response.status}`)
      return { disposition: null, subDisposition: null, dispositionId: null, subDispositionId: null }
    }

    const data = await response.json()
    const result = data.choices[0]?.message?.content?.trim()
    const dispositionMatch = result?.match(/DISPOSITION:\s*(.+)/i)
    const subDispositionMatch = result?.match(/SUB_DISPOSITION:\s*(.+)/i)
    const dispositionTitle = dispositionMatch ? dispositionMatch[1].trim() : null
    const subDispositionTitle = subDispositionMatch ? subDispositionMatch[1].trim() : null

    const validDisposition = agentDepositions.find(dep => dep.title === dispositionTitle)
    if (!validDisposition) {
      console.log(`⚠️ [DISPOSITION-DETECTION] ${timer.end()}ms - Invalid disposition detected: ${dispositionTitle}`)
      return { disposition: null, subDisposition: null, dispositionId: null, subDispositionId: null }
    }

    let validSubDisposition = null
    let subDispositionId = null
    if (subDispositionTitle && subDispositionTitle !== "N/A" && Array.isArray(validDisposition.sub)) {
      validSubDisposition = validDisposition.sub.find(sub => sub === subDispositionTitle)
      if (!validSubDisposition) {
        validSubDisposition = validDisposition.sub.find(sub => sub.toLowerCase() === subDispositionTitle.toLowerCase())
      }
      if (!validSubDisposition) {
        validSubDisposition = validDisposition.sub.find(sub => sub.toLowerCase().includes(subDispositionTitle.toLowerCase()) || subDispositionTitle.toLowerCase().includes(sub.toLowerCase()))
      }
      if (validSubDisposition) {
        subDispositionId = validSubDisposition
        console.log(`✅ [DISPOSITION-DETECTION] Matched sub-disposition: ${subDispositionTitle} -> ${validSubDisposition}`)
      } else {
        console.log(`⚠️ [DISPOSITION-DETECTION] ${timer.end()}ms - Invalid sub-disposition detected: ${subDispositionTitle}`)
      }
    }

    console.log(`🕒 [DISPOSITION-DETECTION] ${timer.end()}ms - Detected: ${dispositionTitle} (ID: ${validDisposition._id}) | ${validSubDisposition || 'N/A'}`)
    return { disposition: dispositionTitle, subDisposition: validSubDisposition || null, dispositionId: validDisposition._id, subDispositionId }
  } catch (error) {
    console.log(`❌ [DISPOSITION-DETECTION] ${timer.end()}ms - Error: ${error.message}`)
    return { disposition: null, subDisposition: null, dispositionId: null, subDispositionId: null }
  }
}

module.exports = { 
  setupSanPbxWebSocketServer, 
  terminateCallByStreamSid,
  disconnectCallByCallId,
  disconnectCallViaAPI,
  isCallActive,
  forceDisconnectWebSocket,
  activeWebSockets,
  // Export termination methods for external use
  terminationMethods: {
    graceful: (callLogger, message, language) => callLogger?.saveToDatabase(callLogger.currentLeadStatus || "maybe", null),
    fast: (callLogger, reason) => callLogger?.saveToDatabase(callLogger.currentLeadStatus || "maybe", null),
    ultraFast: (callLogger, message, language, reason) => callLogger?.saveToDatabase(callLogger.currentLeadStatus || "maybe", null)
  }
}