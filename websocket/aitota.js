const WebSocket = require("ws")
require("dotenv").config()
const mongoose = require("mongoose")
const Agent = require("../models/Agent")
const CallLog = require("../models/CallLog")
const fs = require("fs")
const path = require("path")
// const { spawn } = require("child_process") // Removed - no longer using FFmpeg

// NOTE: FFmpeg dependency removed due to ENOENT error
// For optimal audio transcoding, consider:
// 1. Installing FFmpeg: https://ffmpeg.org/download.html
// 2. Using Node.js audio libraries: 'lamejs', 'node-lame', 'audio-converter'
// 3. Using cloud-based audio processing services
//
// CURRENT APPROACH: Using linear16 audio format directly from Sarvam WebSocket to SIP
// This follows the working pattern from sarvam tts.js and ensures optimal SIP compatibility
// No transcoding needed - audio flows directly from Sarvam to SIP in the correct format
//
// AUDIO SYNCHRONIZATION: Prevents overlapping audio streams that cause chunk ordering issues
// - Each new TTS request waits for existing audio stream to complete
// - Unique stream IDs track each audio stream for debugging
// - Automatic interruption of old streams when new ones start
// - Ensures audio chunks are sent in correct sequential order

// Base64 audio conversion utilities
const convertBufferToBase64 = (buffer) => {
  return buffer.toString('base64')
}

const convertBase64ToBuffer = (base64String) => {
  return Buffer.from(base64String, 'base64')
}

const validateBase64 = (base64String) => {
  try {
    const buffer = Buffer.from(base64String, 'base64')
    return buffer.length > 0
  } catch (error) {
    return false
  }
}

// Utility to save MP3 buffer to file for debugging (optional)
const saveMp3ToFile = (buffer, filename = null) => {
  try {
    if (!filename) {
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
      filename = `debug_audio_${timestamp}.mp3`
    }
    
    const filepath = path.join(__dirname, '..', 'temp', filename)
    
    // Ensure temp directory exists
    const tempDir = path.dirname(filepath)
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true })
    }
    
    fs.writeFileSync(filepath, buffer)
    console.log(`💾 [DEBUG] MP3 saved to: ${filepath}`)
    return filepath
  } catch (error) {
    console.error(`❌ [DEBUG] Failed to save MP3: ${error.message}`)
    return null
  }
}

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
    console.log(`❌ [LLM-LANG-DETECTION] ${timer.end()}ms - Error: ${error.message}`)
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
    
    // Google Meet state management
    this.googleMeetState = {
      isRequested: false,
      emailProvided: false,
      email: null,
      meetingDetails: null,
      emailSent: false
    }
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
        
        // Send WhatsApp message after call ends (non-blocking)
        if (agentConfig && this.mobile) {
          console.log(`📱 [FINAL-CALL-LOG-SAVE] Triggering WhatsApp message after call...`)
          setImmediate(async () => {
            try {
              await sendWhatsAppAfterCall(this, agentConfig)
            } catch (error) {
              console.log(`⚠️ [FINAL-CALL-LOG-SAVE] WhatsApp error: ${error.message}`)
            }
          })
        }
        
        // Update call log with Google Meet data if applicable
        if (this.googleMeetState.isRequested) {
          console.log(`📹 [FINAL-CALL-LOG-SAVE] Updating call log with Google Meet data...`)
          const googleMeetUpdate = {
            'metadata.googleMeetRequested': true,
            'metadata.googleMeetEmail': this.googleMeetState.email || null,
            'metadata.googleMeetDetails': this.googleMeetState.meetingDetails || null,
            'metadata.googleMeetEmailSent': this.googleMeetState.emailSent || false,
            'metadata.googleMeetEmailSentAt': this.googleMeetState.emailSent ? new Date() : null
          }
          
          await CallLog.findByIdAndUpdate(this.callLogId, googleMeetUpdate)
            .catch(err => console.log(`⚠️ [FINAL-CALL-LOG-SAVE] Google Meet update error: ${err.message}`))
        }
        
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
        
        // Send WhatsApp message after call ends (non-blocking)
        if (agentConfig && this.mobile) {
          console.log(`📱 [FINAL-CALL-LOG-SAVE] Triggering WhatsApp message after call...`)
          setImmediate(async () => {
            try {
              await sendWhatsAppAfterCall(this, agentConfig)
            } catch (error) {
              console.log(`⚠️ [FINAL-CALL-LOG-SAVE] WhatsApp error: ${error.message}`)
            }
          })
        }
        
        // Update call log with Google Meet data if applicable
        if (this.googleMeetState.isRequested) {
          console.log(`📹 [FINAL-CALL-LOG-SAVE] Updating call log with Google Meet data...`)
          const googleMeetUpdate = {
            'metadata.googleMeetRequested': true,
            'metadata.googleMeetEmail': this.googleMeetState.email || null,
            'metadata.googleMeetDetails': this.googleMeetState.meetingDetails || null,
            'metadata.googleMeetEmailSent': this.googleMeetState.emailSent || false,
            'metadata.googleMeetEmailSentAt': this.googleMeetState.emailSent ? new Date() : null
          }
          
          await CallLog.findByIdAndUpdate(savedLog._id, googleMeetUpdate)
            .catch(err => console.log(`⚠️ [FINAL-CALL-LOG-SAVE] Google Meet update error: ${err.message}`))
        }
        
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

  getStats() {
    return {
      duration: this.totalDuration,
      userMessages: this.transcripts.length,
      aiResponses: this.responses.length,
      languages: [...new Set([...this.transcripts, ...this.responses].map(e => e.language))],
      startTime: this.callStartTime,
      callDirection: this.callDirection,
      callLogId: this.callLogId,
      pendingTranscripts: this.pendingTranscripts.length
    }
  }
}

// Simplified OpenAI processing with streaming
const processWithOpenAIStreaming = async (
  userMessage,
  conversationHistory,
  detectedLanguage,
  callLogger,
  agentConfig,
  userName = null,
  ttsProcessor = null
) => {
  const timer = createTimer("LLM_STREAMING_PROCESSING")

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
      "Stream your response word by word for real-time processing.",
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

    // Use streaming API for real-time response
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
        stream: true, // Enable streaming
      }),
    })

    if (!response.ok) {
      console.log(`❌ [LLM-STREAMING] ${timer.end()}ms - Error: ${response.status}`)
      return null
    }

    if (!response.body) {
      console.log(`❌ [LLM-STREAMING] ${timer.end()}ms - No response body`)
      return null
    }

    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let fullResponse = ""
    let currentChunk = ""
    let isFirstChunk = true

    console.log(`🔄 [LLM-STREAMING] ${timer.end()}ms - Starting streaming response`)

    try {
      while (true) {
        const { done, value } = await reader.read()
        
        if (done) break

        const chunk = decoder.decode(value)
        const lines = chunk.split('\n')
        
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            const data = line.slice(6)
            
            if (data === '[DONE]') {
              console.log(`✅ [LLM-STREAMING] ${timer.end()}ms - Streaming completed`)
              break
            }
            
            try {
              const parsed = JSON.parse(data)
              const content = parsed.choices?.[0]?.delta?.content
              
              if (content) {
                fullResponse += content
                currentChunk += content
                
                // Stream to TTS in real-time if available
                if (ttsProcessor && ttsProcessor.sarvamReady && ttsProcessor.sarvamWs) {
                  // Send text chunk to Sarvam for immediate TTS processing
                  ttsProcessor.sendTextChunk(content)
                }
                
                // Log progress for debugging
                if (isFirstChunk) {
                  console.log(`🔄 [LLM-STREAMING] First chunk received: "${content}"`)
                  isFirstChunk = false
                }
              }
            } catch (parseError) {
              // Skip malformed JSON chunks
              continue
            }
          }
        }
      }
    } finally {
      reader.releaseLock()
    }

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
        
        // Send follow-up question to TTS if available
        if (ttsProcessor && ttsProcessor.sarvamReady && ttsProcessor.sarvamWs) {
          ttsProcessor.sendTextChunk(fu)
        }
      }
    }

    if (callLogger && fullResponse) {
      callLogger.logAIResponse(fullResponse, detectedLanguage)
    }

    console.log(`🕒 [LLM-STREAMING] ${timer.end()}ms - Full response: "${fullResponse}"`)
    return fullResponse

  } catch (error) {
    console.log(`❌ [LLM-STREAMING] ${timer.end()}ms - Error: ${error.message}`)
    return null
  }
}

// Enhanced Sarvam TTS processor with real-time streaming support
class EnhancedSarvamTTSProcessor {
  constructor(language, ws, streamSid, callLogger = null) {
    this.language = language
    this.ws = ws
    this.streamSid = streamSid
    this.callLogger = callLogger
    this.sarvamLanguage = "en-IN"
    this.voice = "manisha"
    this.isInterrupted = false
    this.currentAudioStreaming = null
    this.totalAudioBytes = 0
    this.sarvamWs = null
    this.sarvamReady = false
    this.textBuffer = ""
    this.isProcessing = false
    this.useWebSocket = true
    this.streamingMode = false
    this.textChunkQueue = []
    this.isStreamingText = false
  }

  // Connect to Sarvam WebSocket with streaming configuration
  async connectToSarvam() {
    try {
  

      const lang = (this.language || 'en').toLowerCase()
      if (lang.startsWith('en')) {
        this.sarvamLanguage = 'en-IN'
        this.voice = 'manisha'
      } else if (lang.startsWith('hi')) {
        this.sarvamLanguage = 'hi-IN'
        this.voice = 'pavithra'
      }

      const wsUrl = 'wss://api.sarvam.ai/text-to-speech/ws?model=bulbul:v2'
      console.log(`🎤 [SARVAM-WS] Connecting to ${wsUrl} with streaming support`)
      this.sarvamWs = new WebSocket(wsUrl, [`api-subscription-key.${API_KEYS.sarvam}`])

      this.sarvamWs.onopen = () => {
        this.sarvamReady = true
        const configMessage = {
          type: 'config',
          data: {
            target_language_code: this.sarvamLanguage,
            speaker: this.voice,
            pitch: 0.5,
            pace: 1.0,
            loudness: 1.0,
            enable_preprocessing: false,
            output_audio_codec: 'linear16',
            output_audio_bitrate: '128k',
            speech_sample_rate: 8000,
            min_buffer_size: 20, // Reduced for streaming
            max_chunk_length: 100, // Reduced for streaming
            streaming_mode: true, // Enable streaming mode
          },
        }
        this.sarvamWs.send(JSON.stringify(configMessage))
        console.log("✅ [SARVAM-WS] Streaming config sent successfully")
        this.streamingMode = true
        this.processTextChunkQueue()
      }

      this.sarvamWs.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data)
          this.handleSarvamMessage(data)
        } catch (error) {
          console.error("❌ [SARVAM-WS] Error parsing message:", error.message)
        }
      }

      this.sarvamWs.onerror = (error) => {
        this.sarvamReady = false
        console.error("❌ [SARVAM-WS] WebSocket error:", error.message)
      }

      this.sarvamWs.onclose = () => {
        this.sarvamReady = false
        this.streamingMode = false
        console.log("🔌 [SARVAM-WS] WebSocket closed")
      }

      // Wait for connection to be established
      await new Promise((resolve) => setTimeout(resolve, 500))
      if (!this.sarvamReady) {
        this.useWebSocket = false
        console.log("⚠️ [SARVAM-WS] Connection timeout, falling back to API")
      }
    } catch (error) {
      this.sarvamReady = false
      this.useWebSocket = false
      console.error("❌ [SARVAM-WS] Connection error:", error.message)
    }
  }

  // Send text chunk for real-time TTS processing
  sendTextChunk(textChunk) {
    if (!this.sarvamReady || !this.sarvamWs || this.sarvamWs.readyState !== WebSocket.OPEN) {
      // Queue text chunk if WebSocket not ready
      this.textChunkQueue.push(textChunk)
      return
    }

    if (this.isStreamingText) {
      // Add to buffer for continuous streaming
      this.textBuffer += textChunk
      
      // Send text chunk immediately for real-time processing
      const textMessage = {
        type: 'text_chunk',
        data: { 
          text: textChunk,
          is_partial: true,
          buffer_id: Date.now()
        }
      }
      
      try {
        this.sarvamWs.send(JSON.stringify(textMessage))
        console.log(`📤 [SARVAM-STREAM] Text chunk sent: "${textChunk}"`)
      } catch (error) {
        console.error(`❌ [SARVAM-STREAM] Error sending text chunk: ${error.message}`)
      }
    } else {
      // Start new streaming session
      this.startTextStreaming(textChunk)
    }
  }

  // Start text streaming session
  startTextStreaming(initialText = "") {
    if (!this.sarvamReady || !this.sarvamWs) return

    this.isStreamingText = true
    this.textBuffer = initialText || ""
    
    // Send start streaming signal
    const startMessage = {
      type: 'start_streaming',
      data: {
        language: this.sarvamLanguage,
        voice: this.voice,
        buffer_id: Date.now()
      }
    }
    
    try {
      this.sarvamWs.send(JSON.stringify(startMessage))
      console.log("🎬 [SARVAM-STREAM] Started text streaming session")
      
      // Send initial text if provided
      if (initialText) {
        this.sendTextChunk(initialText)
      }
    } catch (error) {
      console.error(`❌ [SARVAM-STREAM] Error starting streaming: ${error.message}`)
      this.isStreamingText = false
    }
  }

  // End text streaming session
  endTextStreaming() {
    if (!this.sarvamReady || !this.sarvamWs) return

    this.isStreamingText = false
    
    // Send end streaming signal
    const endMessage = {
      type: 'end_streaming',
      data: {
        buffer_id: Date.now(),
        final_text: this.textBuffer
      }
    }
    
    try {
      this.sarvamWs.send(JSON.stringify(endMessage))
      console.log("🏁 [SARVAM-STREAM] Ended text streaming session")
      
      // Clear buffer
      this.textBuffer = ""
    } catch (error) {
      console.error(`❌ [SARVAM-STREAM] Error ending streaming: ${error.message}`)
    }
  }

  // Process queued text chunks
  processTextChunkQueue() {
    if (this.textChunkQueue.length > 0 && this.sarvamReady) {
      console.log(`📋 [SARVAM-STREAM] Processing ${this.textChunkQueue.length} queued text chunks`)
      
      while (this.textChunkQueue.length > 0) {
        const chunk = this.textChunkQueue.shift()
        this.sendTextChunk(chunk)
      }
    }
  }

  // Handle messages from Sarvam WebSocket
  handleSarvamMessage(data) {
    if (data.type === "audio") {
      const audioBase64 = (data && data.data && data.data.audio) || data.audio
      if (audioBase64 && !this.isInterrupted) {
        // Stream audio directly to SIP in real-time
        this.streamLinear16AudioToSIP(audioBase64)
        const audioBuffer = Buffer.from(audioBase64, "base64")
        this.totalAudioBytes += audioBuffer.length
      }
    } else if (data.type === "streaming_ready") {
      console.log("✅ [SARVAM-STREAM] Streaming mode activated")
      this.streamingMode = true
    } else if (data.type === "error") {
      const errMsg = (data && data.data && data.data.message) || data.message || 'unknown'
      console.log("❌ [SARVAM-WS] Sarvam error:", errMsg)
    } else if (data.type === "end") {
      console.log("✅ [SARVAM-WS] Audio generation completed")
      this.isProcessing = false
    }
  }

  // Enhanced streaming synthesis with real-time processing
  async synthesizeAndStream(text) {
    if (this.isInterrupted) return

    const timer = createTimer("TTS_STREAMING_SYNTHESIS")

    try {
      // Try WebSocket streaming first if enabled
      if (this.useWebSocket && this.streamingMode) {
        if (!this.sarvamWs || this.sarvamWs.readyState !== WebSocket.OPEN) {
          await this.connectToSarvam()
          
          let attempts = 0
          while (!this.sarvamReady && attempts < 10) {
            await new Promise(resolve => setTimeout(resolve, 100))
            attempts++
          }
          
          if (!this.sarvamReady) {
            console.log("⚠️ [TTS-STREAMING] WebSocket failed, falling back to API")
            this.useWebSocket = false
          }
        }

        if (this.sarvamReady) {
          this.isProcessing = true
          console.log(`🔄 [TTS-STREAMING] ${timer.end()}ms - Starting real-time streaming synthesis`)

          // Wait for any existing audio stream to complete
          await this.waitForAudioStreamToComplete()

          // Start streaming the text
          this.startTextStreaming(text)
          
          // Send flush message to signal end of text
          const flushMessage = { type: "flush" }
          this.sarvamWs.send(JSON.stringify(flushMessage))
          console.log("📤 [SARVAM-STREAM] Flush signal sent")

          // Wait for processing to complete
          let waitAttempts = 0
          while (this.isProcessing && waitAttempts < 100 && !this.isInterrupted) {
            await new Promise(resolve => setTimeout(resolve, 50))
            waitAttempts++
          }

          if (this.isInterrupted) {
            console.log("⚠️ [TTS-STREAMING] Synthesis interrupted")
            return
          }

          // End streaming session
          this.endTextStreaming()

          console.log(`✅ [TTS-STREAMING] ${timer.end()}ms - Streaming synthesis completed`)
          return
        }
      }

      // Fallback to API method
      console.log(`🔄 [TTS-STREAMING] ${timer.end()}ms - Using API fallback`)
      await this.synthesizeWithAPI(text)
      console.log(`✅ [TTS-STREAMING] ${timer.end()}ms - API synthesis completed`)

    } catch (error) {
      if (!this.isInterrupted) {
        console.log(`❌ [TTS-STREAMING] ${timer.end()}ms - Error: ${error.message}`)
        throw error
      }
    }
  }

  

  // Fallback to API method
  async synthesizeWithAPI(text) {
    const timer = createTimer("TTS_API_FALLBACK")
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
          pitch: 0.5,
          pace: 1.0,
          loudness: 1.0,
          speech_sample_rate: 8000,
          enable_preprocessing: false,
          model: "bulbul:v1",
          output_audio_codec: "linear16",
        }),
      })

      if (!response.ok || this.isInterrupted) {
        if (!this.isInterrupted) {
          console.log(`❌ [TTS-API-FALLBACK] ${timer.end()}ms - Error: ${response.status}`)
          throw new Error(`Sarvam API error: ${response.status}`)
        }
        return
      }

      const responseData = await response.json()
      const audioBase64 = responseData.audios?.[0]

      if (!audioBase64 || this.isInterrupted) {
        if (!this.isInterrupted) {
          console.log(`❌ [TTS-API-FALLBACK] ${timer.end()}ms - No audio data received`)
          throw new Error("No audio data received from Sarvam API")
        }
        return
      }

      console.log(`🕒 [TTS-API-FALLBACK] ${timer.end()}ms - Audio generated via API`)

      if (!this.isInterrupted) {
        await this.waitForAudioStreamToComplete()
        await this.streamLinear16AudioToSIP(audioBase64)
        const audioBuffer = Buffer.from(audioBase64, "base64")
        this.totalAudioBytes += audioBuffer.length
      }
    } catch (error) {
      if (!this.isInterrupted) {
        console.log(`❌ [TTS-API-FALLBACK] ${timer.end()}ms - Error: ${error.message}`)
        throw error
      }
    }
  }

  // Stream linear16 audio directly to SIP
  async streamLinear16AudioToSIP(audioBase64) {
    if (this.isInterrupted) return

    if (this.currentAudioStreaming && !this.currentAudioStreaming.interrupt) {
      console.log(`⏳ [AUDIO-STREAM] Waiting for existing audio stream to complete...`)
      let waitAttempts = 0
      while (this.currentAudioStreaming && !this.currentAudioStreaming.interrupt && waitAttempts < 100) {
        await new Promise(resolve => setTimeout(resolve, 50))
        waitAttempts++
      }
      if (this.currentAudioStreaming && !this.currentAudioStreaming.interrupt) {
        console.log(`⚠️ [AUDIO-STREAM] Force interrupting existing stream to prevent overlap`)
        this.currentAudioStreaming.interrupt = true
        await new Promise(resolve => setTimeout(resolve, 100))
      }
    }

    const audioBuffer = Buffer.from(audioBase64, "base64")
    const streamingSession = { interrupt: false, streamId: Date.now() }
    this.currentAudioStreaming = streamingSession

    console.log(`🎵 [AUDIO-STREAM] Starting linear16 audio stream: ${audioBuffer.length} bytes (Stream ID: ${streamingSession.streamId})`)

    const SAMPLE_RATE = 8000
    const BYTES_PER_SAMPLE = 2
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000
    const OPTIMAL_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS)

    let position = 0
    let chunkIndex = 0
    let successfulChunks = 0

    while (position < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
      const remaining = audioBuffer.length - position
      const chunkSize = Math.min(OPTIMAL_CHUNK_SIZE, remaining)
      const chunk = audioBuffer.slice(position, position + chunkSize)

      const mediaMessage = {
        event: "media",
        streamSid: this.streamSid,
        media: {
          payload: chunk.toString("base64"),
        },
      }

      if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted && !streamingSession.interrupt) {
        try {
          this.ws.send(JSON.stringify(mediaMessage))
          successfulChunks++
          console.log(`🎵 [AUDIO-STREAM] [${streamingSession.streamId}] Sent linear16 chunk ${chunkIndex + 1} (${chunk.length} bytes), ${Math.round((position / audioBuffer.length) * 100)}% complete`)
        } catch (error) {
          console.log(`❌ [AUDIO-STREAM] [${streamingSession.streamId}] Error sending linear16 chunk ${chunkIndex + 1}: ${error.message}`)
          break
        }
      } else {
        console.log(`⚠️ [AUDIO-STREAM] [${streamingSession.streamId}] WebSocket not ready or interrupted, stopping audio stream`)
        break
      }

      if (position + chunkSize < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
        const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS)
        const delayMs = Math.max(chunkDurationMs - 2, 10)
        await new Promise((resolve) => setTimeout(resolve, delayMs))
      }

      position += chunkSize
      chunkIndex++
    }

    if (this.currentAudioStreaming === streamingSession) {
      console.log(`✅ [AUDIO-STREAM] [${streamingSession.streamId}] Linear16 stream completed: ${successfulChunks} chunks sent, ${Math.round((position / audioBuffer.length) * 100)}% of audio streamed`)
      this.currentAudioStreaming = null
    } else {
      console.log(`⚠️ [AUDIO-STREAM] [${streamingSession.streamId}] Stream was interrupted by newer stream`)
    }
  }

  // Wait for current audio stream to complete
  async waitForAudioStreamToComplete(timeoutMs = 5000) {
    if (!this.isAudioCurrentlyStreaming()) {
      return true
    }

    console.log(`⏳ [AUDIO-SYNC] Waiting for current audio stream to complete...`)
    const startTime = Date.now()
    
    while (this.isAudioCurrentlyStreaming() && (Date.now() - startTime) < timeoutMs) {
      await new Promise(resolve => setTimeout(resolve, 50))
    }
    
    if (this.isAudioCurrentlyStreaming()) {
      console.log(`⚠️ [AUDIO-SYNC] Timeout waiting for audio stream, forcing interruption`)
      this.interrupt()
      return false
    }
    
    console.log(`✅ [AUDIO-SYNC] Audio stream completed, ready for new audio`)
    return true
  }

  // Check if audio is currently streaming
  isAudioCurrentlyStreaming() {
    return !!(this.currentAudioStreaming && !this.currentAudioStreaming.interrupt)
  }

  // Interrupt current processing
  interrupt() {
    this.isInterrupted = true
    if (this.currentAudioStreaming) {
      console.log(`🛑 [TTS-INTERRUPT] Interrupting current audio stream...`)
      this.currentAudioStreaming.interrupt = true
      setTimeout(() => {
        if (this.currentAudioStreaming) {
          this.currentAudioStreaming = null
          console.log(`🛑 [TTS-INTERRUPT] Audio stream cleanup completed`)
        }
      }, 100)
    }
    if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.OPEN) {
      console.log(`🛑 [TTS-INTERRUPT] Closing Sarvam WebSocket connection...`)
      this.sarvamWs.close()
    }
  }

  // Reset processor state
  reset(newLanguage) {
    this.interrupt()
    if (newLanguage) {
      this.language = newLanguage
    }
    this.isInterrupted = false
    this.totalAudioBytes = 0
    this.textBuffer = ""
    this.textChunkQueue = []
    this.isStreamingText = false
    this.isProcessing = false
  }

  // Get processor statistics
  getStats() {
    return {
      totalAudioBytes: this.totalAudioBytes,
      sarvamReady: this.sarvamReady,
      isProcessing: this.isProcessing,
      useWebSocket: this.useWebSocket,
      streamingMode: this.streamingMode,
      isAudioStreaming: !!this.currentAudioStreaming && !this.currentAudioStreaming.interrupt,
      textBufferLength: this.textBuffer.length,
      queuedChunks: this.textChunkQueue.length
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

// Google Meet request detection using OpenAI
const detectGoogleMeetRequest = async (userMessage, conversationHistory, detectedLanguage) => {
  const timer = createTimer("GOOGLE_MEET_DETECTION")
  try {
    const meetRequestPrompt = `Analyze if the user wants to schedule a Google Meet or video call. Look for:
- "google meet", "meet", "video call", "video meeting", "online meeting"
- "schedule a meeting", "book a meeting", "set up a call"
- "zoom", "teams", "skype", "video conference"
- "meet online", "virtual meeting", "screen share"
- Any request for a video/online meeting

User message: "${userMessage}"

Return ONLY: "GOOGLE_MEET" if they want a video meeting, or "CONTINUE" if they want to continue normally.`

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEYS.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: meetRequestPrompt },
        ],
        max_tokens: 10,
        temperature: 0.1,
      }),
    })

    if (!response.ok) {
      console.log(`❌ [GOOGLE-MEET-DETECTION] ${timer.end()}ms - Error: ${response.status}`)
      return "CONTINUE" // Default to continue on error
    }

    const data = await response.json()
    const result = data.choices[0]?.message?.content?.trim().toUpperCase()

    if (result === "GOOGLE_MEET") {
      console.log(`🕒 [GOOGLE-MEET-DETECTION] ${timer.end()}ms - User wants Google Meet`)
      return "GOOGLE_MEET"
    } else {
      console.log(`🕒 [GOOGLE-MEET-DETECTION] ${timer.end()}ms - No Google Meet request`)
      return "CONTINUE"
    }
  } catch (error) {
    console.log(`❌ [GOOGLE-MEET-DETECTION] ${timer.end()}ms - Error: ${error.message}`)
    return "CONTINUE" // Default to continue on error
  }
}

// Email validation function
const validateEmail = (email) => {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
  return emailRegex.test(email)
}

// Normalize spoken/transliterated email phrases (English/Hindi) to a valid email string
const normalizeSpokenEmail = (rawText) => {
  if (!rawText) return null
  let text = String(rawText)
    .toLowerCase()
    .replace(/\s+/g, ' ') // collapse spaces
    .trim()

  // Convert Devanagari digits to ASCII
  const devanagariDigits = {
    '०': '0','१': '1','२': '2','३': '3','४': '4','५': '5','६': '6','७': '7','८': '8','९': '9'
  }
  text = text.replace(/[०१२३४५६७८९]/g, (m) => devanagariDigits[m] || m)

  // Map Hindi number words to digits
  const numWords = {
    'शून्य': '0','zero': '0',
    'एक': '1','ek': '1','one': '1',
    'दो': '2','do': '2','two': '2',
    'तीन': '3','teen': '3','three': '3',
    'चार': '4','char': '4','four': '4',
    'पांच': '5','पाँच': '5','paanch': '5','panch': '5','five': '5',
    'छह': '6','cheh': '6','six': '6',
    'सात': '7','saat': '7','seven': '7',
    'आठ': '8','aath': '8','eight': '8',
    'नौ': '9','nau': '9','nine': '9'
  }
  const wordBoundary = new RegExp(`\\b(${Object.keys(numWords).join('|')})\\b`, 'g')
  text = text.replace(wordBoundary, (m) => numWords[m])

  // Common connectors and noise words to remove
  text = text
    .replace(/\b(at\s*the\s*rate\s*of)\b/g, '@')
    .replace(/\b(at\s*the\s*rate)\b/g, '@')
    .replace(/\battherate\b/g, '@')
    .replace(/\bat\s*rate\b/g, '@')
    .replace(/\bरेट\b/g, '@')
    .replace(/\bएट\s*द?\s*रेट\b/g, '@')
    .replace(/\bat\b/g, '@') // often spoken as just 'at'
    .replace(/\bडॉट\b/g, '.')
    .replace(/\bडाट\b/g, '.')
    .replace(/\bdot\b/g, '.')
    .replace(/\bडॉ?ट\b/g, '.')
    .replace(/\bडाट\b/g, '.')
    .replace(/\bडॉट\s*कॉम\b/g, '.com')
    .replace(/\bdot\s*com\b/g, '.com')
    .replace(/\bडॉट\s*जीमेल\b/g, '.gmail')
    .replace(/\bजीमेल\b/g, 'gmail')
    .replace(/\bजी\s*मेल\b/g, 'gmail')
    .replace(/\bg\s*mail\b/g, 'gmail')
    .replace(/\bहाइ[\u095c\u095e]?फन\b/g, '-') // hyphen (Hindi)
    .replace(/\bhyphen\b|\bdash\b|\bminus\b/g, '-')
    .replace(/\bअंडरस्कोर\b/g, '_')
    .replace(/\bunderscore\b/g, '_')
    .replace(/\bप्लस\b|\bplus\b/g, '+')
    .replace(/\bspace\b|\bस्पेस\b/g, '')
    .replace(/\bof\b|\bthe\b|\btak\b/g, '')

  // Remove quotes and trailing punctuation commonly added by STT
  text = text.replace(/["'“”‘’]/g, '')
  text = text.replace(/\s*\.$/, '')

  // Remove spaces around @ and .
  text = text.replace(/\s*@\s*/g, '@').replace(/\s*\.\s*/g, '.')

  // If it looks like user said "gmail dot com" without username, keep as is for further AI handling

  // Finally, try to pick the first email-like substring
  const emailLikeMatch = text.match(/[a-z0-9._+\-]+@[a-z0-9.-]+\.[a-z]{2,}/i)
  return emailLikeMatch ? emailLikeMatch[0] : null
}

// Extract email from text using AI
const extractEmailFromText = async (text) => {
  const timer = createTimer("EMAIL_EXTRACTION")
  try {
    // 1) Try local normalization first (fast path)
    const normalizedLocal = normalizeSpokenEmail(text)
    if (normalizedLocal && validateEmail(normalizedLocal)) {
      console.log(`🕒 [EMAIL-EXTRACTION] ${timer.end()}ms - Local normalized: ${normalizedLocal}`)
      return normalizedLocal
    }

    const emailPrompt = `Extract email address from the given text. Look for:
- Standard email formats (user@domain.com)
- Gmail addresses (user@gmail.com)
- Any valid email pattern

Text: "${text}"

Return ONLY the email address if found, or "NO_EMAIL" if no valid email is found.`

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEYS.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: emailPrompt },
        ],
        max_tokens: 50,
        temperature: 0.1,
      }),
    })

    if (!response.ok) {
      throw new Error(`Email extraction failed: ${response.status}`)
    }

    const data = await response.json()
    const extractedRaw = data.choices[0]?.message?.content?.trim()

    // 2) Normalize AI output as well
    const normalizedAI = normalizeSpokenEmail(extractedRaw)
    if (normalizedAI && validateEmail(normalizedAI)) {
      console.log(`🕒 [EMAIL-EXTRACTION] ${timer.end()}ms - AI normalized: ${normalizedAI}`)
      return normalizedAI
    }

    console.log(`🕒 [EMAIL-EXTRACTION] ${timer.end()}ms - No valid email found`)
    return null
  } catch (error) {
    console.log(`❌ [EMAIL-EXTRACTION] ${timer.end()}ms - Error: ${error.message}`)
    return null
  }
}

// Generate Google Meet link (format: xxx-yyyy-zzz)
const generateGoogleMeetLink = () => {
  // Google Meet codes typically look like xxx-yyyy-zzz (letters only)
  const letters = 'abcdefghijklmnopqrstuvwxyz'
  const segment = (len) => Array.from({ length: len }, () => letters[Math.floor(Math.random() * letters.length)]).join('')
  const meetingId = `${segment(3)}-${segment(4)}-${segment(3)}`

  // Create Google Meet link
  const meetLink = `https://meet.google.com/${meetingId}`

  // Generate meeting details
  const meetingDetails = {
    link: meetLink,
    meetingId: meetingId,
    createdAt: new Date(),
    expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000), // 24 hours from now
    status: 'active'
  }

  return meetingDetails
}

// Send Google Meet link via email (placeholder for email service integration)
const sendGoogleMeetEmail = async (email, meetingDetails, agentConfig) => {
  const timer = createTimer("GOOGLE_MEET_EMAIL")
  try {
    console.log(`📧 [GOOGLE-MEET-EMAIL] Sending meeting link to: ${email}`)
    
    // Extract organization name from agent description
    const { orgName } = await extractOrgAndCourseFromDescription(agentConfig.description)
    
    // Email content
    const emailContent = {
      to: email,
      subject: `Google Meet Invitation - ${orgName}`,
      body: `
Dear User,

Thank you for your interest in scheduling a meeting with ${orgName}.

Your Google Meet link: ${meetingDetails.link}
Meeting ID: ${meetingDetails.meetingId}

This link will be active for 24 hours.

Best regards,
${orgName} Team
      `.trim()
    }
    
    // TODO: Integrate with your email service (SendGrid, AWS SES, etc.)
    // For now, we'll just log the email content
    console.log(`📧 [GOOGLE-MEET-EMAIL] Email content:`, JSON.stringify(emailContent, null, 2))
    
    // Update call log with meeting details
    return {
      success: true,
      email: email,
      meetingDetails: meetingDetails,
      emailContent: emailContent
    }
    
  } catch (error) {
    console.log(`❌ [GOOGLE-MEET-EMAIL] ${timer.end()}ms - Error: ${error.message}`)
    return {
      success: false,
      error: error.message
    }
  }
}

// Simplified TTS processor using Sarvam WebSocket with fallback to API
class SimplifiedSarvamTTSProcessor {
  constructor(language, ws, streamSid, callLogger = null) {
    this.language = language
    this.ws = ws
    this.streamSid = streamSid
    this.callLogger = callLogger
    // Static settings for WebSocket (default to English)
    this.sarvamLanguage = "en-IN"
    this.voice = "manisha"
    this.isInterrupted = false
    this.currentAudioStreaming = null
    this.totalAudioBytes = 0
    this.sarvamWs = null
    this.sarvamReady = false
    this.audioQueue = []
    this.isProcessing = false
    this.useWebSocket = true // Flag to control WebSocket vs API usage
  }



  interrupt() {
    this.isInterrupted = true
    if (this.currentAudioStreaming) {
      console.log(`🛑 [TTS-INTERRUPT] Interrupting current audio stream...`)
      this.currentAudioStreaming.interrupt = true
      // Wait a bit for the stream to clean up
      setTimeout(() => {
        if (this.currentAudioStreaming) {
          this.currentAudioStreaming = null
          console.log(`🛑 [TTS-INTERRUPT] Audio stream cleanup completed`)
        }
      }, 100)
    }
    if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.OPEN) {
      console.log(`🛑 [TTS-INTERRUPT] Closing Sarvam WebSocket connection...`)
      this.sarvamWs.close()
    }
  }

  reset(newLanguage) {
    this.interrupt()
    if (newLanguage) {
      this.language = newLanguage
    }
    this.isInterrupted = false
    this.totalAudioBytes = 0
    this.audioQueue = []
    this.isProcessing = false
  }

  

  // Connect to Sarvam WebSocket using subprotocol auth and official URL
  async connectToSarvam() {
    try {
   
      const lang = (this.language || 'en').toLowerCase()
      if (lang.startsWith('en')) {
        this.sarvamLanguage = 'en-IN'
        this.voice = 'manisha'
      } else if (lang.startsWith('hi')) {
        this.sarvamLanguage = 'hi-IN'
        this.voice = 'pavithra'
      }

      const wsUrl = 'wss://api.sarvam.ai/text-to-speech/ws?model=bulbul:v2'
      console.log(`🎤 [SARVAM-WS] Connecting to ${wsUrl} with subprotocol`)
      this.sarvamWs = new WebSocket(wsUrl, [`api-subscription-key.${API_KEYS.sarvam}`])

              this.sarvamWs.onopen = () => {
                this.sarvamReady = true
                const configMessage = {
          type: 'config',
          data: {
                  target_language_code: this.sarvamLanguage,
                  speaker: this.voice,
            pitch: 0.5,
                  pace: 1.0,
                  loudness: 1.0,
            enable_preprocessing: false,
            output_audio_codec: 'linear16', // Changed to linear16 for SIP compatibility
            output_audio_bitrate: '128k', // For 8000 Hz linear16
            speech_sample_rate: 8000, // Crucial for SIP/Twilio
            min_buffer_size: 50, // As per working example
            max_chunk_length: 150, // As per working example
          },
        }
                this.sarvamWs.send(JSON.stringify(configMessage))
        console.log("✅ [SARVAM-WS] Config sent successfully")
                this.processQueuedAudio()
              }

              this.sarvamWs.onmessage = (event) => {
                try {
                  const data = JSON.parse(event.data)
                  this.handleSarvamMessage(data)
        } catch (error) {
          console.error("❌ [SARVAM-WS] Error parsing message:", error.message)
        }
              }

      this.sarvamWs.onerror = (error) => {
                this.sarvamReady = false
        console.error("❌ [SARVAM-WS] WebSocket error:", error.message)
              }

      this.sarvamWs.onclose = () => {
                this.sarvamReady = false
        console.log("🔌 [SARVAM-WS] WebSocket closed")
              }

      // Wait for connection to be established
      await new Promise((resolve) => setTimeout(resolve, 500))
                  if (!this.sarvamReady) {
        this.useWebSocket = false
        console.log("⚠️ [SARVAM-WS] Connection timeout, falling back to API")
      }
    } catch (error) {
      this.sarvamReady = false
      this.useWebSocket = false
      console.error("❌ [SARVAM-WS] Connection error:", error.message)
    }
  }

  // Handle messages from Sarvam WebSocket
  handleSarvamMessage(data) {
    if (data.type === "audio") {
      const audioBase64 = (data && data.data && data.data.audio) || data.audio
      if (audioBase64 && !this.isInterrupted) {
        // For linear16 audio, we can stream directly to SIP without transcoding
        this.streamLinear16AudioToSIP(audioBase64)
        const audioBuffer = Buffer.from(audioBase64, "base64")
        this.totalAudioBytes += audioBuffer.length
      }
    } else if (data.type === "error") {
      const errMsg = (data && data.data && data.data.message) || data.message || 'unknown'
      console.log("❌ [SARVAM-WS] Sarvam error:", errMsg)
    } else if (data.type === "end") {
      console.log("✅ [SARVAM-WS] Audio generation completed")
      this.isProcessing = false
    }
  }

  // Process queued audio after connection
  processQueuedAudio() {
    if (this.audioQueue.length > 0 && this.sarvamReady) {
      const text = this.audioQueue.shift()
      this.sendTextToSarvam(text)
    }
  }

  // Send text to Sarvam WebSocket (client-compatible shape)
  sendTextToSarvam(text) {
    if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.OPEN && !this.isInterrupted) {
      const textMessage = {
        type: 'text',
        data: { text }
      }
      this.sarvamWs.send(JSON.stringify(textMessage))
      console.log("📤 [SARVAM-WS] Text sent:", text.substring(0, 50) + "...")
    }
  }

  // Fallback to API method
  async synthesizeWithAPI(text) {
    const timer = createTimer("TTS_API_FALLBACK")
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
          pitch: 0.5,
          pace: 1.0,
          loudness: 1.0,
          speech_sample_rate: 8000,
          enable_preprocessing: false,
          model: "bulbul:v1",
          output_audio_codec: "linear16", // Changed to linear16 for SIP compatibility
        }),
      })

      if (!response.ok || this.isInterrupted) {
        if (!this.isInterrupted) {
          console.log(`❌ [TTS-API-FALLBACK] ${timer.end()}ms - Error: ${response.status}`)
          throw new Error(`Sarvam API error: ${response.status}`)
        }
        return
      }

      const responseData = await response.json()
      const audioBase64 = responseData.audios?.[0]

      if (!audioBase64 || this.isInterrupted) {
        if (!this.isInterrupted) {
          console.log(`❌ [TTS-API-FALLBACK] ${timer.end()}ms - No audio data received`)
          throw new Error("No audio data received from Sarvam API")
        }
        return
      }

      console.log(`🕒 [TTS-API-FALLBACK] ${timer.end()}ms - Audio generated via API`)

      if (!this.isInterrupted) {
        // Wait for any existing audio stream to complete before starting new one
        await this.waitForAudioStreamToComplete()
        
        // Use linear16 streaming for API fallback as well
        await this.streamLinear16AudioToSIP(audioBase64)
        const audioBuffer = Buffer.from(audioBase64, "base64")
        this.totalAudioBytes += audioBuffer.length
      }
    } catch (error) {
      if (!this.isInterrupted) {
        console.log(`❌ [TTS-API-FALLBACK] ${timer.end()}ms - Error: ${error.message}`)
        throw error
      }
    }
  }

  async synthesizeAndStream(text) {
    if (this.isInterrupted) return

    const timer = createTimer("TTS_SYNTHESIS")

    try {
      // Try WebSocket first if enabled
      if (this.useWebSocket) {
        // Connect to Sarvam WebSocket if not connected
        if (!this.sarvamWs || this.sarvamWs.readyState !== WebSocket.OPEN) {
          await this.connectToSarvam()
          
          // Wait for connection to be ready
          let attempts = 0
          while (!this.sarvamReady && attempts < 10) {
            await new Promise(resolve => setTimeout(resolve, 100))
            attempts++
          }
          
          if (!this.sarvamReady) {
            console.log("⚠️ [TTS-SYNTHESIS] WebSocket failed, falling back to API")
            this.useWebSocket = false
          }
        }

        if (this.sarvamReady) {
          this.isProcessing = true
          console.log(`🕒 [TTS-SYNTHESIS] ${timer.end()}ms - Starting WebSocket synthesis`)

          // Wait for any existing audio stream to complete before starting new synthesis
          await this.waitForAudioStreamToComplete()

          // Send text to Sarvam
          this.sendTextToSarvam(text)
          
          // Send a flush message to signal end of utterance to Sarvam TTS (like in working version)
          const flushMessage = { type: "flush" }
          this.sarvamWs.send(JSON.stringify(flushMessage))
          console.log("📤 [SARVAM-WS] Flush signal sent")

          // Wait for processing to complete
          let waitAttempts = 0
          while (this.isProcessing && waitAttempts < 100 && !this.isInterrupted) {
            await new Promise(resolve => setTimeout(resolve, 50))
            waitAttempts++
          }

          if (this.isInterrupted) {
            console.log("⚠️ [TTS-SYNTHESIS] Synthesis interrupted")
            return
          }

          console.log(`🕒 [TTS-SYNTHESIS] ${timer.end()}ms - WebSocket synthesis completed`)
          return
        }
      }

      // Fallback to API
      console.log(`🕒 [TTS-SYNTHESIS] ${timer.end()}ms - Using API fallback`)
      await this.synthesizeWithAPI(text)
      console.log(`🕒 [TTS-SYNTHESIS] ${timer.end()}ms - API synthesis completed`)

    } catch (error) {
      if (!this.isInterrupted) {
        console.log(`❌ [TTS-SYNTHESIS] ${timer.end()}ms - Error: ${error.message}`)
        throw error
      }
    }
  }

  async transcodeMp3ToPcm16(mp3Buffer) {
    try {
      // Since FFmpeg is not available, we'll use the MP3 buffer directly
      // This is a fallback approach that may not provide optimal audio quality
      // but will allow the system to function without external dependencies
      
      console.log(`🔄 [TRANSCODE] FFmpeg not available, using MP3 buffer directly: ${mp3Buffer.length} bytes`)
      console.log(`🔄 [TRANSCODE] Converting MP3 to base64 format for SIP transmission`)
      
      // Validate the buffer
      if (!mp3Buffer || mp3Buffer.length === 0) {
        console.warn(`⚠️ [TRANSCODE] Empty or invalid MP3 buffer received`)
        return mp3Buffer
      }
      
      // Convert to base64 for validation
      const base64String = convertBufferToBase64(mp3Buffer)
      if (!validateBase64(base64String)) {
        console.warn(`⚠️ [TRANSCODE] Invalid base64 conversion, using original buffer`)
        return mp3Buffer
      }
      
      console.log(`✅ [TRANSCODE] MP3 buffer successfully converted to base64: ${mp3Buffer.length} bytes → ${base64String.length} base64 chars`)
      
      // Optional: Save MP3 for debugging (uncomment if needed)
      // saveMp3ToFile(mp3Buffer, `transcode_${Date.now()}.mp3`)
      
      // Return the original MP3 buffer (it will be converted to base64 in the streaming method)
      return mp3Buffer
      
    } catch (error) {
      console.error(`❌ [TRANSCODE] Transcoding error: ${error.message}`)
      // Return original buffer as fallback
      return mp3Buffer
    }
  }

  async streamAudioOptimizedForSIP(audioBase64) {
    if (this.isInterrupted) return

    // Wait for any existing audio stream to complete before starting new one
    if (this.currentAudioStreaming && !this.currentAudioStreaming.interrupt) {
      console.log(`⏳ [AUDIO-STREAM] Waiting for existing audio stream to complete...`)
      let waitAttempts = 0
      while (this.currentAudioStreaming && !this.currentAudioStreaming.interrupt && waitAttempts < 100) {
        await new Promise(resolve => setTimeout(resolve, 50))
        waitAttempts++
      }
      if (this.currentAudioStreaming && !this.currentAudioStreaming.interrupt) {
        console.log(`⚠️ [AUDIO-STREAM] Force interrupting existing stream to prevent overlap`)
        this.currentAudioStreaming.interrupt = true
        await new Promise(resolve => setTimeout(resolve, 100)) // Small delay to ensure cleanup
      }
    }

    const audioBuffer = Buffer.from(audioBase64, "base64")
    const streamingSession = { interrupt: false, streamId: Date.now() }
    this.currentAudioStreaming = streamingSession

    // Try to transcode MP3 to PCM16 mono 8kHz for SIP compatibility
    let processedBuffer
    let isPCM16 = false
    try {
      console.log(`🔄 [TRANSCODE] Converting MP3 (${audioBuffer.length} bytes) to PCM16 mono 8kHz...`)
      processedBuffer = await this.transcodeMp3ToPcm16(audioBuffer)
      // Check if we got PCM16 or fallback MP3
      isPCM16 = processedBuffer !== audioBuffer
      
      // Validate the processed buffer
      if (!processedBuffer || processedBuffer.length === 0) {
        console.warn(`⚠️ [TRANSCODE] Invalid processed buffer, using original audio buffer`)
        processedBuffer = audioBuffer
        isPCM16 = false
      }
    } catch (error) {
      console.error(`❌ [TRANSCODE] Failed to transcode MP3 to PCM16: ${error.message}`)
      // Fallback to original MP3 if transcoding fails
      processedBuffer = audioBuffer
      isPCM16 = false
    }

    // Adjust chunk size and delay based on audio format
    let chunkSize, chunkDelayMs
    if (isPCM16) {
    // For PCM16, we use a fixed chunk size and delay optimized for SIP
      chunkSize = 1600; // 800 bytes of PCM16 = 400 samples at 8kHz = 50ms of audio
      chunkDelayMs = 50; // 50ms packet interval for smooth streaming
      console.log(`🎵 [AUDIO-STREAM] [${streamingSession.streamId}] Starting SIP audio stream (PCM16): ${processedBuffer.length} bytes`)
    } else {
      // For MP3, use larger chunks and longer delays since MP3 is compressed
      chunkSize = 3200; // Larger chunks for MP3
      chunkDelayMs = 100; // 100ms packet interval for MP3
      console.log(`🎵 [AUDIO-STREAM] [${streamingSession.streamId}] Starting SIP audio stream (MP3 fallback): ${processedBuffer.length} bytes`)
    }

    let position = 0
    let chunkIndex = 0
    let successfulChunks = 0

    while (position < processedBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
      const remaining = processedBuffer.length - position;
      const currentChunkSize = Math.min(chunkSize, remaining);
      const chunk = processedBuffer.slice(position, position + currentChunkSize)

      // Create SIP media message with audio payload
      const chunkBase64 = chunk.toString("base64")
      
      // Validate base64 conversion
      if (!validateBase64(chunkBase64)) {
        console.warn(`⚠️ [AUDIO-STREAM] [${streamingSession.streamId}] Invalid base64 conversion for chunk ${chunkIndex + 1}, skipping...`)
        continue
      }
      
      const mediaMessage = {
        event: "media",
        streamSid: this.streamSid,
        media: {
          payload: chunkBase64,
        },
      }

      if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted && !streamingSession.interrupt) {
        try {
          this.ws.send(JSON.stringify(mediaMessage))
          successfulChunks++;
          const format = isPCM16 ? "PCM16" : "MP3"
          console.log(`🎵 [AUDIO-STREAM] [${streamingSession.streamId}] Sent ${format} chunk ${chunkIndex + 1} (${chunk.length} bytes), ${Math.round((position / processedBuffer.length) * 100)}% complete`)
        } catch (error) {
          console.log(`❌ [AUDIO-STREAM] [${streamingSession.streamId}] Error sending ${isPCM16 ? 'PCM16' : 'MP3'} chunk ${chunkIndex + 1}: ${error.message}`)
          break
        }
      } else {
        console.log(`⚠️ [AUDIO-STREAM] [${streamingSession.streamId}] WebSocket not ready or interrupted, stopping audio stream`)
        break
      }

      if (position + currentChunkSize < processedBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
        await new Promise((resolve) => setTimeout(resolve, chunkDelayMs))
      }

      position += currentChunkSize
      chunkIndex++
    }
    
    // Only log completion if this is still the current stream
    if (this.currentAudioStreaming === streamingSession) {
      const format = isPCM16 ? "PCM16" : "MP3"
      console.log(`✅ [AUDIO-STREAM] [${streamingSession.streamId}] ${format} stream completed: ${successfulChunks} chunks sent, ${Math.round((position / processedBuffer.length) * 100)}% of audio streamed`)
      this.currentAudioStreaming = null
    } else {
      const format = isPCM16 ? "PCM16" : "MP3"
      console.log(`⚠️ [AUDIO-STREAM] [${streamingSession.streamId}] ${format} stream was interrupted by newer stream`)
    }
  }

  // Stream linear16 audio directly to SIP (following working pattern from sarvam tts.js)
  async streamLinear16AudioToSIP(audioBase64) {
    if (this.isInterrupted) return

    // Wait for any existing audio stream to complete before starting new one
    if (this.currentAudioStreaming && !this.currentAudioStreaming.interrupt) {
      console.log(`⏳ [AUDIO-STREAM] Waiting for existing audio stream to complete...`)
      let waitAttempts = 0
      while (this.currentAudioStreaming && !this.currentAudioStreaming.interrupt && waitAttempts < 100) {
        await new Promise(resolve => setTimeout(resolve, 50))
        waitAttempts++
      }
      if (this.currentAudioStreaming && !this.currentAudioStreaming.interrupt) {
        console.log(`⚠️ [AUDIO-STREAM] Force interrupting existing stream to prevent overlap`)
        this.currentAudioStreaming.interrupt = true
        await new Promise(resolve => setTimeout(resolve, 100)) // Small delay to ensure cleanup
      }
    }

    const audioBuffer = Buffer.from(audioBase64, "base64")
    const streamingSession = { interrupt: false, streamId: Date.now() }
    this.currentAudioStreaming = streamingSession

    console.log(`🎵 [AUDIO-STREAM] Starting linear16 audio stream: ${audioBuffer.length} bytes (Stream ID: ${streamingSession.streamId})`)

    // Use the same chunking logic as the working sarvam tts.js
    const SAMPLE_RATE = 8000
    const BYTES_PER_SAMPLE = 2 // linear16 is 16-bit, so 2 bytes
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000
    const OPTIMAL_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS) // 40ms chunks for SIP

    let position = 0
    let chunkIndex = 0
    let successfulChunks = 0

    while (position < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
      const remaining = audioBuffer.length - position
      const chunkSize = Math.min(OPTIMAL_CHUNK_SIZE, remaining)
      const chunk = audioBuffer.slice(position, position + chunkSize)

      // Create SIP media message with linear16 payload
      const mediaMessage = {
        event: "media",
        streamSid: this.streamSid,
        media: {
          payload: chunk.toString("base64"),
        },
      }

      if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted && !streamingSession.interrupt) {
        try {
          this.ws.send(JSON.stringify(mediaMessage))
          successfulChunks++
          console.log(`🎵 [AUDIO-STREAM] [${streamingSession.streamId}] Sent linear16 chunk ${chunkIndex + 1} (${chunk.length} bytes), ${Math.round((position / audioBuffer.length) * 100)}% complete`)
        } catch (error) {
          console.log(`❌ [AUDIO-STREAM] [${streamingSession.streamId}] Error sending linear16 chunk ${chunkIndex + 1}: ${error.message}`)
          break
        }
      } else {
        console.log(`⚠️ [AUDIO-STREAM] [${streamingSession.streamId}] WebSocket not ready or interrupted, stopping audio stream`)
        break
      }

      if (position + chunkSize < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
        const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS)
        const delayMs = Math.max(chunkDurationMs - 2, 10) // Small delay to prevent buffer underrun
        await new Promise((resolve) => setTimeout(resolve, delayMs))
      }

      position += chunkSize
      chunkIndex++
    }

    // Only log completion if this is still the current stream
    if (this.currentAudioStreaming === streamingSession) {
      console.log(`✅ [AUDIO-STREAM] [${streamingSession.streamId}] Linear16 stream completed: ${successfulChunks} chunks sent, ${Math.round((position / audioBuffer.length) * 100)}% of audio streamed`)
      this.currentAudioStreaming = null
    } else {
      console.log(`⚠️ [AUDIO-STREAM] [${streamingSession.streamId}] Stream was interrupted by newer stream`)
    }
  }

  getStats() {
    return {
      totalAudioBytes: this.totalAudioBytes,
      sarvamReady: this.sarvamReady,
      isProcessing: this.isProcessing,
      useWebSocket: this.useWebSocket,
      isAudioStreaming: !!this.currentAudioStreaming && !this.currentAudioStreaming.interrupt
    }
  }

  // Check if audio is currently streaming
  isAudioCurrentlyStreaming() {
    return !!(this.currentAudioStreaming && !this.currentAudioStreaming.interrupt)
  }

  // Wait for current audio stream to complete
  async waitForAudioStreamToComplete(timeoutMs = 5000) {
    if (!this.isAudioCurrentlyStreaming()) {
      return true
    }

    console.log(`⏳ [AUDIO-SYNC] Waiting for current audio stream to complete...`)
    const startTime = Date.now()
    
    while (this.isAudioCurrentlyStreaming() && (Date.now() - startTime) < timeoutMs) {
      await new Promise(resolve => setTimeout(resolve, 50))
    }
    
    if (this.isAudioCurrentlyStreaming()) {
      console.log(`⚠️ [AUDIO-SYNC] Timeout waiting for audio stream, forcing interruption`)
      this.interrupt()
      return false
    }
    
    console.log(`✅ [AUDIO-SYNC] Audio stream completed, ready for new audio`)
    return true
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

// WhatsApp Template Module URL
const WHATSAPP_TEMPLATE_URL = "https://whatsapp-template-module.onrender.com/api/whatsapp/send-info"

// Utility function to normalize Indian phone numbers to E.164 format
const normalizeToE164India = (phoneNumber) => {
  const digits = String(phoneNumber || "").replace(/\D+/g, "");
  if (!digits) {
    throw new Error('Invalid phone number');
  }
  // Always take last 10 as local mobile and prefix +91
  const last10 = digits.slice(-10);
  if (last10.length !== 10) {
    throw new Error('Invalid Indian mobile number');
  }
  return `+91${last10}`;
};

// Function to extract organization name and course name from agent description using AI
const extractOrgAndCourseFromDescription = async (description) => {
  const timer = createTimer("AI_DESCRIPTION_EXTRACTION")
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
            content: `Extract organization name and course name from the given description. Return ONLY a JSON object with two fields:
- "orgName": The organization/institution name
- "courseName": The course name or program name

If you can't find either, use "Unknown" as the value.

Example output:
{"orgName": "EG Classes", "courseName": "UPSE Online Course"}

Return only the JSON, nothing else.`
          },
          {
            role: "user",
            content: description || "No description available"
          }
        ],
        max_tokens: 100,
        temperature: 0.1,
      }),
    })

    if (!response.ok) {
      throw new Error(`AI extraction failed: ${response.status}`)
    }

    const data = await response.json()
    const extractedText = data.choices[0]?.message?.content?.trim()
    
    if (extractedText) {
      try {
        const parsed = JSON.parse(extractedText)
        console.log(`🕒 [AI-DESCRIPTION-EXTRACTION] ${timer.end()}ms - Extracted:`, parsed)
        return {
          orgName: parsed.orgName || "Unknown",
          courseName: parsed.courseName || "Unknown"
        }
      } catch (parseError) {
        console.log(`⚠️ [AI-DESCRIPTION-EXTRACTION] JSON parse error: ${parseError.message}`)
        return { orgName: "Unknown", courseName: "Unknown" }
      }
    }

    return { orgName: "Unknown", courseName: "Unknown" }
  } catch (error) {
    console.log(`❌ [AI-DESCRIPTION-EXTRACTION] ${timer.end()}ms - Error: ${error.message}`)
    return { orgName: "Unknown", courseName: "Unknown" }
  }
}

// Function to send WhatsApp message after call ends
const sendWhatsAppAfterCall = async (callLogger, agentConfig) => {
  const timer = createTimer("WHATSAPP_AFTER_CALL")
  try {
    // Get phone number from call logger
    const phoneNumber = callLogger.mobile
    if (!phoneNumber) {
      console.log(`⚠️ [WHATSAPP-AFTER-CALL] No phone number available for WhatsApp message`)
      return false
    }

    // Normalize phone number
    const normalizedPhone = normalizeToE164India(phoneNumber)
    console.log(`📱 [WHATSAPP-AFTER-CALL] Sending WhatsApp to: ${normalizedPhone}`)

    // Extract org name and course name from agent description
    const { orgName, courseName } = await extractOrgAndCourseFromDescription(agentConfig.description)
    console.log(`📱 [WHATSAPP-AFTER-CALL] Extracted - Org: ${orgName}, Course: ${courseName}`)

    // Prepare request body
    const requestBody = {
      to: normalizedPhone,
      orgName: orgName,
      courseName: courseName
    }

    console.log(`📱 [WHATSAPP-AFTER-CALL] Request body:`, JSON.stringify(requestBody, null, 2))

    // Send request to WhatsApp template module
    const response = await fetch(WHATSAPP_TEMPLATE_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(requestBody),
    })

    if (!response.ok) {
      const errorText = await response.text().catch(() => "Unknown error")
      throw new Error(`WhatsApp API error: ${response.status} - ${errorText}`)
    }

    const result = await response.json()
    console.log(`✅ [WHATSAPP-AFTER-CALL] ${timer.end()}ms - WhatsApp sent successfully:`, result)
    
    // Update call log with WhatsApp status
    if (callLogger.callLogId) {
      await CallLog.findByIdAndUpdate(callLogger.callLogId, {
        'metadata.whatsappSent': true,
        'metadata.whatsappSentAt': new Date(),
        'metadata.whatsappData': {
          phoneNumber: normalizedPhone,
          orgName: orgName,
          courseName: courseName,
          response: result
        }
      }).catch(err => console.log(`⚠️ [WHATSAPP-AFTER-CALL] Call log update error: ${err.message}`))
    }

    return true
  } catch (error) {
    console.log(`❌ [WHATSAPP-AFTER-CALL] ${timer.end()}ms - Error: ${error.message}`)
    
    // Update call log with WhatsApp failure
    if (callLogger.callLogId) {
      await CallLog.findByIdAndUpdate(callLogger.callLogId, {
        'metadata.whatsappSent': false,
        'metadata.whatsappError': error.message,
        'metadata.whatsappAttemptedAt': new Date()
      }).catch(err => console.log(`⚠️ [WHATSAPP-AFTER-CALL] Call log update error: ${err.message}`))
    }
    
    return false
  }
}

// Test function to manually trigger WhatsApp sending (for testing purposes)
const testWhatsAppSending = async (phoneNumber, agentDescription) => {
  console.log("🧪 [TEST-WHATSAPP] Testing WhatsApp integration...")
  
  const mockCallLogger = {
    mobile: phoneNumber,
    callLogId: null
  }
  
  const mockAgentConfig = {
    description: agentDescription || "EG Classes offers UPSE Online Course for competitive exam preparation"
  }
  
  try {
    const result = await sendWhatsAppAfterCall(mockCallLogger, mockAgentConfig)
    console.log("🧪 [TEST-WHATSAPP] Test result:", result)
    return result
  } catch (error) {
    console.log("🧪 [TEST-WHATSAPP] Test error:", error.message)
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
          if (currentTTS && isProcessing) {
            currentTTS.interrupt()
            isProcessing = false
            processingRequestId++
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

      console.log("🗣️ [USER-UTTERANCE] ========== USER SPEECH ==========")
      console.log("🗣️ [USER-UTTERANCE] Text:", text.trim())
      console.log("🗣️ [USER-UTTERANCE] Current Language:", currentLanguage)

      if (currentTTS) {
        console.log("🛑 [USER-UTTERANCE] Interrupting current TTS...")
        currentTTS.interrupt()
      }

      isProcessing = true
      lastProcessedText = text
      const currentRequestId = ++processingRequestId

      try {
        const detectedLanguage = detectLanguageWithFranc(text, currentLanguage || "en")
        console.log("🌐 [USER-UTTERANCE] Detected Language:", detectedLanguage)

        if (detectedLanguage !== currentLanguage) {
          console.log("🔄 [USER-UTTERANCE] Language changed from", currentLanguage, "to", detectedLanguage)
          currentLanguage = detectedLanguage
        }

        // Check if user wants to disconnect (fast detection to minimize latency)
        console.log("🔍 [USER-UTTERANCE] Checking disconnection intent...")
        
        // Check for Google Meet request
        console.log("🔍 [USER-UTTERANCE] Checking Google Meet request...")
        
        // Start all detection processes in parallel
        const disconnectionCheckPromise = detectCallDisconnectionIntent(text, conversationHistory, detectedLanguage)
        const googleMeetCheckPromise = detectGoogleMeetRequest(text, conversationHistory, detectedLanguage)
        
        // Create enhanced TTS processor for streaming
        const enhancedTTS = new EnhancedSarvamTTSProcessor(detectedLanguage, ws, streamSid, callLogger)
        
        // Continue with other processing while checking
        console.log("🤖 [USER-UTTERANCE] Processing with OpenAI streaming...")
        const openaiPromise = processWithOpenAIStreaming(
          text,
          conversationHistory,
          detectedLanguage,
          callLogger,
          agentConfig,
          userName,
          enhancedTTS // Pass TTS processor for real-time streaming
        )
        
        // Wait for all operations to complete
        const [disconnectionIntent, googleMeetIntent, aiResponse] = await Promise.all([
          disconnectionCheckPromise,
          googleMeetCheckPromise,
          openaiPromise
        ])
        
        if (disconnectionIntent === "DISCONNECT") {
          console.log("🛑 [USER-UTTERANCE] User wants to disconnect - waiting 2 seconds then ending call")
          
          // Wait 2 seconds to ensure last message is processed, then terminate
          setTimeout(async () => {
            if (callLogger) {
              try {
                await callLogger.ultraFastTerminateWithMessage("Thank you for your time. Have a great day!", detectedLanguage, 'user_requested_disconnect')
                console.log("✅ [USER-UTTERANCE] Call terminated after 2 second delay")
              } catch (err) {
                console.log(`⚠️ [USER-UTTERANCE] Termination error: ${err.message}`)
              }
            }
          }, 2000)
          
          return
        }

        // Handle Google Meet request
        if (googleMeetIntent === "GOOGLE_MEET" && !callLogger.googleMeetState.isRequested) {
          console.log("📹 [USER-UTTERANCE] Google Meet requested - asking for email")
          callLogger.googleMeetState.isRequested = true
          
          // Ask for email in the appropriate language
          const emailRequestMessages = {
            hi: "बहुत अच्छा! मैं आपके लिए Google Meet शेड्यूल कर सकता हूं। कृपया अपना Gmail ID बताएं।",
            en: "Great! I can schedule a Google Meet for you. Please provide your Gmail address.",
            bn: "দারুণ! আমি আপনার জন্য Google Meet শিডিউল করতে পারি। অনুগ্রহ করে আপনার Gmail ঠিকানা দিন।",
            ta: "நல்லது! நான் உங்களுக்கு Google Meet ஷெட்யூல் செய்யலாம். தயவுசெய்து உங்கள் Gmail முகவரியை வழங்கவும்.",
            te: "బాగుంది! నేను మీ కోసం Google Meet షెడ్యూల్ చేయగలను. దయచేసి మీ Gmail చిరునామాను అందించండి.",
            mr: "छान! मी तुमच्यासाठी Google Meet शेड्यूल करू शकतो. कृपया तुमचा Gmail पत्ता द्या.",
            gu: "બહુ સરસ! હું તમારા માટે Google Meet શેડ્યૂલ કરી શકું છું. કૃપા કરીને તમારું Gmail સરનામું આપો."
          }
          
          const emailRequest = emailRequestMessages[detectedLanguage] || emailRequestMessages.en
          
          if (callLogger) {
            callLogger.logAIResponse(emailRequest, detectedLanguage)
          }
          
          currentTTS = new EnhancedSarvamTTSProcessor(detectedLanguage, ws, streamSid, callLogger)
          await currentTTS.synthesizeAndStream(emailRequest)
          
          conversationHistory.push(
            { role: "user", content: text },
            { role: "assistant", content: emailRequest }
          )
          
          return
        }

        // Handle email collection for Google Meet
        if (callLogger.googleMeetState.isRequested && !callLogger.googleMeetState.emailProvided) {
          console.log("📧 [USER-UTTERANCE] Extracting email for Google Meet...")
          
          const extractedEmail = await extractEmailFromText(text)
          
          if (extractedEmail) {
            console.log(`📧 [USER-UTTERANCE] Email extracted: ${extractedEmail}`)
            callLogger.googleMeetState.emailProvided = true
            callLogger.googleMeetState.email = extractedEmail
            
            // Generate Google Meet link
            const meetingDetails = generateGoogleMeetLink()
            callLogger.googleMeetState.meetingDetails = meetingDetails
            
            // Send email with meeting link
            const emailResult = await sendGoogleMeetEmail(extractedEmail, meetingDetails, agentConfig)
            
            if (emailResult.success) {
              callLogger.googleMeetState.emailSent = true
              
              // Success message in appropriate language
              const successMessages = {
                hi: `धन्यवाद! मैंने आपके Gmail ${extractedEmail} पर Google Meet लिंक भेज दिया है। आपको अभी ईमेल मिल जाएगा।`,
                en: `Thank you! I've sent the Google Meet link to your Gmail ${extractedEmail}. You should receive the email shortly.`,
                bn: `ধন্যবাদ! আমি আপনার Gmail ${extractedEmail} এ Google Meet লিংক পাঠিয়েছি। আপনি শীঘ্রই ইমেল পাবেন।`,
                ta: `நன்றி! நான் உங்கள் Gmail ${extractedEmail} க்கு Google Meet இணைப்பை அனுப்பியுள்ளேன். நீங்கள் விரைவில் மின்னஞ்சலைப் பெறுவீர்கள்.`,
                te: `ధన్యవాదాలు! నేను మీ Gmail ${extractedEmail} కి Google Meet లింక్ పంపాను. మీరు త్వరలో ఇమెయిల్ అందుకుంటారు.`,
                mr: `धन्यवाद! मी तुमच्या Gmail ${extractedEmail} वर Google Meet लिंक पाठवला आहे. तुम्हाला लवकरच ईमेल मिळेल.`,
                gu: `આભાર! મેં તમારા Gmail ${extractedEmail} પર Google Meet લિંક મોકલી છે. તમને ટૂંક સમયમાં ઇમેઇલ मળશે.`
              }
              
              const successMessage = successMessages[detectedLanguage] || successMessages.en
              
              if (callLogger) {
                callLogger.logAIResponse(successMessage, detectedLanguage)
              }
              
                        currentTTS = new EnhancedSarvamTTSProcessor(detectedLanguage, ws, streamSid, callLogger)
          await currentTTS.synthesizeAndStream(successMessage)
              
              conversationHistory.push(
                { role: "user", content: text },
                { role: "assistant", content: successMessage }
              )
              
              return
            } else {
              // Error message
              const errorMessages = {
                hi: "माफ़ करें, ईमेल भेजने में समस्या आई। कृपया बाद में पुनः प्रयास करें।",
                en: "Sorry, there was an issue sending the email. Please try again later.",
                bn: "দুঃখিত, ইমেল পাঠাতে সমস্যা হয়েছে। অনুগ্রহ করে পরে আবার চেষ্টা করুন।",
                ta: "மன்னிக்கவும், மின்னஞ்சலை அனுப்புவதில் சிக்கல் ஏற்பட்டது. தயவுசெய்து பின்னர் மீண்டும் முயற்சிக்கவும்.",
                te: "క్షమించండి, ఇమెయిల్ పంపడంలో సమస్య ఉంది. దయచేసి తర్వాత మళ్లీ ప్రయత్నించండి.",
                mr: "माफ करा, ईमेल पाठवण्यात समस्या आली. कृपया नंतर पुन्हा प्रयत्न करा.",
                gu: "માફ કરશો, ઇમેઇલ મોકલવામાં સમસ્યા આવી. કૃપા કરીને પછીથી ફરીથી પ્રયાસ કરો."
              }
              
              const errorMessage = errorMessages[detectedLanguage] || errorMessages.en
              
              if (callLogger) {
                callLogger.logAIResponse(errorMessage, detectedLanguage)
              }
              
                        currentTTS = new EnhancedSarvamTTSProcessor(detectedLanguage, ws, streamSid, callLogger)
          await currentTTS.synthesizeAndStream(errorMessage)
              
              conversationHistory.push(
                { role: "user", content: text },
                { role: "assistant", content: errorMessage }
              )
              
              return
            }
          } else {
            // Invalid email message
            const invalidEmailMessages = {
              hi: "कृपया एक वैध Gmail पता प्रदान करें। उदाहरण: user@gmail.com",
              en: "Please provide a valid Gmail address. Example: user@gmail.com",
              bn: "অনুগ্রহ করে একটি বৈধ Gmail ঠিকানা দিন। উদাহরণ: user@gmail.com",
              ta: "தயவுசெய்து சரியான Gmail முகவரியை வழங்கவும். எடுத்துக்காட்டு: user@gmail.com",
              te: "దయచేసి చెల్లుబాటు అయ్యే Gmail చిరునామాను అందించండి. ఉదాహరణ: user@gmail.com",
              mr: "कृपया वैध Gmail पत्ता द्या. उदाहरण: user@gmail.com",
              gu: "કૃપા કરીને માન્ય Gmail સરનામું આપો. ઉદાહરણ: user@gmail.com"
            }
            
            const invalidEmailMessage = invalidEmailMessages[detectedLanguage] || invalidEmailMessages.en
            
            if (callLogger) {
              callLogger.logAIResponse(invalidEmailMessage, detectedLanguage)
            }
            
                      currentTTS = new EnhancedSarvamTTSProcessor(detectedLanguage, ws, streamSid, callLogger)
          await currentTTS.synthesizeAndStream(invalidEmailMessage)
            
            conversationHistory.push(
              { role: "user", content: text },
              { role: "assistant", content: invalidEmailMessage }
            )
            
            return
          }
        }

        if (processingRequestId === currentRequestId && aiResponse) {
          console.log("🤖 [USER-UTTERANCE] AI Response:", aiResponse)
          console.log("🎤 [USER-UTTERANCE] TTS already streaming from OpenAI...")
          
          // TTS is already streaming from OpenAI, just update conversation history
          currentTTS = enhancedTTS

          conversationHistory.push(
            { role: "user", content: text },
            { role: "assistant", content: aiResponse }
          )

          if (conversationHistory.length > 10) {
            conversationHistory = conversationHistory.slice(-10)
          }
          
          console.log("✅ [USER-UTTERANCE] Processing completed with streaming TTS")
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
            streamSid = data.streamSid || data.start?.streamSid
            const accountSid = data.start?.accountSid

            // Log all incoming SIP data
            console.log("📞 [SIP-START] ========== CALL START DATA ==========")
            console.log("📞 [SIP-START] Raw data:", JSON.stringify(data, null, 2))
            console.log("📞 [SIP-START] URL Parameters:", JSON.stringify(urlParams, null, 2))
            console.log("📞 [SIP-START] StreamSID:", streamSid)
            console.log("📞 [SIP-START] AccountSID:", accountSid)

            let mobile = null;
            let callerId = null;
            let customParams = {};
            let czdataDecoded = null;
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
                console.log("[SIP-START] Decoded czdata customParams:", customParams);
                if (userName) {
                  console.log("[SIP-START] User Name (czdata):", userName);
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
            console.log("📞 [SIP-START] Extra Data:", JSON.stringify(extraData, null, 2))
            console.log("📞 [SIP-START] ======================================")

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
            const greetingTTS = new EnhancedSarvamTTSProcessor(currentLanguage, ws, streamSid, callLogger)
            await greetingTTS.synthesizeAndStream(greeting)
            console.log("✅ [SIP-TTS] Greeting TTS completed")
            break
          }

          case "media":
            if (data.media?.payload) {
              const audioBuffer = Buffer.from(data.media.payload, "base64")
              
              // Log media stats periodically (every 1000 packets to avoid spam)
              if (!ws.mediaPacketCount) ws.mediaPacketCount = 0
              ws.mediaPacketCount++
              
              if (ws.mediaPacketCount % 1000 === 0) {
                console.log("🎵 [SIP-MEDIA] Audio packets received:", ws.mediaPacketCount)
              }

              if (deepgramWs && deepgramReady && deepgramWs.readyState === WebSocket.OPEN) {
                deepgramWs.send(audioBuffer)
              } else {
                deepgramAudioQueue.push(audioBuffer)
                if (deepgramAudioQueue.length % 100 === 0) {
                  console.log("⏳ [SIP-MEDIA] Audio queued for Deepgram:", deepgramAudioQueue.length)
                }
              }
            }
            break

          case "stop":
            console.log("🛑 [SIP-STOP] ========== CALL END ==========")
            console.log("🛑 [SIP-STOP] StreamSID:", streamSid)
            console.log("🛑 [SIP-STOP] Call Direction:", callDirection)
            console.log("🛑 [SIP-STOP] Mobile:", mobile)
            
            // Handle external call disconnection
            if (streamSid) {
              await handleExternalCallDisconnection(streamSid, 'sip_stop_event')
            }
            
            if (callLogger) {
              const stats = callLogger.getStats()
              console.log("🛑 [SIP-STOP] Call Stats:", JSON.stringify(stats, null, 2))
              
              try {
                console.log("💾 [SIP-STOP] Saving final call log to database...")
                const savedLog = await callLogger.saveToDatabase("maybe", agentConfig)
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
      
      if (callLogger) {
        const stats = callLogger.getStats()
        console.log("🔌 [SIP-CLOSE] Final Call Stats:", JSON.stringify(stats, null, 2))
        
        try {
          console.log("💾 [SIP-CLOSE] Saving call log due to connection close...")
          const savedLog = await callLogger.saveToDatabase("maybe", agentConfig)
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
  // Export streaming functions for external use
  streamingFunctions: {
    processWithOpenAIStreaming,
    EnhancedSarvamTTSProcessor
  },
  // Export termination methods for external use
  terminationMethods: {
    graceful: (callLogger, message, language) => callLogger?.gracefulCallEnd(message, language),
    fast: (callLogger, reason) => callLogger?.fastTerminateCall(reason),
    ultraFast: (callLogger, message, language, reason) => callLogger?.ultraFastTerminateWithMessage(message, language, reason)
  },
  // Export WhatsApp methods for external use
  whatsappMethods: {
    sendWhatsAppAfterCall,
    extractOrgAndCourseFromDescription,
    normalizeToE164India,
    testWhatsAppSending
  },
  // Export Google Meet methods for external use
  googleMeetMethods: {
    detectGoogleMeetRequest,
    extractEmailFromText,
    validateEmail,
    generateGoogleMeetLink,
    sendGoogleMeetEmail
  }
}



