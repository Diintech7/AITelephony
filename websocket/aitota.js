const WebSocket = require("ws")
require("dotenv").config()
const mongoose = require("mongoose")
const Agent = require("../models/Agent")
const CallLog = require("../models/CallLog")
const Credit = require("../models/Credit")

// ElevenLabs TTS Integration - Optimized for SIP Telephony
// This file uses ElevenLabs API instead of Sarvam for text-to-speech
// Default voice: Rachel (JBFqnCBsd6RMkjVDRZzb) - Professional female voice
// SIP Audio Format: 8kHz sample rate, Mono, PCM s16le (base64) streamed in 40ms chunks (640 bytes)
// Language detection removed - using default language from agent config

// Load API keys from environment variables
const API_KEYS = {
  deepgram: process.env.DEEPGRAM_API_KEY,
  elevenlabs: process.env.ELEVENLABS_API_KEY,
  openai: process.env.OPENAI_API_KEY,
  whatsapp: process.env.WHATSAPP_TOKEN,
}

// Validate API keys
if (!API_KEYS.deepgram || !API_KEYS.elevenlabs || !API_KEYS.openai) {
  console.error("❌ Missing required API keys in environment variables")
  process.exit(1)
}

const fetch = globalThis.fetch || require("node-fetch")

// SIP Audio Configuration - Optimized for telephony with minimal latency
const SIP_AUDIO_CONFIG = {
  SAMPLE_RATE: 8000,        // 8kHz sample rate required by SIP
  CHANNELS: 1,              // Mono audio
  BITS_PER_SAMPLE: 16,      // 16-bit audio for PCM
  BYTES_PER_SAMPLE: 2,      // 2 bytes per sample for PCM
  BYTES_PER_MS: 16,         // (8000 * 2) / 1000 = 16 bytes per millisecond
  OPTIMAL_CHUNK_SIZE: 640,  // 40ms chunks: 40 * 16 = 640 bytes
  AUDIO_FORMAT: 'pcm_s16le',// PCM 16-bit little-endian
  BITRATE: 128000           // 8kHz * 16 bits
}

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

// Language mapping for TTS and STT services
const LANGUAGE_MAPPING = {
  hi: "hi",
  en: "en",
  bn: "bn",
  te: "te",
  ta: "ta",
  mr: "mr",
  gu: "gu",
  kn: "kn",
  ml: "ml",
  pa: "pa",
  or: "or",
  as: "as",
  ur: "ur",
}

const getElevenLabsLanguage = (language = "en") => {
  const lang = language?.toLowerCase() || "en"
  return LANGUAGE_MAPPING[lang] || "en"
}

const getDeepgramLanguage = (language = "hi") => {
  const lang = language?.toLowerCase() || "hi"
  if (lang === "hi") return "hi"
  if (lang === "en") return "en-IN"
  if (lang === "mr") return "mr"
  return lang
}

// Valid ElevenLabs voice options (voice IDs)
const VALID_ELEVENLABS_VOICES = new Set([
  "JBFqnCBsd6RMkjVDRZzb", // Default voice (Rachel) - Professional female
  "EXAVITQu4vr4xnSDxMaL", // Bella - Professional female
  "MF3mGyEYCl7XYWbV9V6O", // Elli - Friendly female
  "TxGEqnHWrfWFTfGW9XjX", // Josh - Professional male
  "VR6AewLTigWG4xSOukaG", // Arnold - Deep male
  "pNInz6obpgDQGcFmaJgB", // Adam - Friendly male
  "yoZ06aMxZJJ28mfd3POQ", // Sam - Professional male
  "AZnzlk1XvdvUeBnXmlld", // Domi - Energetic female
  "ErXwobaYiN019PkySvjV", // Antoni - Professional male
  "LcfcDJNUP1GQjkzn1xUU", // Bill - Professional male
  "pqHfZKP75CvOlQylNhV4", // Boris - Deep male
  "XB0fqtBnxyJaPExlL7V9", // Charlotte - Professional female
  "2EiwWnXFnvU5JabPnv8n", // Clyde - Professional male
  "9BWtwz2T6Zebi4Lp6iXH", // Dave - Friendly male
  "CYw3kZ02Hs0563khs1Fj", // Fin - Professional male
  "N2lVS1w4EtoT3dr4eOWO", // Gigi - Professional female
  "oWAxZDx7w5VEj9dCyTzz", // Grace - Professional female
  "pqHfZKP75CvOlQylNhV4", // James - Professional male
  "XB0fqtBnxyJaPExlL7V9", // Jeremy - Professional male
  "2EiwWnXFnvU5JabPnv8n", // Joseph - Professional male
  "9BWtwz2T6Zebi4Lp6iXH", // Lili - Professional female
  "CYw3kZ02Hs0563khs1Fj", // Matilda - Professional female
  "N2lVS1w4EtoT3dr4eOWO", // Michael - Professional male
  "oWAxZDx7w5VEj9dCyTzz", // Nicole - Professional female
])

const getValidElevenLabsVoice = (voiceSelection = "JBFqnCBsd6RMkjVDRZzb") => {
  const normalized = (voiceSelection || "").toString().trim()
  
  // If it's already a valid voice ID, return it
  if (VALID_ELEVENLABS_VOICES.has(normalized)) {
    return normalized
  }

  // Map common voice names to ElevenLabs voice IDs
  const voiceMapping = {
    "male-professional": "TxGEqnHWrfWFTfGW9XjX", // Josh
    "female-professional": "EXAVITQu4vr4xnSDxMaL", // Bella
    "male-friendly": "pNInz6obpgDQGcFmaJgB", // Adam
    "female-friendly": "MF3mGyEYCl7XYWbV9V6O", // Elli
    "neutral": "JBFqnCBsd6RMkjVDRZzb", // Rachel (default)
    "default": "JBFqnCBsd6RMkjVDRZzb", // Rachel (default)
    "male": "TxGEqnHWrfWFTfGW9XjX", // Josh
    "female": "EXAVITQu4vr4xnSDxMaL", // Bella
    "rachel": "JBFqnCBsd6RMkjVDRZzb", // Rachel
    "bella": "EXAVITQu4vr4xnSDxMaL", // Bella
    "josh": "TxGEqnHWrfWFTfGW9XjX", // Josh
    "adam": "pNInz6obpgDQGcFmaJgB", // Adam
    "elli": "MF3mGyEYCl7XYWbV9V6O", // Elli
    "arnold": "VR6AewLTigWG4xSOukaG", // Arnold
    "sam": "yoZ06aMxZJJ28mfd3POQ", // Sam
    "domi": "AZnzlk1XvdvUeBnXmlld", // Domi
    "antoni": "ErXwobaYiN019PkySvjV", // Antoni
    "bill": "LcfcDJNUP1GQjkzn1xUU", // Bill
    "boris": "pqHfZKP75CvOlQylNhV4", // Boris
    "charlotte": "XB0fqtBnxyJaPExlL7V9", // Charlotte
    "clyde": "2EiwWnXFnvU5JabPnv8n", // Clyde
    "dave": "9BWtwz2T6Zebi4Lp6iXH", // Dave
    "fin": "CYw3kZ02Hs0563khs1Fj", // Fin
    "gigi": "N2lVS1w4EtoT3dr4eOWO", // Gigi
    "grace": "oWAxZDx7w5VEj9dCyTzz", // Grace
    "james": "pqHfZKP75CvOlQylNhV4", // James
    "jeremy": "XB0fqtBnxyJaPExlL7V9", // Jeremy
    "joseph": "2EiwWnXFnvU5JabPnv8n", // Joseph
    "lili": "9BWtwz2T6Zebi4Lp6iXH", // Lili
    "matilda": "CYw3kZ02Hs0563khs1Fj", // Matilda
    "michael": "N2lVS1w4EtoT3dr4eOWO", // Michael
    "nicole": "oWAxZDx7w5VEj9dCyTzz", // Nicole
  }

  return voiceMapping[normalized.toLowerCase()] || "JBFqnCBsd6RMkjVDRZzb" // Default to Rachel
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

// Language detection removed - using agent's configured language

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
    this.currentLanguage = 'en' // Track current language from agent config
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
          ttsProvider: 'elevenlabs',
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
  async gracefulCallEnd(goodbyeMessage = "Thank you for your time. Have a great day!") {
    try {
      console.log("👋 [GRACEFUL-END] Ending call gracefully with goodbye message")
      
      // Log the goodbye message
      this.logAIResponse(goodbyeMessage)
      
      // Update call log immediately (non-blocking)
      const callLogUpdate = CallLog.findByIdAndUpdate(this.callLogId, {
        'metadata.lastUpdated': new Date()
      }).catch(err => console.log(`⚠️ [GRACEFUL-END] Call log update error: ${err.message}`))
      
      // Start TTS synthesis for goodbye message (non-blocking)
      const ttsPromise = this.synthesizeGoodbyeMessage(goodbyeMessage)
      
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
  async synthesizeGoodbyeMessage(message) {
    try {
      console.log("🎤 [GRACEFUL-END] Starting goodbye message TTS...")
      
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const tts = new SimplifiedElevenLabsTTSProcessor(this.currentLanguage, this.ws, this.streamSid, this.callLogger)
        
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
      this.logAIResponse(goodbyeMessage)
      
      // 2. Start TTS synthesis first to ensure message is sent (non-blocking, but wait for start)
      let ttsStarted = false
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const tts = new SimplifiedElevenLabsTTSProcessor(language, this.ws, this.streamSid, this.callLogger)
        
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
      this.logAIResponse(goodbyeMessage)
      
      // 2. Start TTS synthesis and wait for completion
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const tts = new SimplifiedElevenLabsTTSProcessor(language, this.ws, this.streamSid, this.callLogger)
        
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
  logUserTranscript(transcript, timestamp = new Date()) {
    const entry = {
      type: "user",
      text: transcript,
      language: this.currentLanguage,
      timestamp: timestamp,
      source: "deepgram",
    }

    this.transcripts.push(entry)
    this.pendingTranscripts.push(entry)
    
    // Trigger batch save
    this.scheduleBatchSave()
  }

  // Add AI response with batched live saving
  logAIResponse(response, timestamp = new Date()) {
    const entry = {
      type: "ai",
      text: response,
      language: this.currentLanguage,
      timestamp: timestamp,
      source: "elevenlabs",
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

      // Detect disposition based on conversation history and agent's depositions
      let disposition = null
      let subDisposition = null
      let dispositionId = null
      let subDispositionId = null
      
      if (agentConfig && agentConfig.depositions && Array.isArray(agentConfig.depositions) && agentConfig.depositions.length > 0) {
        try {
          console.log("🔍 [DISPOSITION-DETECTION] Analyzing conversation for disposition...")
          const conversationHistory = this.generateConversationHistory()
          const dispositionResult = await detectDispositionWithOpenAI(conversationHistory, agentConfig.depositions)
          disposition = dispositionResult.disposition
          subDisposition = dispositionResult.subDisposition
          dispositionId = dispositionResult.dispositionId
          subDispositionId = dispositionResult.subDispositionId
          
          if (disposition) {
            console.log(`📊 [DISPOSITION-DETECTION] Detected disposition: ${disposition} (ID: ${dispositionId}) | ${subDisposition || 'N/A'} (ID: ${subDispositionId || 'N/A'})`)
          } else {
            console.log(`⚠️ [DISPOSITION-DETECTION] No disposition detected`)
          }
        } catch (dispositionError) {
          console.log(`❌ [DISPOSITION-DETECTION] Error detecting disposition: ${dispositionError.message}`)
        }
      } else {
        console.log(`⚠️ [DISPOSITION-DETECTION] No depositions configured for agent`)
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

  // Generate conversation history for disposition analysis
  generateConversationHistory() {
    const allEntries = [...this.transcripts, ...this.responses].sort(
      (a, b) => new Date(a.timestamp) - new Date(b.timestamp)
    )

    return allEntries.map((entry) => ({
      role: entry.type === "user" ? "user" : "assistant",
      content: entry.text
    }))
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

// Simplified OpenAI processing
const processWithOpenAI = async (
  userMessage,
  conversationHistory,
  language,
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
      "Use the language of the user's message in the same language give the response.",
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
        const fu = followUps[language] || followUps.en
        fullResponse = `${fullResponse} ${fu}`.trim()
      }
    }

    if (callLogger && fullResponse) {
      callLogger.logAIResponse(fullResponse)
    }

    return fullResponse
  } catch (error) {
    console.log(`❌ [LLM-PROCESSING] ${timer.end()}ms - Error: ${error.message}`)
    return null
  }
}

// Intelligent lead status detection using OpenAI
const detectLeadStatusWithOpenAI = async (userMessage, conversationHistory, language) => {
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
const detectCallDisconnectionIntent = async (userMessage, conversationHistory, language) => {
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
const detectWhatsAppRequest = async (userMessage, conversationHistory, language) => {
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

/**
 * Intelligent disposition detection using OpenAI based on agent's depositions
 * 
 * This function analyzes the conversation history and automatically selects the most
 * appropriate disposition and sub-disposition from the agent's configured depositions.
 * 
 * @param {Array} conversationHistory - Array of conversation messages with role and content
 * @param {Array} agentDepositions - Array of disposition objects from agent config
 * @param {String} detectedLanguage - Current language for context
 * @returns {Object} - { disposition: string, subDisposition: string, dispositionId: string, subDispositionId: string }
 * 
 * Example agentDepositions:
 * [
 *   {
 *     _id: "68c3e47072168419ceb3631e",
 *     title: "Interested",
 *     sub: ["For All Services", "For PPC and Lead Generation"]
 *   },
 *   {
 *     _id: "68c3e47072168419ceb3631f",
 *     title: "Not Interested", 
 *     sub: []
 *   }
 * ]
 */
const detectDispositionWithOpenAI = async (conversationHistory, agentDepositions) => {
  const timer = createTimer("DISPOSITION_DETECTION")
  try {
    if (!agentDepositions || !Array.isArray(agentDepositions) || agentDepositions.length === 0) {
      console.log(`⚠️ [DISPOSITION-DETECTION] ${timer.end()}ms - No depositions configured for agent`)
      return { disposition: null, subDisposition: null, dispositionId: null, subDispositionId: null }
    }

    // Build depositions list for the prompt with proper structure
    const depositionsList = agentDepositions.map((dep, index) => {
      const subDeps = dep.sub && Array.isArray(dep.sub) && dep.sub.length > 0 
        ? dep.sub.map((sub, subIndex) => `${subIndex + 1}. ${sub}`).join('\n        ')
        : 'No sub-dispositions'
      return `${index + 1}. ${dep.title}:
        Sub-dispositions:
        ${subDeps}`
    }).join('\n\n')

    const conversationText = conversationHistory
      .slice(-10) // Last 10 messages for context
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
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEYS.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: dispositionPrompt },
        ],
        max_tokens: 100,
        temperature: 0.1,
      }),
    })

    if (!response.ok) {
      console.log(`❌ [DISPOSITION-DETECTION] ${timer.end()}ms - Error: ${response.status}`)
      return { disposition: null, subDisposition: null, dispositionId: null, subDispositionId: null }
    }

    const data = await response.json()
    const result = data.choices[0]?.message?.content?.trim()

    // Parse the response
    const dispositionMatch = result.match(/DISPOSITION:\s*(.+)/i)
    const subDispositionMatch = result.match(/SUB_DISPOSITION:\s*(.+)/i)

    const dispositionTitle = dispositionMatch ? dispositionMatch[1].trim() : null
    const subDispositionTitle = subDispositionMatch ? subDispositionMatch[1].trim() : null

    // Find the disposition object and get its _id
    const validDisposition = agentDepositions.find(dep => dep.title === dispositionTitle)
    if (!validDisposition) {
      console.log(`⚠️ [DISPOSITION-DETECTION] ${timer.end()}ms - Invalid disposition detected: ${dispositionTitle}`)
      return { disposition: null, subDisposition: null, dispositionId: null, subDispositionId: null }
    }

    // Find the sub-disposition and get its _id if it exists
    let validSubDisposition = null
    let subDispositionId = null
    
    if (subDispositionTitle && subDispositionTitle !== "N/A" && validDisposition.sub && Array.isArray(validDisposition.sub)) {
      // Try exact match first
      validSubDisposition = validDisposition.sub.find(sub => sub === subDispositionTitle)
      
      // If no exact match, try case-insensitive match
      if (!validSubDisposition) {
        validSubDisposition = validDisposition.sub.find(sub => 
          sub.toLowerCase() === subDispositionTitle.toLowerCase()
        )
      }
      
      // If still no match, try partial match
      if (!validSubDisposition) {
        validSubDisposition = validDisposition.sub.find(sub => 
          sub.toLowerCase().includes(subDispositionTitle.toLowerCase()) ||
          subDispositionTitle.toLowerCase().includes(sub.toLowerCase())
        )
      }
      
      if (!validSubDisposition) {
        console.log(`⚠️ [DISPOSITION-DETECTION] ${timer.end()}ms - Invalid sub-disposition detected: ${subDispositionTitle}`)
        console.log(`Available sub-dispositions: ${validDisposition.sub.join(', ')}`)
        validSubDisposition = null
      } else {
        // For sub-dispositions, we'll use the title as the ID since they don't have separate _id fields
        subDispositionId = validSubDisposition
        console.log(`✅ [DISPOSITION-DETECTION] Matched sub-disposition: ${subDispositionTitle} -> ${validSubDisposition}`)
      }
    }

    console.log(`🕒 [DISPOSITION-DETECTION] ${timer.end()}ms - Detected: ${dispositionTitle} (ID: ${validDisposition._id}) | ${validSubDisposition || 'N/A'}`)
    
    return { 
      disposition: dispositionTitle, 
      subDisposition: validSubDisposition || null,
      dispositionId: validDisposition._id,
      subDispositionId: subDispositionId
    }
  } catch (error) {
    console.log(`❌ [DISPOSITION-DETECTION] ${timer.end()}ms - Error: ${error.message}`)
    return { disposition: null, subDisposition: null, dispositionId: null, subDispositionId: null }
  }
}

// Simplified TTS processor
class SimplifiedElevenLabsTTSProcessor {
  constructor(language, ws, streamSid, callLogger = null) {
    this.language = language
    this.ws = ws
    this.streamSid = streamSid
    this.callLogger = callLogger
    this.elevenLabsLanguage = getElevenLabsLanguage(language)
    this.voice = getValidElevenLabsVoice(ws.sessionAgentConfig?.voiceSelection || "default")
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
  }

  reset(language) {
    this.interrupt()
    if (language) {
      this.language = language
      this.elevenLabsLanguage = getElevenLabsLanguage(language)
    }
    this.isInterrupted = false
    this.totalAudioBytes = 0
  }

  async synthesizeAndStream(text) {
    if (this.isInterrupted) return

    const timer = createTimer("TTS_SYNTHESIS")

    try {
      const response = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${this.voice}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "xi-api-key": API_KEYS.elevenlabs,
        },
        body: JSON.stringify({
          text: text,
          model_id: "eleven_multilingual_v2",
          voice_settings: {
            stability: 0.5,
            similarity_boost: 0.5,
            style: 0.0,
            use_speaker_boost: true
          },
          output_format: "pcm_8000"  // Return raw PCM 8kHz; headerless for SIP
        }),
      })

      if (!response.ok || this.isInterrupted) {
        if (!this.isInterrupted) {
          console.log(`❌ [TTS-SYNTHESIS] ${timer.end()}ms - Error: ${response.status}`)
          throw new Error(`ElevenLabs API error: ${response.status}`)
        }
        return
      }

      // ElevenLabs returns PCM 8kHz; verify headerless and guard against ID3/RIFF
      const audioBuffer = await response.arrayBuffer()
      let pcmBase64 = Buffer.from(audioBuffer).toString('base64')
      try {
        const b = Buffer.from(pcmBase64, 'base64')
        const sig4 = b.slice(0,4).toString('ascii')
        if (sig4 === 'RIFF') {
          console.log('🧪 [TTS-SYNTHESIS] Unexpected WAV header in PCM response; extracting data chunk')
          pcmBase64 = this.extractPcmLinear16Mono8kBase64(pcmBase64)
        } else if (sig4.startsWith('ID3')) {
          console.log('❌ [TTS-SYNTHESIS] Unexpected MP3 (ID3) in PCM response; aborting playback to avoid distortion')
          throw new Error('ElevenLabs returned MP3 despite pcm_8000 request')
        }
      } catch (_) {}

      if (!pcmBase64 || this.isInterrupted) {
        if (!this.isInterrupted) {
          console.log(`❌ [TTS-SYNTHESIS] ${timer.end()}ms - No audio data received`)
          throw new Error("No audio data received from ElevenLabs API")
        }
        return
      }

      console.log(`🕒 [TTS-SYNTHESIS] ${timer.end()}ms - Audio generated`)

      if (!this.isInterrupted) {
        // Stream clean PCM s16le 8kHz
        await this.streamAudioOptimizedForSIP(pcmBase64)
        const sentBuffer = Buffer.from(pcmBase64, "base64")
        this.totalAudioBytes += sentBuffer.length
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
    const response = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${this.voice}`, {
      method: "POST",
      headers: { 
        "Content-Type": "application/json", 
        "xi-api-key": API_KEYS.elevenlabs 
      },
      body: JSON.stringify({
        text: text,
        model_id: "eleven_multilingual_v2",
        voice_settings: {
          stability: 0.5,
          similarity_boost: 0.5,
          style: 0.0,
          use_speaker_boost: true
        },
        output_format: "pcm_8000"  // Return raw PCM 8kHz; headerless for SIP
      }),
    })
    if (!response.ok) {
      console.log(`❌ [TTS-PREPARE] ${timer.end()}ms - Error: ${response.status}`)
      throw new Error(`ElevenLabs API error: ${response.status}`)
    }
    const audioBuffer = await response.arrayBuffer()
    // PCM 8kHz expected; guard against headers
    let audioBase64 = Buffer.from(audioBuffer).toString('base64')
    try {
      const b = Buffer.from(audioBase64, 'base64')
      const sig4 = b.slice(0,4).toString('ascii')
      if (sig4 === 'RIFF') {
        console.log('🧪 [TTS-PREPARE] Unexpected WAV header in PCM response; extracting data chunk')
        audioBase64 = this.extractPcmLinear16Mono8kBase64(audioBase64)
      } else if (sig4.startsWith('ID3')) {
        console.log('❌ [TTS-PREPARE] Unexpected MP3 (ID3) in PCM response; using raw buffer but audio may distort')
      }
    } catch (_) {}
    if (!audioBase64) {
      console.log(`❌ [TTS-PREPARE] ${timer.end()}ms - No audio data received`)
      throw new Error("No audio data received from ElevenLabs API")
    }
    console.log(`🕒 [TTS-PREPARE] ${timer.end()}ms - Audio prepared`)
    return audioBase64
  }

  async enqueueText(text) {
    if (this.isInterrupted) return
    const item = { text, audioBase64: null, preparing: true }
    this.pendingQueue.push(item)
    ;(async () => {
      try { item.audioBase64 = await this.synthesizeToBuffer(text) } catch (_) { item.audioBase64 = null } finally { item.preparing = false }
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
          await this.streamAudioOptimizedForSIP(audioBase64)
          // Align to testing3.js pacing between queued chunks
          await new Promise(r => setTimeout(r, 60))
        }
      }
    } finally {
      this.isProcessingQueue = false
    }
  }

  async streamAudioOptimizedForSIP(audioBase64) {
    if (this.isInterrupted) return

    // Ensure we have PCM s16le 8kHz base64; strip WAV header if present
    let workingBase64 = audioBase64
    try {
      const probe = Buffer.from(audioBase64, 'base64')
      if (probe.length >= 12) {
        const isRIFF = probe.toString('ascii', 0, 4) === 'RIFF'
        const isWAVE = probe.toString('ascii', 8, 12) === 'WAVE'
        if (isRIFF && isWAVE) {
          console.log('🧪 [SIP-STREAM] Detected WAV header in payload; extracting PCM before streaming')
          workingBase64 = this.extractPcmLinear16Mono8kBase64(audioBase64)
        }
      }
    } catch (_) {}

    const audioBuffer = Buffer.from(workingBase64, "base64")
    const streamingSession = { interrupt: false }
    this.currentAudioStreaming = streamingSession

    // SIP Audio Requirements: 8kHz sample rate, Mono, PCM s16le format
    // Match testing3.js behavior: 40ms chunks (640 bytes) and pacing

    const SAMPLE_RATE = 8000
    const BYTES_PER_SAMPLE = 2  // PCM16 uses 2 bytes per sample
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000  // 16 bytes per ms
    const OPTIMAL_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS)  // 40ms of PCM16 at 8kHz (640 bytes)

    let position = 0
    let chunkIndex = 0
    let successfulChunks = 0

    // One-time stream header log to confirm format and base64 preview
    try {
      const estimatedMs = Math.floor(audioBuffer.length / BYTES_PER_MS)
      console.log(`🎧 [SIP-STREAM] Format=PCM s16le, Rate=${SAMPLE_RATE}Hz, Channels=1, Bytes=${audioBuffer.length}, EstDuration=${estimatedMs}ms, ChunkSize=${OPTIMAL_CHUNK_SIZE}`)
      const previewChunk = audioBuffer.slice(0, Math.min(OPTIMAL_CHUNK_SIZE, audioBuffer.length))
      const previewB64 = previewChunk.toString("base64")
      console.log(`🎧 [SIP-STREAM] FirstChunk Base64 (preview ${Math.min(previewB64.length, 120)} chars): ${previewB64.slice(0, 120)}${previewB64.length > 120 ? '…' : ''}`)
    } catch (_) {}

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

      if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted) {
        try {
          if (chunkIndex === 0) {
            // Extra detail for the very first SEND
            console.log(`📤 [SIP-STREAM] Sending first chunk: bytes=${chunk.length}, base64Len=${mediaMessage.media.payload.length}`)
          }
          this.ws.send(JSON.stringify(mediaMessage))
          successfulChunks++
        } catch (error) {
          break
        }
      } else {
        break
      }

      if (position + chunkSize < audioBuffer.length && !this.isInterrupted) {
        const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS)
        const delayMs = Math.max(chunkDurationMs - 2, 10) // align with testing3 pacing
        await new Promise((resolve) => setTimeout(resolve, delayMs))
      }

      position += chunkSize
      chunkIndex++
    }

    this.currentAudioStreaming = null
    console.log(`✅ [SIP-STREAM] Completed: sentChunks=${successfulChunks}, totalBytes=${audioBuffer.length}`)
  }

  // Convert MP3 to μ-law format for optimal SIP telephony performance
  convertMp3ToPcmLinear16Mono8k(audioBase64) {
    try {
      console.log("🔄 [AUDIO-CONVERSION] Converting MP3 to 8kHz μ-law for SIP telephony")
      
      // Try to extract PCM data from WAV format first
      const pcmData = this.extractPcmLinear16Mono8kBase64(audioBase64)
      
      if (pcmData !== audioBase64) {
        console.log("✅ [AUDIO-CONVERSION] WAV format detected and converted")
        return pcmData
      }
      
      // For MP3 format, we need to convert to 8kHz μ-law
      // This is a simplified conversion - for production, use ffmpeg
      console.log("🔄 [AUDIO-CONVERSION] MP3 detected - converting to 8kHz μ-law")
      console.log("📊 [AUDIO-CONVERSION] Source: MP3 22.05kHz/32kbps → Target: μ-law 8kHz/64kbps")
      
      // Convert MP3 to PCM and then to μ-law
      const audioBuffer = Buffer.from(audioBase64, 'base64')
      const pcmBuffer = this.convertMp3ToPcm(audioBuffer)
      const ulawBuffer = this.convertPcmToUlaw(pcmBuffer)
      
      console.log("✅ [AUDIO-CONVERSION] Converted to μ-law format for SIP compatibility")
      return ulawBuffer.toString('base64')
    } catch (error) {
      console.log(`❌ [AUDIO-CONVERSION] Error converting audio: ${error.message}`)
      return audioBase64
    }
  }

  // Convert MP3 to PCM (simplified - for production use ffmpeg)
  convertMp3ToPcm(mp3Buffer) {
    try {
      // This is a placeholder - in production, use ffmpeg or similar
      // For now, we'll assume the MP3 is already at a reasonable sample rate
      console.log("⚠️ [MP3-TO-PCM] Simplified conversion - consider using ffmpeg for production")
      return mp3Buffer
    } catch (error) {
      console.log(`❌ [MP3-TO-PCM] Error converting MP3: ${error.message}`)
      return mp3Buffer
    }
  }

  // Convert PCM to μ-law format for telephony optimization
  convertPcmToUlaw(pcmBuffer) {
    try {
      const ulawBuffer = Buffer.alloc(pcmBuffer.length / 2)
      
      for (let i = 0; i < pcmBuffer.length; i += 2) {
        // Read 16-bit PCM sample (little-endian)
        const sample = pcmBuffer.readInt16LE(i)
        // Convert to μ-law
        const ulaw = this.linearToUlaw(sample)
        ulawBuffer[i / 2] = ulaw
      }
      
      console.log("✅ [PCM-TO-ULAW] Converted PCM to μ-law format")
      return ulawBuffer
    } catch (error) {
      console.log(`❌ [PCM-TO-ULAW] Error converting to μ-law: ${error.message}`)
      return pcmBuffer
    }
  }

  // Convert μ-law base64 → PCM s16le 8kHz base64
  convertUlawBase64ToPcm16Base64(ulawBase64) {
    try {
      const ulaw = Buffer.from(ulawBase64, 'base64')
      const pcm = Buffer.alloc(ulaw.length * 2)
      for (let i = 0; i < ulaw.length; i++) {
        const s = this.ulawToLinear(ulaw[i])
        pcm.writeInt16LE(s, i * 2)
      }
      return pcm.toString('base64')
    } catch (e) {
      return ulawBase64
    }
  }

  // μ-law byte → linear PCM sample (16-bit)
  ulawToLinear(uVal) {
    uVal = ~uVal & 0xff
    const BIAS = 0x84
    const SIGN = (uVal & 0x80)
    let exponent = (uVal >> 4) & 0x07
    let mantissa = uVal & 0x0f
    let sample = (((mantissa << 1) + 1) << (exponent + 2)) - BIAS
    if (SIGN !== 0) sample = -sample
    return sample
  }

  // Convert linear PCM to μ-law encoding
  linearToUlaw(pcm) {
    const BIAS = 0x84
    const CLIP = 32635
    
    let sign = (pcm >> 8) & 0x80
    if (sign !== 0) pcm = -pcm
    if (pcm > CLIP) pcm = CLIP
    
    pcm += BIAS
    let exponent = 0
    let expMask = 0x4000
    let expShift = 13
    
    while ((pcm & expMask) === 0 && expShift > 0) {
      pcm <<= 1
      exponent++
      expShift--
    }
    
    const mantissa = (pcm >> expShift) & 0x0F
    const ulaw = ~(sign | (exponent << 4) | mantissa)
    return ulaw & 0xFF
  }

  // Simple audio resampling method (basic implementation)
  resampleAudioTo8kHz(audioBuffer, originalSampleRate = 22050) {
    try {
      if (originalSampleRate === 8000) {
        return audioBuffer // Already at correct sample rate
      }
      
      const ratio = originalSampleRate / SIP_AUDIO_CONFIG.SAMPLE_RATE
      const newLength = Math.floor(audioBuffer.length / ratio)
      const resampledBuffer = Buffer.alloc(newLength)
      
      // Simple linear interpolation resampling
      for (let i = 0; i < newLength; i += 2) {
        const sourceIndex = Math.floor(i * ratio * 2) & ~1 // Ensure even index
        if (sourceIndex < audioBuffer.length - 1) {
          resampledBuffer[i] = audioBuffer[sourceIndex]
          resampledBuffer[i + 1] = audioBuffer[sourceIndex + 1]
        }
      }
      
      console.log(`🔄 [AUDIO-RESAMPLING] Resampled from ${originalSampleRate}Hz to 8kHz`)
      return resampledBuffer
    } catch (error) {
      console.log(`❌ [AUDIO-RESAMPLING] Error resampling audio: ${error.message}`)
      return audioBuffer
    }
  }

  // Parse WAV header from base64, return minimal metadata
  parseWavHeaderFromBase64(wavBase64) {
    try {
      const buf = Buffer.from(wavBase64, 'base64')
      if (buf.length < 44) return null
      if (buf.toString('ascii', 0, 4) !== 'RIFF') return null
      if (buf.toString('ascii', 8, 12) !== 'WAVE') return null
      const fmtOffset = 12
      // Find 'fmt ' chunk
      let offset = fmtOffset
      let sampleRate = null
      let channels = null
      let bitsPerSample = null
      while (offset + 8 <= buf.length) {
        const id = buf.toString('ascii', offset, offset + 4)
        const size = buf.readUInt32LE(offset + 4)
        const next = offset + 8 + size
        if (id === 'fmt ') {
          channels = buf.readUInt16LE(offset + 10)
          sampleRate = buf.readUInt32LE(offset + 12)
          bitsPerSample = buf.readUInt16LE(offset + 22)
          break
        }
        offset = next
      }
      return { sampleRate, channels, bitsPerSample }
    } catch (_) {
      return null
    }
  }

  // Resample PCM s16le mono buffer from originalSampleRate → targetSampleRate using linear interpolation
  resamplePcm16Mono(pcmBuffer, originalSampleRate, targetSampleRate) {
    try {
      if (!originalSampleRate || !targetSampleRate || originalSampleRate === targetSampleRate) return pcmBuffer
      const samples = pcmBuffer.length / 2
      const src = new Int16Array(samples)
      for (let i = 0; i < samples; i++) src[i] = pcmBuffer.readInt16LE(i * 2)
      const durationSec = samples / originalSampleRate
      const targetSamples = Math.max(1, Math.round(durationSec * targetSampleRate))
      const dst = new Int16Array(targetSamples)
      const ratio = (samples - 1) / (targetSamples - 1)
      for (let i = 0; i < targetSamples; i++) {
        const pos = i * ratio
        const idx = Math.floor(pos)
        const frac = pos - idx
        const s1 = src[idx]
        const s2 = src[Math.min(idx + 1, samples - 1)]
        const value = s1 + (s2 - s1) * frac
        dst[i] = Math.max(-32768, Math.min(32767, Math.round(value)))
      }
      const out = Buffer.alloc(targetSamples * 2)
      for (let i = 0; i < targetSamples; i++) out.writeInt16LE(dst[i], i * 2)
      return out
    } catch (_) {
      return pcmBuffer
    }
  }

  extractPcmLinear16Mono8kBase64(audioBase64) {
    try {
      const buf = Buffer.from(audioBase64, 'base64')
      if (buf.length >= 12 && buf.toString('ascii', 0, 4) === 'RIFF' && buf.toString('ascii', 8, 12) === 'WAVE') {
        let offset = 12
        let dataOffset = null
        let dataSize = null
        while (offset + 8 <= buf.length) {
          const chunkId = buf.toString('ascii', offset, offset + 4)
          const chunkSize = buf.readUInt32LE(offset + 4)
          const next = offset + 8 + chunkSize
          if (chunkId === 'data') {
            dataOffset = offset + 8
            dataSize = chunkSize
            break
          }
          offset = next
        }
        if (dataOffset != null && dataSize != null) {
          return buf.slice(dataOffset, dataOffset + dataSize).toString('base64')
        }
      }
      return audioBase64
    } catch (_) {
      return audioBase64
    }
  }

  getStats() {
    return {
      totalAudioBytes: this.totalAudioBytes,
    }
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
              callLogger.logUserTranscript(transcript.trim())
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
            callLogger.logUserTranscript(userUtteranceBuffer.trim())
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
        console.log("🔍 [USER-UTTERANCE] Running AI detections + streaming...")

        // Kick off LLM streaming and partial TTS
        let aiResponse = null
        const tts = new SimplifiedElevenLabsTTSProcessor(currentLanguage, ws, streamSid, callLogger)
        currentTTS = tts
        let sentIndex = 0
        const MIN_TOKENS = 8
        const MAX_TOKENS = 10

        // Fallback to non-streaming generation to avoid runtime error
        aiResponse = await processWithOpenAI(
          text,
          conversationHistory,
          currentLanguage,
          callLogger,
          agentConfig,
          userName
        )

        // Final flush for short tail
        if (processingRequestId === currentRequestId && aiResponse && aiResponse.length > sentIndex) {
          const tail = aiResponse.slice(sentIndex).trim()
          if (tail) {
            try { await currentTTS.enqueueText(tail) } catch (_) {}
            sentIndex = aiResponse.length
          }
        }

        // Ensure follow-up question at end
        if (aiResponse && !/[?]\s*$/.test(aiResponse)) {
          const followUps = { hi: "क्या मैं और किसी बात में आपकी मदद कर सकता/सकती हूँ?", en: "Is there anything else I can help you with?", mr: "आणखी काही मदत हवी आहे का?", bn: "আর কিছু কি আপনাকে সাহায্য করতে পারি?", ta: "வேறு எதற்காவது உதவி வேண்டுமா?", te: "ఇంకేమైనా సహాయం కావాలా?", gu: "શું બીજી કોઈ મદદ કરી શકું?" }
          aiResponse = `${aiResponse} ${(followUps[currentLanguage?.toLowerCase()] || followUps.en)}`.trim()
        }

        // Save detections (lead status, WA request) in parallel (non-blocking)
        ;(async () => {
          try {
            const [leadStatus, whatsappRequest] = await Promise.all([
              detectLeadStatusWithOpenAI(text, conversationHistory, currentLanguage),
              detectWhatsAppRequest(text, conversationHistory, currentLanguage),
            ])
            if (callLogger) {
              callLogger.updateLeadStatus(leadStatus)
              if (whatsappRequest === "WHATSAPP_REQUEST") callLogger.markWhatsAppRequested()
            }
          } catch (_) {}
        })()

        if (processingRequestId === currentRequestId && aiResponse) {
          // Log full AI response once
          try { if (callLogger) { callLogger.logAIResponse(aiResponse) } } catch (_) {}

          conversationHistory.push(
            { role: "user", content: text },
            { role: "assistant", content: aiResponse }
          )
          if (conversationHistory.length > 10) conversationHistory = conversationHistory.slice(-10)
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
              console.log("✅ [SIP-AGENT-LOOKUP] Depositions:", agentConfig.depositions ? `${agentConfig.depositions.length} configured` : "None configured")
              if (agentConfig.depositions && agentConfig.depositions.length > 0) {
                console.log("✅ [SIP-AGENT-LOOKUP] Disposition Categories:")
                agentConfig.depositions.forEach((dep, index) => {
                  console.log(`  ${index + 1}. ${dep.title}`)
                  if (dep.sub && dep.sub.length > 0) {
                    dep.sub.forEach((sub, subIndex) => {
                      console.log(`     ${subIndex + 1}. ${sub}`)
                    })
                  }
                })
              }
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
            callLogger.currentLanguage = currentLanguage; // Set initial language

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
              callLogger.logAIResponse(greeting)
            }

            console.log("🎤 [SIP-TTS] Starting greeting TTS...")
            const tts = new SimplifiedElevenLabsTTSProcessor(currentLanguage, ws, streamSid, callLogger)
            await tts.synthesizeAndStream(greeting)
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
                const savedLog = await callLogger.saveToDatabase(finalLeadStatus, agentConfig)
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
          const savedLog = await callLogger.saveToDatabase(finalLeadStatus, agentConfig)
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