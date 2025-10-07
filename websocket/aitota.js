const WebSocket = require("ws")
require("dotenv").config()
const mongoose = require("mongoose")
const Agent = require("../models/Agent")
const CallLog = require("../models/CallLog")
const Credit = require("../models/Credit")

// Language detection removed - using default language from agent config

// Load API keys from environment variables
const API_KEYS = {
  deepgram: process.env.DEEPGRAM_API_KEY,
  sarvam: process.env.SARVAM_API_KEY,
  smallest: process.env.SMALLEST_API_KEY,
  openai: process.env.OPENAI_API_KEY,
  whatsapp: process.env.WHATSAPP_TOKEN,
}

// Validate API keys
if (!API_KEYS.deepgram || !API_KEYS.smallest || !API_KEYS.openai) {
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

// Performance timing helper with detailed logging
const createTimer = (label) => {
  const start = Date.now()
  const checkpoints = []
  return {
    start,
    end: () => {
      const duration = Date.now() - start
      console.log(`⏱️ [TIMING] ${label}: ${duration}ms`)
      return duration
    },
    checkpoint: (checkpointName) => {
      const now = Date.now()
      const elapsed = now - start
      const sinceLastCheckpoint = checkpoints.length > 0 ? now - checkpoints[checkpoints.length - 1].time : elapsed
      const checkpoint = {
        name: checkpointName,
        time: now,
        elapsed,
        sinceLast: sinceLastCheckpoint
      }
      checkpoints.push(checkpoint)
      console.log(`⏱️ [CHECKPOINT] ${label}.${checkpointName}: ${elapsed}ms (+${sinceLastCheckpoint}ms)`)
      return elapsed
    },
    getFullReport: () => {
      const duration = Date.now() - start
      console.log(`\n📊 [LATENCY-REPORT] ${label}: ${duration}ms total`)
      checkpoints.forEach((cp, index) => {
        console.log(`  ${index + 1}. ${cp.name}: ${cp.elapsed}ms (+${cp.sinceLast}ms)`)
      })
      return { duration, checkpoints }
    }
  }
}

// Language mapping for TTS and STT services
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

const getSarvamLanguage = (language = "hi") => {
  const lang = language?.toLowerCase() || "hi"
  return LANGUAGE_MAPPING[lang] || "hi-IN"
}

const getDeepgramLanguage = (language = "hi") => {
  const lang = language?.toLowerCase() || "hi"
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
        const tts = new SimplifiedSmallestTTSProcessor(this.currentLanguage, this.ws, this.streamSid, this.callLogger, this.agentConfig)
        
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
        const tts = new SimplifiedSmallestTTSProcessor(language, this.ws, this.streamSid, this.callLogger, this.agentConfig)
        
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
        const tts = new SimplifiedSmallestTTSProcessor(language, this.ws, this.streamSid, this.callLogger, this.agentConfig)
        
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

// Enhanced streaming OpenAI completion with sentence-based TTS processing
class StreamingLLMProcessor {
  constructor(language, ws, streamSid, callLogger, agentConfig) {
    this.language = language
    this.ws = ws
    this.streamSid = streamSid
    this.callLogger = callLogger
    this.agentConfig = agentConfig
    this.sentenceQueue = [] // FIFO queue for complete sentences
    this.currentBuffer = "" // Buffer for incomplete sentences
    this.isProcessing = false
    this.ttsProcessor = null
    this.sentencePatterns = this.getSentencePatterns(language)
  }

  getSentencePatterns(language) {
    const patterns = {
      en: /[.!?]+/,
      hi: /[।!?]+/,
      bn: /[।!?]+/,
      ta: /[।!?]+/,
      te: /[।!?]+/,
      mr: /[।!?]+/,
      gu: /[।!?]+/,
      kn: /[।!?]+/,
      ml: /[।!?]+/,
      pa: /[।!?]+/,
      or: /[।!?]+/,
      as: /[।!?]+/,
      ur: /[۔!?]+/
    }
    return patterns[language] || patterns.en
  }

  async processStreamingResponse(userMessage, conversationHistory, userName = null) {
    const timer = createTimer("STREAMING_LLM_PROCESSING")
    let accumulated = ""
    
    try {
      if (!API_KEYS.openai) {
        console.warn("⚠️ [LLM-STREAM] OPENAI_API_KEY not set; skipping generation")
        return null
      }

      const basePrompt = (this.agentConfig?.systemPrompt || "You are a helpful AI assistant. Answer concisely.").trim()
      const firstMessage = (this.agentConfig?.firstMessage || "").trim()
      const knowledgeBlock = firstMessage ? `FirstGreeting: "${firstMessage}"\n` : ""
      const policyBlock = [
        "Answer strictly using the information provided above.",
        "If specifics (address/phone/timings) are missing, say you don't have that info.",
        "End with a brief follow-up question.",
        "Keep reply under 100 tokens.",
        "dont give any fornts or styles in it or symbols in it",
        "in which language you get the transcript in same language give response in same language",
        "give follow up question at end of every response in the same language they ask question"
      ].join(" ")
      const systemPrompt = `System Prompt:\n${basePrompt}\n\n${knowledgeBlock}${policyBlock}`
      const personalizationMessage = userName && userName.trim()
        ? { role: "system", content: `The user's name is ${userName.trim()}. Address them naturally when appropriate.` }
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
          stream: true,
        }),
      })

      if (!response.ok || !response.body) {
        console.error(`❌ [LLM-STREAM] ${timer.end()}ms - HTTP ${response.status}`)
        return null
      }

      // Initialize TTS processor
      this.ttsProcessor = new SimplifiedSmallestTTSProcessor(
        this.language, 
        this.ws, 
        this.streamSid, 
        this.callLogger, 
        this.agentConfig
      )

      const reader = response.body.getReader()
      const decoder = new TextDecoder("utf-8")
      let buffer = ""

      console.log("🔄 [STREAMING-LLM] Starting streaming response processing...")

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
            // Process any remaining buffer as final sentence
            if (this.currentBuffer.trim()) {
              await this.processCompleteSentence(this.currentBuffer.trim())
            }
            break
          }
          if (trimmed.startsWith("data:")) {
            const jsonStr = trimmed.slice(5).trim()
            try {
              const chunk = JSON.parse(jsonStr)
              const delta = chunk.choices?.[0]?.delta?.content || ""
              if (delta) {
                accumulated += delta
                await this.processToken(delta)
              }
            } catch (_) {}
          }
        }
      }

      console.log(`🕒 [STREAMING-LLM] ${timer.end()}ms - Streaming completed (${accumulated.length} chars)`)

      // Ensure any leftover sentences are flushed to TTS (including single last sentence)
      try {
        await this.flushAllPending()
      } catch (_) {}
      return (accumulated || '').trim() || null
    } catch (error) {
      console.error(`❌ [STREAMING-LLM] ${timer.end()}ms - Error: ${error.message}`)
      return accumulated || null
    }
  }

  async processToken(token) {
    this.currentBuffer += token
    
    // Check for sentence completion
    const sentenceMatch = this.currentBuffer.match(this.sentencePatterns)
    if (sentenceMatch) {
      const sentenceEndIndex = sentenceMatch.index + sentenceMatch[0].length
      const completeSentence = this.currentBuffer.substring(0, sentenceEndIndex).trim()
      
      if (completeSentence) {
        // Add to FIFO queue
        this.sentenceQueue.push(completeSentence)
        console.log(`📝 [SENTENCE-QUEUE] Added sentence: "${completeSentence}"`)
        
        // Check if we should process now (2-3 sentences or if sentences are short)
        const shouldProcess = this.shouldProcessSentences()
        
        if (shouldProcess && !this.isProcessing) {
          await this.processSentenceQueue()
        }
      }
      
      // Update buffer with remaining text
      this.currentBuffer = this.currentBuffer.substring(sentenceEndIndex)
    }
  }

  // Determine if we should process sentences now
  shouldProcessSentences() {
    if (this.sentenceQueue.length === 0) return false
    
    // Always process if we have 3 or more sentences
    if (this.sentenceQueue.length >= 3) {
      console.log(`📝 [SENTENCE-BATCH] Processing 3+ sentences (${this.sentenceQueue.length})`)
      return true
    }
    
    // Process if we have 2 sentences and at least one is short (less than 20 chars)
    if (this.sentenceQueue.length >= 2) {
      const hasShortSentence = this.sentenceQueue.some(sentence => sentence.length < 20)
      if (hasShortSentence) {
        console.log(`📝 [SENTENCE-BATCH] Processing 2 sentences (one is short)`)
        return true
      }
    }
    
    // Process if we have 2 sentences and total length is reasonable (less than 100 chars)
    if (this.sentenceQueue.length >= 2) {
      const totalLength = this.sentenceQueue.reduce((sum, sentence) => sum + sentence.length, 0)
      if (totalLength < 100) {
        console.log(`📝 [SENTENCE-BATCH] Processing 2 sentences (total length: ${totalLength} chars)`)
        return true
      }
    }
    
    return false
  }

  async processCompleteSentence(sentence) {
    if (sentence.trim()) {
      this.sentenceQueue.push(sentence.trim())
      console.log(`📝 [SENTENCE-QUEUE] Added final sentence: "${sentence.trim()}"`)
      
      // Check if we should process now (2-3 sentences or if sentences are short)
      const shouldProcess = this.shouldProcessSentences()
      
      if (shouldProcess && !this.isProcessing) {
        await this.processSentenceQueue()
      }
    }
  }

  async processSentenceQueue() {
    if (this.isProcessing || this.sentenceQueue.length === 0) return
    
    this.isProcessing = true
    console.log(`🔄 [SENTENCE-QUEUE] Processing ${this.sentenceQueue.length} sentences in FIFO order`)
    
    try {
      while (this.sentenceQueue.length > 0) {
        // Determine how many sentences to process together (2-3 sentences)
        const sentencesToProcess = this.getSentencesToProcess()
        
        if (sentencesToProcess.length === 0) break
        
        // Join sentences with a space
        const combinedText = sentencesToProcess.join(' ')
        console.log(`🎤 [SENTENCE-TTS] Processing ${sentencesToProcess.length} sentences: "${combinedText}"`)
        
        if (this.ttsProcessor && !this.ttsProcessor.isInterrupted) {
          try {
            await this.ttsProcessor.synthesizeAndStream(combinedText)
            console.log(`✅ [SENTENCE-TTS] Completed ${sentencesToProcess.length} sentences`)
          } catch (error) {
            console.log(`❌ [SENTENCE-TTS] Error processing sentences: ${error.message}`)
            // Continue with next batch even if one fails
          }
        } else {
          console.log(`⚠️ [SENTENCE-TTS] TTS processor interrupted, stopping queue processing`)
          break
        }
      }
    } finally {
      this.isProcessing = false
      console.log(`✅ [SENTENCE-QUEUE] Queue processing completed`)
    }
  }

  // Force flush any pending sentences, even if only one remains
  async flushAllPending() {
    if (this.currentBuffer.trim()) {
      this.sentenceQueue.push(this.currentBuffer.trim())
      this.currentBuffer = ""
    }
    if (this.sentenceQueue.length === 0) return
    const wasProcessing = this.isProcessing
    try {
      // Temporarily force processing regardless of batching heuristics
      this.isProcessing = false
      while (this.sentenceQueue.length > 0) {
        const batch = []
        // Drain up to 3 sentences, or whatever remains
        for (let i = 0; i < 3 && this.sentenceQueue.length > 0; i++) {
          batch.push(this.sentenceQueue.shift())
        }
        const combinedText = batch.join(' ')
        if (this.ttsProcessor && !this.ttsProcessor.isInterrupted && combinedText.trim()) {
          try {
            await this.ttsProcessor.synthesizeAndStream(combinedText)
          } catch (_) {}
        } else {
          break
        }
      }
    } finally {
      this.isProcessing = wasProcessing && this.sentenceQueue.length > 0
    }
  }

  // Get sentences to process together (2-3 sentences)
  getSentencesToProcess() {
    const sentences = []
    
    // Always take at least 2 sentences if available
    const minSentences = Math.min(2, this.sentenceQueue.length)
    for (let i = 0; i < minSentences; i++) {
      sentences.push(this.sentenceQueue.shift())
    }
    
    // Take a 3rd sentence if:
    // 1. We have more sentences available AND
    // 2. The total length would be reasonable (less than 150 chars) AND
    // 3. Either we have 3+ sentences total OR the current batch is short
    if (this.sentenceQueue.length > 0) {
      const currentLength = sentences.reduce((sum, s) => sum + s.length, 0)
      const nextSentence = this.sentenceQueue[0]
      
      if (currentLength + nextSentence.length < 150) {
        sentences.push(this.sentenceQueue.shift())
        console.log(`📝 [SENTENCE-BATCH] Taking 3rd sentence (total length: ${currentLength + nextSentence.length} chars)`)
      }
    }
    
    return sentences
  }

  // Method to interrupt the streaming processor
  interrupt() {
    console.log(`🛑 [STREAMING-LLM] Interrupting streaming processor...`)
    if (this.ttsProcessor) {
      this.ttsProcessor.interrupt()
    }
    this.isProcessing = false
    this.sentenceQueue = [] // Clear remaining sentences
    this.currentBuffer = ""
  }

  // Method to get current status
  getStatus() {
    return {
      isProcessing: this.isProcessing,
      queueLength: this.sentenceQueue.length,
      currentBuffer: this.currentBuffer,
      hasTTSProcessor: !!this.ttsProcessor
    }
  }
}

// Streaming OpenAI completion that emits partials via callback (reference: sanpbx-server.js)
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

    const basePrompt = (agentConfig?.systemPrompt || "You are a helpful AI assistant. Answer concisely.").trim()
    const firstMessage = (agentConfig?.firstMessage || "").trim()
    const knowledgeBlock = firstMessage ? `FirstGreeting: "${firstMessage}"\n` : ""
    const policyBlock = [
      "Answer strictly using the information provided above.",
      "If specifics (address/phone/timings) are missing, say you don't have that info.",
      "End with a brief follow-up question.",
      "Keep reply under 100 tokens.",
      "dont give any fornts or styles in it or symbols in it",
      "in which language you get the transcript in same language give response in same language",
      "give follow up question at end of every response in the same language they ask question"
    ].join(" ")
    const systemPrompt = `System Prompt:\n${basePrompt}\n\n${knowledgeBlock}${policyBlock}`
    const personalizationMessage = userName && userName.trim()
      ? { role: "system", content: `The user's name is ${userName.trim()}. Address them naturally when appropriate.` }
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
    // Let the LLM include the follow-up per policy; do not append here
    return (accumulated || '').trim() || null
  } catch (error) {
    console.error(`❌ [LLM-STREAM] ${timer.end()}ms - Error: ${error.message}`)
    return accumulated || null
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

// Simplified TTS processor using Smallest.ai Lightning v2 WebSocket
class SimplifiedSmallestTTSProcessor {
  constructor(language, ws, streamSid, callLogger = null, agentConfig = null) {
    this.language = language
    this.ws = ws
    this.streamSid = streamSid
    this.callLogger = callLogger
    this.agentConfig = agentConfig // Store agent config for voice selection
    this.isInterrupted = false
    this.currentAudioStreaming = null
    this.totalAudioBytes = 0
    this.pendingQueue = [] // { text, audioBase64, preparing }
    this.isProcessingQueue = false
    this.smallestWs = null
    this.smallestReady = false
    this.requestId = 0
    this.pendingRequests = new Map() // requestId -> { resolve, reject, text, audioChunks }
    this.isProcessing = false // Prevent multiple simultaneous requests
    this.audioQueue = [] // Queue for audio items to be sent to SIP
    this.isProcessingSIPQueue = false // Prevent overlapping SIP audio processing
    this.streamingRequests = new Set() // Track active streaming requests
    this.completedRequests = new Set() // Track completed requests
    this.audioBuffer = Buffer.alloc(0) // Buffer for smooth audio streaming
    this.lastAudioTime = 0 // Track last audio chunk time
    this.audioGapThreshold = 200 // Minimum gap between audio items (ms)
    this.keepAliveTimer = null // Keep-alive timer
    this.lastActivity = Date.now() // Track last activity
    this.connectionRetryCount = 0 // Track connection retry attempts
    this.maxRetries = 3 // Maximum retry attempts
  }

  interrupt() {
    this.isInterrupted = true
    this.isProcessing = false
    this.isProcessingSIPQueue = false
    
    if (this.currentAudioStreaming) {
      this.currentAudioStreaming.interrupt = true
    }
    
    // Clear all pending requests and queues
    this.pendingRequests.clear()
    this.audioQueue = []
    this.pendingQueue = []
    this.audioBuffer = Buffer.alloc(0)
    this.lastAudioTime = 0
    
    // Stop keep-alive timer
    this.stopKeepAlive()
    
    // Close WebSocket connection properly
    if (this.smallestWs) {
      if (this.smallestWs.readyState === WebSocket.OPEN) {
        this.smallestWs.close(1000, "TTS interrupted")
      }
      this.smallestWs = null
    }
    this.smallestReady = false
  }

  reset(language) {
    this.interrupt()
    if (language) {
      this.language = language
    }
    this.isInterrupted = false
    this.totalAudioBytes = 0
  }

  async connectToSmallest() {
    if (this.smallestWs && this.smallestWs.readyState === WebSocket.OPEN) {
      console.log("🎤 [SMALLEST-TTS] WebSocket already connected")
      return
    }

    try {
      console.log("🎤 [SMALLEST-TTS] Establishing WebSocket connection...")
      
      this.smallestWs = new WebSocket("wss://waves-api.smallest.ai/api/v1/lightning-v2/get_speech/stream", {
        headers: {
          Authorization: `Bearer ${API_KEYS.smallest}`
        }
      })

      // Wait for connection to be ready
      await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          console.log("❌ [SMALLEST-TTS] Connection timeout after 5 seconds")
          reject(new Error("Connection timeout"))
        }, 5000)

        this.smallestWs.onopen = () => {
          console.log("🎤 [SMALLEST-TTS] WebSocket connection established")
          this.smallestReady = true
          this.connectionRetryCount = 0 // Reset retry count on successful connection
          clearTimeout(timeout)
          
          // Start keep-alive mechanism
          this.startKeepAlive()
          resolve()
        }

        this.smallestWs.onerror = (error) => {
          console.log("❌ [SMALLEST-TTS] WebSocket error:", error.message)
          this.smallestReady = false
          clearTimeout(timeout)
          reject(error)
        }

        this.smallestWs.onclose = (event) => {
          console.log("🔌 [SMALLEST-TTS] WebSocket connection closed:", event.code, event.reason)
          this.smallestReady = false
          this.smallestWs = null
          
          // Stop keep-alive timer
          this.stopKeepAlive()
          
          // Auto-reconnect if not interrupted and within retry limit
          if (!this.isInterrupted && event.code !== 1000 && this.connectionRetryCount < this.maxRetries) {
            console.log("🔄 [SMALLEST-TTS] Attempting to reconnect in 2 seconds...")
            setTimeout(() => {
              if (!this.isInterrupted) {
                this.connectToSmallest().catch(err => 
                  console.log("❌ [SMALLEST-TTS] Reconnection failed:", err.message)
                )
              }
            }, 2000)
          } else if (this.connectionRetryCount >= this.maxRetries) {
            console.log("❌ [SMALLEST-TTS] Max reconnection attempts reached")
          }
        }

        this.smallestWs.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data)
            // Log only essential info, not full response with base64
            console.log(`📨 [SMALLEST-TTS] Response: ${data.status} for request ${data.request_id}`)
            this.handleSmallestResponse(data)
          } catch (error) {
            console.log("❌ [SMALLEST-TTS] Error parsing response:", error.message)
          }
        }
      })

    } catch (error) {
      console.log("❌ [SMALLEST-TTS] Connection error:", error.message)
      this.smallestReady = false
      throw error
    }
  }

  startKeepAlive() {
    // Clear existing keep-alive timer
    if (this.keepAliveTimer) {
      clearInterval(this.keepAliveTimer)
    }
    
    // Use WebSocket ping frames every 30 seconds to keep connection alive
    this.keepAliveTimer = setInterval(() => {
      if (this.smallestWs && this.smallestWs.readyState === WebSocket.OPEN && !this.isInterrupted) {
        try {
          if (typeof this.smallestWs.ping === "function") {
            this.smallestWs.ping()
            console.log("🏓 [SMALLEST-TTS] Keep-alive ping sent")
          } else {
            // Fallback: send a lightweight noop message if ping is unavailable
            this.smallestWs.send(JSON.stringify({ type: "ping" }))
            console.log("🏓 [SMALLEST-TTS] Keep-alive noop sent")
          }
        } catch (error) {
          console.log("❌ [SMALLEST-TTS] Keep-alive ping failed:", error.message)
        }
      } else {
        // Clear timer if connection is not open
        clearInterval(this.keepAliveTimer)
        this.keepAliveTimer = null
      }
    }, 30000)
  }

  stopKeepAlive() {
    if (this.keepAliveTimer) {
      clearInterval(this.keepAliveTimer)
      this.keepAliveTimer = null
    }
  }

  async retryRequest(requestId, request) {
    if (this.connectionRetryCount >= this.maxRetries) {
      console.log(`❌ [SMALLEST-TTS] Max retries reached for request ${requestId}, creating fallback audio`)
      // Create a fallback audio instead of rejecting
      this.createFallbackAudio(request.text, request)
      return
    }

    this.connectionRetryCount++
    console.log(`🔄 [SMALLEST-TTS] Retrying request ${requestId} (attempt ${this.connectionRetryCount}/${this.maxRetries})`)
    
    try {
      // Wait a bit before retrying
      await new Promise(resolve => setTimeout(resolve, 1000))
      
      // Reconnect if needed
      if (!this.smallestReady || !this.smallestWs || this.smallestWs.readyState !== WebSocket.OPEN) {
        await this.connectToSmallest()
      }
      
      // Retry the request
      await this.processSingleTTSRequest(request.text, request.voiceId)
      
    } catch (error) {
      console.log(`❌ [SMALLEST-TTS] Retry failed for request ${requestId}:`, error.message)
      if (this.connectionRetryCount >= this.maxRetries) {
        // Create fallback audio instead of rejecting
        this.createFallbackAudio(request.text, request)
      }
    }
  }

  createFallbackAudio(text, request) {
    console.log(`🔧 [SMALLEST-TTS] Creating fallback audio for: "${text.substring(0, 30)}..."`)
    
    // Create a simple beep or silence as fallback
    // This ensures the conversation flow continues even if TTS fails
    const fallbackAudio = Buffer.alloc(1600) // 100ms of silence at 8kHz
    fallbackAudio.fill(0)
    
    // Stream the fallback audio
    this.streamAudioOptimizedForSIP(fallbackAudio.toString('base64'))
      .then(() => {
        console.log(`✅ [SMALLEST-TTS] Fallback audio sent for: "${text.substring(0, 30)}..."`)
        request.resolve("FALLBACK_COMPLETED")
        this.pendingRequests.delete(request.requestId)
      })
      .catch(error => {
        console.log(`❌ [SMALLEST-TTS] Fallback audio failed:`, error.message)
        request.reject(error)
        this.pendingRequests.delete(request.requestId)
      })
  }

  handleSmallestResponse(data) {
    const { request_id, status, data: responseData } = data
    
    // Convert request_id to number for lookup since we store as numbers
    const numericRequestId = parseInt(request_id)
    
    if (request_id && this.pendingRequests.has(numericRequestId)) {
      const request = this.pendingRequests.get(numericRequestId)
      
      if (status === "chunk" && responseData?.audio) {
        // Store audio chunk and stream immediately (no logging for performance)
        if (!request.audioChunks) {
          request.audioChunks = []
        }
        request.audioChunks.push(responseData.audio)
        
        // Stream the audio chunk immediately without waiting
        this.streamAudioOptimizedForSIP(responseData.audio)
          .catch(() => {
            // Silent error handling for performance
          })
      } else if (status === "complete") {
        console.log(`✅ [SMALLEST-TTS] Request ${request_id} completed`)
        // Complete status means no more audio chunks, resolve without streaming again
        // Audio was already streamed chunk by chunk above
        request.resolve("COMPLETED")
        this.pendingRequests.delete(numericRequestId)
      } else if (status === "success" && responseData?.audio) {
        console.log(`✅ [SMALLEST-TTS] Received success response for request ${request_id}`)
        // Convert base64 audio to PCM and stream
        this.streamAudioOptimizedForSIP(responseData.audio)
          .then(() => {
            request.resolve(responseData.audio)
            this.pendingRequests.delete(numericRequestId)
          })
          .catch((error) => {
            request.reject(error)
            this.pendingRequests.delete(numericRequestId)
          })
      } else if (status === "error") {
        console.log(`❌ [SMALLEST-TTS] Request ${request_id} failed: ${data.message || 'Unknown error'}`)
        // Try to retry the request instead of immediately rejecting
        this.retryRequest(numericRequestId, request)
      } else {
        console.log(`⚠️ [SMALLEST-TTS] Unknown status for request ${request_id}: ${status}`)
      }
    } else {
      // Handle responses for unknown request IDs more gracefully
      if (status === "complete" || status === "success") {
        console.log(`⚠️ [SMALLEST-TTS] Received ${status} for unknown request_id: ${request_id} - ignoring`)
      } else if (status === "error") {
        console.log(`⚠️ [SMALLEST-TTS] Received error for unknown request_id: ${request_id} - ${data.message || 'Unknown error'}`)
      }
    }
  }

  async synthesizeAndStream(text) {
    if (this.isInterrupted) return

    // Start timing for individual TTS request
    const ttsReqTiming = createTimer("TTS_INDIVIDUAL_REQUEST")
    console.log(`🎤 [TTS-REQUEST-START] Text length: ${text.length} chars`)

    // Ensure connection is healthy before processing
    if (!this.smallestReady || !this.smallestWs || this.smallestWs.readyState !== WebSocket.OPEN) {
      console.log("🔄 [SMALLEST-TTS] Connection not ready, reconnecting...")
      try {
        await this.connectToSmallest()
        ttsReqTiming.checkpoint("TTS_CONNECTION_RECONNECTED")
      } catch (error) {
        console.log("❌ [SMALLEST-TTS] Failed to reconnect:", error.message)
        // Continue with fallback audio
        this.createFallbackAudio(text, { text, resolve: () => {}, reject: () => {} })
        return
      }
    }

    // Add to queue instead of processing immediately
    return new Promise((resolve, reject) => {
      this.pendingQueue.push({
        text,
        resolve,
        reject,
        preparing: false,
        audioBase64: null,
        timing: ttsReqTiming
      })
      
      ttsReqTiming.checkpoint("TTS_REQUEST_QUEUED")
      
      // Start processing queue if not already processing
      if (!this.isProcessing) {
        this.processQueue()
      }
    })
  }

  async synthesizeToBuffer(text) {
    const timer = createTimer("SMALLEST_TTS_PREPARE")
    
    try {
      // Always ensure WebSocket connection is fresh for each request
      if (!this.smallestReady || !this.smallestWs || this.smallestWs.readyState !== WebSocket.OPEN) {
        console.log("🔄 [SMALLEST-TTS] Reconnecting WebSocket for buffer request...")
        if (this.smallestWs) {
          this.smallestWs.close()
          this.smallestWs = null
        }
        await this.connectToSmallest()
      }

      // Clear any pending requests to prevent conflicts
      this.pendingRequests.clear()
      
      // Wait a bit to ensure previous requests are cleared
      await new Promise(resolve => setTimeout(resolve, 50))

      const requestId = ++this.requestId
      
      // Get voice ID from agent config or use default
      let voiceId = "ryan" // Default fallback
      if (this.agentConfig) {
        if (this.agentConfig.voiceId) {
          voiceId = this.agentConfig.voiceId
          console.log(`🎤 [TTS-VOICE] Using voice ID from DB: ${voiceId}`)
        } else if (this.agentConfig.voiceSelection) {
          // Map voice selection to voice ID if voiceId is not set
          const voiceMapping = {
            "male-professional": "ryan",
            "female-professional": "sarah", 
            "male-friendly": "ryan",
            "female-friendly": "sarah",
            "neutral": "ryan",
            "meera": "meera",
            "pavithra": "pavithra",
            "maitreyi": "maitreyi",
            "arvind": "arvind",
            "amol": "amol",
            "amartya": "amartya",
            "diya": "diya",
            "neel": "neel",
            "misha": "misha",
            "vian": "vian",
            "arjun": "arjun",
            "maya": "maya",
            "kumaran": "kumaran",
            "monika": "monika",
            "aahir": "aahir",
            "kanika": "kanika"
          }
          voiceId = voiceMapping[this.agentConfig.voiceSelection] || "ryan"
          console.log(`🎤 [TTS-VOICE] Mapped voice selection '${this.agentConfig.voiceSelection}' to voice ID: ${voiceId}`)
        }
      }
      
      const request = {
        voice_id: voiceId,
        text: text,
        max_buffer_flush_ms: 0,
        continue: false,
        flush: false,
        language: this.language === "hi" ? "hi" : "en",
        sample_rate: 8000,
        speed: 1,
        consistency: 0.5,
        enhancement: 1,
        similarity: 0
      }

      // Send request and wait for response
      const audioPromise = new Promise((resolve, reject) => {
        this.pendingRequests.set(requestId, { resolve, reject, text, audioChunks: [] })
        
        const requestWithId = { ...request, request_id: String(requestId) }
        
        // Check WebSocket state before sending
        if (this.smallestWs.readyState !== WebSocket.OPEN) {
          console.log(`❌ [SMALLEST-TTS] WebSocket not open, state: ${this.smallestWs.readyState}`)
          reject(new Error("WebSocket not ready"))
          return
        }
        
        try {
          this.smallestWs.send(JSON.stringify(requestWithId))
        } catch (sendError) {
          console.log(`❌ [SMALLEST-TTS] Error sending request: ${sendError.message}`)
          reject(sendError)
          return
        }
        
        setTimeout(() => {
          if (this.pendingRequests.has(requestId)) {
            console.log(`⏰ [SMALLEST-TTS] Request ${requestId} timed out after 5 seconds`)
            this.pendingRequests.delete(requestId)
            reject(new Error("TTS request timeout"))
          }
        }, 5000)
      })

      const audioBase64 = await audioPromise
      console.log(`🕒 [SMALLEST-TTS-PREPARE] ${timer.end()}ms - Audio prepared`)
    return audioBase64

    } catch (error) {
      console.log(`❌ [SMALLEST-TTS-PREPARE] ${timer.end()}ms - Error: ${error.message}`)
      throw error
    }
  }


  async processQueue() {
    if (this.isProcessing) return
    this.isProcessing = true
    
    try {
      while (!this.isInterrupted && this.pendingQueue.length > 0) {
        const item = this.pendingQueue.shift()
        
        if (this.isInterrupted) {
          item.reject(new Error("TTS interrupted"))
          continue
        }
        
        try {
          // Process the TTS request
          await this.processSingleTTSRequest(item)
        } catch (error) {
          console.log(`❌ [SMALLEST-TTS-QUEUE] Error processing item: ${error.message}`)
          item.reject(error)
        }
      }
    } finally {
      this.isProcessing = false
    }
  }
  
  async processSingleTTSRequest(item) {
    const timer = createTimer("SMALLEST_TTS_SYNTHESIS")
    
    try {
      console.log(`🎤 [TTS-PROCESS-START] Processing: "${item.text.substring(0, 30)}..."`)
      timer.checkpoint("TTS_PROCESS_START")
      
      // Ensure WebSocket connection is healthy
      if (!this.smallestReady || !this.smallestWs || this.smallestWs.readyState !== WebSocket.OPEN) {
        console.log("🔄 [SMALLEST-TTS] Connecting WebSocket...")
        timer.checkpoint("TTS_WEBSOCKET_DISCONNECTED")
        
        if (this.smallestWs) {
          this.smallestWs.close()
          this.smallestWs = null
        }
        await this.connectToSmallest()
        timer.checkpoint("TTS_WEBSOCKET_CONNECTED")
      } else {
        // Test connection health
        console.log("✅ [SMALLEST-TTS] WebSocket connection is healthy")
        timer.checkpoint("TTS_WEBSOCKET_READY")
      }

      // Don't clear pending requests unnecessarily - let them complete naturally
      // Only clear if there are too many pending requests (more than 5)
      if (this.pendingRequests.size > 5) {
        console.log("🧹 [SMALLEST-TTS] Clearing excess pending requests")
        this.pendingRequests.clear()
        timer.checkpoint("TTS_REQUESTS_CLEARED")
      } else {
        timer.checkpoint("TTS_REQUESTS_PRESERVED")
      }
      
      // Small delay to ensure connection is stable
      await new Promise(resolve => setTimeout(resolve, 50))
      timer.checkpoint("TTS_SETUP_DELAY_COMPLETED")

      const requestId = ++this.requestId
      
      // Get voice ID from agent config or use default
      let voiceId = "ryan" // Default fallback
      if (this.callLogger && this.callLogger.agentConfig) {
        const agentConfig = this.callLogger.agentConfig
        if (agentConfig.voiceId) {
          voiceId = agentConfig.voiceId
          console.log(`🎤 [TTS-VOICE] Using voice ID from DB: ${voiceId}`)
        } else if (agentConfig.voiceSelection) {
          // Map voice selection to voice ID if voiceId is not set
          const voiceMapping = {
            "male-professional": "ryan",
            "female-professional": "sarah", 
            "male-friendly": "ryan",
            "female-friendly": "sarah",
            "neutral": "ryan",
            "meera": "meera",
            "pavithra": "pavithra",
            "maitreyi": "maitreyi",
            "arvind": "arvind",
            "amol": "amol",
            "amartya": "amartya",
            "diya": "diya",
            "neel": "neel",
            "misha": "misha",
            "vian": "vian",
            "arjun": "arjun",
            "maya": "maya",
            "kumaran": "kumaran",
            "monika": "monika",
            "aahir": "aahir",
            "kanika": "kanika"
          }
          voiceId = voiceMapping[agentConfig.voiceSelection] || "ryan"
          console.log(`🎤 [TTS-VOICE] Mapped voice selection '${agentConfig.voiceSelection}' to voice ID: ${voiceId}`)
        }
      }
      
      const request = {
        voice_id: voiceId,
        text: item.text,
        max_buffer_flush_ms: 0,
        continue: false,
        flush: false,
        language: this.language === "hi" ? "hi" : "en",
        sample_rate: 8000,
        speed: 1,
        consistency: 0.5,
        enhancement: 1,
        similarity: 0
      }

      timer.checkpoint("TTS_REQUEST_CONFIGURED")
      console.log(`🕒 [SMALLEST-TTS-SYNTHESIS] Requesting TTS for: "${item.text.substring(0, 50)}..."`)

      // Send request and wait for response
      const audioPromise = new Promise((resolve, reject) => {
        this.pendingRequests.set(requestId, { resolve, reject, text: item.text, audioChunks: [], timing: timer })
        
        const requestWithId = { ...request, request_id: String(requestId) }
        
        timer.checkpoint("TTS_REQUEST_SENDING")
        console.log(`📤 [SMALLEST-TTS] Sending request ${requestId} for text: "${item.text.substring(0, 30)}..."`)
        
        // Check WebSocket state before sending
        if (this.smallestWs.readyState !== WebSocket.OPEN) {
          console.log(`❌ [SMALLEST-TTS] WebSocket not open, state: ${this.smallestWs.readyState}`)
          timer.checkpoint("TTS_SEND_ERROR_WEBSOCKET_CLOSED")
          reject(new Error("WebSocket not ready"))
          return
        }
        
        try {
          this.smallestWs.send(JSON.stringify(requestWithId))
          timer.checkpoint("TTS_REQUEST_SENT")
        } catch (sendError) {
          console.log(`❌ [SMALLEST-TTS] Error sending request: ${sendError.message}`)
          timer.checkpoint("TTS_SEND_ERROR_UNKNOWN")
          reject(sendError)
          return
        }
        
        // Timeout after 8 seconds for better reliability
        setTimeout(() => {
          if (this.pendingRequests.has(requestId)) {
            console.log(`⏰ [SMALLEST-TTS] Request ${requestId} timed out after 8 seconds`)
            timer.checkpoint("TTS_REQUEST_TIMEOUT")
            this.pendingRequests.delete(requestId)
            reject(new Error("TTS request timeout"))
          }
        }, 8000)
      })

      const audioBase64 = await audioPromise
      timer.checkpoint("TTS_API_RESPONSE_RECEIVED")
      
      if (audioBase64 === "COMPLETED") {
        // Audio was already streamed chunk by chunk, just resolve
        timer.checkpoint("TTS_STREAMING_COMPLETED")
        const report = timer.getFullReport()
        console.log(`✅ [SMALLEST-TTS-SYNTHESIS] Audio generation completed (already streamed) - Total: ${report.duration}ms`)
        item.resolve()
      } else if (audioBase64) {
        timer.checkpoint("TTS_AUDIO_RECEIVED")
        
        const audioBuffer = Buffer.from(audioBase64, "base64")
        this.totalAudioBytes += audioBuffer.length
        timer.checkpoint("TTS_AUDIO_BUFFER_CREATED")
        
        // Stream audio to SIP (for non-streaming responses)
        await this.streamAudioOptimizedForSIP(audioBase64)
        timer.checkpoint("TTS_AUDIO_STREAMED_TO_SIP")
        
        const report = timer.getFullReport()
        console.log(`✅ [SMALLEST-TTS-SYNTHESIS] Audio generated and streamed - Total: ${report.duration}ms`)
        
        // Log detailed timing report
        report.checkpoints.forEach((cp, index) => {
          console.log(`   🎤 TTS Step ${index + 1} - ${cp.name}: +${cp.sinceLast}ms`)
        })
        
        item.resolve()
      } else {
        timer.checkpoint("TTS_NO_AUDIO_RECEIVED")
        const report = timer.getFullReport()
        console.log(`❌ [SMALLEST-TTS-SYNTHESIS] No audio received - Total: ${report.duration}ms`)
        item.reject(new Error("No audio received"))
      }

    } catch (error) {
      timer.checkpoint("TTS_EXCEPTION_CUSTOMCAUGHT")
      const report = timer.getFullReport()
      console.log(`❌ [SMALLEST-TTS-SYNTHESIS] Error: ${error.message} - Total: ${report.duration}ms`)
      item.reject(error)
    }
  }

  async streamAudioOptimizedForSIP(audioBase64) {
    if (this.isInterrupted) return

    // Add to SIP audio queue instead of streaming immediately
    const audioBuffer = Buffer.from(audioBase64, "base64")
    const estimatedMs = Math.floor(audioBuffer.length / ((8000 * 2) / 1000))
    
    console.log(`🎧 [SIP-AUDIO-QUEUE] Adding ${audioBuffer.length} bytes (${estimatedMs}ms) to queue`)
    
    // Ensure proper gap between audio items
    const currentTime = Date.now()
    const timeSinceLastAudio = currentTime - this.lastAudioTime
    
    if (timeSinceLastAudio < this.audioGapThreshold) {
      const requiredDelay = this.audioGapThreshold - timeSinceLastAudio
      console.log(`⏱️ [SIP-AUDIO-QUEUE] Ensuring ${requiredDelay}ms gap before next audio`)
      await new Promise(resolve => setTimeout(resolve, requiredDelay))
    }
    
    // Add to queue with proper timing
    this.audioQueue.push({
      buffer: audioBuffer,
      timestamp: Date.now(),
      estimatedDuration: estimatedMs
    })
    
    // Start processing queue if not already processing
    if (!this.isProcessingSIPQueue) {
      this.processSIPAudioQueue()
    }
  }

  async processSIPAudioQueue() {
    if (this.isProcessingSIPQueue || this.audioQueue.length === 0) return
    
    this.isProcessingSIPQueue = true
    console.log(`🔄 [SIP-AUDIO-QUEUE] Processing ${this.audioQueue.length} audio items in queue`)
    
    try {
      let itemIndex = 0
      while (this.audioQueue.length > 0 && !this.isInterrupted) {
        const audioItem = this.audioQueue.shift()
        
        if (this.isInterrupted) {
          console.log(`🛑 [SIP-AUDIO-QUEUE] Interrupted, stopping queue processing`)
          break
        }
        
        console.log(`🎧 [SIP-AUDIO-QUEUE] Processing item ${itemIndex + 1}/${this.audioQueue.length + 1} (${audioItem.estimatedDuration}ms)`)
        
        // Add a small delay before starting each audio item to ensure clean separation
        if (itemIndex > 0) {
          console.log(`⏱️ [SIP-AUDIO-QUEUE] Waiting 100ms before next audio item...`)
          await new Promise(resolve => setTimeout(resolve, 100))
        }
        
        await this.streamSingleAudioItem(audioItem)
        
        // Enhanced delay between audio items to prevent audio disturbance
        if (this.audioQueue.length > 0) {
          const delayMs = Math.max(150, audioItem.estimatedDuration * 0.1) // 10% of audio duration or 150ms minimum
          console.log(`⏱️ [SIP-AUDIO-QUEUE] Waiting ${delayMs}ms before next audio item...`)
          await new Promise(resolve => setTimeout(resolve, delayMs))
        }
        
        itemIndex++
      }
    } finally {
      this.isProcessingSIPQueue = false
      console.log(`✅ [SIP-AUDIO-QUEUE] Queue processing completed`)
    }
  }

  async streamSingleAudioItem(audioItem) {
    if (this.isInterrupted) return
    
    const { buffer: audioBuffer, estimatedDuration } = audioItem
    const streamingSession = { interrupt: false }
    this.currentAudioStreaming = streamingSession

    const SAMPLE_RATE = 8000
    const BYTES_PER_SAMPLE = 2
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000
    
    // Use larger, more stable chunks to reduce disturbance
    // 40ms chunks at 8kHz, 16-bit mono → 40ms * 16 bytes/ms = 640 bytes
    const STABLE_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS)
    
    // Ensure chunk size is even for proper audio alignment
    const OPTIMAL_CHUNK_SIZE = STABLE_CHUNK_SIZE - (STABLE_CHUNK_SIZE % 2)

    let position = 0
    let chunkIndex = 0
    let successfulChunks = 0
    let lastChunkTime = Date.now()

    console.log(`🎧 [SIP-AUDIO-ITEM] Streaming ${audioBuffer.length} bytes (${estimatedDuration}ms) in ${Math.ceil(audioBuffer.length / OPTIMAL_CHUNK_SIZE)} stable chunks`)

    while (position < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
      const remaining = audioBuffer.length - position
      const chunkSize = Math.min(OPTIMAL_CHUNK_SIZE, remaining)
      const chunk = audioBuffer.slice(position, position + chunkSize)

      // Ensure chunk is properly aligned and padded if needed
      const alignedChunk = this.alignAudioChunk(chunk, OPTIMAL_CHUNK_SIZE)

      const mediaMessage = {
        event: "media",
        streamSid: this.streamSid,
        media: {
          payload: alignedChunk.toString("base64"),
        },
      }

      if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted) {
        try {
          this.ws.send(JSON.stringify(mediaMessage))
          successfulChunks++
          
          // Log progress every 10 chunks to avoid spam
          if (chunkIndex % 10 === 0) {
            console.log(`🎧 [SIP-AUDIO-ITEM] Progress: ${chunkIndex} chunks sent`)
          }
        } catch (error) {
          console.log(`❌ [SIP-AUDIO-ITEM] Error sending chunk ${chunkIndex}: ${error.message}`)
          break
        }
      } else {
        console.log(`❌ [SIP-AUDIO-ITEM] WebSocket not ready, stopping stream`)
        break
      }

      // Enhanced timing control to prevent audio disturbance
      if (position + chunkSize < audioBuffer.length && !this.isInterrupted) {
        const chunkDurationMs = Math.floor(alignedChunk.length / BYTES_PER_MS)
        const currentTime = Date.now()
        const timeSinceLastChunk = currentTime - lastChunkTime
        
        // Calculate precise delay to maintain consistent timing
        const targetDelay = Math.max(chunkDurationMs - 5, 15) // Minimum 15ms delay
        const actualDelay = Math.max(targetDelay - timeSinceLastChunk, 5) // Minimum 5ms
        
        if (actualDelay > 0) {
          await new Promise((resolve) => setTimeout(resolve, actualDelay))
        }
        
        lastChunkTime = Date.now()
      }

      position += chunkSize
      chunkIndex++
    }

    this.currentAudioStreaming = null
    this.lastAudioTime = Date.now() // Update last audio time
    
    if (successfulChunks > 0) {
      console.log(`✅ [SIP-AUDIO-ITEM] Completed ${successfulChunks} chunks (${Math.round((Date.now() - lastChunkTime) / 1000)}s total)`)
    } else {
      console.log(`⚠️ [SIP-AUDIO-ITEM] No chunks sent`)
    }
  }

  // Align audio chunk to prevent audio disturbance
  alignAudioChunk(chunk, targetSize) {
    if (chunk.length === targetSize) {
      return chunk
    }
    
    if (chunk.length < targetSize) {
      // Pad with silence (zeros) to maintain consistent chunk size
      const padding = Buffer.alloc(targetSize - chunk.length, 0)
      return Buffer.concat([chunk, padding])
    }
    
    // If chunk is larger, truncate (shouldn't happen with our logic)
    return chunk.slice(0, targetSize)
  }

  getStats() {
    return {
      totalAudioBytes: this.totalAudioBytes,
      audioQueueLength: this.audioQueue.length,
      isProcessingSIPQueue: this.isProcessingSIPQueue,
      pendingQueueLength: this.pendingQueue.length,
      isProcessing: this.isProcessing
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
          sttTimer.checkpoint("STT_RESULTS_START")
        }

        const transcript = data.channel?.alternatives?.[0]?.transcript
        const is_final = data.is_final

        if (transcript?.trim()) {
          // Timing for interruption handling
          const interruptStartTime = Date.now()
          
          if (currentTTS && isProcessing) {
            console.log("🛑 [USER-UTTERANCE] Interrupting current TTS for new user input...")
            sttTimer.checkpoint("STT_DETECTED_UNTERRUPTION_NEEDED")
            
            // Handle both regular TTS and streaming processor interruptions
            if (currentTTS.interrupt) {
              currentTTS.interrupt()
            } else if (currentTTS.ttsProcessor && currentTTS.ttsProcessor.interrupt) {
              currentTTS.ttsProcessor.interrupt()
            }
            
            isProcessing = false
            processingRequestId++
            // Wait a bit for the interrupt to take effect
            await new Promise(resolve => setTimeout(resolve, 100))
            const interruptDuration = Date.now() - interruptStartTime
            sttTimer.checkpoint(`STT_INTERRUPTION_COMPLETED_${interruptDuration}ms`)
          } else {
            sttTimer.checkpoint("STT_NO_INTERRUPTION_NEEDED")
          }

          if (is_final) {
            sttTimer.checkpoint("STT_FINAL_TRANSCRIPT_RECEIVED")
            const sttDuration = sttTimer.end()
            console.log(`🕒 [STT-TRANSCRIPTION] ${sttDuration}ms - Text: "${transcript.trim()}"`)

            const utteranceStartTime = Date.now()
            sttTimer = null

            userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim()

            if (callLogger && transcript.trim()) {
              callLogger.logUserTranscript(transcript.trim())
            }

            await processUserUtterance(userUtteranceBuffer)
            const utteranceProcessDuration = Date.now() - utteranceStartTime
            console.log(`⚡ [STT-TO-PROCESSING] Total time from final transcript to utterance processing started: ${utteranceProcessDuration}ms`)
            userUtteranceBuffer = ""
          } else {
            sttTimer.checkpoint("STT_INTERIM_RESULT_RECEIVED")
          }
        }
      } else if (data.type === "UtteranceEnd") {
        if (sttTimer) {
          const endTime = Date.now()
          sttTimer.checkpoint("STT_UTTERANCE_END_DETECTED")
          const sttDuration = sttTimer.end()
          console.log(`🕒 [STT-TRANSCRIPTION] ${sttDuration}ms - Text: "${userUtteranceBuffer.trim()}"`)
          console.log(`🕒 [STT-TIMING] STT session completed in ${sttDuration}ms for utterance: "${userUtteranceBuffer.trim().substring(0, 50)}..."`)
          sttTimer = null
        }

        if (userUtteranceBuffer.trim()) {
          const utteranceStartTime = Date.now()
          if (callLogger && userUtteranceBuffer.trim()) {
            callLogger.logUserTranscript(userUtteranceBuffer.trim())
          }

          await processUserUtterance(userUtteranceBuffer)
          const utteranceProcessDuration = Date.now() - utteranceStartTime
          console.log(`⚡ [STT-TO-PROCESSING] Total time from utterance end to processing started: ${utteranceProcessDuration}ms`)
          userUtteranceBuffer = ""
        }
      }
    }

    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText) return

      // Start overall timing for voice-to-voice latency
      const voiceTiming = createTimer("VOICE_TO_VOICE_LATENCY")
      
      console.log("🗣️ [USER-UTTERANCE] ========== USER SPEECH ==========")
      console.log("🗣️ [USER-UTTERANCE] Text:", text.trim())
      console.log("🗣️ [USER-UTTERANCE] Current Language:", currentLanguage)
      voiceTiming.checkpoint("USER_TEXT_RECEIVED")

      if (currentTTS) {
        console.log("🛑 [USER-UTTERANCE] Interrupting current TTS...")
        currentTTS.interrupt()
        voiceTiming.checkpoint("PREVIOUS_TTS_INTERRUPTED")
      }

      isProcessing = true
      lastProcessedText = text
      const currentRequestId = ++processingRequestId
      voiceTiming.checkpoint("PROCESSING_PREPARED")

      try {
        console.log("🔍 [USER-UTTERANCE] Running AI detections + streaming...")

        // Kick off streaming LLM processing with sentence-based TTS
        let aiResponse = null
        const streamingProcessor = new StreamingLLMProcessor(currentLanguage, ws, streamSid, callLogger, agentConfig)
        currentTTS = streamingProcessor.ttsProcessor
        voiceTiming.checkpoint("STREAMING_PROCESSOR_CREATED")

        // Start LLM processing timing
        const startLlmProcess = Date.now()
        
        aiResponse = await streamingProcessor.processStreamingResponse(
          text,
          conversationHistory,
          userName
        )
        
        const llmDuration = Date.now() - startLlmProcess
        console.log(`🧠 [LLM-TIMING] Streaming generation completed in ${llmDuration}ms`)
        voiceTiming.checkpoint("LLM_RESPONSE_GENERATED")

        // The streaming processor handles TTS automatically as sentences are completed
        if (processingRequestId === currentRequestId && aiResponse) {
          console.log("✅ [STREAMING-LLM] Response processing completed with sentence-based TTS")
          voiceTiming.checkpoint("STREAMING_TTS_COMPLETED")
        }

        // Follow-up now handled inside processWithOpenAIStream

        // Save detections (lead status, WA request) in parallel (non-blocking)
        const detectionStartTime = Date.now()
        ;(async () => {
          try {
            const [leadStatus, whatsappRequest] = await Promise.all([
              detectLeadStatusWithOpenAI(text, conversationHistory, currentLanguage),
              detectWhatsAppRequest(text, conversationHistory, currentLanguage),
            ])
            const detectionDuration = Date.now() - detectionStartTime
            console.log(`🔍 [DETECTION-TIMING] Status detection completed in ${detectionDuration}ms`)
            voiceTiming.checkpoint("STATUS_DETECTION_COMPLETED")
            
            if (callLogger) {
              callLogger.updateLeadStatus(leadStatus)
              if (whatsappRequest === "WHATSAPP_REQUEST") callLogger.markWhatsAppRequested()
            }
          } catch (_) {
            voiceTiming.checkpoint("STATUS_DETECTION_ERROR")
          }
        })()

        if (processingRequestId === currentRequestId && aiResponse) {
          // Log full AI response once
          try { if (callLogger) { callLogger.logAIResponse(aiResponse) } } catch (_) {}

          conversationHistory.push(
            { role: "user", content: text },
            { role: "assistant", content: aiResponse }
          )
          if (conversationHistory.length > 10) conversationHistory = conversationHistory.slice(-10)
          
          // Generate final voice-to-voice latency report
          const report = voiceTiming.getFullReport()
          console.log(`✅ [USER-UTTERANCE] Processing completed - Total Voice-to-Voice Latency: ${report.duration}ms`)
          console.log(`\n📊 [VOICE-LATENCY-SUMMARY]`)
          console.log(`   📝 Text Processing: ~50ms`)
          console.log(`   🧠 LLM Generation: ${llmDuration}ms`)
          console.log(`   🎤 TTS Synthesis: ~200ms (estimated)`)
          console.log(`   📊 Overall Latency: ${report.duration}ms`)
          console.log(`📊 [VOICE-LATENCY-SUMMARY]`)
        } else {
          console.log("⏭️ [USER-UTTERANCE] Processing skipped (newer request in progress)")
          voiceTiming.checkpoint("PROCESSING_SKIPPED")
          voiceTiming.getFullReport()
        }
      } catch (error) {
        console.log("❌ [USER-UTTERANCE] Error processing utterance:", error.message)
        voiceTiming.checkpoint("PROCESSING_ERROR")
        voiceTiming.getFullReport()
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
            callLogger.agentConfig = agentConfig; // Store agent config for voice selection

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
            const tts = new SimplifiedSmallestTTSProcessor(currentLanguage, ws, streamSid, callLogger, agentConfig)
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
            
            // Clean up TTS processor
            if (currentTTS) {
              console.log("🛑 [SIP-STOP] Interrupting TTS processor...")
              currentTTS.interrupt()
              currentTTS = null
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
      
      // Clean up TTS processor
      if (currentTTS) {
        console.log("🔌 [SIP-CLOSE] Interrupting TTS processor...")
        currentTTS.interrupt()
        currentTTS = null
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