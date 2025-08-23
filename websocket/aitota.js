const WebSocket = require("ws")
require("dotenv").config()
const mongoose = require("mongoose")
const Agent = require("../models/Agent")
const CallLog = require("../models/CallLog")
const fs = require("fs")
const path = require("path")

const convertBufferToBase64 = (buffer) => {
  return buffer.toString("base64")
}

const convertBase64ToBuffer = (base64String) => {
  return Buffer.from(base64String, "base64")
}

const validateBase64 = (base64String) => {
  try {
    const buffer = Buffer.from(base64String, "base64")
    return buffer.length > 0
  } catch (error) {
    return false
  }
}

const saveMp3ToFile = (buffer, filename = null) => {
  try {
    if (!filename) {
      const timestamp = new Date().toISOString().replace(/[:.]/g, "-")
      filename = `debug_audio_${timestamp}.mp3`
    }

    const filepath = path.join(__dirname, "..", "temp", filename)

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

let franc
try {
  franc = require("franc").franc
  if (!franc) {
    franc = require("franc")
  }
} catch (error) {
  franc = () => "und"
}

const API_KEYS = {
  deepgram: process.env.DEEPGRAM_API_KEY,
  sarvam: process.env.SARVAM_API_KEY,
  openai: process.env.OPENAI_API_KEY,
}

// TTS Interruption Configuration
const TTS_CONFIG = {
  ALLOW_COMPLETION_FOR_SHORT_SPEECH: true,        // Allow TTS to complete for speech < 20 chars
  SHORT_SPEECH_THRESHOLD: 20,                     // Character threshold for "short" speech
  COMPLETION_WAIT_TIME: 1000,                     // Wait time when TTS is near completion (ms)
  INTERIM_SPEECH_WAIT_TIME: 500,                  // Wait time for interim speech detection (ms)
  MIN_AUDIO_BYTES_FOR_COMPLETION: 50000,          // Minimum audio bytes to consider TTS near completion
  ENABLE_SMART_INTERRUPTION: true,                // Enable/disable smart interruption logic
}

if (!API_KEYS.deepgram || !API_KEYS.sarvam || !API_KEYS.openai) {
  console.error("❌ Missing required API keys in environment variables")
  process.exit(1)
}

const fetch = globalThis.fetch || require("node-fetch")

const createTimer = (label) => {
  const start = Date.now()
  return {
    start,
    end: () => Date.now() - start,
    checkpoint: (checkpointName) => Date.now() - start,
  }
}

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

const decodeCzdata = (czdataBase64) => {
  try {
    if (!czdataBase64) return null
    const decoded = Buffer.from(czdataBase64, "base64").toString("utf-8")
    return JSON.parse(decoded)
  } catch (e) {
    return null
  }
}

const detectLanguageWithFranc = (text, fallbackLanguage = "en") => {
  try {
    const cleanText = text.trim()

    if (cleanText.length < 10) {
      const englishPatterns =
        /^(what|how|why|when|where|who|can|do|does|did|is|are|am|was|were|have|has|had|will|would|could|should|may|might|hello|hi|hey|yes|no|ok|okay|thank|thanks|please|sorry|our|your|my|name|help)\b/i
      const hindiPatterns = /[\u0900-\u097F]/
      const englishWords = /^[a-zA-Z\s?!.,'"]+$/

      if (hindiPatterns.test(cleanText)) {
        return "hi"
      } else if (englishPatterns.test(cleanText) || englishWords.test(cleanText)) {
        return "en"
      } else {
        return fallbackLanguage
      }
    }

    if (typeof franc !== "function") {
      return fallbackLanguage
    }

    const detected = franc(cleanText)

    if (detected === "und" || !detected) {
      const hindiPatterns = /[\u0900-\u097F]/
      if (hindiPatterns.test(cleanText)) {
        return "hi"
      }

      const latinScript = /^[a-zA-Z\s?!.,'"0-9\-$$$$]+$/
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

      const latinScript = /^[a-zA-Z\s?!.,'"0-9\-$$$$]+$/
      if (latinScript.test(cleanText)) {
        return "en"
      }

      return fallbackLanguage
    }
  } catch (error) {
    return fallbackLanguage
  }
}

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

const detectLanguageHybrid = async (text, useOpenAIFallback = false) => {
  return detectLanguageWithFranc(text)
}

const ALLOWED_LEAD_STATUSES = new Set([
  "vvi",
  "maybe",
  "enrolled",
  "junk_lead",
  "not_required",
  "enrolled_other",
  "decline",
  "not_eligible",
  "wrong_number",
  "hot_followup",
  "cold_followup",
  "schedule",
  "not_connected",
])

const normalizeLeadStatus = (value, fallback = "maybe") => {
  if (!value || typeof value !== "string") return fallback
  const normalized = value.trim().toLowerCase()
  return ALLOWED_LEAD_STATUSES.has(normalized) ? normalized : fallback
}

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
    this.batchSize = 5
    this.batchTimeout = 3000
    this.customParams = {}
    this.callerId = null
    this.streamSid = null
    this.callSid = null
    this.accountSid = null
    this.ws = null
  }

  async createInitialCallLog(agentId = null, leadStatusInput = "not_connected") {
    const timer = createTimer("INITIAL_CALL_LOG_CREATE")
    try {
      const initialCallLogData = {
        clientId: this.clientId,
        agentId: agentId,
        mobile: this.mobile,
        time: this.callStartTime,
        transcript: "",
        duration: 0,
        leadStatus: normalizeLeadStatus(leadStatusInput, "not_connected"),
        streamSid: this.streamSid,
        callSid: this.callSid,
        metadata: {
          userTranscriptCount: 0,
          aiResponseCount: 0,
          languages: [],
          callDirection: this.callDirection,
          isActive: true,
          lastUpdated: new Date(),
          sttProvider: "deepgram",
          ttsProvider: "sarvam",
          llmProvider: "openai",
          customParams: this.customParams || {},
          callerId: this.callerId || undefined,
        },
      }

      const callLog = new CallLog(initialCallLogData)
      const savedLog = await callLog.save()
      this.callLogId = savedLog._id
      this.isCallLogCreated = true

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

  async disconnectCall(reason = "user_disconnected") {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.log("⚠️ [CALL-DISCONNECT] WebSocket not available for disconnection")
      return false
    }

    try {
      console.log(`🛑 [CALL-DISCONNECT] Disconnecting call: ${reason}`)

      const stopMessage = {
        event: "stop",
        sequenceNumber: stopEventSequence++,
        stop: {
          accountSid: this.accountSid || "ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
          callSid: this.callSid || "CAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
        },
        streamSid: this.streamSid,
      }

      console.log(`🛑 [CALL-DISCONNECT] Sending stop event:`, JSON.stringify(stopMessage, null, 2))

      const disconnectionPromises = []

      if (this.ws.readyState === WebSocket.OPEN) {
        try {
          this.ws.send(JSON.stringify(stopMessage))
          console.log(`🛑 [CALL-DISCONNECT] Stop event sent successfully`)
        } catch (error) {
          console.log(`⚠️ [CALL-DISCONNECT] Error sending stop event: ${error.message}`)
        }
      }

      const fallbackClosePromise = new Promise((resolve) => {
        setTimeout(() => {
          if (this.ws.readyState === WebSocket.OPEN) {
            const closeMessage = {
              event: "close",
              streamSid: this.streamSid,
              reason: reason,
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
        }, 500)
      })
      disconnectionPromises.push(fallbackClosePromise)

      const forceClosePromise = new Promise((resolve) => {
        setTimeout(() => {
          if (this.ws.readyState === WebSocket.OPEN) {
            console.log(`🛑 [CALL-DISCONNECT] Force closing WebSocket connection`)
            this.ws.close(1000, `Call terminated: ${reason}`)
          }
          resolve()
        }, 1500)
      })
      disconnectionPromises.push(forceClosePromise)

      const callLogUpdatePromise = CallLog.findByIdAndUpdate(this.callLogId, {
        "metadata.isActive": false,
        "metadata.callEndTime": new Date(),
        "metadata.lastUpdated": new Date(),
        "metadata.terminationReason": reason,
        "metadata.terminatedAt": new Date(),
        "metadata.terminationMethod": "manual_api",
      }).catch((err) => console.log(`⚠️ [CALL-DISCONNECT] Call log update error: ${err.message}`))
      disconnectionPromises.push(callLogUpdatePromise)

      await Promise.allSettled(disconnectionPromises)

      console.log("✅ [CALL-DISCONNECT] Call disconnected successfully")
      return true
    } catch (error) {
      console.log(`❌ [CALL-DISCONNECT] Error disconnecting call: ${error.message}`)
      return false
    }
  }

  getCallInfo() {
    return {
      streamSid: this.streamSid,
      callSid: this.callSid,
      accountSid: this.accountSid,
      callLogId: this.callLogId,
      clientId: this.clientId,
      mobile: this.mobile,
      isActive: this.isCallLogCreated && this.callLogId,
    }
  }

  async gracefulCallEnd(goodbyeMessage = "Thank you for your time. Have a great day!", language = "en") {
    try {
      console.log("👋 [GRACEFUL-END] Ending call gracefully with goodbye message")

      this.logAIResponse(goodbyeMessage, language)

      const callLogUpdate = CallLog.findByIdAndUpdate(this.callLogId, {
        "metadata.lastUpdated": new Date(),
      }).catch((err) => console.log(`⚠️ [GRACEFUL-END] Call log update error: ${err.message}`))

      const ttsPromise = this.synthesizeGoodbyeMessage(goodbyeMessage, language)

      const disconnectPromise = this.disconnectCall("graceful_termination")

      await Promise.allSettled([callLogUpdate, ttsPromise, disconnectPromise])

      console.log("✅ [GRACEFUL-END] All operations completed in parallel")
      return true
    } catch (error) {
      console.log(`❌ [GRACEFUL-END] Error in graceful call end: ${error.message}`)
      return false
    }
  }

  async synthesizeGoodbyeMessage(message, language) {
    try {
      console.log("🎤 [GRACEFUL-END] Starting goodbye message TTS...")

      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const tts = new SimplifiedSarvamTTSProcessor(language, this.ws, this.streamSid, this.callLogger)

        tts.synthesizeAndStream(message).catch((err) => console.log(`⚠️ [GRACEFUL-END] TTS error: ${err.message}`))

        console.log("✅ [GRACEFUL-END] Goodbye message TTS started")
      } else {
        console.log("⚠️ [GRACEFUL-END] WebSocket not available for TTS")
      }
    } catch (error) {
      console.log(`❌ [GRACEFUL-END] TTS synthesis error: ${error.message}`)
    }
  }

  async fastTerminateCall(reason = "fast_termination") {
    try {
      console.log(`⚡ [FAST-TERMINATE] Fast terminating call: ${reason}`)

      const terminationPromises = []

      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        const stopMessage = {
          event: "stop",
          sequenceNumber: stopEventSequence++,
          stop: {
            accountSid: this.accountSid || "ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            callSid: this.callSid || "CAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
          },
          streamSid: this.streamSid,
        }

        try {
          this.ws.send(JSON.stringify(stopMessage))
          console.log(`⚡ [FAST-TERMINATE] Stop event sent immediately`)
        } catch (error) {
          console.log(`⚠️ [FAST-TERMINATE] Error sending stop event: ${error.message}`)
        }
      }

      if (this.callLogId) {
        const callLogUpdate = CallLog.findByIdAndUpdate(this.callLogId, {
          "metadata.isActive": false,
          "metadata.callEndTime": new Date(),
          "metadata.lastUpdated": new Date(),
          "metadata.terminationReason": reason,
          "metadata.terminatedAt": new Date(),
          "metadata.terminationMethod": "fast_termination",
        }).catch((err) => console.log(`⚠️ [FAST-TERMINATE] Call log update error: ${err.message}`))

        terminationPromises.push(callLogUpdate)
      }

      const forceClosePromise = new Promise((resolve) => {
        setTimeout(() => {
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            console.log(`⚡ [FAST-TERMINATE] Force closing WebSocket connection`)
            this.ws.close(1000, `Call terminated: ${reason}`)
          }
          resolve()
        }, 300)
      })
      terminationPromises.push(forceClosePromise)

      await Promise.allSettled(terminationPromises)

      console.log("✅ [FAST-TERMINATE] Call terminated with minimal latency")
      return true
    } catch (error) {
      console.log(`❌ [FAST-TERMINATE] Error in fast termination: ${error.message}`)
      return false
    }
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
    this.pendingTranscripts.push(entry)

    this.scheduleBatchSave()
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
    this.pendingTranscripts.push(entry)

    this.scheduleBatchSave()
  }

  scheduleBatchSave() {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer)
    }

    if (this.pendingTranscripts.length >= this.batchSize) {
      this.savePendingTranscripts()
      return
    }

    this.batchTimer = setTimeout(() => {
      this.savePendingTranscripts()
    }, this.batchTimeout)
  }

  async savePendingTranscripts() {
    if (!this.isCallLogCreated || this.pendingTranscripts.length === 0) {
      return
    }

    const transcriptsToSave = [...this.pendingTranscripts]
    this.pendingTranscripts = []

    if (this.batchTimer) {
      clearTimeout(this.batchTimer)
      this.batchTimer = null
    }

    setImmediate(async () => {
      const timer = createTimer("LIVE_TRANSCRIPT_BATCH_SAVE")
      try {
        const currentTranscript = this.generateFullTranscript()
        const currentDuration = Math.round((new Date() - this.callStartTime) / 1000)

        const updateData = {
          transcript: currentTranscript,
          duration: currentDuration,
          "metadata.userTranscriptCount": this.transcripts.length,
          "metadata.aiResponseCount": this.responses.length,
          "metadata.languages": [...new Set([...this.transcripts, ...this.responses].map((e) => e.language))],
          "metadata.lastUpdated": new Date(),
        }

        await CallLog.findByIdAndUpdate(this.callLogId, updateData, {
          new: false,
          runValidators: false,
        })

        console.log(`🕒 [LIVE-TRANSCRIPT-SAVE] ${timer.end()}ms - Saved ${transcriptsToSave.length} entries`)
      } catch (error) {
        console.log(`❌ [LIVE-TRANSCRIPT-SAVE] ${timer.end()}ms - Error: ${error.message}`)
        this.pendingTranscripts.unshift(...transcriptsToSave)
      }
    })
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

  async saveToDatabase(leadStatusInput = "maybe", agentConfig = null) {
    const timer = createTimer("FINAL_CALL_LOG_SAVE")
    try {
      const callEndTime = new Date()
      this.totalDuration = Math.round((callEndTime - this.callStartTime) / 1000)

      if (this.pendingTranscripts.length > 0) {
        await this.savePendingTranscripts()
        await new Promise((resolve) => setTimeout(resolve, 100))
      }

      const leadStatus = normalizeLeadStatus(leadStatusInput, "maybe")

      if (this.isCallLogCreated && this.callLogId) {
        const finalUpdateData = {
          transcript: this.generateFullTranscript(),
          duration: this.totalDuration,
          leadStatus: leadStatus,
          streamSid: this.streamSid,
          callSid: this.callSid,
          "metadata.userTranscriptCount": this.transcripts.length,
          "metadata.aiResponseCount": this.responses.length,
          "metadata.languages": [...new Set([...this.transcripts, ...this.responses].map((e) => e.language))],
          "metadata.callEndTime": callEndTime,
          "metadata.isActive": false,
          "metadata.lastUpdated": callEndTime,
          "metadata.customParams": this.customParams || {},
          "metadata.callerId": this.callerId || undefined,
        }

        const updatedLog = await CallLog.findByIdAndUpdate(this.callLogId, finalUpdateData, { new: true })

        console.log(`🕒 [FINAL-CALL-LOG-SAVE] ${timer.end()}ms - Updated: ${updatedLog._id}`)

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

        return updatedLog
      } else {
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
            languages: [...new Set([...this.transcripts, ...this.responses].map((e) => e.language))],
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

        return savedLog
      }
    } catch (error) {
      console.log(`❌ [FINAL-CALL-LOG-SAVE] ${timer.end()}ms - Error: ${error.message}`)
      throw error
    }
  }

  cleanup() {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer)
      this.batchTimer = null
    }

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
      languages: [...new Set([...this.transcripts, ...this.responses].map((e) => e.language))],
      startTime: this.callStartTime,
      callDirection: this.callDirection,
      callLogId: this.callLogId,
      pendingTranscripts: this.pendingTranscripts.length,
    }
  }
}

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
    const basePrompt = agentConfig.systemPrompt || "You are a helpful AI assistant."
    const firstMessage = (agentConfig.firstMessage || "").trim()
    const knowledgeBlock = firstMessage ? `FirstGreeting: "${firstMessage}"\n` : ""

    const policyBlock = [
      "Answer strictly using the information provided above.",
      "If the user asks for address, phone, timings, or other specifics, check the System Prompt or FirstGreeting.",
      "If the information is not present, reply briefly that you don't have that information.",
      "Always end your answer with a short, relevant follow-up question to keep the conversation going.",
      "Keep the entire reply under 100 tokens.",
    ].join(" ")

    const systemPrompt = `System Prompt:\n${basePrompt}\n\n${knowledgeBlock}${policyBlock}`

    const personalizationMessage =
      userName && userName.trim()
        ? {
            role: "system",
            content: `The user's name is ${userName.trim()}. Address them by name naturally when appropriate.`,
          }
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
        max_tokens: 80,
        temperature: 0.1,
      }),
    })

    if (!response.ok) {
      console.log(`❌ [LLM-PROCESSING] ${timer.end()}ms - Error: ${response.status}`)
      return null
    }

    const data = await response.json()
    let fullResponse = data.choices[0]?.message?.content?.trim()

    console.log(`🕒 [LLM-PROCESSING] ${timer.end()}ms - Response generated`)

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
        messages: [{ role: "system", content: disconnectionPrompt }],
        max_tokens: 10,
        temperature: 0.1,
      }),
    })

    if (!response.ok) {
      console.log(`❌ [DISCONNECTION-DETECTION] ${timer.end()}ms - Error: ${response.status}`)
      return "CONTINUE"
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
    return "CONTINUE"
  }
}

class SimplifiedSarvamTTSProcessor {
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
    this.audioQueue = []
    this.isProcessing = false
    this.useWebSocket = true
  }

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

  async testApiKey() {
    return true
  }

  async connectToSarvam() {
    try {
      const lang = (this.language || "en").toLowerCase()
      if (lang.startsWith("en")) {
        this.sarvamLanguage = "en-IN"
        this.voice = "manisha"
      } else if (lang.startsWith("hi")) {
        this.sarvamLanguage = "hi-IN"
        this.voice = "pavithra"
      }

      const wsUrl = "wss://api.sarvam.ai/text-to-speech/ws?model=bulbul:v2"
      console.log(`🎤 [SARVAM-WS] Connecting to ${wsUrl} with subprotocol`)
      this.sarvamWs = new WebSocket(wsUrl, [`api-subscription-key.${API_KEYS.sarvam}`])

      this.sarvamWs.onopen = () => {
        this.sarvamReady = true
        const configMessage = {
          type: "config",
          data: {
            target_language_code: this.sarvamLanguage,
            speaker: this.voice,
            pitch: 0.5,
            pace: 1.0,
            loudness: 1.0,
            enable_preprocessing: false,
            output_audio_codec: "linear16",
            output_audio_bitrate: "128k",
            speech_sample_rate: 8000,
            min_buffer_size: 50,
            max_chunk_length: 150,
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

      await new Promise((resolve) => setTimeout(resolve, 200))
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

  handleSarvamMessage(data) {
    if (data.type === "audio") {
      const audioBase64 = (data && data.data && data.data.audio) || data.audio
      if (audioBase64 && !this.isInterrupted) {
        this.streamLinear16AudioToSIP(audioBase64)
        const audioBuffer = Buffer.from(audioBase64, "base64")
        this.totalAudioBytes += audioBuffer.length
      }
    } else if (data.type === "error") {
      const errMsg = (data && data.data && data.data.message) || data.message || "unknown"
      console.log("❌ [SARVAM-WS] Sarvam error:", errMsg)
    } else if (data.type === "end") {
      console.log("✅ [SARVAM-WS] Audio generation completed")
      this.isProcessing = false
    }
  }

  processQueuedAudio() {
    if (this.audioQueue.length > 0 && this.sarvamReady) {
      const text = this.audioQueue.shift()
      this.sendTextToSarvam(text)
    }
  }

  sendTextToSarvam(text) {
    if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.OPEN && !this.isInterrupted) {
      const textMessage = {
        type: "text",
        data: { text },
      }
      this.sarvamWs.send(JSON.stringify(textMessage))
      console.log("📤 [SARVAM-WS] Text sent:", text.substring(0, 50) + "...")
    }
  }

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
        if (this.currentAudioStreaming) {
          this.currentAudioStreaming.interrupt = true
        }

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
      if (this.useWebSocket) {
        if (!this.sarvamWs || this.sarvamWs.readyState !== WebSocket.OPEN) {
          await this.connectToSarvam()

          let attempts = 0
          while (!this.sarvamReady && attempts < 5) {
            await new Promise((resolve) => setTimeout(resolve, 50))
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

          if (this.currentAudioStreaming) {
            this.currentAudioStreaming.interrupt = true
          }

          this.sendTextToSarvam(text)

          const flushMessage = { type: "flush" }
          this.sarvamWs.send(JSON.stringify(flushMessage))
          console.log("📤 [SARVAM-WS] Flush signal sent")

          // Wait for audio generation to complete with better completion detection
          let waitAttempts = 0
          let lastAudioBytes = 0
          let stableAudioCount = 0
          
          console.log(`🔄 [TTS-SYNTHESIS] Starting completion detection loop...`)
          
          while (this.isProcessing && waitAttempts < 100 && !this.isInterrupted) {
            await new Promise((resolve) => setTimeout(resolve, 50))
            waitAttempts++
            
            // Check if audio generation has stabilized (no new audio for 3 consecutive checks)
            const currentAudioBytes = this.totalAudioBytes
            if (currentAudioBytes === lastAudioBytes) {
              stableAudioCount++
              if (stableAudioCount >= 3) {
                console.log(`🔄 [TTS-SYNTHESIS] Audio generation appears stable after ${stableAudioCount} checks, marking as complete`)
                break
              }
            } else {
              stableAudioCount = 0
              lastAudioBytes = currentAudioBytes
              console.log(`🔄 [TTS-SYNTHESIS] Audio still being generated: ${currentAudioBytes} bytes (attempt ${waitAttempts})`)
            }
          }

          if (this.isInterrupted) {
            console.log("⚠️ [TTS-SYNTHESIS] Synthesis interrupted")
            return
          }

          // Wait a bit more to ensure all audio chunks are processed
          console.log(`🔄 [TTS-SYNTHESIS] Waiting additional 200ms for audio processing...`)
          await new Promise((resolve) => setTimeout(resolve, 200))
          
          console.log(`🕒 [TTS-SYNTHESIS] ${timer.end()}ms - WebSocket synthesis completed (${this.totalAudioBytes} bytes generated)`)
          return
        }
      }

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
      console.log(`🔄 [TRANSCODE] FFmpeg not available, using MP3 buffer directly: ${mp3Buffer.length} bytes`)
      console.log(`🔄 [TRANSCODE] Converting MP3 to base64 format for SIP transmission`)

      if (!mp3Buffer || mp3Buffer.length === 0) {
        console.warn(`⚠️ [TRANSCODE] Empty or invalid MP3 buffer received`)
        return mp3Buffer
      }

      const base64String = convertBufferToBase64(mp3Buffer)
      if (!validateBase64(base64String)) {
        console.warn(`⚠️ [TRANSCODE] Invalid base64 conversion, using original buffer`)
        return mp3Buffer
      }

      console.log(
        `✅ [TRANSCODE] MP3 buffer successfully converted to base64: ${mp3Buffer.length} bytes → ${base64String.length} base64 chars`,
      )

      return mp3Buffer
    } catch (error) {
      console.error(`❌ [TRANSCODE] Transcoding error: ${error.message}`)
      return mp3Buffer
    }
  }

  async streamLinear16AudioToSIP(audioBase64) {
    if (this.isInterrupted) return

    if (this.currentAudioStreaming && !this.currentAudioStreaming.interrupt) {
      console.log(`🛑 [AUDIO-STREAM] Force interrupting existing stream for faster response`)
      this.currentAudioStreaming.interrupt = true
      await new Promise((resolve) => setTimeout(resolve, 50))
    }

    const audioBuffer = Buffer.from(audioBase64, "base64")
    const streamingSession = { interrupt: false, streamId: Date.now() }
    this.currentAudioStreaming = streamingSession

    console.log(
      `🎵 [AUDIO-STREAM] Starting linear16 audio stream: ${audioBuffer.length} bytes (Stream ID: ${streamingSession.streamId})`,
    )

    const SAMPLE_RATE = 8000
    const BYTES_PER_SAMPLE = 2
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000
    const OPTIMAL_CHUNK_SIZE = Math.floor(20 * BYTES_PER_MS)

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
          console.log(
            `🎵 [AUDIO-STREAM] [${streamingSession.streamId}] Sent linear16 chunk ${chunkIndex + 1} (${chunk.length} bytes), ${Math.round((position / audioBuffer.length) * 100)}% complete`,
          )
        } catch (error) {
          console.log(
            `❌ [AUDIO-STREAM] [${streamingSession.streamId}] Error sending linear16 chunk ${chunkIndex + 1}: ${error.message}`,
          )
          break
        }
      } else {
        console.log(
          `⚠️ [AUDIO-STREAM] [${streamingSession.streamId}] WebSocket not ready or interrupted, stopping audio stream`,
        )
        break
      }

      if (position + chunkSize < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
        const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS)
        const delayMs = Math.max(chunkDurationMs - 5, 5)
        await new Promise((resolve) => setTimeout(resolve, delayMs))
      }

      position += chunkSize
      chunkIndex++
    }

    // Ensure all chunks are sent before marking as complete
    if (position < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
      console.log(`⚠️ [AUDIO-STREAM] [${streamingSession.streamId}] Audio stream incomplete, sending remaining chunks...`)
      
      // Send remaining chunks without delay
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
            console.log(
              `🎵 [AUDIO-STREAM] [${streamingSession.streamId}] Sent remaining chunk ${chunkIndex + 1} (${chunk.length} bytes), ${Math.round((position / audioBuffer.length) * 100)}% complete`,
            )
          } catch (error) {
            console.log(
              `❌ [AUDIO-STREAM] [${streamingSession.streamId}] Error sending remaining chunk ${chunkIndex + 1}: ${error.message}`,
            )
            break
          }
        } else {
          break
        }

        position += chunkSize
        chunkIndex++
      }
    }

    if (this.currentAudioStreaming === streamingSession) {
      console.log(
        `✅ [AUDIO-STREAM] [${streamingSession.streamId}] Linear16 stream completed: ${successfulChunks} chunks sent, ${Math.round((position / audioBuffer.length) * 100)}% of audio streamed`,
      )
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
      isAudioStreaming: !!this.currentAudioStreaming && !this.currentAudioStreaming.interrupt,
      audioStreamProgress: this.currentAudioStreaming ? this.getAudioStreamProgress() : 0,
      isNearCompletion: this.isNearCompletion(),
    }
  }

  getAudioStreamProgress() {
    if (!this.currentAudioStreaming) return 0
    // This is a rough estimate - in a real implementation you'd track actual progress
    return 0.5 // Placeholder - would need to track actual chunk progress
  }

  isNearCompletion() {
    // Check if TTS is likely near completion based on various factors
    if (!this.isProcessing) return true
    
    // If we've received substantial audio and processing has been going for a while
    if (this.totalAudioBytes > TTS_CONFIG.MIN_AUDIO_BYTES_FOR_COMPLETION && this.isProcessing) {
      return true
    }
    
    return false
  }

  isAudioCurrentlyStreaming() {
    return !!(this.currentAudioStreaming && !this.currentAudioStreaming.interrupt)
  }

  async waitForAudioStreamToComplete(timeoutMs = 2000) {
    if (!this.isAudioCurrentlyStreaming()) {
      return true
    }

    console.log(`⏳ [AUDIO-SYNC] Waiting for current audio stream to complete...`)
    const startTime = Date.now()

    while (this.isAudioCurrentlyStreaming() && Date.now() - startTime < timeoutMs) {
      await new Promise((resolve) => setTimeout(resolve, 25))
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

const findAgentForCall = async (callData) => {
  const timer = createTimer("MONGODB_AGENT_LOOKUP")
  try {
    const { accountSid, callDirection, extraData } = callData

    let agent = null

    if (callDirection === "inbound") {
      if (!accountSid) {
        throw new Error("Missing accountSid for inbound call")
      }

      agent = await Agent.findOne({
        accountSid,
        isActive: true,
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

      agent = await Agent.findOne({
        callerId: callVaId,
        isActive: true,
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

const handleExternalCallDisconnection = async (streamSid, reason = "external_disconnection") => {
  try {
    const activeCall = await CallLog.findActiveCallByStreamSid(streamSid)
    if (activeCall) {
      console.log(`🛑 [EXTERNAL-DISCONNECT] Disconnecting call ${streamSid}: ${reason}`)

      await CallLog.findByIdAndUpdate(activeCall._id, {
        "metadata.isActive": false,
        "metadata.callEndTime": new Date(),
        "metadata.lastUpdated": new Date(),
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

const WHATSAPP_TEMPLATE_URL = "https://whatsapp-template-module.onrender.com/api/whatsapp/send-info"

const normalizeToE164India = (phoneNumber) => {
  const digits = String(phoneNumber || "").replace(/\D+/g, "")
  if (!digits) {
    throw new Error("Invalid phone number")
  }
  const last10 = digits.slice(-10)
  if (last10.length !== 10) {
    throw new Error("Invalid Indian mobile number")
  }
  return `+91${last10}`
}

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

Return only the JSON, nothing else.`,
          },
          {
            role: "user",
            content: description || "No description available",
          },
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
          courseName: parsed.courseName || "Unknown",
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

const sendWhatsAppAfterCall = async (callLogger, agentConfig) => {
  const timer = createTimer("WHATSAPP_AFTER_CALL")
  try {
    const phoneNumber = callLogger.mobile
    if (!phoneNumber) {
      console.log(`⚠️ [WHATSAPP-AFTER-CALL] No phone number available for WhatsApp message`)
      return false
    }

    const normalizedPhone = normalizeToE164India(phoneNumber)
    console.log(`📱 [WHATSAPP-AFTER-CALL] Sending WhatsApp to: ${normalizedPhone}`)

    const { orgName, courseName } = await extractOrgAndCourseFromDescription(agentConfig.description)
    console.log(`📱 [WHATSAPP-AFTER-CALL] Extracted - Org: ${orgName}, Course: ${courseName}`)

    const requestBody = {
      to: normalizedPhone,
      orgName: orgName,
      courseName: courseName,
    }

    console.log(`📱 [WHATSAPP-AFTER-CALL] Request body:`, JSON.stringify(requestBody, null, 2))

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

    if (callLogger.callLogId) {
      await CallLog.findByIdAndUpdate(callLogger.callLogId, {
        "metadata.whatsappSent": true,
        "metadata.whatsappSentAt": new Date(),
        "metadata.whatsappData": {
          phoneNumber: normalizedPhone,
          orgName: orgName,
          courseName: courseName,
          response: result,
        },
      }).catch((err) => console.log(`⚠️ [WHATSAPP-AFTER-CALL] Call log update error: ${err.message}`))
    }

    return true
  } catch (error) {
    console.log(`❌ [WHATSAPP-AFTER-CALL] ${timer.end()}ms - Error: ${error.message}`)

    if (callLogger.callLogId) {
      await CallLog.findByIdAndUpdate(callLogger.callLogId, {
        "metadata.whatsappSent": false,
        "metadata.whatsappError": error.message,
        "metadata.whatsappAttemptedAt": new Date(),
      }).catch((err) => console.log(`⚠️ [WHATSAPP-AFTER-CALL] Call log update error: ${err.message}`))
    }

    return false
  }
}

const testWhatsAppSending = async (phoneNumber, agentDescription) => {
  console.log("🧪 [TEST-WHATSAPP] Testing WhatsApp integration...")

  const mockCallLogger = {
    mobile: phoneNumber,
    callLogId: null,
  }

  const mockAgentConfig = {
    description: agentDescription || "EG Classes offers UPSE Online Course for competitive exam preparation",
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

const setupUnifiedVoiceServer = (wss) => {
  wss.on("connection", (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`)
    const urlParams = Object.fromEntries(url.searchParams.entries())

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
    let mobile = null
    let callerId = null
    let customParams = {}

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

      // Smart TTS interruption: only interrupt if TTS is still in early stages or if user speech is substantial
      if (currentTTS && TTS_CONFIG.ENABLE_SMART_INTERRUPTION) {
        const ttsStats = currentTTS.getStats()
        const isAudioStreaming = ttsStats.isAudioStreaming
        const isNearCompletion = ttsStats.isNearCompletion
        const userSpeechLength = text.trim().length
        
        // Don't interrupt if TTS is near completion
        if (isNearCompletion) {
          console.log("🔄 [USER-UTTERANCE] TTS is near completion, allowing it to finish")
          // Wait for TTS to complete
          await new Promise(resolve => setTimeout(resolve, TTS_CONFIG.COMPLETION_WAIT_TIME))
          return
        }
        
        // Allow TTS to complete if it's already streaming and user speech is short/interim
        if (isAudioStreaming && userSpeechLength < TTS_CONFIG.SHORT_SPEECH_THRESHOLD) {
          console.log("🔄 [USER-UTTERANCE] TTS is streaming, allowing completion for short interim speech")
          // Wait a bit to see if this is just interim speech
          await new Promise(resolve => setTimeout(resolve, TTS_CONFIG.INTERIM_SPEECH_WAIT_TIME))
          
          // If TTS is still streaming after delay, don't interrupt for short speech
          const updatedStats = currentTTS.getStats()
          if (updatedStats.isAudioStreaming && userSpeechLength < TTS_CONFIG.SHORT_SPEECH_THRESHOLD) {
            console.log("🔄 [USER-UTTERANCE] Skipping TTS interruption for interim speech, allowing completion")
            return
          }
        }
        
        console.log("🛑 [USER-UTTERANCE] Interrupting current TTS...")
        currentTTS.interrupt()
      } else if (currentTTS) {
        // Fallback to immediate interruption if smart interruption is disabled
        console.log("🛑 [USER-UTTERANCE] Interrupting current TTS (smart interruption disabled)...")
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

        console.log("🤖 [USER-UTTERANCE] Processing with OpenAI...")
        const aiResponse = await processWithOpenAI(text, conversationHistory, detectedLanguage, callLogger, agentConfig)

        if (processingRequestId === currentRequestId && aiResponse) {
          console.log("🤖 [USER-UTTERANCE] AI Response:", aiResponse)
          console.log("🎤 [USER-UTTERANCE] Starting TTS...")

          currentTTS = new SimplifiedSarvamTTSProcessor(detectedLanguage, ws, streamSid, callLogger)
          await currentTTS.synthesizeAndStream(aiResponse)

          conversationHistory.push({ role: "user", content: text }, { role: "assistant", content: aiResponse })

          if (conversationHistory.length > 10) {
            conversationHistory = conversationHistory.slice(-10)
          }

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

            console.log("📞 [SIP-START] ========== CALL START DATA ==========")
            console.log("📞 [SIP-START] Raw data:", JSON.stringify(data, null, 2))
            console.log("📞 [SIP-START] URL Parameters:", JSON.stringify(urlParams, null, 2))
            console.log("📞 [SIP-START] StreamSID:", streamSid)
            console.log("📞 [SIP-START] AccountSID:", accountSid)

            let czdataDecoded = null
            if (urlParams.czdata) {
              czdataDecoded = decodeCzdata(urlParams.czdata)
              if (czdataDecoded) {
                customParams = czdataDecoded
                userName =
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
                console.log("[SIP-START] Decoded czdata customParams:", customParams)
                if (userName) {
                  console.log("[SIP-START] User Name (czdata):", userName)
                }
              }
            }

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

            if (extraData?.CallCli) {
              mobile = extraData.CallCli
            }
            if (extraData?.CallVaId) {
              callerId = extraData.CallVaId
            }
            if (!userName && extraData) {
              userName =
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
              if (userName) {
                console.log("[SIP-START] User Name (extraData):", userName)
              }
            }

            if (!userName && urlParams.name) {
              userName = urlParams.name
              console.log("[SIP-START] User Name (url param):", userName)
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
            console.log(
              "🎯 [SIP-CALL-SETUP] CallSID:",
              data.start?.callSid || data.start?.CallSid || data.callSid || data.CallSid,
            )

            callLogger = new EnhancedCallLogger(agentConfig.clientId || accountSid, mobile, callDirection)
            callLogger.customParams = customParams
            callLogger.callerId = callerId || undefined
            callLogger.streamSid = streamSid
            callLogger.callSid = data.start?.callSid || data.start?.CallSid || data.callSid || data.CallSid
            callLogger.accountSid = accountSid
            callLogger.ws = ws

            try {
              await callLogger.createInitialCallLog(agentConfig._id, "not_connected")
              console.log("✅ [SIP-CALL-SETUP] Initial call log created successfully")
              console.log("✅ [SIP-CALL-SETUP] Call Log ID:", callLogger.callLogId)
            } catch (error) {
              console.log("❌ [SIP-CALL-SETUP] Failed to create initial call log:", error.message)
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
            const tts = new SimplifiedSarvamTTSProcessor(currentLanguage, ws, streamSid, callLogger)
            await tts.synthesizeAndStream(greeting)
            console.log("✅ [SIP-TTS] Greeting TTS completed")
            break
          }

          case "media":
            if (data.media?.payload) {
              const audioBuffer = Buffer.from(data.media.payload, "base64")

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

            if (streamSid) {
              await handleExternalCallDisconnection(streamSid, "sip_stop_event")
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
      mobile = null
      callerId = null
      customParams = {}
      userName = null

      console.log("🔌 [SIP-CLOSE] ======================================")
    })

    ws.on("error", (error) => {
      console.log("❌ [SIP-ERROR] WebSocket error:", error.message)
      console.log("❌ [SIP-ERROR] StreamSID:", streamSid)
      console.log("❌ [SIP-ERROR] Call Direction:", callDirection)
    })
  })
}

const activeCallLoggers = new Map()

let stopEventSequence = 1

const terminateCallByStreamSid = async (streamSid, reason = "manual_termination") => {
  try {
    console.log(`🛑 [MANUAL-TERMINATION] Attempting to terminate call with streamSid: ${streamSid}`)

    const callLogger = activeCallLoggers.get(streamSid)

    if (callLogger) {
      console.log(`🛑 [MANUAL-TERMINATION] Found active call logger, terminating gracefully...`)
      console.log(`🛑 [MANUAL-TERMINATION] Call Logger Info:`, callLogger.getCallInfo())

      if (callLogger.ws) {
        console.log(
          `🛑 [MANUAL-TERMINATION] WebSocket State: ${callLogger.ws.readyState} (0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED)`,
        )
      }

      await callLogger.disconnectCall(reason)
      return {
        success: true,
        message: "Call terminated successfully",
        streamSid,
        reason,
        method: "graceful_termination",
      }
    } else {
      console.log(`🛑 [MANUAL-TERMINATION] No active call logger found, updating database directly...`)

      try {
        const CallLog = require("../models/CallLog")
        const result = await CallLog.updateMany(
          { streamSid, "metadata.isActive": true },
          {
            "metadata.isActive": false,
            "metadata.terminationReason": reason,
            "metadata.terminatedAt": new Date(),
            "metadata.terminationMethod": "api_manual",
            leadStatus: "disconnected_api",
          },
        )

        if (result.modifiedCount > 0) {
          return {
            success: true,
            message: "Call marked as terminated in database",
            streamSid,
            reason,
            method: "database_update",
            modifiedCount: result.modifiedCount,
          }
        } else {
          return {
            success: false,
            message: "No active calls found with this streamSid",
            streamSid,
            reason,
            method: "database_update",
          }
        }
      } catch (dbError) {
        console.error(`❌ [MANUAL-TERMINATION] Database update error:`, dbError.message)
        return {
          success: false,
          message: "Failed to update database",
          streamSid,
          reason,
          method: "database_update",
          error: dbError.message,
        }
      }
    }
  } catch (error) {
    console.error(`❌ [MANUAL-TERMINATION] Error terminating call:`, error.message)
    return {
      success: false,
      message: "Failed to terminate call",
      streamSid,
      reason,
      method: "error",
      error: error.message,
    }
  }
}

module.exports = {
  setupUnifiedVoiceServer,
  terminateCallByStreamSid,
  terminationMethods: {
    graceful: (callLogger, message, language) => callLogger?.gracefulCallEnd(message, language),
    fast: (callLogger, reason) => callLogger?.fastTerminateCall(reason),
  },
  whatsappMethods: {
    sendWhatsAppAfterCall,
    extractOrgAndCourseFromDescription,
    normalizeToE164India,
    testWhatsAppSending,
  },
}
