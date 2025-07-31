const WebSocket = require("ws")
require("dotenv").config()
const mongoose = require("mongoose")
const Agent = require("../models/Agent")
const CallLog = require("../models/CallLog")

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

// SIP Header Decoder Utility (unchanged)
class SIPHeaderDecoder {
  static decodeBase64Extra(base64String) {
    try {
      const decoded = Buffer.from(base64String, "base64").toString("utf-8")
      const parsed = JSON.parse(decoded)

      console.log(`🔓 [SIP-DECODE] Base64 decoded successfully`)
      console.log(`📋 [SIP-DECODE] Parsed data:`, JSON.stringify(parsed, null, 2))

      return parsed
    } catch (error) {
      console.error(`❌ [SIP-DECODE] Failed to decode base64 extra: ${error.message}`)
      return null
    }
  }

  static parseConnectionURL(url) {
    try {
      let fullUrl = url
      if (!url.startsWith("http") && !url.startsWith("ws")) {
        if (url.startsWith("/")) {
          fullUrl = "wss://dummy.com" + url
        } else {
          console.log(`⚠️ [SIP-PARSE] Invalid URL format: ${url}`)
          return null
        }
      }

      const urlObj = new URL(fullUrl)
      const params = new URLSearchParams(urlObj.search)

      const hasParams = params.has("app_id") || params.has("caller_id") || params.has("did") || params.has("extra")

      if (!hasParams) {
        console.log(`ℹ️ [SIP-PARSE] No SIP parameters found in URL: ${url}`)
        return null
      }

      const sipData = {
        app_id: params.get("app_id"),
        caller_id: params.get("caller_id"),
        did: params.get("did"),
        direction: params.get("direction"),
        session_id: params.get("session_id"),
        extra_raw: params.get("extra"),
        czdata: params.get("czdata"),
      }

      if (sipData.extra_raw) {
        try {
          const decodedExtra = this.decodeBase64Extra(decodeURIComponent(sipData.extra_raw))
          sipData.extra = decodedExtra
        } catch (decodeError) {
          console.error(`❌ [SIP-PARSE] Failed to decode extra field: ${decodeError.message}`)
          sipData.extra = null
        }
      }

      return sipData
    } catch (error) {
      console.log(`ℹ️ [SIP-PARSE] URL parsing failed (likely non-SIP connection): ${error.message}`)
      return null
    }
  }

  static logSIPData(sipData) {
    console.log(`\n🌐 [SIP-HEADERS] ==========================================`)
    console.log(`📱 [SIP-HEADERS] Customer Mobile (caller_id): ${sipData.caller_id}`)
    console.log(`📞 [SIP-HEADERS] DID Number: ${sipData.did}`)
    console.log(`🔄 [SIP-HEADERS] Direction: ${sipData.direction || "Not specified"}`)
    console.log(`🆔 [SIP-HEADERS] App ID: ${sipData.app_id}`)
    console.log(`🔗 [SIP-HEADERS] Session ID: ${sipData.session_id}`)

    if (sipData.extra) {
      console.log(`📋 [SIP-HEADERS] Extra Data:`)
      console.log(`   • Call CLI: ${sipData.extra.CallCli}`)
      console.log(`   • Call Session ID: ${sipData.extra.CallSessionId}`)
      console.log(`   • Call VA ID: ${sipData.extra.CallVaId}`)
      console.log(`   • DID (from extra): ${sipData.extra.DID}`)
      console.log(`   • CZ Service App ID: ${sipData.extra.CZSERVICEAPPID}`)
      console.log(`   • Call Direction: ${sipData.extra.CallDirection}`)
    }
    console.log(`🌐 [SIP-HEADERS] ==========================================\n`)
  }

  static determineCallType(sipData) {
    if (sipData.extra?.CallDirection) {
      const direction = sipData.extra.CallDirection.toLowerCase()
      if (direction === "outdial") return "outbound"
      if (direction === "indial") return "inbound"
    }

    if (sipData.direction) {
      const direction = sipData.direction.toLowerCase()
      if (direction === "outbound" || direction === "outdial") return "outbound"
      if (direction === "inbound" || direction === "indial") return "inbound"
    }

    if (sipData.extra?.CallVaId) {
      return "inbound"
    }

    return "unknown"
  }

  static getCustomerNumber(sipData) {
    if (sipData.extra?.CallCli) {
      return sipData.extra.CallCli
    }

    if (sipData.caller_id && sipData.caller_id !== sipData.extra?.CallVaId) {
      return sipData.caller_id
    }

    return null
  }

  static getAgentIdentifier(sipData) {
    const callType = this.determineCallType(sipData)

    if (callType === "outbound") {
      return sipData.extra?.CallVaId || sipData.caller_id
    } else {
      return sipData.app_id
    }
  }
}

// Enhanced language mappings
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
const VALID_SARVAM_VOICES = ["meera", "pavithra", "arvind", "amol", "maya"]

const getValidSarvamVoice = (voiceSelection = "pavithra") => {
  if (VALID_SARVAM_VOICES.includes(voiceSelection)) {
    return voiceSelection
  }

  const voiceMapping = {
    "male-professional": "arvind",
    "female-professional": "pavithra",
    "male-friendly": "amol",
    "female-friendly": "maya",
    neutral: "pavithra",
    default: "pavithra",
  }

  return voiceMapping[voiceSelection] || "pavithra"
}

// Language detection with OpenAI
const detectLanguageWithOpenAI = async (text) => {
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
      console.log(`🔍 [LANG-DETECT] Detected: "${detectedLang}" from text: "${text.substring(0, 50)}..."`)
      return detectedLang
    }

    console.log(`⚠️ [LANG-DETECT] Invalid language "${detectedLang}", defaulting to "hi"`)
    return "hi"
  } catch (error) {
    console.error(`❌ [LANG-DETECT] Error: ${error.message}`)
    return "hi"
  }
}

// Enhanced Call logging utility class
class CallLogger {
  constructor(clientId, sipData = null) {
    this.clientId = clientId
    this.sipData = sipData
    this.mobile = SIPHeaderDecoder.getCustomerNumber(sipData) || sipData?.caller_id || null
    this.callStartTime = new Date()
    this.transcripts = []
    this.responses = []
    this.totalDuration = 0

    if (sipData) {
      console.log(`📝 [CALL-LOG] Initialized with SIP data for client: ${clientId}`)
      console.log(`📱 [CALL-LOG] Customer number: ${this.mobile}`)
      SIPHeaderDecoder.logSIPData(sipData)
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
    console.log(`📝 [CALL-LOG] User: "${transcript}" (${language})`)
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
    console.log(`🤖 [CALL-LOG] AI: "${response}" (${language})`)
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
          sipData: this.sipData,
          callType: this.sipData ? SIPHeaderDecoder.determineCallType(this.sipData) : "unknown",
          did: this.sipData?.did,
          appId: this.sipData?.app_id,
          sessionId: this.sipData?.session_id,
          extraData: this.sipData?.extra,
          customerNumber: this.mobile,
        },
      }

      const callLog = new CallLog(callLogData)
      const savedLog = await callLog.save()

      console.log(`💾 [CALL-LOG] Saved to DB - ID: ${savedLog._id}, Duration: ${this.totalDuration}s`)
      console.log(
        `📊 [CALL-LOG] Stats - User messages: ${this.transcripts.length}, AI responses: ${this.responses.length}`,
      )

      if (this.sipData) {
        console.log(
          `📞 [CALL-LOG] SIP Data - Type: ${SIPHeaderDecoder.determineCallType(this.sipData)}, DID: ${this.sipData.did}, Customer: ${this.mobile}`,
        )
      }

      return savedLog
    } catch (error) {
      console.error(`❌ [CALL-LOG] Database save error: ${error.message}`)
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
      sipData: this.sipData,
      callType: this.sipData ? SIPHeaderDecoder.determineCallType(this.sipData) : "unknown",
      customerNumber: this.mobile,
    }
  }
}

// OPTIMIZED: Persistent Deepgram Connection Manager
class PersistentDeepgramManager {
  constructor(language, onTranscript, onError, customerNumber) {
    this.language = language
    this.onTranscript = onTranscript
    this.onError = onError
    this.customerNumber = customerNumber
    this.deepgramWs = null
    this.isConnected = false
    this.audioQueue = []
    this.reconnectAttempts = 0
    this.maxReconnectAttempts = 3
    this.reconnectDelay = 1000 // Start with 1 second
    this.connectionTimer = null
    this.lastActivity = Date.now()
    this.keepAliveInterval = null
  }

  async connect() {
    if (this.deepgramWs && this.isConnected) {
      console.log(`✅ [DEEPGRAM-PERSISTENT] Already connected for ${this.customerNumber}`)
      return true
    }

    try {
      console.log(
        `🔌 [DEEPGRAM-PERSISTENT] Connecting for ${this.customerNumber} (attempt ${this.reconnectAttempts + 1})`,
      )

      const deepgramLanguage = getDeepgramLanguage(this.language)
      console.log(`🌍 [DEEPGRAM-PERSISTENT] Using language: ${deepgramLanguage}`)

      const deepgramUrl = new URL("wss://api.deepgram.com/v1/listen")
      deepgramUrl.searchParams.append("sample_rate", "8000")
      deepgramUrl.searchParams.append("channels", "1")
      deepgramUrl.searchParams.append("encoding", "linear16")
      deepgramUrl.searchParams.append("model", "nova-2")
      deepgramUrl.searchParams.append("language", deepgramLanguage)
      deepgramUrl.searchParams.append("interim_results", "true")
      deepgramUrl.searchParams.append("smart_format", "true")
      deepgramUrl.searchParams.append("endpointing", "300")
      deepgramUrl.searchParams.append("keep_alive", "true") // Keep connection alive

      this.deepgramWs = new WebSocket(deepgramUrl.toString(), {
        headers: {
          Authorization: `Token ${API_KEYS.deepgram}`,
          "User-Agent": `VoiceServer/1.0 Customer-${this.customerNumber}`,
        },
      })

      return new Promise((resolve, reject) => {
        const connectionTimeout = setTimeout(() => {
          console.error(`❌ [DEEPGRAM-PERSISTENT] Connection timeout for ${this.customerNumber}`)
          this.cleanup()
          reject(new Error("Connection timeout"))
        }, 10000) // 10 second timeout

        this.deepgramWs.onopen = () => {
          clearTimeout(connectionTimeout)
          this.isConnected = true
          this.reconnectAttempts = 0
          this.reconnectDelay = 1000 // Reset delay
          this.lastActivity = Date.now()

          console.log(`✅ [DEEPGRAM-PERSISTENT] Connected successfully for ${this.customerNumber}`)

          // Process queued audio
          if (this.audioQueue.length > 0) {
            console.log(`📦 [DEEPGRAM-PERSISTENT] Processing ${this.audioQueue.length} queued audio buffers`)
            this.audioQueue.forEach((buffer, index) => {
              this.deepgramWs.send(buffer)
              console.log(`📤 [DEEPGRAM-PERSISTENT] Sent queued buffer ${index + 1}/${this.audioQueue.length}`)
            })
            this.audioQueue = []
          }

          // Start keep-alive mechanism
          this.startKeepAlive()

          resolve(true)
        }

        this.deepgramWs.onmessage = (event) => {
          this.lastActivity = Date.now()
          const data = JSON.parse(event.data)
          this.handleMessage(data)
        }

        this.deepgramWs.onerror = (error) => {
          clearTimeout(connectionTimeout)
          console.error(`❌ [DEEPGRAM-PERSISTENT] WebSocket error for ${this.customerNumber}:`, error)
          this.isConnected = false

          if (this.onError) {
            this.onError(error)
          }

          reject(error)
        }

        this.deepgramWs.onclose = (event) => {
          clearTimeout(connectionTimeout)
          console.log(
            `🔌 [DEEPGRAM-PERSISTENT] Connection closed for ${this.customerNumber} - Code: ${event.code}, Reason: ${event.reason}`,
          )
          this.isConnected = false
          this.stopKeepAlive()

          // Auto-reconnect logic for unexpected closures
          if (event.code !== 1000 && this.reconnectAttempts < this.maxReconnectAttempts) {
            console.log(
              `🔄 [DEEPGRAM-PERSISTENT] Attempting reconnection for ${this.customerNumber} in ${this.reconnectDelay}ms`,
            )
            setTimeout(() => {
              this.reconnectAttempts++
              this.reconnectDelay *= 2 // Exponential backoff
              this.connect().catch((err) => {
                console.error(`❌ [DEEPGRAM-PERSISTENT] Reconnection failed for ${this.customerNumber}:`, err.message)
              })
            }, this.reconnectDelay)
          }

          if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`❌ [DEEPGRAM-PERSISTENT] Max reconnection attempts reached for ${this.customerNumber}`)
          }
        }
      })
    } catch (error) {
      console.error(`❌ [DEEPGRAM-PERSISTENT] Setup error for ${this.customerNumber}: ${error.message}`)
      throw error
    }
  }

  startKeepAlive() {
    // Send keep-alive messages every 30 seconds
    this.keepAliveInterval = setInterval(() => {
      if (this.isConnected && this.deepgramWs && this.deepgramWs.readyState === WebSocket.OPEN) {
        // Check if connection has been idle for too long
        const idleTime = Date.now() - this.lastActivity
        if (idleTime > 60000) {
          // 1 minute idle
          console.log(
            `💓 [DEEPGRAM-KEEP-ALIVE] Sending keep-alive for ${this.customerNumber} (idle: ${Math.round(idleTime / 1000)}s)`,
          )

          // Send a small keep-alive message
          const keepAliveMessage = {
            type: "KeepAlive",
            timestamp: Date.now(),
          }

          try {
            this.deepgramWs.send(JSON.stringify(keepAliveMessage))
            this.lastActivity = Date.now()
          } catch (error) {
            console.error(
              `❌ [DEEPGRAM-KEEP-ALIVE] Failed to send keep-alive for ${this.customerNumber}:`,
              error.message,
            )
          }
        }
      }
    }, 30000)
  }

  stopKeepAlive() {
    if (this.keepAliveInterval) {
      clearInterval(this.keepAliveInterval)
      this.keepAliveInterval = null
    }
  }

  handleMessage(data) {
    if (data.type === "Results") {
      const transcript = data.channel?.alternatives?.[0]?.transcript
      const is_final = data.is_final
      const confidence = data.channel?.alternatives?.[0]?.confidence

      if (transcript?.trim()) {
        console.log(
          `🎤 [DEEPGRAM-PERSISTENT] ${is_final ? "FINAL" : "interim"} from ${this.customerNumber}: "${transcript}" (confidence: ${confidence || "unknown"})`,
        )

        if (this.onTranscript) {
          this.onTranscript(transcript, is_final, confidence)
        }
      }
    } else if (data.type === "UtteranceEnd") {
      console.log(`🔚 [DEEPGRAM-PERSISTENT] Utterance end for ${this.customerNumber}`)
      if (this.onTranscript) {
        this.onTranscript(null, true, null, "utterance_end")
      }
    } else if (data.type === "Metadata") {
      console.log(`📊 [DEEPGRAM-PERSISTENT] Metadata for ${this.customerNumber}:`, {
        request_id: data.request_id,
        model_info: data.model_info,
      })
    }
  }

  sendAudio(audioBuffer) {
    if (this.isConnected && this.deepgramWs && this.deepgramWs.readyState === WebSocket.OPEN) {
      try {
        this.deepgramWs.send(audioBuffer)
        this.lastActivity = Date.now()
        console.log(`📤 [DEEPGRAM-PERSISTENT] Sent ${audioBuffer.length} bytes for ${this.customerNumber}`)
        return true
      } catch (error) {
        console.error(`❌ [DEEPGRAM-PERSISTENT] Error sending audio for ${this.customerNumber}:`, error.message)
        this.isConnected = false
        return false
      }
    } else {
      // Queue audio if not connected
      this.audioQueue.push(audioBuffer)
      console.log(
        `📦 [DEEPGRAM-PERSISTENT] Queued ${audioBuffer.length} bytes for ${this.customerNumber} (queue size: ${this.audioQueue.length})`,
      )

      // Try to reconnect if not connected
      if (!this.isConnected) {
        this.connect().catch((error) => {
          console.error(`❌ [DEEPGRAM-PERSISTENT] Auto-reconnect failed for ${this.customerNumber}:`, error.message)
        })
      }

      return false
    }
  }

  updateLanguage(newLanguage) {
    if (newLanguage !== this.language) {
      console.log(
        `🔄 [DEEPGRAM-PERSISTENT] Language change for ${this.customerNumber}: ${this.language} → ${newLanguage}`,
      )
      this.language = newLanguage

      // Reconnect with new language
      this.disconnect()
      this.connect().catch((error) => {
        console.error(
          `❌ [DEEPGRAM-PERSISTENT] Language update reconnection failed for ${this.customerNumber}:`,
          error.message,
        )
      })
    }
  }

  disconnect() {
    console.log(`🔌 [DEEPGRAM-PERSISTENT] Disconnecting for ${this.customerNumber}`)

    this.stopKeepAlive()

    if (this.deepgramWs) {
      if (this.deepgramWs.readyState === WebSocket.OPEN) {
        this.deepgramWs.close(1000, "Normal closure")
      }
      this.deepgramWs = null
    }

    this.isConnected = false
    this.audioQueue = []
  }

  cleanup() {
    this.disconnect()
    this.onTranscript = null
    this.onError = null
  }

  getStatus() {
    return {
      isConnected: this.isConnected,
      language: this.language,
      queueSize: this.audioQueue.length,
      reconnectAttempts: this.reconnectAttempts,
      lastActivity: this.lastActivity,
      customerNumber: this.customerNumber,
    }
  }
}

// OpenAI streaming processing (unchanged)
const processWithOpenAIStreaming = async (
  userMessage,
  conversationHistory,
  detectedLanguage,
  onPhrase,
  onComplete,
  onInterrupt,
  callLogger,
) => {
  const timer = createTimer("OPENAI_STREAMING")

  try {
    const getSystemPrompt = (lang) => {
      const prompts = {
        hi: "आप एआई तोता हैं, एक विनम्र और भावनात्मक रूप से बुद्धिमान AI ग्राहक सेवा कार्यकारी। आप हिंदी में धाराप्रवाह बोलते हैं। प्राकृतिक, बातचीत की भाषा का प्रयोग करें जो गर्मजोशी और सहानुभूति से भरी हो। जवाब छोटे रखें—केवल 1-2 लाइन। ग्राहकों को सुना, समर्थित और मूल्यवान महसूस कराना आपका लक्ष्य है।",

        en: "You are Aitota, a polite, emotionally intelligent AI customer care executive. You speak fluently in English. Use natural, conversational language with warmth and empathy. Keep responses short—just 1–2 lines. Your goal is to make customers feel heard, supported, and valued.",

        bn: "আপনি আইতোতা, একজন ভদ্র এবং আবেগপ্রবণভাবে বুদ্ধিমান AI গ্রাহক সেবা কর্মকর্তা। আপনি বাংলায় সাবলীলভাবে কথা বলেন। উষ্ণতা এবং সহানুভূতি সহ প্রাকৃতিক, কথোপকথনমূলক ভাষা ব্যবহার করুন।",

        te: "మీరు ఐతోతా, మర్యాదపూర్వక, భావోద్వేగంతో తెలివైన AI కస్టమర్ కేర్ ఎగ్జిక్యూటివ్. మీరు తెలుగులో సరళంగా మాట్లాడుతారు। వెచ్చదనం మరియు సానుభూతితో సహజమైన, సంభాషణా భాషను ఉపయోగించండి।",

        ta: "நீங்கள் ஐதோதா, ஒரு கண்ணியமான, உணர்வுபூர்வமாக புத்திசாலித்தனமான AI வாடிக்கையாளர் சேவை நிர்வாகி. நீங்கள் தமிழில் சரளமாக பேசுகிறீர்கள். அன்பு மற்றும் அனுதாபத்துடன் இயற்கையான, உரையாடல் மொழியைப் பயன்படுத்துங்கள்।",

        mr: "तुम्ही एआयतोता आहात, एक नम्र आणि भावनिकदृष्ट्या बुद्धिमान AI ग्राहक सेवा कार्यकारी. तुम्ही मराठीत अस्खलितपणे बोलता. उबदारपणा आणि सहानुभूतीसह नैसर्गिक, संभाषणात्मक भाषा वापरा. उत्तरे लहान ठेवा—फक्त 1-2 ओळी. ग्राहकांना ऐकले, समर्थित आणि मूल्यवान वाटण्याचे तुमचे ध्येय आहे।",
      }

      return prompts[lang] || prompts.en
    }

    const systemPrompt = getSystemPrompt(detectedLanguage)

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
        max_tokens: 80,
        temperature: 0.3,
        stream: true,
      }),
    })

    if (!response.ok) {
      console.error(`❌ [OPENAI] Error: ${response.status}`)
      return null
    }

    let fullResponse = ""
    let phraseBuffer = ""
    let isFirstPhrase = true
    let isInterrupted = false

    const reader = response.body.getReader()
    const decoder = new TextDecoder()

    const checkInterruption = () => {
      return onInterrupt && onInterrupt()
    }

    while (true) {
      if (checkInterruption()) {
        isInterrupted = true
        console.log(`⚠️ [OPENAI] Stream interrupted by new user input`)
        reader.cancel()
        break
      }

      const { done, value } = await reader.read()
      if (done) break

      const chunk = decoder.decode(value, { stream: true })
      const lines = chunk.split("\n").filter((line) => line.trim())

      for (const line of lines) {
        if (line.startsWith("data: ")) {
          const data = line.slice(6)

          if (data === "[DONE]") {
            if (phraseBuffer.trim() && !isInterrupted) {
              onPhrase(phraseBuffer.trim(), detectedLanguage)
              fullResponse += phraseBuffer
            }
            break
          }

          try {
            const parsed = JSON.parse(data)
            const content = parsed.choices?.[0]?.delta?.content

            if (content) {
              phraseBuffer += content

              if (checkInterruption()) {
                isInterrupted = true
                break
              }

              if (shouldSendPhrase(phraseBuffer)) {
                const phrase = phraseBuffer.trim()
                if (phrase.length > 0 && !isInterrupted) {
                  if (isFirstPhrase) {
                    console.log(`⚡ [OPENAI] First phrase (${timer.checkpoint("first_phrase")}ms)`)
                    isFirstPhrase = false
                  }
                  onPhrase(phrase, detectedLanguage)
                  fullResponse += phrase
                  phraseBuffer = ""
                }
              }
            }
          } catch (e) {
            // Skip malformed JSON
          }
        }
      }

      if (isInterrupted) break
    }

    if (!isInterrupted) {
      console.log(`🤖 [OPENAI] Complete: "${fullResponse}" (${timer.end()}ms)`)

      if (callLogger && fullResponse.trim()) {
        callLogger.logAIResponse(fullResponse.trim(), detectedLanguage)
      }

      onComplete(fullResponse)
    } else {
      console.log(`🤖 [OPENAI] Interrupted after ${timer.end()}ms`)
    }

    return isInterrupted ? null : fullResponse
  } catch (error) {
    console.error(`❌ [OPENAI] Error: ${error.message}`)
    return null
  }
}

const shouldSendPhrase = (buffer) => {
  const trimmed = buffer.trim()

  if (/[.!?।॥।]$/.test(trimmed)) return true
  if (trimmed.length >= 8 && /[,;।]\s*$/.test(trimmed)) return true
  if (trimmed.length >= 25 && /\s/.test(trimmed)) return true

  return false
}

// TTS processor (unchanged)
class OptimizedSarvamTTSProcessor {
  constructor(language, ws, streamSid, callLogger = null) {
    this.language = language
    this.ws = ws
    this.streamSid = streamSid
    this.callLogger = callLogger
    this.queue = []
    this.isProcessing = false
    this.sarvamLanguage = getSarvamLanguage(language)
    this.voice = getValidSarvamVoice(ws.sessionAgentConfig?.voiceSelection || "pavithra")

    this.isInterrupted = false
    this.currentAudioStreaming = null

    this.sentenceBuffer = ""
    this.processingTimeout = 100
    this.sentenceTimer = null

    this.totalChunks = 0
    this.totalAudioBytes = 0
  }

  interrupt() {
    console.log(`⚠️ [SARVAM-TTS] Interrupting current processing`)
    this.isInterrupted = true

    this.queue = []
    this.sentenceBuffer = ""

    if (this.sentenceTimer) {
      clearTimeout(this.sentenceTimer)
      this.sentenceTimer = null
    }

    if (this.currentAudioStreaming) {
      this.currentAudioStreaming.interrupt = true
    }

    console.log(`🛑 [SARVAM-TTS] Processing interrupted and cleaned up`)
  }

  reset(newLanguage) {
    this.interrupt()

    if (newLanguage) {
      this.language = newLanguage
      this.sarvamLanguage = getSarvamLanguage(newLanguage)
      console.log(`🔄 [SARVAM-TTS] Language updated to: ${this.sarvamLanguage}`)
    }

    this.isInterrupted = false
    this.isProcessing = false
    this.totalChunks = 0
    this.totalAudioBytes = 0
  }

  addPhrase(phrase, detectedLanguage) {
    if (!phrase.trim() || this.isInterrupted) return

    if (detectedLanguage && detectedLanguage !== this.language) {
      console.log(`🔄 [SARVAM-TTS] Language change detected: ${this.language} → ${detectedLanguage}`)
      this.language = detectedLanguage
      this.sarvamLanguage = getSarvamLanguage(detectedLanguage)
    }

    this.sentenceBuffer += (this.sentenceBuffer ? " " : "") + phrase.trim()

    if (this.hasCompleteSentence(this.sentenceBuffer)) {
      this.processCompleteSentences()
    } else {
      this.scheduleProcessing()
    }
  }

  hasCompleteSentence(text) {
    return /[.!?।॥।]/.test(text)
  }

  extractCompleteSentences(text) {
    const sentences = text.split(/([.!?।॥।])/).filter((s) => s.trim())

    let completeSentences = ""
    let remainingText = ""

    for (let i = 0; i < sentences.length; i += 2) {
      const sentence = sentences[i]
      const punctuation = sentences[i + 1]

      if (punctuation) {
        completeSentences += sentence + punctuation + " "
      } else {
        remainingText = sentence
      }
    }

    return {
      complete: completeSentences.trim(),
      remaining: remainingText.trim(),
    }
  }

  processCompleteSentences() {
    if (this.isInterrupted) return

    if (this.sentenceTimer) {
      clearTimeout(this.sentenceTimer)
      this.sentenceTimer = null
    }

    const { complete, remaining } = this.extractCompleteSentences(this.sentenceBuffer)

    if (complete && !this.isInterrupted) {
      this.queue.push(complete)
      this.sentenceBuffer = remaining
      this.processQueue()
    }
  }

  scheduleProcessing() {
    if (this.isInterrupted) return

    if (this.sentenceTimer) clearTimeout(this.sentenceTimer)

    this.sentenceTimer = setTimeout(() => {
      if (this.sentenceBuffer.trim() && !this.isInterrupted) {
        this.queue.push(this.sentenceBuffer.trim())
        this.sentenceBuffer = ""
        this.processQueue()
      }
    }, this.processingTimeout)
  }

  async processQueue() {
    if (this.isProcessing || this.queue.length === 0 || this.isInterrupted) return

    this.isProcessing = true
    const textToProcess = this.queue.shift()

    try {
      if (!this.isInterrupted) {
        await this.synthesizeAndStream(textToProcess)
      }
    } catch (error) {
      if (!this.isInterrupted) {
        console.error(`❌ [SARVAM-TTS] Error: ${error.message}`)
      }
    } finally {
      this.isProcessing = false

      if (this.queue.length > 0 && !this.isInterrupted) {
        setTimeout(() => this.processQueue(), 10)
      }
    }
  }

  async synthesizeAndStream(text) {
    if (this.isInterrupted) return

    const timer = createTimer("SARVAM_TTS_SENTENCE")

    try {
      console.log(`🎵 [SARVAM-TTS] Synthesizing: "${text}" (${this.sarvamLanguage})`)

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
          enable_preprocessing: false,
          model: "bulbul:v1",
        }),
      })

      if (!response.ok || this.isInterrupted) {
        if (this.isInterrupted) return
        throw new Error(`Sarvam API error: ${response.status} - ${response.statusText}`)
      }

      const responseData = await response.json()
      const audioBase64 = responseData.audios?.[0]

      if (!audioBase64 || this.isInterrupted) {
        if (!this.isInterrupted) {
          throw new Error("No audio data received from Sarvam API")
        }
        return
      }

      console.log(`⚡ [SARVAM-TTS] Synthesis completed in ${timer.end()}ms`)

      if (!this.isInterrupted) {
        await this.streamAudioOptimizedForSIP(audioBase64)

        const audioBuffer = Buffer.from(audioBase64, "base64")
        this.totalAudioBytes += audioBuffer.length
        this.totalChunks++
      }
    } catch (error) {
      if (!this.isInterrupted) {
        console.error(`❌ [SARVAM-TTS] Synthesis error: ${error.message}`)
        throw error
      }
    }
  }

  async streamAudioOptimizedForSIP(audioBase64) {
    if (this.isInterrupted) return

    const audioBuffer = Buffer.from(audioBase64, "base64")
    const streamingSession = { interrupt: false }
    this.currentAudioStreaming = streamingSession

    const SAMPLE_RATE = 8000
    const BYTES_PER_SAMPLE = 2
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000
    const OPTIMAL_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS)

    console.log(`📦 [SARVAM-SIP] Streaming ${audioBuffer.length} bytes`)

    let position = 0
    let chunkIndex = 0

    while (position < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
      const remaining = audioBuffer.length - position
      const chunkSize = Math.min(OPTIMAL_CHUNK_SIZE, remaining)
      const chunk = audioBuffer.slice(position, position + chunkSize)

      console.log(`📤 [SARVAM-SIP] Chunk ${chunkIndex + 1}: ${chunk.length} bytes`)

      const mediaMessage = {
        event: "media",
        streamSid: this.streamSid,
        media: {
          payload: chunk.toString("base64"),
        },
      }

      if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted) {
        this.ws.send(JSON.stringify(mediaMessage))
      }

      if (position + chunkSize < audioBuffer.length && !this.isInterrupted) {
        const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS)
        const delayMs = Math.max(chunkDurationMs - 2, 10)
        await new Promise((resolve) => setTimeout(resolve, delayMs))
      }

      position += chunkSize
      chunkIndex++
    }

    if (this.isInterrupted || streamingSession.interrupt) {
      console.log(`🛑 [SARVAM-SIP] Audio streaming interrupted at chunk ${chunkIndex}`)
    } else {
      console.log(`✅ [SARVAM-SIP] Completed streaming ${chunkIndex} chunks`)
    }

    this.currentAudioStreaming = null
  }

  complete() {
    if (this.isInterrupted) return

    if (this.sentenceBuffer.trim()) {
      this.queue.push(this.sentenceBuffer.trim())
      this.sentenceBuffer = ""
    }

    if (this.queue.length > 0) {
      this.processQueue()
    }

    console.log(`📊 [SARVAM-STATS] Total: ${this.totalChunks} sentences, ${this.totalAudioBytes} bytes`)
  }

  getStats() {
    return {
      totalChunks: this.totalChunks,
      totalAudioBytes: this.totalAudioBytes,
      avgBytesPerChunk: this.totalChunks > 0 ? Math.round(this.totalAudioBytes / this.totalChunks) : 0,
    }
  }
}

// Agent configuration fetcher (unchanged)
class AgentConfigFetcher {
  static async fetchAgentConfig(sipData) {
    const callType = SIPHeaderDecoder.determineCallType(sipData)
    const agentIdentifier = SIPHeaderDecoder.getAgentIdentifier(sipData)
    const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData)

    console.log(`\n🔍 [AGENT-FETCH] ==========================================`)
    console.log(`📞 [AGENT-FETCH] Call Type: ${callType}`)
    console.log(`🆔 [AGENT-FETCH] Agent Identifier: ${agentIdentifier}`)
    console.log(`📱 [AGENT-FETCH] Customer Number: ${customerNumber}`)
    console.log(`🌐 [AGENT-FETCH] SIP Data received:`, JSON.stringify(sipData, null, 2))

    let agentConfig = null

    try {
      if (callType === "outbound") {
        console.log(`📤 [AGENT-FETCH] Searching for OUTBOUND agent with callerId: "${agentIdentifier}"`)
        console.log(`🔍 [AGENT-FETCH] Database query: Agent.findOne({ callerId: "${agentIdentifier}" })`)

        agentConfig = await Agent.findOne({ callerId: agentIdentifier }).lean()

        if (!agentConfig) {
          console.error(`❌ [AGENT-FETCH] No outbound agent found for callerId: "${agentIdentifier}"`)

          try {
            const availableAgents = await Agent.find({}, { callerId: 1, agentName: 1, clientId: 1 }).lean()
            console.log(`📊 [AGENT-FETCH] Available agents in database:`)
            availableAgents.forEach((agent, index) => {
              console.log(
                `   ${index + 1}. Agent: "${agent.agentName}" | CallerId: "${agent.callerId}" | ClientId: "${agent.clientId}"`,
              )
            })

            const partialMatches = availableAgents.filter(
              (agent) => agent.callerId && agent.callerId.includes(agentIdentifier),
            )

            if (partialMatches.length > 0) {
              console.log(`🔍 [AGENT-FETCH] Partial matches found:`)
              partialMatches.forEach((agent, index) => {
                console.log(`   ${index + 1}. Agent: "${agent.agentName}" | CallerId: "${agent.callerId}"`)
              })
            }
          } catch (debugError) {
            console.error(`❌ [AGENT-FETCH] Error fetching available agents: ${debugError.message}`)
          }

          return {
            success: false,
            error: `No outbound agent found for callerId: "${agentIdentifier}". Please check agent configuration.`,
            callType,
            agentIdentifier,
            customerNumber,
          }
        }

        console.log(`✅ [AGENT-FETCH] OUTBOUND agent found successfully!`)
        console.log(`   🏷️  Agent Name: "${agentConfig.agentName}"`)
        console.log(`   🆔 Client ID: "${agentConfig.clientId}"`)
        console.log(`   📞 Caller ID: "${agentConfig.callerId}"`)
        console.log(`   🌍 Language: "${agentConfig.language}"`)
        console.log(`   🎵 Voice: "${agentConfig.voiceSelection}"`)
        console.log(`   📝 Category: "${agentConfig.category}"`)
        console.log(`   👤 Personality: "${agentConfig.personality}"`)
        console.log(`   💬 First Message: "${agentConfig.firstMessage}"`)
      } else {
        console.log(`📥 [AGENT-FETCH] Searching for INBOUND agent with accountSid: "${agentIdentifier}"`)
        console.log(`🔍 [AGENT-FETCH] Database query: Agent.findOne({ accountSid: "${agentIdentifier}" })`)

        agentConfig = await Agent.findOne({ accountSid: agentIdentifier }).lean()

        if (!agentConfig) {
          console.error(`❌ [AGENT-FETCH] No inbound agent found for accountSid: "${agentIdentifier}"`)

          try {
            const availableAgents = await Agent.find({}, { accountSid: 1, agentName: 1, clientId: 1 }).lean()
            console.log(`📊 [AGENT-FETCH] Available agents in database:`)
            availableAgents.forEach((agent, index) => {
              console.log(
                `   ${index + 1}. Agent: "${agent.agentName}" | AccountSid: "${agent.accountSid}" | ClientId: "${agent.clientId}"`,
              )
            })

            const partialMatches = availableAgents.filter(
              (agent) => agent.accountSid && agent.accountSid.includes(agentIdentifier),
            )

            if (partialMatches.length > 0) {
              console.log(`🔍 [AGENT-FETCH] Partial matches found:`)
              partialMatches.forEach((agent, index) => {
                console.log(`   ${index + 1}. Agent: "${agent.agentName}" | AccountSid: "${agent.accountSid}"`)
              })
            }
          } catch (debugError) {
            console.error(`❌ [AGENT-FETCH] Error fetching available agents: ${debugError.message}`)
          }

          return {
            success: false,
            error: `No inbound agent found for accountSid: "${agentIdentifier}". Please check agent configuration.`,
            callType,
            agentIdentifier,
            customerNumber,
          }
        }

        console.log(`✅ [AGENT-FETCH] INBOUND agent found successfully!`)
        console.log(`   🏷️  Agent Name: "${agentConfig.agentName}"`)
        console.log(`   🆔 Client ID: "${agentConfig.clientId}"`)
        console.log(`   🏢 Account SID: "${agentConfig.accountSid}"`)
        console.log(`   🌍 Language: "${agentConfig.language}"`)
        console.log(`   🎵 Voice: "${agentConfig.voiceSelection}"`)
        console.log(`   📝 Category: "${agentConfig.category}"`)
        console.log(`   👤 Personality: "${agentConfig.personality}"`)
        console.log(`   💬 First Message: "${agentConfig.firstMessage}"`)
      }

      console.log(`📋 [AGENT-FETCH] Complete agent configuration loaded:`)
      console.log(`   🎯 STT Selection: "${agentConfig.sttSelection}"`)
      console.log(`   🔊 TTS Selection: "${agentConfig.ttsSelection}"`)
      console.log(`   🤖 LLM Selection: "${agentConfig.llmSelection}"`)
      console.log(`   📅 Created: ${agentConfig.createdAt}`)
      console.log(`   🔄 Updated: ${agentConfig.updatedAt}`)

      if (agentConfig.systemPrompt) {
        console.log(`   📝 System Prompt: "${agentConfig.systemPrompt.substring(0, 100)}..."`)
      }

      if (agentConfig.audioBytes) {
        console.log(`   🎵 Audio Bytes Length: ${agentConfig.audioBytes.length} characters`)
      }

      console.log(`🔍 [AGENT-FETCH] ==========================================\n`)

      return {
        success: true,
        agentConfig,
        callType,
        agentIdentifier,
        customerNumber,
      }
    } catch (error) {
      console.error(`❌ [AGENT-FETCH] Database error: ${error.message}`)
      console.error(`❌ [AGENT-FETCH] Stack trace:`, error.stack)

      return {
        success: false,
        error: `Database error: ${error.message}`,
        callType,
        agentIdentifier,
        customerNumber,
      }
    }
  }

  static validateAgentConfig(agentConfig) {
    const requiredFields = ["clientId", "agentName", "language", "firstMessage"]
    const missingFields = []

    for (const field of requiredFields) {
      if (!agentConfig[field]) {
        missingFields.push(field)
      }
    }

    if (missingFields.length > 0) {
      console.warn(`⚠️ [AGENT-VALIDATION] Missing required fields: ${missingFields.join(", ")}`)
      return { valid: false, missingFields }
    }

    console.log(`✅ [AGENT-VALIDATION] Agent configuration is valid`)
    return { valid: true, missingFields: [] }
  }

  static logCallerIdConnection(sipData, agentConfig) {
    const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData)
    const callType = SIPHeaderDecoder.determineCallType(sipData)
    const agentIdentifier = SIPHeaderDecoder.getAgentIdentifier(sipData)

    console.log(`\n🎯 [CALLER-ID-MATCH] ==========================================`)
    console.log(`📞 [CALLER-ID-MATCH] Call Type: ${callType}`)
    console.log(`📱 [CALLER-ID-MATCH] Customer Number: ${customerNumber}`)

    if (callType === "outbound") {
      console.log(`🔍 [CALLER-ID-MATCH] Searched for callerId: "${agentIdentifier}"`)
      console.log(`✅ [CALLER-ID-MATCH] Matched callerId: "${agentConfig.callerId}"`)
      console.log(`🎯 [CALLER-ID-MATCH] Connection Status: SUCCESSFUL - Agent "${agentConfig.agentName}" connected`)
    } else {
      console.log(`🔍 [CALLER-ID-MATCH] Searched for accountSid: "${agentIdentifier}"`)
      console.log(`✅ [CALLER-ID-MATCH] Matched accountSid: "${agentConfig.accountSid}"`)
      console.log(`🎯 [CALLER-ID-MATCH] Connection Status: SUCCESSFUL - Agent "${agentConfig.agentName}" connected`)
    }

    console.log(`🏷️  [CALLER-ID-MATCH] Agent Details:`)
    console.log(`   • Name: ${agentConfig.agentName}`)
    console.log(`   • Client ID: ${agentConfig.clientId}`)
    console.log(`   • Language: ${agentConfig.language}`)
    console.log(`   • Voice: ${agentConfig.voiceSelection}`)
    console.log(`   • Category: ${agentConfig.category}`)
    console.log(`🎯 [CALLER-ID-MATCH] ==========================================\n`)
  }
}

// MAIN: Enhanced WebSocket server setup with persistent Deepgram connections
const setupUnifiedVoiceServer = (wss) => {
  console.log("🚀 [OPTIMIZED] Voice Server started with persistent Deepgram connections")

  wss.on("connection", (ws, req) => {
    const connectionTime = new Date()
    const clientIP = req.socket.remoteAddress

    console.log(`\n🔗 [CONNECTION] ==========================================`)
    console.log(`🔗 [CONNECTION] New optimized WebSocket connection`)
    console.log(`🌐 [CONNECTION] Client IP: ${clientIP}`)
    console.log(`⏰ [CONNECTION] Time: ${connectionTime.toISOString()}`)
    console.log(`📡 [CONNECTION] User Agent: ${req.headers["user-agent"] || "unknown"}`)
    console.log(`🔗 [CONNECTION] URL: ${req.url || "unknown"}`)

    // Parse SIP data from connection URL
    let sipData = null
    try {
      if (req.url) {
        console.log(`🔍 [CONNECTION] Parsing URL for SIP parameters...`)
        sipData = SIPHeaderDecoder.parseConnectionURL(req.url)

        if (sipData) {
          console.log(`✅ [CONNECTION] SIP parameters detected - this is a SIP call`)
          SIPHeaderDecoder.logSIPData(sipData)
          ws.sipData = sipData

          const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData)
          const callType = SIPHeaderDecoder.determineCallType(sipData)
          const agentIdentifier = SIPHeaderDecoder.getAgentIdentifier(sipData)

          console.log(`🎯 [CONNECTION] Call Analysis:`)
          console.log(`   • Customer Number: ${customerNumber}`)
          console.log(`   • Call Type: ${callType}`)
          console.log(`   • Agent Identifier: ${agentIdentifier}`)
          console.log(`   • DID: ${sipData.did}`)
          console.log(`   • Session ID: ${sipData.session_id}`)
        } else {
          console.log(`ℹ️ [CONNECTION] Non-SIP WebSocket connection (no SIP parameters found)`)
        }
      } else {
        console.log(`ℹ️ [CONNECTION] No URL provided in connection request`)
      }
    } catch (error) {
      console.log(`ℹ️ [CONNECTION] Error parsing URL for SIP data: ${error.message}`)
    }

    console.log(`🔗 [CONNECTION] ==========================================\n`)

    // Session state
    let streamSid = null
    let conversationHistory = []
    let isProcessing = false
    let userUtteranceBuffer = ""
    let lastProcessedText = ""
    let optimizedTTS = null
    let currentLanguage = undefined
    let processingRequestId = 0
    let callLogger = null

    // OPTIMIZED: Single persistent Deepgram connection
    let deepgramManager = null

    // OPTIMIZED: Initialize persistent Deepgram manager
    const initializeDeepgramManager = (language, customerNumber) => {
      if (deepgramManager) {
        deepgramManager.cleanup()
      }

      deepgramManager = new PersistentDeepgramManager(
        language,
        async (transcript, is_final, confidence, type) => {
          await handleDeepgramResponse(transcript, is_final, confidence, type)
        },
        (error) => {
          console.error(`❌ [DEEPGRAM-MANAGER] Error for ${customerNumber}:`, error)
        },
        customerNumber,
      )

      return deepgramManager.connect()
    }

    // Enhanced Deepgram response handler
    const handleDeepgramResponse = async (transcript, is_final, confidence, type) => {
      const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"

      if (type === "utterance_end") {
        console.log(`🔚 [DEEPGRAM-PERSISTENT] Utterance end detected for ${customerNumber}`)

        if (userUtteranceBuffer.trim()) {
          if (callLogger && userUtteranceBuffer.trim()) {
            const detectedLang = await detectLanguageWithOpenAI(userUtteranceBuffer.trim())
            callLogger.logUserTranscript(userUtteranceBuffer.trim(), detectedLang)

            console.log(
              `📝 [UTTERANCE-END] Customer (${customerNumber}): "${userUtteranceBuffer.trim()}" (${detectedLang})`,
            )
          }

          await processUserUtterance(userUtteranceBuffer)
          userUtteranceBuffer = ""
        }
        return
      }

      if (transcript?.trim()) {
        console.log(
          `🎤 [DEEPGRAM-PERSISTENT] ${is_final ? "FINAL" : "interim"} transcript from ${customerNumber}: "${transcript}" (confidence: ${confidence || "unknown"})`,
        )

        // Interrupt current TTS if new speech detected
        if (optimizedTTS && (isProcessing || optimizedTTS.isProcessing)) {
          console.log(`🛑 [INTERRUPT] New speech from ${customerNumber} detected, interrupting current response`)
          optimizedTTS.interrupt()
          isProcessing = false
          processingRequestId++
        }

        if (is_final) {
          userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim()

          if (callLogger && transcript.trim()) {
            const detectedLang = await detectLanguageWithOpenAI(transcript.trim())
            callLogger.logUserTranscript(transcript.trim(), detectedLang)

            console.log(`📝 [TRANSCRIPT] Customer (${customerNumber}): "${transcript.trim()}" (${detectedLang})`)
          }

          await processUserUtterance(userUtteranceBuffer)
          userUtteranceBuffer = ""
        }
      }
    }

    // Enhanced utterance processing with SIP context logging
    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText) return

      if (optimizedTTS) {
        optimizedTTS.interrupt()
      }

      isProcessing = true
      lastProcessedText = text
      const currentRequestId = ++processingRequestId
      const timer = createTimer("UTTERANCE_PROCESSING")

      try {
        const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
        const callType = SIPHeaderDecoder.determineCallType(sipData)
        const agentName = ws.sessionAgentConfig?.agentName || "unknown"

        console.log(`\n🎤 [USER] ==========================================`)
        console.log(`🎤 [USER] Processing utterance from ${customerNumber}`)
        console.log(`📞 [USER] Call Type: ${callType}`)
        console.log(`🤖 [USER] Agent: ${agentName}`)
        console.log(`📝 [USER] Text: "${text}"`)
        console.log(`📍 [USER] DID: ${sipData?.did || "unknown"}`)
        console.log(`🆔 [USER] Session: ${sipData?.session_id || "unknown"}`)

        const detectedLanguage = await detectLanguageWithOpenAI(text)

        if (detectedLanguage !== currentLanguage) {
          console.log(`🌍 [LANGUAGE] Changed: ${currentLanguage} → ${detectedLanguage} for ${customerNumber}`)
          currentLanguage = detectedLanguage

          // Update Deepgram language if needed
          if (deepgramManager) {
            deepgramManager.updateLanguage(detectedLanguage)
          }
        }

        optimizedTTS = new OptimizedSarvamTTSProcessor(detectedLanguage, ws, streamSid, callLogger)

        const checkInterruption = () => {
          return processingRequestId !== currentRequestId
        }

        console.log(`🤖 [PROCESSING] Starting OpenAI processing for ${customerNumber}...`)

        const response = await processWithOpenAIStreaming(
          text,
          conversationHistory,
          detectedLanguage,
          (phrase, lang) => {
            if (processingRequestId === currentRequestId && !checkInterruption()) {
              console.log(`📤 [PHRASE] "${phrase}" (${lang}) -> ${customerNumber}`)
              optimizedTTS.addPhrase(phrase, lang)
            }
          },
          (fullResponse) => {
            if (processingRequestId === currentRequestId && !checkInterruption()) {
              console.log(`✅ [COMPLETE] "${fullResponse}" -> ${customerNumber}`)
              optimizedTTS.complete()

              const stats = optimizedTTS.getStats()
              console.log(
                `📊 [TTS-STATS] ${stats.totalChunks} chunks, ${stats.avgBytesPerChunk} avg bytes/chunk for ${customerNumber}`,
              )

              conversationHistory.push({ role: "user", content: text }, { role: "assistant", content: fullResponse })

              if (conversationHistory.length > 10) {
                conversationHistory = conversationHistory.slice(-10)
              }
            }
          },
          checkInterruption,
          callLogger,
        )

        console.log(`⚡ [TOTAL] Processing completed in ${timer.end()}ms for ${customerNumber}`)
        console.log(`🎤 [USER] ==========================================\n`)
      } catch (error) {
        const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
        console.error(`❌ [PROCESSING] Error for ${customerNumber}: ${error.message}`)
        console.error(`❌ [PROCESSING] Stack trace:`, error.stack)
      } finally {
        if (processingRequestId === currentRequestId) {
          isProcessing = false
        }
      }
    }

    // Enhanced WebSocket message handling
    ws.on("message", async (message) => {
      try {
        if (!message || message.length === 0) {
          const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
          console.log(`ℹ️ [MESSAGE] Received empty message from ${customerNumber}`)
          return
        }

        let messageString
        try {
          messageString = message.toString()
          if (!messageString || messageString.trim() === "") {
            const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
            console.log(`ℹ️ [MESSAGE] Received empty string message from ${customerNumber}`)
            return
          }
        } catch (error) {
          console.log(`⚠️ [MESSAGE] Failed to convert message to string: ${error.message}`)
          return
        }

        const isBinaryData = /[\x00-\x08\x0E-\x1F\x7F-\xFF]/.test(messageString)

        if (isBinaryData || messageString.includes("\x00") || messageString.includes("��")) {
          const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
          console.log(
            `🎵 [BINARY-DATA] Received binary audio data from ${customerNumber} (${messageString.length} bytes)`,
          )

          try {
            const audioBuffer = Buffer.from(message)
            console.log(
              `🎵 [AUDIO-STREAM] Processing raw audio buffer (${audioBuffer.length} bytes) from ${customerNumber}`,
            )

            if (deepgramManager) {
              const success = deepgramManager.sendAudio(audioBuffer)
              if (!success) {
                console.log(`⚠️ [AUDIO-STREAM] Failed to send audio to Deepgram for ${customerNumber}`)
              }
            } else {
              console.log(`⚠️ [AUDIO-STREAM] No Deepgram manager available for ${customerNumber}`)
            }
          } catch (audioError) {
            console.error(`❌ [AUDIO-STREAM] Error processing audio data from ${customerNumber}: ${audioError.message}`)
          }
          return
        }

        let data
        try {
          data = JSON.parse(messageString)
        } catch (jsonError) {
          const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"

          if (messageString.startsWith("<") || messageString.includes("HTTP/")) {
            console.log(
              `ℹ️ [MESSAGE] Received non-JSON message (likely HTTP/HTML) from ${customerNumber}: ${messageString.substring(0, 50)}...`,
            )
            return
          }

          if (messageString.includes("candidate") || messageString.includes("sdp")) {
            console.log(
              `ℹ️ [MESSAGE] Received WebRTC signaling data from ${customerNumber}: ${messageString.substring(0, 50)}...`,
            )
            return
          }

          if (messageString.includes("{") || messageString.includes("}")) {
            console.log(`⚠️ [MESSAGE] Partial/malformed JSON from ${customerNumber}: ${jsonError.message}`)
            console.log(`⚠️ [MESSAGE] Raw message (first 200 chars): "${messageString.substring(0, 200)}..."`)
          } else {
            console.log(`⚠️ [MESSAGE] Non-JSON message from ${customerNumber}: "${messageString.substring(0, 100)}..."`)
          }
          return
        }

        if (!data || typeof data !== "object" || !data.event) {
          const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
          console.log(`⚠️ [MESSAGE] Invalid message format from ${customerNumber}:`, data)
          return
        }

        let customerNumber

        switch (data.event) {
          case "connected":
            customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
            console.log(`🔗 [OPTIMIZED] Connected from ${customerNumber} - Protocol: ${data.protocol}`)
            console.log(`🔗 [OPTIMIZED] Version: ${data.version || "unknown"}`)
            break

          case "start": {
            streamSid = data.streamSid || data.start?.streamSid
            const accountSid = data.start?.accountSid
            customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData)
            const callType = SIPHeaderDecoder.determineCallType(sipData)

            console.log(`\n🎯 [OPTIMIZED] Stream started:`)
            console.log(`   • StreamSid: ${streamSid}`)
            console.log(`   • AccountSid: ${accountSid}`)
            console.log(`   • Customer Number: ${customerNumber}`)
            console.log(`   • Call Type: ${callType}`)

            if (sipData) {
              console.log(`   • SIP App ID: ${sipData.app_id}`)
              console.log(`   • SIP DID: ${sipData.did}`)
              console.log(`   • SIP Session ID: ${sipData.session_id}`)
              console.log(`   • SIP Direction: ${sipData.extra?.CallDirection || sipData.direction}`)
              console.log(`   • VA ID: ${sipData.extra?.CallVaId}`)
            }

            const fetchResult = await AgentConfigFetcher.fetchAgentConfig(sipData)

            if (!fetchResult.success) {
              console.error(`❌ [AGENT-CONFIG] ${fetchResult.error}`)

              const errorResponse = {
                event: "error",
                message: fetchResult.error,
                details: {
                  callType: fetchResult.callType,
                  agentIdentifier: fetchResult.agentIdentifier,
                  customerNumber: fetchResult.customerNumber,
                  sipData: sipData,
                },
              }

              ws.send(JSON.stringify(errorResponse))

              console.log(`🔌 [AGENT-CONFIG] Closing connection due to missing agent configuration`)
              ws.close()
              return
            }

            const agentConfig = fetchResult.agentConfig
            const detectedCallType = fetchResult.callType

            AgentConfigFetcher.logCallerIdConnection(sipData, agentConfig)

            const validation = AgentConfigFetcher.validateAgentConfig(agentConfig)
            if (!validation.valid) {
              console.warn(`⚠️ [AGENT-VALIDATION] Agent configuration has issues but continuing...`)
            }

            console.log(`✅ [AGENT-CONFIG] Successfully loaded for ${detectedCallType} call:`)
            console.log(`   • Client ID: ${agentConfig.clientId}`)
            console.log(`   • Agent Name: ${agentConfig.agentName}`)
            console.log(`   • Language: ${agentConfig.language}`)
            console.log(`   • Voice: ${agentConfig.voiceSelection}`)
            console.log(`   • STT: ${agentConfig.sttSelection}`)
            console.log(`   • TTS: ${agentConfig.ttsSelection}`)
            console.log(`   • LLM: ${agentConfig.llmSelection}`)

            if (detectedCallType === "outbound") {
              console.log(`   • Caller ID: ${agentConfig.callerId}`)
            } else {
              console.log(`   • Account SID: ${agentConfig.accountSid}`)
            }

            ws.sessionAgentConfig = agentConfig
            currentLanguage = agentConfig.language || "hi"

            callLogger = new CallLogger(agentConfig.clientId || accountSid, sipData)
            console.log(
              `📝 [CALL-LOG] Initialized for client: ${agentConfig.clientId}, customer: ${customerNumber}, call type: ${detectedCallType}`,
            )

            // OPTIMIZED: Initialize persistent Deepgram connection
            try {
              await initializeDeepgramManager(currentLanguage, customerNumber)
              console.log(`✅ [DEEPGRAM-PERSISTENT] Manager initialized for ${customerNumber}`)
            } catch (deepgramError) {
              console.error(
                `❌ [DEEPGRAM-PERSISTENT] Failed to initialize manager for ${customerNumber}: ${deepgramError.message}`,
              )
            }

            console.log(`\n🎉 [CONNECTION-SUCCESS] ==========================================`)
            console.log(`✅ [CONNECTION-SUCCESS] Agent "${agentConfig.agentName}" successfully connected!`)
            console.log(`📞 [CONNECTION-SUCCESS] Call Type: ${detectedCallType}`)
            console.log(`📱 [CONNECTION-SUCCESS] Customer: ${customerNumber}`)
            console.log(
              `🏷️  [CONNECTION-SUCCESS] Matched ID: ${detectedCallType === "outbound" ? agentConfig.callerId : agentConfig.accountSid}`,
            )
            console.log(`🎉 [CONNECTION-SUCCESS] ==========================================\n`)

            const greeting = agentConfig.firstMessage || "Hello! How can I help you today?"
            console.log(`👋 [GREETING] "${greeting}" -> ${customerNumber || "unknown customer"} (${detectedCallType})`)

            if (callLogger) {
              callLogger.logAIResponse(greeting, currentLanguage)
            }

            const tts = new OptimizedSarvamTTSProcessor(currentLanguage, ws, streamSid, callLogger)
            await tts.synthesizeAndStream(greeting)
            break
          }

          case "media":
            customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"

            if (data.media?.payload) {
              try {
                const audioBuffer = Buffer.from(data.media.payload, "base64")
                console.log(`🎵 [MEDIA] Received ${audioBuffer.length} bytes from ${customerNumber}`)

                if (deepgramManager) {
                  const success = deepgramManager.sendAudio(audioBuffer)
                  if (success) {
                    console.log(`📤 [MEDIA] Sent to persistent Deepgram for ${customerNumber}`)
                  } else {
                    console.log(`⚠️ [MEDIA] Failed to send to Deepgram for ${customerNumber}`)
                  }
                } else {
                  console.log(`⚠️ [MEDIA] No Deepgram manager available for ${customerNumber}`)
                }
              } catch (mediaError) {
                console.error(`❌ [MEDIA] Error processing media from ${customerNumber}: ${mediaError.message}`)
              }
            } else {
              console.log(`⚠️ [MEDIA] No payload in media message from ${customerNumber}`)
            }
            break

          case "stop":
            customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
            const callType = SIPHeaderDecoder.determineCallType(sipData)
            console.log(`\n📞 [OPTIMIZED] Stream stopped for ${customerNumber} (${callType} call)`)

            if (callLogger) {
              try {
                const savedLog = await callLogger.saveToDatabase("completed")
                console.log(`💾 [CALL-LOG] Final save completed - ID: ${savedLog._id}`)

                const stats = callLogger.getStats()
                console.log(`\n📊 [FINAL-STATS] Call Summary:`)
                console.log(`   • Duration: ${stats.duration}s`)
                console.log(`   • User Messages: ${stats.userMessages}`)
                console.log(`   • AI Responses: ${stats.aiResponses}`)
                console.log(`   • Languages: ${stats.languages.join(", ")}`)
                console.log(`   • Customer: ${customerNumber}`)
                console.log(`   • DID: ${sipData?.did || "unknown"}`)
                console.log(`   • Call Type: ${stats.callType}`)
                console.log(`   • Session ID: ${sipData?.session_id || "unknown"}`)

                if (sipData?.extra) {
                  console.log(`   • VA ID: ${sipData.extra.CallVaId || "unknown"}`)
                  console.log(`   • Service App ID: ${sipData.extra.CZSERVICEAPPID || "unknown"}`)
                }
              } catch (error) {
                console.error(`❌ [CALL-LOG] Failed to save final log: ${error.message}`)
              }
            }

            // OPTIMIZED: Cleanup persistent Deepgram connection
            if (deepgramManager) {
              deepgramManager.disconnect()
              console.log(`🔌 [DEEPGRAM-PERSISTENT] Disconnected for ${customerNumber}`)
            }
            break

          case "mark":
            customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
            console.log(`🏁 [MARK] Received mark event from ${customerNumber}: ${data.mark?.name || "unnamed"}`)
            break

          default:
            customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
            console.log(`❓ [OPTIMIZED] Unknown event: ${data.event} from ${customerNumber}`)
            console.log(`❓ [OPTIMIZED] Event data:`, JSON.stringify(data, null, 2))
        }
      } catch (error) {
        const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
        console.error(`❌ [OPTIMIZED] Unexpected error processing message from ${customerNumber}: ${error.message}`)
        console.error(`❌ [OPTIMIZED] Stack trace:`, error.stack)

        try {
          const messagePreview = message.toString().substring(0, 200)
          console.error(`❌ [OPTIMIZED] Problematic message preview: "${messagePreview}..."`)
        } catch (previewError) {
          console.error(`❌ [OPTIMIZED] Could not preview message: ${previewError.message}`)
        }
      }
    })

    // Enhanced connection cleanup with persistent Deepgram cleanup
    ws.on("close", async (code, reason) => {
      const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
      const callType = SIPHeaderDecoder.determineCallType(sipData)
      const agentName = ws.sessionAgentConfig?.agentName || "unknown"
      const connectionDuration = Date.now() - connectionTime.getTime()

      console.log(`\n🔗 [DISCONNECT] ==========================================`)
      console.log(`🔗 [DISCONNECT] Connection closed for ${customerNumber}`)
      console.log(`📞 [DISCONNECT] Call Type: ${callType}`)
      console.log(`🤖 [DISCONNECT] Agent: ${agentName}`)
      console.log(`⏰ [DISCONNECT] Connection Duration: ${Math.round(connectionDuration / 1000)}s`)
      console.log(`🔢 [DISCONNECT] Close Code: ${code || "unknown"}`)
      console.log(`📝 [DISCONNECT] Close Reason: ${reason || "no reason provided"}`)

      if (sipData) {
        console.log(`📍 [DISCONNECT] DID: ${sipData.did}`)
        console.log(`🆔 [DISCONNECT] Session ID: ${sipData.session_id}`)
        console.log(`🏢 [DISCONNECT] App ID: ${sipData.app_id}`)

        if (sipData.extra) {
          console.log(`📱 [DISCONNECT] VA ID: ${sipData.extra.CallVaId}`)
          console.log(`🔄 [DISCONNECT] Call Direction: ${sipData.extra.CallDirection}`)
        }
      }

      if (callLogger) {
        try {
          console.log(`💾 [DISCONNECT] Saving call log for ${customerNumber}...`)

          let disconnectReason = "disconnected"
          if (code === 1000) disconnectReason = "normal_closure"
          else if (code === 1001) disconnectReason = "going_away"
          else if (code === 1006) disconnectReason = "abnormal_closure"
          else if (code === 1011) disconnectReason = "server_error"

          const savedLog = await callLogger.saveToDatabase(disconnectReason)

          console.log(`✅ [DISCONNECT] Call log saved successfully`)
          console.log(`   • Log ID: ${savedLog._id}`)
          console.log(`   • Customer: ${customerNumber}`)
          console.log(`   • Agent: ${agentName}`)
          console.log(`   • Call Type: ${callType}`)
          console.log(`   • Status: ${disconnectReason}`)

          const stats = callLogger.getStats()
          console.log(`📊 [DISCONNECT] Final Call Statistics:`)
          console.log(`   • Total Duration: ${stats.duration}s`)
          console.log(`   • User Messages: ${stats.userMessages}`)
          console.log(`   • AI Responses: ${stats.aiResponses}`)
          console.log(`   • Languages Used: ${stats.languages.join(", ")}`)
          console.log(`   • Customer Number: ${stats.customerNumber}`)
          console.log(`   • Call Type: ${stats.callType}`)
        } catch (error) {
          console.error(`❌ [DISCONNECT] Failed to save call log for ${customerNumber}: ${error.message}`)
          console.error(`❌ [DISCONNECT] Call log error details:`, error.stack)
        }
      } else {
        console.log(`ℹ️ [DISCONNECT] No call logger instance found for ${customerNumber}`)
      }

      // OPTIMIZED: Cleanup persistent Deepgram connection
      if (deepgramManager) {
        console.log(`🔌 [DISCONNECT] Cleaning up persistent Deepgram connection for ${customerNumber}`)
        deepgramManager.cleanup()

        const status = deepgramManager.getStatus()
        console.log(
          `📊 [DISCONNECT] Final Deepgram status: Connected: ${status.isConnected}, Queue: ${status.queueSize}, Reconnects: ${status.reconnectAttempts}`,
        )
      } else {
        console.log(`ℹ️ [DISCONNECT] No Deepgram manager found for ${customerNumber}`)
      }

      if (optimizedTTS) {
        console.log(`🔊 [DISCONNECT] Interrupting TTS processor for ${customerNumber}`)
        optimizedTTS.interrupt()
      }

      // Reset all state variables
      streamSid = null
      conversationHistory = []
      isProcessing = false
      userUtteranceBuffer = ""
      lastProcessedText = ""
      optimizedTTS = null
      currentLanguage = undefined
      processingRequestId = 0
      callLogger = null
      deepgramManager = null

      console.log(`🧹 [DISCONNECT] Cleaned up all session data for ${customerNumber}`)
      console.log(`🔗 [DISCONNECT] ==========================================\n`)
    })

    ws.on("error", (error) => {
      const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
      const callType = SIPHeaderDecoder.determineCallType(sipData)
      const agentName = ws.sessionAgentConfig?.agentName || "unknown"

      console.log(`\n❌ [ERROR] ==========================================`)
      console.error(`❌ [ERROR] WebSocket error for ${customerNumber}`)
      console.error(`📞 [ERROR] Call Type: ${callType}`)
      console.error(`🤖 [ERROR] Agent: ${agentName}`)
      console.error(`📝 [ERROR] Error Message: ${error.message}`)
      console.error(`🔍 [ERROR] Error Code: ${error.code || "unknown"}`)

      if (sipData) {
        console.error(`📍 [ERROR] DID: ${sipData.did}`)
        console.error(`🆔 [ERROR] Session ID: ${sipData.session_id}`)
      }

      console.error(`📚 [ERROR] Stack Trace:`, error.stack)
      console.log(`❌ [ERROR] ==========================================\n`)

      if (callLogger) {
        callLogger.saveToDatabase("error").catch((logError) => {
          console.error(`❌ [ERROR] Failed to save emergency call log: ${logError.message}`)
        })
      }

      // OPTIMIZED: Cleanup Deepgram on error
      if (deepgramManager) {
        console.log(`🔌 [ERROR] Cleaning up Deepgram manager due to error for ${customerNumber}`)
        deepgramManager.cleanup()
      }
    })

    // Optional: Add ping/pong heartbeat for connection monitoring
    const heartbeatInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
        console.log(`💓 [HEARTBEAT] Connection alive for ${customerNumber}`)

        // Also check Deepgram status
        if (deepgramManager) {
          const status = deepgramManager.getStatus()
          console.log(
            `💓 [HEARTBEAT] Deepgram status for ${customerNumber}: Connected: ${status.isConnected}, Queue: ${status.queueSize}`,
          )
        }

        ws.ping()
      } else {
        clearInterval(heartbeatInterval)
      }
    }, 30000)

    ws.on("pong", () => {
      const customerNumber = SIPHeaderDecoder.getCustomerNumber(sipData) || "unknown"
      console.log(`💓 [PONG] Heartbeat response from ${customerNumber}`)
    })

    ws.on("close", () => {
      clearInterval(heartbeatInterval)
    })
  })
}

module.exports = {
  setupUnifiedVoiceServer,
  SIPHeaderDecoder,
  CallLogger,
  AgentConfigFetcher,
  PersistentDeepgramManager,
}
