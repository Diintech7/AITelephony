const WebSocket = require("ws")
require("dotenv").config()

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

// Utility function to validate and analyze audio data
const validateAudioData = (base64Payload, chunkDurationMs = 20) => {
  try {
    const audioBuffer = Buffer.from(base64Payload, "base64")
    
    // Basic validation
    if (audioBuffer.length === 0) {
      return { valid: false, error: "Empty audio buffer" }
    }
    
    if (audioBuffer.length > 1000000) { // 1MB limit
      return { valid: false, error: "Audio buffer too large" }
    }
    
    // Calculate expected size for 20ms of 8kHz 16-bit mono audio
    const expectedBytesPerMs = (8000 * 2) / 1000 // 16 bytes per ms
    const expectedSize = Math.floor(chunkDurationMs * expectedBytesPerMs)
    const sizeRatio = audioBuffer.length / expectedSize
    
    // Check if size is reasonable (within 50% of expected)
    const sizeValid = sizeRatio >= 0.5 && sizeRatio <= 2.0
    
    // Analyze first few bytes to detect format
    const header = audioBuffer.slice(0, 16).toString('hex')
    const isLikelyPCM = !header.includes('52494646') && !header.includes('494433') // Not WAV or MP3
    
    return {
      valid: true,
      bufferSize: audioBuffer.length,
      expectedSize: expectedSize,
      sizeRatio: sizeRatio.toFixed(2),
      sizeValid: sizeValid,
      header: header,
      isLikelyPCM: isLikelyPCM,
      chunkDurationMs: chunkDurationMs
    }
  } catch (error) {
    return { valid: false, error: error.message }
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
  
  return francResult
}

// STATIC CONFIGURATION - No database integration
const STATIC_CONFIG = {
  language: "en",
  voiceSelection: "pavithra",
  firstMessage: "Hello! Welcome to our AI assistant. How can I help you today?",
  systemPrompt: "You are a helpful and friendly AI assistant. You assist users with their queries in a professional manner. Keep your responses concise and helpful. Always end with a relevant follow-up question to keep the conversation engaging.",
  clientId: "static-client",
  agentName: "Static AI Assistant",
  isActive: true
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
  }

  // Create initial call log entry immediately when call starts
  async createInitialCallLog(agentId = null, leadStatusInput = 'not_connected') {
    const timer = createTimer("INITIAL_CALL_LOG_CREATE")
    try {
      // Static implementation - just log the creation
      const callLogId = `call_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
      this.callLogId = callLogId
      this.isCallLogCreated = true

      // Add to active call loggers map for manual termination
      if (this.streamSid) {
        activeCallLoggers.set(this.streamSid, this)
        console.log(`📋 [ACTIVE-CALL-LOGGERS] Added call logger for streamSid: ${this.streamSid}`)
      }

      console.log(`🕐 [INITIAL-CALL-LOG] ${timer.end()}ms - Created: ${callLogId}`)
      return { _id: callLogId }
    } catch (error) {
      console.log(`❌ [INITIAL-CALL-LOG] ${timer.end()}ms - Error: ${error.message}`)
      throw error
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

  // Save pending transcripts in background (non-blocking) - Static implementation
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

    // Save asynchronously without awaiting (fire and forget) - Static implementation
    setImmediate(async () => {
      const timer = createTimer("LIVE_TRANSCRIPT_BATCH_SAVE")
      try {
        const currentTranscript = this.generateFullTranscript()
        const currentDuration = Math.round((new Date() - this.callStartTime) / 1000)
        
        // Static implementation - just log the save operation
        console.log(`📝 [STATIC-SAVE] Transcript batch saved - ${transcriptsToSave.length} entries`)
        console.log(`📝 [STATIC-SAVE] Duration: ${currentDuration}s, User: ${this.transcripts.length}, AI: ${this.responses.length}`)

        console.log(`🕐 [LIVE-TRANSCRIPT-SAVE] ${timer.end()}ms - Saved ${transcriptsToSave.length} entries (static mode)`)
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

  // Final save with complete call data - Static implementation
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

      // Static implementation - just log the final save
      const finalTranscript = this.generateFullTranscript()
      
      console.log(`📝 [STATIC-FINAL-SAVE] Call completed:`)
      console.log(`📝 [STATIC-FINAL-SAVE] Duration: ${this.totalDuration}s`)
      console.log(`📝 [STATIC-FINAL-SAVE] User messages: ${this.transcripts.length}`)
      console.log(`📝 [STATIC-FINAL-SAVE] AI responses: ${this.responses.length}`)
      console.log(`📝 [STATIC-FINAL-SAVE] Lead Status: ${leadStatusInput}`)
      console.log(`📝 [STATIC-FINAL-SAVE] Full Transcript:\n${finalTranscript}`)

      console.log(`🕐 [FINAL-CALL-LOG-SAVE] ${timer.end()}ms - Logged: ${this.callLogId} (static mode)`)
      return { _id: this.callLogId, transcript: finalTranscript, duration: this.totalDuration }
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

// Simplified OpenAI processing - Static configuration
const processWithOpenAI = async (
  userMessage,
  conversationHistory,
  detectedLanguage,
  callLogger,
  agentConfig,
  userName = null,
) => {
  const timer = createTimer("LLM_PROCESSING")
  console.log("🤖 [OPENAI-PROCESSING] Starting OpenAI processing")
  console.log("🤖 [OPENAI-PROCESSING] User message:", userMessage)
  console.log("🤖 [OPENAI-PROCESSING] Detected language:", detectedLanguage)
  console.log("🤖 [OPENAI-PROCESSING] User name:", userName)
  console.log("🤖 [OPENAI-PROCESSING] Conversation history length:", conversationHistory.length)

  try {
    // Use static configuration
    const basePrompt = STATIC_CONFIG.systemPrompt
    const firstMessage = STATIC_CONFIG.firstMessage
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

    console.log("🤖 [OPENAI-PROCESSING] Sending request to OpenAI API")
    console.log("🤖 [OPENAI-PROCESSING] Request messages:", JSON.stringify(messages, null, 2))
    
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
    
    console.log("🤖 [OPENAI-PROCESSING] OpenAI API response status:", response.status)

    if (!response.ok) {
      console.log(`❌ [LLM-PROCESSING] ${timer.end()}ms - Error: ${response.status}`)
      return null
    }

    const data = await response.json()
    console.log("🤖 [OPENAI-PROCESSING] OpenAI API response data:", JSON.stringify(data, null, 2))
    let fullResponse = data.choices[0]?.message?.content?.trim()
    console.log("🤖 [OPENAI-PROCESSING] Extracted response:", fullResponse)

    console.log(`🕐 [LLM-PROCESSING] ${timer.end()}ms - Response generated`)

    // Ensure a follow-up question is present at the end
    if (fullResponse) {
      const needsFollowUp = !/[?]\s*$/.test(fullResponse)
      console.log("🤖 [OPENAI-PROCESSING] Response needs follow-up question:", needsFollowUp)
      
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
        console.log("🤖 [OPENAI-PROCESSING] Adding follow-up question:", fu)
        fullResponse = `${fullResponse} ${fu}`.trim()
        console.log("🤖 [OPENAI-PROCESSING] Final response with follow-up:", fullResponse)
      }
    }

    if (callLogger && fullResponse) {
      console.log("🤖 [OPENAI-PROCESSING] Logging AI response to call logger")
      callLogger.logAIResponse(fullResponse, detectedLanguage)
    }

    console.log("🤖 [OPENAI-PROCESSING] Returning final response:", fullResponse)
    return fullResponse
  } catch (error) {
    console.log(`❌ [LLM-PROCESSING] ${timer.end()}ms - Error: ${error.message}`)
    console.log(`❌ [LLM-PROCESSING] Error stack: ${error.stack}`)
    return null
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
    this.voice = getValidSarvamVoice(STATIC_CONFIG.voiceSelection)
    this.isInterrupted = false
    this.currentAudioStreaming = null
    this.totalAudioBytes = 0
  }

  interrupt() {
    this.isInterrupted = true
    if (this.currentAudioStreaming) {
      this.currentAudioStreaming.interrupt = true
    }
  }

  reset(newLanguage) {
    this.interrupt()
    if (newLanguage) {
      this.language = newLanguage
      this.sarvamLanguage = getSarvamLanguage(newLanguage)
    }
    this.isInterrupted = false
    this.totalAudioBytes = 0
  }

  async synthesizeAndStream(text) {
    if (this.isInterrupted) return

    const timer = createTimer("TTS_SYNTHESIS")
    console.log("🎤 [TTS-SYNTHESIS] Starting TTS synthesis for text:", text)
    console.log("🎤 [TTS-SYNTHESIS] Language:", this.sarvamLanguage)
    console.log("🎤 [TTS-SYNTHESIS] Voice:", this.voice)

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
          enable_preprocessing: false,
          enable_preprocessing: true,
          model: "bulbul:v1",
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
      console.log("🎤 [TTS-SYNTHESIS] Sarvam API response:", JSON.stringify(responseData, null, 2))
      const audioBase64 = responseData.audios?.[0]
      console.log("🎤 [TTS-SYNTHESIS] Audio base64 length:", audioBase64?.length || 0)

      if (!audioBase64 || this.isInterrupted) {
        if (!this.isInterrupted) {
          console.log(`❌ [TTS-SYNTHESIS] ${timer.end()}ms - No audio data received`)
          throw new Error("No audio data received from Sarvam API")
        }
        return
      }

      console.log(`🕐 [TTS-SYNTHESIS] ${timer.end()}ms - Audio generated`)

      if (!this.isInterrupted) {
        await this.streamAudioOptimizedForSIP(audioBase64)
        const audioBuffer = Buffer.from(audioBase64, "base64")
        this.totalAudioBytes += audioBuffer.length
      }
    } catch (error) {
      if (!this.isInterrupted) {
        console.log(`❌ [TTS-SYNTHESIS] ${timer.end()}ms - Error: ${error.message}`)
        throw error
      }
    }
  }

  async streamAudioOptimizedForSIP(audioBase64) {
    if (this.isInterrupted) return

    console.log("🎤 [TTS-STREAMING] Starting audio streaming")
    console.log("🎤 [TTS-STREAMING] Base64 audio length:", audioBase64.length)
    
    const audioBuffer = Buffer.from(audioBase64, "base64")
    console.log("🎤 [TTS-STREAMING] Audio buffer size:", audioBuffer.length, "bytes")
    
    const streamingSession = { interrupt: false }
    this.currentAudioStreaming = streamingSession

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

      if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted) {
        try {
          console.log("🎤 [TTS-STREAMING] Sending media chunk:", chunkIndex + 1, "size:", chunk.length, "bytes")
          console.log("🎤 [TTS-STREAMING] Media message:", JSON.stringify(mediaMessage, null, 2))
          this.ws.send(JSON.stringify(mediaMessage))
          successfulChunks++
        } catch (error) {
          console.log("❌ [TTS-STREAMING] Error sending media chunk:", error.message)
          break
        }
      } else {
        console.log("⚠️ [TTS-STREAMING] WebSocket not open or interrupted, stopping streaming")
        break
      }

      if (position + chunkSize < audioBuffer.length && !this.isInterrupted) {
        const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS)
        const delayMs = Math.max(chunkDurationMs - 2, 10)
        await new Promise((resolve) => setTimeout(resolve, delayMs))
      }

      position += chunkSize
      chunkIndex++
    }

    console.log("🎤 [TTS-STREAMING] Streaming completed")
    console.log("🎤 [TTS-STREAMING] Total chunks sent:", successfulChunks)
    console.log("🎤 [TTS-STREAMING] Total audio bytes:", this.totalAudioBytes)
    this.currentAudioStreaming = null
  }

  getStats() {
    return {
      totalAudioBytes: this.totalAudioBytes,
    }
  }
}

// Static agent configuration - No database lookup needed
const getStaticAgentConfig = () => {
  return {
    ...STATIC_CONFIG,
    _id: "static_agent_001",
    accountSid: "static_account",
    callerId: "static_caller"
  }
}

// Main WebSocket server setup with enhanced live transcript functionality
const setupSanPbxWebSocketServer = (wss) => {
  console.log('🚀 [SANPBX-WS] Setting up enhanced SanIPPBX WebSocket server with AI integration...')
  
  wss.on("connection", (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`)
    const urlParams = Object.fromEntries(url.searchParams.entries())

    // Log connection details
    console.log("🔗 [SANPBX-CONNECTION] ========== NEW CONNECTION ==========")
    console.log("🔗 [SANPBX-CONNECTION] Client IP:", req.socket.remoteAddress)
    console.log("🔗 [SANPBX-CONNECTION] User Agent:", req.headers["user-agent"])
    console.log("🔗 [SANPBX-CONNECTION] URL:", req.url)
    console.log("🔗 [SANPBX-CONNECTION] URL Parameters:", JSON.stringify(urlParams, null, 2))
    console.log("🔗 [SANPBX-CONNECTION] Headers:", JSON.stringify(req.headers, null, 2))
    console.log("🔗 [SANPBX-CONNECTION] ======================================")

    // Session state
    let streamSid = null
    let conversationHistory = []
    let isProcessing = false
    let userUtteranceBuffer = ""
    let lastProcessedText = ""
    let currentTTS = null
    let currentLanguage = STATIC_CONFIG.language // Use static language
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

        console.log("🎤 [DEEPGRAM-CONNECT] Connecting to Deepgram with URL:", deepgramUrl.toString())
        console.log("🎤 [DEEPGRAM-CONNECT] Language:", deepgramLanguage)
        console.log("🎤 [DEEPGRAM-CONNECT] Sample Rate: 8000")
        console.log("🎤 [DEEPGRAM-CONNECT] Channels: 1")
        console.log("🎤 [DEEPGRAM-CONNECT] Encoding: linear16")
        console.log("🎤 [DEEPGRAM-CONNECT] Model: nova-2")

        deepgramWs = new WebSocket(deepgramUrl.toString(), {
          headers: { Authorization: `Token ${API_KEYS.deepgram}` },
        })

        deepgramWs.onopen = () => {
          console.log("🎤 [DEEPGRAM] Connection established")
          console.log("🎤 [DEEPGRAM] WebSocket ready state:", deepgramWs.readyState)
          deepgramReady = true
          console.log("🎤 [DEEPGRAM] Processing queued audio packets:", deepgramAudioQueue.length)
          
          if (deepgramAudioQueue.length > 0) {
            console.log("🎤 [DEEPGRAM] Sending queued audio packets to Deepgram...")
            deepgramAudioQueue.forEach((buffer, index) => {
              console.log(`🎤 [DEEPGRAM] Sending queued packet ${index + 1}/${deepgramAudioQueue.length}, size: ${buffer.length} bytes`)
              deepgramWs.send(buffer)
            })
            deepgramAudioQueue = []
            console.log("🎤 [DEEPGRAM] All queued audio packets sent")
          }
        }

        deepgramWs.onmessage = async (event) => {
          try {
            const data = JSON.parse(event.data)
            console.log("🎤 [DEEPGRAM-RESPONSE] Received from Deepgram:", JSON.stringify(data, null, 2))
            console.log("🎤 [DEEPGRAM-RESPONSE] Event type:", data.type)
            console.log("🎤 [DEEPGRAM-RESPONSE] Is final:", data.is_final)
            
            if (data.channel?.alternatives?.[0]?.transcript) {
              console.log("🎤 [DEEPGRAM-RESPONSE] Transcript:", data.channel.alternatives[0].transcript)
            }
            
            await handleDeepgramResponse(data)
          } catch (error) {
            console.log("❌ [DEEPGRAM-RESPONSE] Error parsing Deepgram response:", error.message)
            console.log("❌ [DEEPGRAM-RESPONSE] Raw response:", event.data)
          }
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
        console.log("❌ [DEEPGRAM-CONNECT] Error:", error.message)
      }
    }

    const handleDeepgramResponse = async (data) => {
      if (data.type === "Results") {
        if (!sttTimer) {
          sttTimer = createTimer("STT_TRANSCRIPTION")
        }

        const transcript = data.channel?.alternatives?.[0]?.transcript
        const is_final = data.is_final

        console.log(`🎤 [DEEPGRAM-RESULTS] Type: ${data.type}, Final: ${is_final}`)
        console.log(`🎤 [DEEPGRAM-RESULTS] Transcript: "${transcript}"`)
        console.log(`🎤 [DEEPGRAM-RESULTS] Full Data:`, JSON.stringify(data, null, 2))

        if (transcript?.trim()) {
          if (currentTTS && isProcessing) {
            console.log("🛑 [DEEPGRAM-RESULTS] Interrupting current TTS...")
            currentTTS.interrupt()
            isProcessing = false
            processingRequestId++
          }

          if (is_final) {
            console.log(`🕐 [STT-TRANSCRIPTION] ${sttTimer.end()}ms - Final Text: "${transcript.trim()}"`)
            sttTimer = null

            userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim()

            if (callLogger && transcript.trim()) {
              const detectedLang = detectLanguageWithFranc(transcript.trim(), currentLanguage || "en")
              console.log(`🌐 [LANGUAGE-DETECTION] Detected language: ${detectedLang}`)
              callLogger.logUserTranscript(transcript.trim(), detectedLang)
            }

            await processUserUtterance(userUtteranceBuffer)
            userUtteranceBuffer = ""
          }
        }
      } else if (data.type === "UtteranceEnd") {
        console.log(`🎤 [DEEPGRAM-UTTERANCE-END] Utterance ended, buffer: "${userUtteranceBuffer.trim()}"`)
        
        if (sttTimer) {
          console.log(`🕐 [STT-TRANSCRIPTION] ${sttTimer.end()}ms - Text: "${userUtteranceBuffer.trim()}"`)
          sttTimer = null
        }

        if (userUtteranceBuffer.trim()) {
          if (callLogger && userUtteranceBuffer.trim()) {
            const detectedLang = detectLanguageWithFranc(userUtteranceBuffer.trim(), currentLanguage || "en")
            console.log(`🌐 [LANGUAGE-DETECTION] Utterance end - Detected language: ${detectedLang}`)
            callLogger.logUserTranscript(userUtteranceBuffer.trim(), detectedLang)
          }

          await processUserUtterance(userUtteranceBuffer)
          userUtteranceBuffer = ""
        }
      } else {
        console.log(`🎤 [DEEPGRAM-OTHER] Other event type: ${data.type}`, JSON.stringify(data, null, 2))
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

        console.log("🤖 [USER-UTTERANCE] Processing with OpenAI...")
        const aiResponse = await processWithOpenAI(
          text,
          conversationHistory,
          detectedLanguage,
          callLogger,
          agentConfig,
          userName,
        )

        if (processingRequestId === currentRequestId && aiResponse) {
          console.log("🤖 [USER-UTTERANCE] AI Response:", aiResponse)
          console.log("🎤 [USER-UTTERANCE] Starting TTS...")
          
          currentTTS = new SimplifiedSarvamTTSProcessor(detectedLanguage, ws, streamSid, callLogger)
          await currentTTS.synthesizeAndStream(aiResponse)

          conversationHistory.push(
            { role: "user", content: text },
            { role: "assistant", content: aiResponse }
          )

          if (conversationHistory.length > 10) {
            conversationHistory = conversationHistory.slice(-10)
          }
          
          console.log("✅ [USER-UTTERANCE] Processing completed")
        } else {
          console.log("⭕ [USER-UTTERANCE] Processing skipped (newer request in progress)")
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
        console.log("📨 [SANPBX-MESSAGE] ========== INCOMING MESSAGE ==========")
        console.log("📨 [SANPBX-MESSAGE] Raw message:", messageStr)
        console.log("📨 [SANPBX-MESSAGE] Message length:", messageStr.length)
        console.log("📨 [SANPBX-MESSAGE] Message type:", typeof messageStr)

        if (messageStr === "EOS" || messageStr === "BOS" || !messageStr.startsWith("{")) {
          console.log("📨 [SANPBX-MESSAGE] Skipping non-JSON message:", messageStr)
          console.log("📨 [SANPBX-MESSAGE] ======================================")
          return
        }

        const data = JSON.parse(messageStr)
        console.log("📨 [SANPBX-MESSAGE] Parsed JSON data:", JSON.stringify(data, null, 2))
        console.log("📨 [SANPBX-MESSAGE] Event type:", data.event)
        console.log("📨 [SANPBX-MESSAGE] ======================================")

        switch (data.event) {
          case "connected":
            console.log("🔗 [SANPBX-CONNECTED] ========== CONNECTED EVENT ==========")
            console.log("🔗 [SANPBX-CONNECTED] Full data:", JSON.stringify(data, null, 2))
            console.log("🔗 [SANPBX-CONNECTED] ======================================")
            break

          case "start": {
            console.log("📞 [SANPBX-START] ========== CALL START EVENT ==========")
            console.log("📞 [SANPBX-START] Full start data:", JSON.stringify(data, null, 2))
            
            streamSid = data.streamSid || data.start?.streamSid
            const accountSid = data.start?.accountSid

            // Log all incoming SIP data
            console.log("📞 [SANPBX-START] ========== CALL START DATA ==========")
            console.log("📞 [SANPBX-START] Raw data:", JSON.stringify(data, null, 2))
            console.log("📞 [SANPBX-START] URL Parameters:", JSON.stringify(urlParams, null, 2))
            console.log("📞 [SANPBX-START] StreamSID:", streamSid)
            console.log("📞 [SANPBX-START] AccountSID:", accountSid)
            console.log("📞 [SANPBX-START] Data.start object:", JSON.stringify(data.start, null, 2))
            console.log("📞 [SANPBX-START] Data.start.from:", data.start?.from)
            console.log("📞 [SANPBX-START] Data.start.to:", data.start?.to)
            console.log("📞 [SANPBX-START] Data.start.callSid:", data.start?.callSid)
            console.log("📞 [SANPBX-START] Data.start.CallSid:", data.start?.CallSid)
            console.log("📞 [SANPBX-START] Data.start.extraData:", JSON.stringify(data.start?.extraData, null, 2))
            console.log("📞 [SANPBX-START] Data.start.extraData (raw):", data.start?.extraData)

            let mobile = null;
            let callerId = null;
            let customParams = {};
            let czdataDecoded = null;
            
            if (urlParams.czdata) {
              console.log("📞 [SANPBX-START] Found czdata in URL params:", urlParams.czdata)
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
                console.log("📞 [SANPBX-START] Decoded czdata customParams:", JSON.stringify(customParams, null, 2));
                if (userName) {
                  console.log("📞 [SANPBX-START] User Name (czdata):", userName);
                }
              } else {
                console.log("📞 [SANPBX-START] Failed to decode czdata")
              }
            }

            if (data.start?.from) {
              mobile = data.start.from;
              console.log("📞 [SANPBX-START] Mobile from data.start.from:", mobile)
            } else if (urlParams.caller_id) {
              mobile = urlParams.caller_id;
              console.log("📞 [SANPBX-START] Mobile from URL caller_id:", mobile)
            } else if (data.start?.extraData?.CallCli) {
              mobile = data.start.extraData.CallCli;
              console.log("📞 [SANPBX-START] Mobile from extraData.CallCli:", mobile)
            }

            let to = null
            if (data.start?.to) {
              to = data.start.to
              console.log("📞 [SANPBX-START] To from data.start.to:", to)
            } else if (urlParams.did) {
              to = urlParams.did
              console.log("📞 [SANPBX-START] To from URL did:", to)
            } else if (data.start?.extraData?.DID) {
              to = data.start.extraData.DID
              console.log("📞 [SANPBX-START] To from extraData.DID:", to)
            }

            let extraData = null;

            if (data.start?.extraData) {
              console.log("📞 [SANPBX-START] Found extraData in start object")
              extraData = decodeExtraData(data.start.extraData);
              console.log("📞 [SANPBX-START] Decoded extraData:", JSON.stringify(extraData, null, 2))
            } else if (urlParams.extra) {
              console.log("📞 [SANPBX-START] Found extra in URL params")
              extraData = decodeExtraData(urlParams.extra);
              console.log("📞 [SANPBX-START] Decoded URL extra:", JSON.stringify(extraData, null, 2))
            }

            if (extraData?.CallCli) {
              mobile = extraData.CallCli;
              console.log("📞 [SANPBX-START] Mobile updated from extraData.CallCli:", mobile)
            }
            if (extraData?.CallVaId) {
              callerId = extraData.CallVaId;
              console.log("📞 [SANPBX-START] CallerId from extraData.CallVaId:", callerId)
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
                console.log("📞 [SANPBX-START] User Name (extraData):", userName);
              }
            }

            if (!userName && urlParams.name) {
              userName = urlParams.name;
              console.log("📞 [SANPBX-START] User Name (url param):", userName);
            }

            if (extraData && extraData.CallDirection === "OutDial") {
              callDirection = "outbound";
              console.log("📞 [SANPBX-START] Call direction set to outbound from extraData")
            } else if (urlParams.direction === "OutDial") {
              callDirection = "outbound";
              console.log("📞 [SANPBX-START] Call direction set to outbound from URL")
              if (!extraData && urlParams.extra) {
                extraData = decodeExtraData(urlParams.extra);
                console.log("📞 [SANPBX-START] Decoded URL extra for outbound:", JSON.stringify(extraData, null, 2))
              }
            } else {
              callDirection = "inbound";
              console.log("📞 [SANPBX-START] Call direction set to inbound (default)")
            }

            // Log parsed call information
            console.log("📞 [SANPBX-START] ========== PARSED CALL INFO ==========")
            console.log("📞 [SANPBX-START] Call Direction:", callDirection)
            console.log("📞 [SANPBX-START] From/Mobile:", mobile)
            console.log("📞 [SANPBX-START] To/DID:", to)
            console.log("📞 [SANPBX-START] Extra Data:", JSON.stringify(extraData, null, 2))
            console.log("📞 [SANPBX-START] User Name:", userName)
            console.log("📞 [SANPBX-START] Custom Params:", JSON.stringify(customParams, null, 2))
            console.log("📞 [SANPBX-START] ======================================")

            // Use static agent configuration instead of database lookup
            console.log("📋 [SANPBX-AGENT-CONFIG] ========== USING STATIC CONFIG ==========")
            console.log("📋 [SANPBX-AGENT-CONFIG] Agent Name:", STATIC_CONFIG.agentName)
            console.log("📋 [SANPBX-AGENT-CONFIG] Language:", STATIC_CONFIG.language)
            console.log("📋 [SANPBX-AGENT-CONFIG] Voice Selection:", STATIC_CONFIG.voiceSelection)
            console.log("📋 [SANPBX-AGENT-CONFIG] First Message:", STATIC_CONFIG.firstMessage)
            console.log("📋 [SANPBX-AGENT-CONFIG] System Prompt:", STATIC_CONFIG.systemPrompt)
            console.log("✅ [SANPBX-AGENT-CONFIG] Static configuration loaded successfully")
            console.log("✅ [SANPBX-AGENT-CONFIG] ======================================")

            agentConfig = getStaticAgentConfig()
            ws.sessionAgentConfig = agentConfig
            currentLanguage = agentConfig.language || "en"

            console.log("🎯 [SANPBX-CALL-SETUP] ========== CALL SETUP ==========")
            console.log("🎯 [SANPBX-CALL-SETUP] Current Language:", currentLanguage)
            console.log("🎯 [SANPBX-CALL-SETUP] Mobile Number:", mobile)
            console.log("🎯 [SANPBX-CALL-SETUP] Call Direction:", callDirection)
            console.log("🎯 [SANPBX-CALL-SETUP] Client ID:", agentConfig.clientId || accountSid)
            console.log("🎯 [SANPBX-CALL-SETUP] StreamSID:", streamSid)
            console.log("🎯 [SANPBX-CALL-SETUP] CallSID:", data.start?.callSid || data.start?.CallSid || data.callSid || data.CallSid)

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

            // Create initial call log entry immediately (static implementation)
            try {
              await callLogger.createInitialCallLog(agentConfig._id, 'not_connected');
              console.log("✅ [SANPBX-CALL-SETUP] Initial call log created successfully")
              console.log("✅ [SANPBX-CALL-SETUP] Call Log ID:", callLogger.callLogId)
            } catch (error) {
              console.log("❌ [SANPBX-CALL-SETUP] Failed to create initial call log:", error.message)
              // Continue anyway - fallback will create log at end
            }

            console.log("🎯 [SANPBX-CALL-SETUP] Call Logger initialized")
            console.log("🎯 [SANPBX-CALL-SETUP] Connecting to Deepgram...")

            await connectToDeepgram()

            let greeting = agentConfig.firstMessage || STATIC_CONFIG.firstMessage
            if (userName && userName.trim()) {
              const base = agentConfig.firstMessage || STATIC_CONFIG.firstMessage
              greeting = `Hello ${userName.trim()}! ${base}`
            }

            console.log("🎯 [SANPBX-CALL-SETUP] Greeting Message:", greeting)
            console.log("🎯 [SANPBX-CALL-SETUP] ======================================")

            if (callLogger) {
              callLogger.logAIResponse(greeting, currentLanguage)
            }

            console.log("🎤 [SANPBX-TTS] Starting greeting TTS...")
            const tts = new SimplifiedSarvamTTSProcessor(currentLanguage, ws, streamSid, callLogger)
            await tts.synthesizeAndStream(greeting)
            console.log("✅ [SANPBX-TTS] Greeting TTS completed")
            break
          }

          case "media":
            console.log("🎵 [SANPBX-MEDIA] ========== MEDIA EVENT ==========")
            console.log("🎵 [SANPBX-MEDIA] Full media data:", JSON.stringify(data, null, 2))
            console.log("🎵 [SANPBX-MEDIA] Payload exists:", !!data.payload)
            console.log("🎵 [SANPBX-MEDIA] Payload length:", data.payload?.length || 0)
            console.log("🎵 [SANPBX-MEDIA] Chunk:", data.chunk)
            console.log("🎵 [SANPBX-MEDIA] Chunk duration (ms):", data.chunk_durn_ms)
            console.log("🎵 [SANPBX-MEDIA] Channel ID:", data.channelId)
            console.log("🎵 [SANPBX-MEDIA] Call ID:", data.callId)
            console.log("🎵 [SANPBX-MEDIA] Stream ID:", data.streamId)
            console.log("🎵 [SANPBX-MEDIA] Caller ID:", data.callerId)
            console.log("🎵 [SANPBX-MEDIA] Call Direction:", data.callDirection)
            console.log("🎵 [SANPBX-MEDIA] DID:", data.did)
            console.log("🎵 [SANPBX-MEDIA] Timestamp:", data.timestamp)
            
            if (data.payload) {
              try {
                // Validate and analyze audio data
                const audioValidation = validateAudioData(data.payload, data.chunk_durn_ms || 20)
                console.log("🎵 [SANPBX-MEDIA] Audio validation result:", JSON.stringify(audioValidation, null, 2))
                
                if (!audioValidation.valid) {
                  console.log("❌ [SANPBX-MEDIA] Audio validation failed:", audioValidation.error)
                  break
                }
                
                const audioBuffer = Buffer.from(data.payload, "base64")
                console.log("🎵 [SANPBX-MEDIA] Audio buffer created, length:", audioBuffer.length, "bytes")
                console.log("🎵 [SANPBX-MEDIA] Audio buffer first 16 bytes:", audioBuffer.slice(0, 16).toString('hex'))
                
                // Check if audio size is reasonable
                if (!audioValidation.sizeValid) {
                  console.log("⚠️ [SANPBX-MEDIA] Audio size ratio unusual:", audioValidation.sizeRatio, "(expected ~1.0)")
                  console.log("⚠️ [SANPBX-MEDIA] This might indicate audio format issues")
                }
                
                if (!audioValidation.isLikelyPCM) {
                  console.log("⚠️ [SANPBX-MEDIA] Audio doesn't appear to be raw PCM format")
                  console.log("⚠️ [SANPBX-MEDIA] Deepgram expects linear16 PCM at 8kHz")
                }
                
                // Log media stats periodically (every 100 packets to avoid spam)
                if (!ws.mediaPacketCount) ws.mediaPacketCount = 0
                ws.mediaPacketCount++
                
                if (ws.mediaPacketCount % 100 === 0) {
                  console.log("🎵 [SANPBX-MEDIA] Audio packets received:", ws.mediaPacketCount)
                }

                if (deepgramWs && deepgramReady && deepgramWs.readyState === WebSocket.OPEN) {
                  console.log("🎵 [SANPBX-MEDIA] Sending audio to Deepgram, buffer size:", audioBuffer.length)
                  console.log("🎵 [SANPBX-MEDIA] Deepgram WebSocket state:", deepgramWs.readyState)
                  deepgramWs.send(audioBuffer)
                  console.log("✅ [SANPBX-MEDIA] Audio sent to Deepgram successfully")
                } else {
                  console.log("🎵 [SANPBX-MEDIA] Queuing audio for Deepgram, queue size:", deepgramAudioQueue.length)
                  console.log("🎵 [SANPBX-MEDIA] Deepgram ready:", deepgramReady)
                  console.log("🎵 [SANPBX-MEDIA] Deepgram WebSocket state:", deepgramWs?.readyState)
                  deepgramAudioQueue.push(audioBuffer)
                  if (deepgramAudioQueue.length % 50 === 0) {
                    console.log("⏳ [SANPBX-MEDIA] Audio queued for Deepgram:", deepgramAudioQueue.length)
                  }
                }
              } catch (error) {
                console.log("❌ [SANPBX-MEDIA] Error processing audio payload:", error.message)
                console.log("❌ [SANPBX-MEDIA] Payload preview:", data.payload.substring(0, 100))
              }
            } else {
              console.log("⚠️ [SANPBX-MEDIA] No payload found in media event")
            }
            console.log("🎵 [SANPBX-MEDIA] ======================================")
            break

          case "stop":
            console.log("🛑 [SANPBX-STOP] ========== CALL END EVENT ==========")
            console.log("🛑 [SANPBX-STOP] Full stop data:", JSON.stringify(data, null, 2))
            console.log("🛑 [SANPBX-STOP] StreamSID:", streamSid)
            console.log("🛑 [SANPBX-STOP] Call Direction:", callDirection)
            console.log("🛑 [SANPBX-STOP] Mobile:", mobile)
            
            if (callLogger) {
              const stats = callLogger.getStats()
              console.log("🛑 [SANPBX-STOP] Call Stats:", JSON.stringify(stats, null, 2))
              
              try {
                console.log("💾 [SANPBX-STOP] Saving final call log...")
                const savedLog = await callLogger.saveToDatabase("maybe")
                console.log("✅ [SANPBX-STOP] Final call log saved with ID:", savedLog._id)
              } catch (error) {
                console.log("❌ [SANPBX-STOP] Error saving final call log:", error.message)
              } finally {
                callLogger.cleanup()
              }
            }

            if (deepgramWs?.readyState === WebSocket.OPEN) {
              console.log("🛑 [SANPBX-STOP] Closing Deepgram connection...")
              deepgramWs.close()
            }
            
            console.log("🛑 [SANPBX-STOP] ======================================")
            break

          default:
            console.log("❓ [SANPBX-UNKNOWN] Unknown event type:", data.event)
            console.log("❓ [SANPBX-UNKNOWN] Full data:", JSON.stringify(data, null, 2))
            break
        }
      } catch (error) {
        console.log("❌ [SANPBX-MESSAGE-ERROR] Error processing message:", error.message)
        console.log("❌ [SANPBX-MESSAGE-ERROR] Stack trace:", error.stack)
      }
    })

    ws.on("close", async () => {
      console.log("🔌 [SANPBX-CLOSE] ========== WEBSOCKET CLOSED ==========")
      console.log("🔌 [SANPBX-CLOSE] StreamSID:", streamSid)
      console.log("🔌 [SANPBX-CLOSE] Call Direction:", callDirection)
      console.log("🔌 [SANPBX-CLOSE] Close code:", ws.closeCode)
      console.log("🔌 [SANPBX-CLOSE] Close reason:", ws.closeReason)
      
      if (callLogger) {
        const stats = callLogger.getStats()
        console.log("🔌 [SANPBX-CLOSE] Final Call Stats:", JSON.stringify(stats, null, 2))
        
        try {
          console.log("💾 [SANPBX-CLOSE] Saving call log due to connection close...")
          const savedLog = await callLogger.saveToDatabase("maybe")
          console.log("✅ [SANPBX-CLOSE] Call log saved with ID:", savedLog._id)
        } catch (error) {
          console.log("❌ [SANPBX-CLOSE] Error saving call log:", error.message)
        } finally {
          callLogger.cleanup()
        }
      }

      if (deepgramWs?.readyState === WebSocket.OPEN) {
        console.log("🔌 [SANPBX-CLOSE] Closing Deepgram connection...")
        deepgramWs.close()
      }

      console.log("🔌 [SANPBX-CLOSE] Resetting session state...")
      
      // Reset state
      streamSid = null
      conversationHistory = []
      isProcessing = false
      userUtteranceBuffer = ""
      lastProcessedText = ""
      deepgramReady = false
      deepgramAudioQueue = []
      currentTTS = null
      currentLanguage = STATIC_CONFIG.language
      processingRequestId = 0
      callLogger = null
      callDirection = "inbound"
      agentConfig = null
      sttTimer = null
      
      console.log("🔌 [SANPBX-CLOSE] ======================================")
    })

    ws.on("error", (error) => {
      console.log("❌ [SANPBX-ERROR] ========== WEBSOCKET ERROR ==========")
      console.log("❌ [SANPBX-ERROR] Error message:", error.message)
      console.log("❌ [SANPBX-ERROR] Error stack:", error.stack)
      console.log("❌ [SANPBX-ERROR] StreamSID:", streamSid)
      console.log("❌ [SANPBX-ERROR] Call Direction:", callDirection)
      console.log("❌ [SANPBX-ERROR] ======================================")
    })
  })
}

// Global map to store active call loggers by streamSid
const activeCallLoggers = new Map()

module.exports = { 
  setupSanPbxWebSocketServer, 
  STATIC_CONFIG
}