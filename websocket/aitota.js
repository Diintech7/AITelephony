const WebSocket = require("ws")
require("dotenv").config()
const mongoose = require("mongoose")
const Agent = require("../models/Agent")
const CallLog = require("../models/CallLog")
const franc = require("franc") // Add franc for fast language detection

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
  mr: "mr-IN", // Marathi added
  gu: "gu-IN",
  kn: "kn-IN",
  ml: "ml-IN",
  pa: "pa-IN",
  or: "or-IN",
  as: "as-IN",
  ur: "ur-IN",
}

// Franc language code mapping to our supported languages
const FRANC_TO_SUPPORTED = {
  'hin': 'hi',    // Hindi
  'eng': 'en',    // English
  'ben': 'bn',    // Bengali
  'tel': 'te',    // Telugu
  'tam': 'ta',    // Tamil
  'mar': 'mr',    // Marathi
  'guj': 'gu',    // Gujarati
  'kan': 'kn',    // Kannada
  'mal': 'ml',    // Malayalam
  'pan': 'pa',    // Punjabi
  'ori': 'or',    // Odia
  'asm': 'as',    // Assamese
  'urd': 'ur',    // Urdu
}

const getSarvamLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang
  return LANGUAGE_MAPPING[lang] || "hi-IN"
}

const getDeepgramLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang
  if (lang === "hi") return "hi"
  if (lang === "en") return "en-IN"
  if (lang === "mr") return "mr" // Marathi support for Deepgram
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

// Utility function to decode base64 extra data
const decodeExtraData = (extraBase64) => {
  try {
    if (!extraBase64) return null

    // Decode base64
    const decodedString = Buffer.from(extraBase64, "base64").toString("utf-8")
    console.log(`🔍 [DECODE] Raw decoded string: ${decodedString}`)

    // Fix common JSON formatting issues
    const fixedString = decodedString
      .replace(/="([^"]*?)"/g, ':"$1"') // Replace = with : in key-value pairs
      .replace(/=([^",}\s]+)/g, ':"$1"') // Handle unquoted values after =
      .replace(/,\s*}/g, "}") // Remove trailing commas
      .replace(/,\s*]/g, "]") // Remove trailing commas in arrays

    console.log(`🔧 [DECODE] Fixed JSON string: ${fixedString}`)

    // Parse JSON
    const parsedData = JSON.parse(fixedString)
    console.log(`✅ [DECODE] Parsed extra data:`, parsedData)

    return parsedData
  } catch (error) {
    console.error(`❌ [DECODE] Failed to decode extra data: ${error.message}`)
    console.error(`❌ [DECODE] Original string: ${extraBase64}`)
    return null
  }
}

// Fast language detection using franc
const detectLanguageWithFranc = (text, fallbackLanguage = "hi") => {
  const timer = createTimer("FRANC_DETECTION")
  
  try {
    // Clean the text for better detection
    const cleanText = text.trim()
    
    // Need minimum text length for reliable detection
    if (cleanText.length < 10) {
      console.log(`⚠️ [FRANC] Text too short (${cleanText.length} chars), using fallback: ${fallbackLanguage}`)
      return fallbackLanguage
    }

    // Use franc to detect language (returns ISO 639-3 codes)
    const detected = franc(cleanText)
    console.log(`🔍 [FRANC] Raw detection: "${detected}" from text: "${cleanText.substring(0, 50)}..."`)

    // Handle 'und' (undetermined) case
    if (detected === 'und') {
      console.log(`⚠️ [FRANC] Language undetermined, using fallback: ${fallbackLanguage}`)
      return fallbackLanguage
    }

    // Map franc code to our supported language
    const mappedLang = FRANC_TO_SUPPORTED[detected]
    
    if (mappedLang) {
      console.log(`✅ [FRANC] Detected: "${mappedLang}" (${detected}) in ${timer.end()}ms`)
      return mappedLang
    } else {
      console.log(`⚠️ [FRANC] Unsupported language "${detected}", using fallback: ${fallbackLanguage}`)
      return fallbackLanguage
    }
    
  } catch (error) {
    console.error(`❌ [FRANC] Error: ${error.message}, using fallback: ${fallbackLanguage}`)
    return fallbackLanguage
  }
}

// Fallback to OpenAI for uncertain cases (optional, for improved accuracy)
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

    // Validate detected language
    const validLanguages = Object.keys(LANGUAGE_MAPPING)
    if (validLanguages.includes(detectedLang)) {
      console.log(`🔍 [LANG-DETECT] Detected: "${detectedLang}" from text: "${text.substring(0, 50)}..."`)
      return detectedLang
    }

    console.log(`⚠️ [LANG-DETECT] Invalid language "${detectedLang}", defaulting to "hi"`)
    return "hi" // Default fallback
  } catch (error) {
    console.error(`❌ [LANG-DETECT] Error: ${error.message}`)
    return "hi" // Default fallback
  }
}

// Hybrid language detection: Franc first, OpenAI fallback for uncertain cases
const detectLanguageHybrid = async (text, useOpenAIFallback = false) => {
  // Always try franc first (fast)
  const francResult = detectLanguageWithFranc(text)
  
  // If franc is confident or we don't want OpenAI fallback, use franc result
  if (!useOpenAIFallback || francResult !== "hi") {
    return francResult
  }
  
  // For uncertain cases, optionally use OpenAI for better accuracy
  console.log(`🔄 [HYBRID] Franc uncertain, trying OpenAI fallback...`)
  return await detectLanguageWithOpenAI(text)
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

  // Log user transcript from Deepgram
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

  // Log AI response from Sarvam
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

  // Generate full transcript combining user and AI messages
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

  // Save call log to database
  async saveToDatabase(leadStatus = "medium") {
    try {
      const callEndTime = new Date()
      this.totalDuration = Math.round((callEndTime - this.callStartTime) / 1000) // Duration in seconds

      const callLogData = {
        clientId: this.clientId,
        mobile: this.mobile,
        time: this.callStartTime,
        transcript: this.generateFullTranscript(),
        duration: this.totalDuration,
        leadStatus: leadStatus,
        // Additional metadata
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

      console.log(
        `💾 [CALL-LOG] Saved to DB - ID: ${savedLog._id}, Duration: ${this.totalDuration}s, Direction: ${this.callDirection}`,
      )
      console.log(
        `📊 [CALL-LOG] Stats - User messages: ${this.transcripts.length}, AI responses: ${this.responses.length}`,
      )

      return savedLog
    } catch (error) {
      console.error(`❌ [CALL-LOG] Database save error: ${error.message}`)
      throw error
    }
  }

  // Get call statistics
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

// Simplified OpenAI processing - returns complete response immediately
const processWithOpenAI = async (
  userMessage,
  conversationHistory,
  detectedLanguage,
  callLogger,
  agentConfig,
) => {
  const timer = createTimer("OPENAI_PROCESSING")

  try {
    // Use system prompt from database (limited to 150 bytes)
    let systemPrompt = agentConfig.systemPrompt || "You are a helpful AI assistant."

    // Truncate system prompt to 150 bytes if it exceeds the limit
    if (Buffer.byteLength(systemPrompt, "utf8") > 150) {
      // Truncate to 150 bytes while preserving UTF-8 encoding
      let truncated = systemPrompt
      while (Buffer.byteLength(truncated, "utf8") > 150) {
        truncated = truncated.slice(0, -1)
      }
      systemPrompt = truncated
      console.log(`⚠️ [SYSTEM-PROMPT] Truncated to 150 bytes: "${systemPrompt}"`)
    }

    console.log(
      `📝 [SYSTEM-PROMPT] Using from DB (${Buffer.byteLength(systemPrompt, "utf8")} bytes): "${systemPrompt}"`,
    )

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
      console.error(`❌ [OPENAI] Error: ${response.status}`)
      return null
    }

    const data = await response.json()
    const fullResponse = data.choices[0]?.message?.content?.trim()

    console.log(`🤖 [OPENAI] Complete: "${fullResponse}" (${timer.end()}ms)`)

    // Log AI response to call logger
    if (callLogger && fullResponse) {
      callLogger.logAIResponse(fullResponse, detectedLanguage)
    }

    return fullResponse
  } catch (error) {
    console.error(`❌ [OPENAI] Error: ${error.message}`)
    return null
  }
}

// Simplified TTS processor without phrase chunking
class SimplifiedSarvamTTSProcessor {
  constructor(language, ws, streamSid, callLogger = null) {
    this.language = language
    this.ws = ws
    this.streamSid = streamSid
    this.callLogger = callLogger
    this.sarvamLanguage = getSarvamLanguage(language)
    this.voice = getValidSarvamVoice(ws.sessionAgentConfig?.voiceSelection || "pavithra")

    // Interruption handling
    this.isInterrupted = false
    this.currentAudioStreaming = null

    // Audio streaming stats
    this.totalAudioBytes = 0
  }

  // Method to interrupt current processing
  interrupt() {
    console.log(`⚠️ [SARVAM-TTS] Interrupting current processing`)
    this.isInterrupted = true

    // Stop current audio streaming if active
    if (this.currentAudioStreaming) {
      this.currentAudioStreaming.interrupt = true
    }

    console.log(`🛑 [SARVAM-TTS] Processing interrupted and cleaned up`)
  }

  // Reset for new processing
  reset(newLanguage) {
    this.interrupt()

    // Update language settings
    if (newLanguage) {
      this.language = newLanguage
      this.sarvamLanguage = getSarvamLanguage(newLanguage)
      console.log(`🔄 [SARVAM-TTS] Language updated to: ${this.sarvamLanguage}`)
    }

    // Reset state
    this.isInterrupted = false
    this.totalAudioBytes = 0
  }

  async synthesizeAndStream(text) {
    if (this.isInterrupted) return

    const timer = createTimer("SARVAM_TTS")

    try {
      console.log(`🎵 [SARVAM-TTS] Synthesizing complete text: "${text}" (${this.sarvamLanguage})`)

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

      // Stream audio if not interrupted
      if (!this.isInterrupted) {
        await this.streamAudioOptimizedForSIP(audioBase64)

        const audioBuffer = Buffer.from(audioBase64, "base64")
        this.totalAudioBytes += audioBuffer.length
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

    // SIP audio specifications
    const SAMPLE_RATE = 8000
    const BYTES_PER_SAMPLE = 2
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000
    const OPTIMAL_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS)

    console.log(`📦 [SARVAM-SIP] Streaming ${audioBuffer.length} bytes to StreamSid: ${this.streamSid}`)

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
          this.ws.send(JSON.stringify(mediaMessage))
          successfulChunks++
          console.log(
            `📤 [SARVAM-SIP] Chunk ${chunkIndex + 1}/${Math.ceil(audioBuffer.length / OPTIMAL_CHUNK_SIZE)}: ${chunk.length} bytes sent`,
          )
        } catch (error) {
          console.error(`❌ [SARVAM-SIP] Failed to send chunk ${chunkIndex + 1}: ${error.message}`)
          break
        }
      } else {
        console.error(
          `❌ [SARVAM-SIP] WebSocket not ready: readyState=${this.ws.readyState}, interrupted=${this.isInterrupted}`,
        )
        break
      }

      // Delay between chunks
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
      console.log(`✅ [SARVAM-SIP] Completed streaming ${successfulChunks}/${chunkIndex} chunks successfully`)
    }

    this.currentAudioStreaming = null
  }

  getStats() {
    return {
      totalAudioBytes: this.totalAudioBytes,
    }
  }
}

// Enhanced agent lookup function for both inbound and outbound calls
const findAgentForCall = async (callData) => {
  try {
    const { accountSid, callDirection, extraData } = callData

    console.log(`🔍 [AGENT-LOOKUP] Direction: ${callDirection}, AccountSid: ${accountSid}`)
    console.log(`🔍 [AGENT-LOOKUP] ExtraData:`, extraData)

    let agent = null

    if (callDirection === "inbound") {
      // Inbound call: Use accountSid to find agent
      if (!accountSid) {
        throw new Error("Missing accountSid for inbound call")
      }

      agent = await Agent.findOne({ accountSid }).lean()
      if (!agent) {
        throw new Error(`No agent found for accountSid: ${accountSid}`)
      }

      console.log(`✅ [AGENT-LOOKUP] Inbound agent found: ${agent.agentName} (Client: ${agent.clientId})`)
    } else if (callDirection === "outbound") {
      // Outbound call: Use CallVaId from extraData to match callerId
      if (!extraData) {
        throw new Error("Missing extraData for outbound call")
      }

      if (!extraData.CallVaId) {
        console.error(`❌ [AGENT-LOOKUP] ExtraData structure:`, JSON.stringify(extraData, null, 2))
        throw new Error("Missing CallVaId in extraData for outbound call")
      }

      const callVaId = extraData.CallVaId
      console.log(`🔍 [AGENT-LOOKUP] Looking for agent with callerId: ${callVaId}`)

      agent = await Agent.findOne({ callerId: callVaId }).lean()
      if (!agent) {
        throw new Error(`No agent found for callerId: ${callVaId}`)
      }

      console.log(`✅ [AGENT-LOOKUP] Outbound agent found: ${agent.agentName} (Client: ${agent.clientId})`)
    } else {
      throw new Error(`Unknown call direction: ${callDirection}`)
    }

    return agent
  } catch (error) {
    console.error(`❌ [AGENT-LOOKUP] Error: ${error.message}`)
    throw error
  }
}

// Main WebSocket server setup with simplified processing
const setupUnifiedVoiceServer = (wss) => {
  console.log("🚀 [SIMPLIFIED] Voice Server started with Franc language detection")

  wss.on("connection", (ws, req) => {
    console.log("🔗 [CONNECTION] New simplified WebSocket connection")

    // Parse URL parameters for call direction detection
    const url = new URL(req.url, `http://${req.headers.host}`)
    const urlParams = Object.fromEntries(url.searchParams.entries())

    console.log(`🔍 [URL-PARAMS] Received parameters:`, urlParams)

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

    // Deepgram WebSocket connection
    let deepgramWs = null
    let deepgramReady = false
    let deepgramAudioQueue = []

    // Optimized Deepgram connection
    const connectToDeepgram = async () => {
      try {
        console.log("🔌 [DEEPGRAM] Connecting...")
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
          console.log("✅ [DEEPGRAM] Connected")

          deepgramAudioQueue.forEach((buffer) => deepgramWs.send(buffer))
          deepgramAudioQueue = []
        }

        deepgramWs.onmessage = async (event) => {
          const data = JSON.parse(event.data)
          await handleDeepgramResponse(data)
        }

        deepgramWs.onerror = (error) => {
          console.error("❌ [DEEPGRAM] Error:", error)
          deepgramReady = false
        }

        deepgramWs.onclose = () => {
          console.log("🔌 [DEEPGRAM] Connection closed")
          deepgramReady = false
        }
      } catch (error) {
        console.error("❌ [DEEPGRAM] Setup error:", error.message)
      }
    }

    // Handle Deepgram responses
    const handleDeepgramResponse = async (data) => {
      if (data.type === "Results") {
        const transcript = data.channel?.alternatives?.[0]?.transcript
        const is_final = data.is_final

        if (transcript?.trim()) {
          // Interrupt current processing if new speech detected
          if (currentTTS && isProcessing) {
            console.log(`🛑 [INTERRUPT] New speech detected, interrupting current response`)
            currentTTS.interrupt()
            isProcessing = false
            processingRequestId++ // Invalidate current processing
          }

          if (is_final) {
            userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim()

            // Log the final transcript to call logger with fast language detection
            if (callLogger && transcript.trim()) {
              const detectedLang = detectLanguageWithFranc(transcript.trim())
              callLogger.logUserTranscript(transcript.trim(), detectedLang)
            }

            await processUserUtterance(userUtteranceBuffer)
            userUtteranceBuffer = ""
          }
        }
      } else if (data.type === "UtteranceEnd") {
        if (userUtteranceBuffer.trim()) {
          // Log the utterance end transcript with fast language detection
          if (callLogger && userUtteranceBuffer.trim()) {
            const detectedLang = detectLanguageWithFranc(userUtteranceBuffer.trim())
            callLogger.logUserTranscript(userUtteranceBuffer.trim(), detectedLang)
          }

          await processUserUtterance(userUtteranceBuffer)
          userUtteranceBuffer = ""
        }
      }
    }

    // Simplified utterance processing with Franc
    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText) return

      // Interrupt any ongoing processing
      if (currentTTS) {
        currentTTS.interrupt()
      }

      isProcessing = true
      lastProcessedText = text
      const currentRequestId = ++processingRequestId
      const timer = createTimer("UTTERANCE_PROCESSING")

      try {
        console.log(`🎤 [USER] Processing: "${text}"`)

        // Step 1: Fast language detection using Franc
        const detectedLanguage = detectLanguageWithFranc(text, currentLanguage || "hi")

        // Step 2: Update current language
        if (detectedLanguage !== currentLanguage) {
          console.log(`🌍 [LANGUAGE] Changed: ${currentLanguage} → ${detectedLanguage}`)
          currentLanguage = detectedLanguage
        }

        // Step 3: Get complete response from OpenAI
        const response = await processWithOpenAI(
          text,
          conversationHistory,
          detectedLanguage,
          callLogger,
          agentConfig,
        )

        // Step 4: Check if still the current request (not interrupted)
        if (processingRequestId === currentRequestId && response) {
          console.log(`🤖 [RESPONSE] "${response}"`)

          // Step 5: Create TTS processor and synthesize complete response
          currentTTS = new SimplifiedSarvamTTSProcessor(detectedLanguage, ws, streamSid, callLogger)
          await currentTTS.synthesizeAndStream(response)

          // Step 6: Update conversation history
          conversationHistory.push(
            { role: "user", content: text },
            { role: "assistant", content: response }
          )

          // Keep last 10 messages for context
          if (conversationHistory.length > 10) {
            conversationHistory = conversationHistory.slice(-10)
          }

          const stats = currentTTS.getStats()
          console.log(`📊 [TTS-STATS] ${stats.totalAudioBytes} bytes processed`)
        }

        console.log(`⚡ [TOTAL] Processing time: ${timer.end()}ms`)
      } catch (error) {
        console.error(`❌ [PROCESSING] Error: ${error.message}`)
      } finally {
        if (processingRequestId === currentRequestId) {
          isProcessing = false
        }
      }
    }

    // WebSocket message handling
    ws.on("message", async (message) => {
      try {
        const messageStr = message.toString()

        // Skip non-JSON messages
        if (messageStr === "EOS" || messageStr === "BOS" || !messageStr.startsWith("{")) {
          console.log(`📝 [WEBSOCKET] Skipping non-JSON message: ${messageStr}`)
          return
        }

        const data = JSON.parse(messageStr)

        switch (data.event) {
          case "connected":
            console.log(`🔗 [SIMPLIFIED] Connected - Protocol: ${data.protocol}`)
            break

          case "start": {
            streamSid = data.streamSid || data.start?.streamSid
            const accountSid = data.start?.accountSid

            // Extract mobile number from different possible fields
            let mobile = null
            if (data.start?.from) {
              mobile = data.start.from
            } else if (urlParams.caller_id) {
              mobile = urlParams.caller_id
            } else if (data.start?.extraData?.CallCli) {
              mobile = data.start.extraData.CallCli
            }

            // Extract DID/To number
            let to = null
            if (data.start?.to) {
              to = data.start.to
            } else if (urlParams.did) {
              to = urlParams.did
            } else if (data.start?.extraData?.DID) {
              to = data.start.extraData.DID
            }

            console.log(`📞 [CALL-INFO] Mobile: ${mobile}, DID: ${to}, AccountSid: ${accountSid}`)

            // Determine call direction and decode extra data if present
            let extraData = null

            // Try to get extra data from multiple sources
            if (data.start?.extraData) {
              extraData = decodeExtraData(data.start.extraData)
            } else if (urlParams.extra) {
              // Decode extra data from URL parameters
              extraData = decodeExtraData(urlParams.extra)
              console.log(`🔍 [EXTRA-DATA] Decoded from URL params:`, extraData)
            }

            // Update mobile number from decoded extra data if available
            if (extraData?.CallCli && !mobile) {
              mobile = extraData.CallCli
              console.log(`📱 [MOBILE-UPDATE] Updated mobile from extraData: ${mobile}`)
            }

            // Determine call direction based on multiple indicators
            if (extraData && extraData.CallDirection === "OutDial") {
              callDirection = "outbound"
              console.log(
                `📞 [OUTBOUND] Call detected - Mobile: ${mobile}, DID: ${to}, CallVaId: ${extraData.CallVaId}`,
              )
            } else if (urlParams.direction === "OutDial") {
              callDirection = "outbound"
              console.log(`📞 [OUTBOUND] Call detected via URL param - Mobile: ${mobile}, DID: ${to}`)

              // For outbound calls detected via URL param, ensure we have extraData
              if (!extraData && urlParams.extra) {
                extraData = decodeExtraData(urlParams.extra)
                console.log(`🔍 [EXTRA-DATA] Decoded for outbound call:`, extraData)
              }
            } else {
              callDirection = "inbound"
              console.log(`📞 [INBOUND] Call detected - Mobile: ${mobile}, DID: ${to}, AccountSid: ${accountSid}`)
            }

            console.log(`🎯 [SIMPLIFIED] Stream started - StreamSid: ${streamSid}, Direction: ${callDirection}`)

            // Find appropriate agent based on call direction
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
              console.error(`❌ [AGENT-LOOKUP] ${err.message}`)
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
            currentLanguage = agentConfig.language || "hi"

            // Initialize call logger with direction
            callLogger = new CallLogger(agentConfig.clientId || accountSid, mobile, callDirection)
            console.log(
              `📝 [CALL-LOG] Initialized for client: ${agentConfig.clientId}, mobile: ${mobile}, direction: ${callDirection}`,
            )

            await connectToDeepgram()

            // Use agent's firstMessage for greeting and log it
            const greeting = agentConfig.firstMessage || "Hello! How can I help you today?"
            console.log(`👋 [GREETING] ${greeting}`)

            // Log the initial greeting
            if (callLogger) {
              callLogger.logAIResponse(greeting, currentLanguage)
            }

            const tts = new SimplifiedSarvamTTSProcessor(currentLanguage, ws, streamSid, callLogger)
            await tts.synthesizeAndStream(greeting)
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
            console.log(`📞 [SIMPLIFIED] Stream stopped - Direction: ${callDirection}`)

            // Save call log to database before closing
            if (callLogger) {
              try {
                const savedLog = await callLogger.saveToDatabase("medium") // Default lead status
                console.log(`💾 [CALL-LOG] Final save completed - ID: ${savedLog._id}, Direction: ${callDirection}`)

                // Print call statistics
                const stats = callLogger.getStats()
                console.log(
                  `📊 [CALL-STATS] Duration: ${stats.duration}s, User: ${stats.userMessages}, AI: ${stats.aiResponses}, Languages: ${stats.languages.join(", ")}, Direction: ${stats.callDirection}`,
                )
              } catch (error) {
                console.error(`❌ [CALL-LOG] Failed to save final log: ${error.message}`)
              }
            }

            if (deepgramWs?.readyState === WebSocket.OPEN) {
              deepgramWs.close()
            }
            break

          default:
            console.log(`❓ [SIMPLIFIED] Unknown event: ${data.event}`)
        }
      } catch (error) {
        console.error(`❌ [SIMPLIFIED] Message error: ${error.message}`)
      }
    })

    // Connection cleanup
    ws.on("close", async () => {
      console.log(`🔗 [SIMPLIFIED] Connection closed - Direction: ${callDirection}`)

      // Save call log before cleanup if not already saved
      if (callLogger) {
        try {
          const savedLog = await callLogger.saveToDatabase("not_connected") // Status for unexpected disconnection
          console.log(`💾 [CALL-LOG] Emergency save completed - ID: ${savedLog._id}, Direction: ${callDirection}`)
        } catch (error) {
          console.error(`❌ [CALL-LOG] Emergency save failed: ${error.message}`)
        }
      }

      if (deepgramWs?.readyState === WebSocket.OPEN) {
        deepgramWs.close()
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
    })

    ws.on("error", (error) => {
      console.error(`❌ [SIMPLIFIED] WebSocket error: ${error.message}`)
    })
  })
}

module.exports = { setupUnifiedVoiceServer }