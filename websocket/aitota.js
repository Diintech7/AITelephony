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

// Enhanced language detection with Marathi support
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

// Optimized OpenAI streaming with phrase-based chunking and language detection
const processWithOpenAIStreaming = async (
  userMessage,
  conversationHistory,
  detectedLanguage,
  onPhrase,
  onComplete,
  onInterrupt,
  callLogger,
  agentConfig, // Add agent config parameter
) => {
  const timer = createTimer("OPENAI_STREAMING")

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
        max_tokens: 30,
        temperature: 0.3,
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

    // Check for interruption periodically
    const checkInterruption = () => {
      return onInterrupt && onInterrupt()
    }

    while (true) {
      // Check for interruption
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

              // Check for interruption before sending phrase
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

      // Log AI response to call logger
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

// Smart phrase detection for better chunking
const shouldSendPhrase = (buffer) => {
  const trimmed = buffer.trim()

  // Complete sentences
  if (/[.!?।॥।]$/.test(trimmed)) return true

  // Meaningful phrases with natural breaks
  if (trimmed.length >= 8 && /[,;।]\s*$/.test(trimmed)) return true

  // Longer phrases (prevent too much buffering)
  if (trimmed.length >= 25 && /\s/.test(trimmed)) return true

  return false
}

// Enhanced TTS processor with call logging
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

    // Interruption handling
    this.isInterrupted = false
    this.currentAudioStreaming = null

    // Sentence-based processing settings
    this.sentenceBuffer = ""
    this.processingTimeout = 100
    this.sentenceTimer = null

    // Audio streaming stats
    this.totalChunks = 0
    this.totalAudioBytes = 0
  }

  // Method to interrupt current processing
  interrupt() {
    console.log(`⚠️ [SARVAM-TTS] Interrupting current processing`)
    this.isInterrupted = true

    // Clear queue and buffer
    this.queue = []
    this.sentenceBuffer = ""

    // Clear any pending timeout
    if (this.sentenceTimer) {
      clearTimeout(this.sentenceTimer)
      this.sentenceTimer = null
    }

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
    this.isProcessing = false
    this.totalChunks = 0
    this.totalAudioBytes = 0
  }

  addPhrase(phrase, detectedLanguage) {
    if (!phrase.trim() || this.isInterrupted) return

    // Update language if different from current
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

      // Process next item in queue if not interrupted
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

      // Stream audio if not interrupted
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

    // SIP audio specifications
    const SAMPLE_RATE = 8000
    const BYTES_PER_SAMPLE = 2
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000
    const OPTIMAL_CHUNK_SIZE = Math.floor(10 * BYTES_PER_MS)

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

// Main WebSocket server setup with enhanced call logging and outbound support
const setupUnifiedVoiceServer = (wss) => {
  console.log("🚀 [ENHANCED] Voice Server started with inbound/outbound support, call logging and Marathi support")

  wss.on("connection", (ws, req) => {
    console.log("🔗 [CONNECTION] New enhanced WebSocket connection")

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
    let optimizedTTS = null
    let currentLanguage = undefined
    let processingRequestId = 0
    let callLogger = null // Call logger instance
    let callDirection = "inbound" // Default to inbound
    let agentConfig = null // Store agent configuration

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

    // Handle Deepgram responses with call logging
    const handleDeepgramResponse = async (data) => {
      if (data.type === "Results") {
        const transcript = data.channel?.alternatives?.[0]?.transcript
        const is_final = data.is_final

        if (transcript?.trim()) {
          // Interrupt current TTS if new speech detected
          if (optimizedTTS && (isProcessing || optimizedTTS.isProcessing)) {
            console.log(`🛑 [INTERRUPT] New speech detected, interrupting current response`)
            optimizedTTS.interrupt()
            isProcessing = false
            processingRequestId++ // Invalidate current processing
          }

          if (is_final) {
            userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim()

            // Log the final transcript to call logger
            if (callLogger && transcript.trim()) {
              const detectedLang = await detectLanguageWithOpenAI(transcript.trim())
              callLogger.logUserTranscript(transcript.trim(), detectedLang)
            }

            await processUserUtterance(userUtteranceBuffer)
            userUtteranceBuffer = ""
          }
        }
      } else if (data.type === "UtteranceEnd") {
        if (userUtteranceBuffer.trim()) {
          // Log the utterance end transcript
          if (callLogger && userUtteranceBuffer.trim()) {
            const detectedLang = await detectLanguageWithOpenAI(userUtteranceBuffer.trim())
            callLogger.logUserTranscript(userUtteranceBuffer.trim(), detectedLang)
          }

          await processUserUtterance(userUtteranceBuffer)
          userUtteranceBuffer = ""
        }
      }
    }

    // Enhanced utterance processing with call logging
    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText) return

      // Interrupt any ongoing processing
      if (optimizedTTS) {
        optimizedTTS.interrupt()
      }

      isProcessing = true
      lastProcessedText = text
      const currentRequestId = ++processingRequestId
      const timer = createTimer("UTTERANCE_PROCESSING")

      try {
        console.log(`🎤 [USER] Processing: "${text}"`)

        // Step 1: Detect language using OpenAI
        const detectedLanguage = await detectLanguageWithOpenAI(text)

        // Step 2: Update current language and initialize TTS processor
        if (detectedLanguage !== currentLanguage) {
          console.log(`🌍 [LANGUAGE] Changed: ${currentLanguage} → ${detectedLanguage}`)
          currentLanguage = detectedLanguage
        }

        // Create new TTS processor with detected language
        optimizedTTS = new OptimizedSarvamTTSProcessor(detectedLanguage, ws, streamSid, callLogger)

        // Step 3: Check for interruption function
        const checkInterruption = () => {
          return processingRequestId !== currentRequestId
        }

        // Step 4: Process with OpenAI streaming (pass agentConfig)
        const response = await processWithOpenAIStreaming(
          text,
          conversationHistory,
          detectedLanguage,
          (phrase, lang) => {
            // Handle phrase chunks - only if not interrupted
            if (processingRequestId === currentRequestId && !checkInterruption()) {
              console.log(`📤 [PHRASE] "${phrase}" (${lang})`)
              optimizedTTS.addPhrase(phrase, lang)
            }
          },
          (fullResponse) => {
            // Handle completion - only if not interrupted
            if (processingRequestId === currentRequestId && !checkInterruption()) {
              console.log(`✅ [COMPLETE] "${fullResponse}"`)
              optimizedTTS.complete()

              const stats = optimizedTTS.getStats()
              console.log(`📊 [TTS-STATS] ${stats.totalChunks} chunks, ${stats.avgBytesPerChunk} avg bytes/chunk`)

              // Update conversation history
              conversationHistory.push({ role: "user", content: text }, { role: "assistant", content: fullResponse })

              // Keep last 10 messages for context
              if (conversationHistory.length > 10) {
                conversationHistory = conversationHistory.slice(-10)
              }
            }
          },
          checkInterruption,
          callLogger, // Pass call logger to OpenAI processing
          agentConfig, // Pass agent config for system prompt
        )

        console.log(`⚡ [TOTAL] Processing time: ${timer.end()}ms`)
      } catch (error) {
        console.error(`❌ [PROCESSING] Error: ${error.message}`)
      } finally {
        if (processingRequestId === currentRequestId) {
          isProcessing = false
        }
      }
    }

    // WebSocket message handling with enhanced inbound/outbound support
    ws.on("message", async (message) => {
      try {
        const messageStr = message.toString()

        // Skip non-JSON messages (like "EOS" or other control messages)
        if (messageStr === "EOS" || messageStr === "BOS" || !messageStr.startsWith("{")) {
          console.log(`📝 [WEBSOCKET] Skipping non-JSON message: ${messageStr}`)
          return
        }

        const data = JSON.parse(messageStr)

        switch (data.event) {
          case "connected":
            console.log(`🔗 [ENHANCED] Connected - Protocol: ${data.protocol}`)
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

            console.log(`🎯 [ENHANCED] Stream started - StreamSid: ${streamSid}, Direction: ${callDirection}`)

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

            const tts = new OptimizedSarvamTTSProcessor(currentLanguage, ws, streamSid, callLogger)
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
            console.log(`📞 [ENHANCED] Stream stopped - Direction: ${callDirection}`)

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
            console.log(`❓ [ENHANCED] Unknown event: ${data.event}`)
        }
      } catch (error) {
        console.error(`❌ [ENHANCED] Message error: ${error.message}`)
      }
    })

    // Enhanced connection cleanup with call logging
    ws.on("close", async () => {
      console.log(`🔗 [ENHANCED] Connection closed - Direction: ${callDirection}`)

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
      optimizedTTS = null
      currentLanguage = undefined
      processingRequestId = 0
      callLogger = null
      callDirection = "inbound"
      agentConfig = null
    })

    ws.on("error", (error) => {
      console.error(`❌ [ENHANCED] WebSocket error: ${error.message}`)
    })
  })
}

module.exports = { setupUnifiedVoiceServer }
