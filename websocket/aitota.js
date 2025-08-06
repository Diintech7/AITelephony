const WebSocket = require("ws")
require("dotenv").config()
const mongoose = require("mongoose")
const Agent = require("../models/Agent")
const CallLog = require("../models/CallLog")

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

// Fast language detection using franc
const detectLanguageWithFranc = (text, fallbackLanguage = "hi") => {
  try {
    const cleanText = text.trim()
    
    if (cleanText.length < 10) {
      return fallbackLanguage
    }

    if (typeof franc !== 'function') {
      return fallbackLanguage
    }

    const detected = franc(cleanText)
    
    if (detected === 'und' || !detected) {
      return fallbackLanguage
    }

    const mappedLang = FRANC_TO_SUPPORTED[detected]
    
    if (mappedLang) {
      return mappedLang
    } else {
      return fallbackLanguage
    }
    
  } catch (error) {
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

    const validLanguages = Object.keys(LANGUAGE_MAPPING)
    if (validLanguages.includes(detectedLang)) {
      return detectedLang
    }

    return "hi"
  } catch (error) {
    return "hi"
  }
}

// Hybrid language detection: Franc first, OpenAI fallback for uncertain cases
const detectLanguageHybrid = async (text, useOpenAIFallback = false) => {
  const francResult = detectLanguageWithFranc(text)
  
  if (!useOpenAIFallback || francResult !== "hi") {
    return francResult
  }
  
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

  logUserTranscript(transcript, language, timestamp = new Date()) {
    const entry = {
      type: "user",
      text: transcript,
      language: language,
      timestamp: timestamp,
      source: "deepgram",
    }

    this.transcripts.push(entry)
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
          callDirection: this.callDirection,
        },
      }

      const callLog = new CallLog(callLogData)
      const savedLog = await callLog.save()

      return savedLog
    } catch (error) {
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
  const timer = createTimer("LLM")

  try {
    let systemPrompt = agentConfig.systemPrompt || "You are a helpful AI assistant."

    if (Buffer.byteLength(systemPrompt, "utf8") > 150) {
      let truncated = systemPrompt
      while (Buffer.byteLength(truncated, "utf8") > 150) {
        truncated = truncated.slice(0, -1)
      }
      systemPrompt = truncated
    }

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
      return null
    }

    const data = await response.json()
    const fullResponse = data.choices[0]?.message?.content?.trim()

    console.log(`⚡ [LLM] ${timer.end()}ms`)

    if (callLogger && fullResponse) {
      callLogger.logAIResponse(fullResponse, detectedLanguage)
    }

    return fullResponse
  } catch (error) {
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

    const timer = createTimer("TTS")

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

      console.log(`⚡ [TTS] ${timer.end()}ms`)

      if (!this.isInterrupted) {
        await this.streamAudioOptimizedForSIP(audioBase64)

        const audioBuffer = Buffer.from(audioBase64, "base64")
        this.totalAudioBytes += audioBuffer.length
      }
    } catch (error) {
      if (!this.isInterrupted) {
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
        } catch (error) {
          break
        }
      } else {
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

    let agent = null

    if (callDirection === "inbound") {
      if (!accountSid) {
        throw new Error("Missing accountSid for inbound call")
      }

      agent = await Agent.findOne({ accountSid }).lean()
      if (!agent) {
        throw new Error(`No agent found for accountSid: ${accountSid}`)
      }
    } else if (callDirection === "outbound") {
      if (!extraData) {
        throw new Error("Missing extraData for outbound call")
      }

      if (!extraData.CallVaId) {
        throw new Error("Missing CallVaId in extraData for outbound call")
      }

      const callVaId = extraData.CallVaId
      agent = await Agent.findOne({ callerId: callVaId }).lean()
      if (!agent) {
        throw new Error(`No agent found for callerId: ${callVaId}`)
      }
    } else {
      throw new Error(`Unknown call direction: ${callDirection}`)
    }

    return agent
  } catch (error) {
    throw error
  }
}

// Main WebSocket server setup with simplified processing
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

    let deepgramWs = null
    let deepgramReady = false
    let deepgramAudioQueue = []

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
          deepgramReady = true
          deepgramAudioQueue.forEach((buffer) => deepgramWs.send(buffer))
          deepgramAudioQueue = []
        }

        deepgramWs.onmessage = async (event) => {
          const data = JSON.parse(event.data)
          await handleDeepgramResponse(data)
        }

        deepgramWs.onerror = (error) => {
          deepgramReady = false
        }

        deepgramWs.onclose = () => {
          deepgramReady = false
        }
      } catch (error) {
        // Silent error handling
      }
    }

    const handleDeepgramResponse = async (data) => {
      if (data.type === "Results") {
        const transcript = data.channel?.alternatives?.[0]?.transcript
        const is_final = data.is_final

        if (transcript?.trim()) {
          if (currentTTS && isProcessing) {
            currentTTS.interrupt()
            isProcessing = false
            processingRequestId++
          }

          if (is_final) {
            userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim()

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
          if (callLogger && userUtteranceBuffer.trim()) {
            const detectedLang = detectLanguageWithFranc(userUtteranceBuffer.trim())
            callLogger.logUserTranscript(userUtteranceBuffer.trim(), detectedLang)
          }

          await processUserUtterance(userUtteranceBuffer)
          userUtteranceBuffer = ""
        }
      }
    }

    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText) return

      // Start timing STT (Speech-to-Text) - conceptually, this includes the time from audio to text
      const sttTimer = createTimer("STT")

      if (currentTTS) {
        currentTTS.interrupt()
      }

      isProcessing = true
      lastProcessedText = text
      const currentRequestId = ++processingRequestId
      const totalTimer = createTimer("TOTAL")

      try {
        // Log STT completion time
        console.log(`⚡ [STT] ${sttTimer.end()}ms`)

        const detectedLanguage = detectLanguageWithFranc(text, currentLanguage || "hi")

        if (detectedLanguage !== currentLanguage) {
          currentLanguage = detectedLanguage
        }

        const response = await processWithOpenAI(
          text,
          conversationHistory,
          detectedLanguage,
          callLogger,
          agentConfig,
        )

        if (processingRequestId === currentRequestId && response) {
          currentTTS = new SimplifiedSarvamTTSProcessor(detectedLanguage, ws, streamSid, callLogger)
          await currentTTS.synthesizeAndStream(response)

          conversationHistory.push(
            { role: "user", content: text },
            { role: "assistant", content: response }
          )

          if (conversationHistory.length > 10) {
            conversationHistory = conversationHistory.slice(-10)
          }
        }

        console.log(`⚡ [TOTAL] ${totalTimer.end()}ms`)
      } catch (error) {
        // Silent error handling
      } finally {
        if (processingRequestId === currentRequestId) {
          isProcessing = false
        }
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
            break

          case "start": {
            streamSid = data.streamSid || data.start?.streamSid
            const accountSid = data.start?.accountSid

            let mobile = null
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

            if (extraData?.CallCli && !mobile) {
              mobile = extraData.CallCli
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

            callLogger = new CallLogger(agentConfig.clientId || accountSid, mobile, callDirection)

            await connectToDeepgram()

            const greeting = agentConfig.firstMessage || "Hello! How can I help you today?"

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
            if (callLogger) {
              try {
                const savedLog = await callLogger.saveToDatabase("medium")
              } catch (error) {
                // Silent error handling
              }
            }

            if (deepgramWs?.readyState === WebSocket.OPEN) {
              deepgramWs.close()
            }
            break

          default:
            break
        }
      } catch (error) {
        // Silent error handling
      }
    })

    ws.on("close", async () => {
      if (callLogger) {
        try {
          const savedLog = await callLogger.saveToDatabase("not_connected")
        } catch (error) {
          // Silent error handling
        }
      }

      if (deepgramWs?.readyState === WebSocket.OPEN) {
        deepgramWs.close()
      }

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
      // Silent error handling
    })
  })
}

module.exports = { setupUnifiedVoiceServer }