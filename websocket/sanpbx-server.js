const WebSocket = require("ws")
require("dotenv").config()
const Agent = require("../models/Agent")

// API Keys from environment
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY
const SARVAM_API_KEY = process.env.SARVAM_API_KEY
const OPENAI_API_KEY = process.env.OPENAI_API_KEY

// Validate API keys
if (!DEEPGRAM_API_KEY || !SARVAM_API_KEY || !OPENAI_API_KEY) {
  console.error("Missing required API keys in environment variables")
  process.exit(1)
}

const fetch = globalThis.fetch || require("node-fetch")

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

// Helpers
function extractDigits(value) {
  if (!value) return ""
  return String(value).replace(/\D+/g, "")
}
function last10Digits(value) {
  const digits = extractDigits(value)
  return digits.slice(-10)
}

// Map simple lang codes to Sarvam codes (align with aitota.js)
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

const getSarvamLanguage = (detectedLang, defaultLang = "en") => {
  const lang = (detectedLang || defaultLang || "en").toLowerCase()
  return LANGUAGE_MAPPING[lang] || LANGUAGE_MAPPING.en
}

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

/**
 * Setup unified voice server for SanIPPBX integration
 * @param {WebSocket} ws - The WebSocket connection from SanIPPBX
 */
const setupSanPbxWebSocketServer = (ws) => {
  console.log("Setting up SanIPPBX voice server connection")

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
  let conversationHistory = []
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

      while (position < audioBuffer.length && ws.readyState === WebSocket.OPEN) {
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
          await new Promise((resolve) => setTimeout(resolve, CHUNK_DURATION_MS))
        }
      }

      // Add silence buffer at the end to ensure clean audio termination
      try {
        for (let i = 0; i < 3; i++) {
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

      const streamDuration = Date.now() - streamStart
      const completionTime = new Date().toISOString()
      console.log(
        `[SANPBX-STREAM-COMPLETE] ${completionTime} - Completed in ${streamDuration}ms, sent ${chunksSuccessfullySent} chunks successfully`,
      )
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
          "API-Subscription-Key": SARVAM_API_KEY,
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
          model: "bulbul:v1",
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

      // Stream audio in SanIPPBX format (reverse-media)
      await streamAudioToSanIPPBX(pcmBase64)
      
    } catch (error) {
      console.error("[TTS] Error:", error.message)

      // Send simple silence as fallback
      const fallbackAudio = Buffer.alloc(8000).toString("base64") // 1 second of silence
      await streamAudioToSanIPPBX(fallbackAudio)
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
   * Connect to Deepgram with optimized settings for 8kHz audio
   */
  const connectToDeepgram = (attemptCount = 0) => {
    console.log(`[STT] Connecting to Deepgram (attempt ${attemptCount + 1})`)

    // FIXED: Updated for SanIPPBX audio format
    let params = {
      encoding: inputEncoding,
      sample_rate: String(inputSampleRateHz),
      channels: String(inputChannels),
    }

    if (attemptCount === 0) {
      params = {
        ...params,
        model: "nova-2",
        language: "en",
        interim_results: "true",
        smart_format: "true",
        endpointing: "120",
        punctuate: "true",
        diarize: "false",
        multichannel: "false",
      }
    } else if (attemptCount === 1) {
      params = {
        ...params,
        model: "base",
        language: "en",
        interim_results: "true",
        endpointing: "100",
      }
    }

    const deepgramUrl = `wss://api.deepgram.com/v1/listen?${new URLSearchParams(params).toString()}`

    console.log(`[STT] Deepgram params -> encoding=${params.encoding}, sample_rate=${params.sample_rate}, channels=${params.channels}`)
    console.log(`[STT] Connecting to URL: ${deepgramUrl}`)

    deepgramWs = new WebSocket(deepgramUrl, {
      headers: {
        Authorization: `Token ${DEEPGRAM_API_KEY}`,
      },
    })

    deepgramWs.on("open", () => {
      console.log("[STT] Connected to Deepgram successfully")
    })

    deepgramWs.on("message", async (data) => {
      const response = JSON.parse(data)

      if (response.type === "Results") {
        const transcript = response.channel?.alternatives?.[0]?.transcript
        const isFinal = response.is_final
        const confidence = response.channel?.alternatives?.[0]?.confidence

        if (transcript?.trim()) {
          // Explicit readable logs for interim/final text from Deepgram
          if (isFinal) {
            console.log(`[STT:FINAL] ${transcript.trim()} (conf=${typeof confidence === 'number' ? confidence.toFixed(2) : confidence})`)
          } else {
            console.log(`[STT:INTERIM] ${transcript.trim()} (conf=${typeof confidence === 'number' ? confidence.toFixed(2) : confidence})`)
          }

          if (isFinal) {
            if (silenceTimer) {
              clearTimeout(silenceTimer)
              silenceTimer = null
            }
            await processUserInput(transcript.trim())
          }
        }
      } else if (response.type === "UtteranceEnd") {
        if (userUtteranceBuffer.trim() && userUtteranceBuffer !== lastProcessedTranscript) {
          console.log(`[STT] Utterance end: "${userUtteranceBuffer}"`)
          await processUserInput(userUtteranceBuffer)
          userUtteranceBuffer = ""
        }
      } else {
        // Log any other Deepgram message types for full visibility
        try {
          console.log(`[STT:DG-RAW] ${JSON.stringify(response)}`)
        } catch (_) {
          console.log('[STT:DG-RAW] <unserializable message>')
        }
      }
    })

    deepgramWs.on("error", (error) => {
      console.error(`[STT] Deepgram error (attempt ${attemptCount + 1}):`, error.message)

      if (attemptCount < 2) {
        console.log(`[STT] Retrying with different parameters...`)
        setTimeout(
          () => {
            connectToDeepgram(attemptCount + 1)
          },
          1000 * (attemptCount + 1),
        )
      } else {
        console.error("[STT] All Deepgram connection attempts failed. Check API key and permissions.")
        sttFailed = true

        const fallbackMessage = "I'm having trouble with speech recognition right now, but I can still help you. Please use the keypad to navigate options."
        synthesizeAndStreamAudio(fallbackMessage).catch((err) =>
          console.error("[STT] Fallback message error:", err.message),
        )
      }
    })

    deepgramWs.on("close", (code, reason) => {
      console.log(`[STT] Deepgram connection closed: ${code} - ${reason}`)
    })
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
      const basePrompt = (ws.sessionAgentConfig?.systemPrompt || "You are a helpful AI assistant.").trim()
      const firstMessage = (ws.sessionAgentConfig?.firstMessage || "").trim()
      const knowledgeBlock = firstMessage ? `FirstGreeting: "${firstMessage}"\n` : ""
      const policyBlock = [
        "Answer strictly using the information provided above.",
        "If the user asks for address, phone, timings, or other specifics, check the System Prompt or FirstGreeting.",
        "If the information is not present, reply briefly that you don't have that information.",
        "Always end your answer with a short, relevant follow-up question to keep the conversation going.",
        "Keep the entire reply under 100 tokens.",
      ].join(" ")
      const systemPrompt = `System Prompt:\n${basePrompt}\n\n${knowledgeBlock}${policyBlock}`

      const messages = [
        { role: "system", content: systemPrompt },
        ...conversationHistory.slice(-6),
        { role: "user", content: userMessage },
      ]

      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 2500)

      const response = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${OPENAI_API_KEY}`,
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
        conversationHistory.push({ role: "user", content: transcript }, { role: "assistant", content: quickResponse })

        await synthesizeAndStreamAudio(quickResponse)
        console.log(`[PROCESS] TTS finished for quick response.`)
      } else if (isResponseActive(responseId)) {
        console.log(`[PROCESS] Getting AI response for: "${transcript}"`)
        
        const aiResponse = await getAIResponse(transcript)
        if (aiResponse && isResponseActive(responseId)) {
          console.log(`[PROCESS] AI response received: "${aiResponse}"`)
          
          conversationHistory.push({ role: "user", content: transcript }, { role: "assistant", content: aiResponse })

          // Keep history lean for performance
          if (conversationHistory.length > 6) {
            conversationHistory = conversationHistory.slice(-6)
          }

          await synthesizeAndStreamAudio(aiResponse)
          console.log(`[PROCESS] TTS finished for AI response.`)
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
          console.log("[SANPBX] Connected")
          
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
          break

        case "start":
          console.log("[SANPBX] Call started")
          
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

          // Cache identifiers if provided (prefer start values if present)
          callerIdValue = data.callerId || callerIdValue
          callDirectionValue = data.callDirection || callDirectionValue
          didValue = data.did || didValue

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

          // Resolve agent using DID→Agent.callerId priority (like aitota.js style)
          try {
            const fromNumber = (data.start && data.start.from) || data.from || callerIdValue
            const toNumber = (data.start && data.start.to) || data.to || didValue
            const fromLast = last10Digits(fromNumber)
            const toLast = last10Digits(toNumber)

            let agent = null
            let matchReason = "none"

            if (!agent && didValue) {
              agent = await Agent.findOne({ isActive: true, callerId: String(didValue) })
                .select("_id clientId agentName callingNumber sttSelection ttsSelection llmSelection systemPrompt firstMessage voiceSelection language callerId")
                .lean()
              if (agent) matchReason = "callerId==DID"
            }

            if (!agent && callerIdValue) {
              agent = await Agent.findOne({ isActive: true, callerId: String(callerIdValue) })
                .select("_id clientId agentName callingNumber sttSelection ttsSelection llmSelection systemPrompt firstMessage voiceSelection language callerId")
                .lean()
              if (agent) matchReason = "callerId==CallerID"
            }

            if (!agent) {
              try {
                const candidates = await Agent.find({ isActive: true, callingNumber: { $exists: true } })
                  .select("_id clientId agentName callingNumber sttSelection ttsSelection llmSelection systemPrompt firstMessage voiceSelection language callerId")
                  .lean()
                agent = candidates.find((a) => last10Digits(a.callingNumber) === toLast || last10Digits(a.callingNumber) === fromLast) || null
                if (agent) matchReason = "callingNumber(last10)==to/from"
              } catch (_) {}
            }

            if (agent) {
              console.log(`[SANPBX] Agent matched: ${agent.agentName} (reason=${matchReason})`)
              // Bind into session for downstream use (TTS, prompts, etc.)
              ws.sessionAgentConfig = agent
            } else {
              console.log(`[SANPBX] No agent matched via DID/CallerID/last10. Proceeding without agent binding.`)
            }
          } catch (e) {
            console.log(`[SANPBX] Agent matching error: ${e.message}`)
          }

          // Connect to Deepgram for speech recognition
          connectToDeepgram(0)

          // Send greeting after call is established
          // Use agent-configured greeting (mirror aitota.js)
          let greeting = ws.sessionAgentConfig?.firstMessage || "Hello! How can I help you today?"
          if (callerIdValue && typeof callerIdValue === 'string') {
            // optional personalization can be added if you later parse name
          }
          console.log("[SANPBX] Sending greeting:", greeting)

          setTimeout(async () => {
            await synthesizeAndStreamAudio(greeting, ws.sessionAgentConfig?.language || "en")
          }, 1500) // Wait for call to be fully established
          break

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
          if (data.payload && deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
            const audioBuffer = Buffer.from(data.payload, "base64")
            deepgramWs.send(audioBuffer)
            chunkCounter++
            if (chunkCounter % 25 === 0) {
              console.log(`[SANPBX-MEDIA-IN] chunks=${chunkCounter}, last_bytes=${audioBuffer.length}`)
            }
          } else if (sttFailed) {
            console.log("[STT] Audio received but STT unavailable - consider implementing DTMF fallback")
          }
          break

        case "stop":
          console.log("[SANPBX] Call ended")

          if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
            deepgramWs.close()
          }

          if (silenceTimer) {
            clearTimeout(silenceTimer)
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

        default:
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
          break
      }
    } catch (error) {
      console.error("[SANPBX] Error processing message:", error.message)
    }
  })

  // Handle connection close
  ws.on("close", () => {
    console.log("[SANPBX] WebSocket connection closed")

    // Cleanup
    if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
      deepgramWs.close()
    }

    if (silenceTimer) {
      clearTimeout(silenceTimer)
    }

    // Reset session state
    streamId = null
    callId = null
    channelId = null
    conversationHistory = []
    isProcessing = false
    userUtteranceBuffer = ""
    sttFailed = false
    chunkCounter = 0
    lastProcessedTranscript = ""
    lastProcessedTime = 0
    activeResponseId = null
  })

  // Handle errors
  ws.on("error", (error) => {
    console.error("[SANPBX] WebSocket error:", error.message)
  })
}

module.exports = {
  setupSanPbxWebSocketServer,
}