const WebSocket = require("ws")
require("dotenv").config()

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

/**
 * Setup unified voice server for C-Zentrix integration
 * @param {WebSocket} ws - The WebSocket connection from C-Zentrix
 */
const setupUnifiedVoiceServer = (ws) => {
  console.log("Setting up C-Zentrix voice server connection")

  // Session state for this connection
  let streamSid = null
  let callSid = null
  let accountSid = null
  let conversationHistory = []
  let deepgramWs = null
  let isProcessing = false
  let userUtteranceBuffer = ""
  let silenceTimer = null
  let audioQueue = []
  let isStreaming = false
  let sttFailed = false
  
  // Enhanced debugging variables
  let audioReceived = false
  let inboundAudioCount = 0
  let outboundAudioCount = 0
  let deepgramConnectTime = null
  let lastAudioReceived = null
  let deepgramAudioChunkCount = 0
  
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
   * Generate a simple tone as fallback audio
   */
  const generateSimpleTone = (frequency = 440, duration = 0.5) => {
    const sampleRate = 8000
    const samples = Math.floor(sampleRate * duration)
    const buffer = Buffer.alloc(samples * 2) // 16-bit samples

    for (let i = 0; i < samples; i++) {
      const sample = Math.sin((2 * Math.PI * frequency * i) / sampleRate) * 0.3 // 30% volume
      const intSample = Math.floor(sample * 32767)
      buffer.writeInt16LE(intSample, i * 2)
    }

    return buffer.toString("base64")
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
   * Real-time audio streaming - sends each chunk immediately to SIP
   */
  const streamAudioToCallRealtime = async (audioBase64) => {
    const audioBuffer = Buffer.from(audioBase64, "base64")

    // C-Zentrix expects specific audio format
    // 8kHz, 16-bit PCM, mono = 160 bytes per 10ms chunk
    const CHUNK_SIZE = 160 // 10ms chunks for 8kHz 16-bit mono
    const CHUNK_DELAY = 5

    let position = 0
    const streamStart = Date.now()
    const streamStartTime = new Date().toISOString()

    console.log(
      `[STREAM-REALTIME] ${streamStartTime} - Starting stream: ${audioBuffer.length} bytes in ${Math.ceil(audioBuffer.length / CHUNK_SIZE)} chunks`,
    )
    console.log(`[STREAM] StreamSID: ${streamSid}, WS State: ${ws.readyState}`)

    let chunksSuccessfullySent = 0

    while (position < audioBuffer.length && ws.readyState === WebSocket.OPEN) {
      const chunk = audioBuffer.slice(position, position + CHUNK_SIZE)

      // Pad smaller chunks with silence if needed
      const paddedChunk =
        chunk.length < CHUNK_SIZE ? Buffer.concat([chunk, Buffer.alloc(CHUNK_SIZE - chunk.length)]) : chunk

      const mediaMessage = {
        event: "media",
        streamSid: streamSid,
        media: {
          payload: paddedChunk.toString("base64"),
        },
      }

      try {
        ws.send(JSON.stringify(mediaMessage))
        chunksSuccessfullySent++

        if (chunksSuccessfullySent % 20 === 0) {
          console.log(`[STREAM] Sent ${chunksSuccessfullySent} chunks`)
        }
      } catch (error) {
        console.error(`[STREAM] Failed to send chunk ${chunksSuccessfullySent + 1}:`, error.message)
        break
      }

      position += CHUNK_SIZE

      if (position < audioBuffer.length) {
        await new Promise((resolve) => setTimeout(resolve, CHUNK_DELAY))
      }
    }

    // Add a small silence buffer at the end
    try {
      const silenceChunk = Buffer.alloc(CHUNK_SIZE)
      const silenceMessage = {
        event: "media",
        streamSid: streamSid,
        media: {
          payload: silenceChunk.toString("base64"),
        },
      }
      ws.send(JSON.stringify(silenceMessage))
    } catch (error) {
      console.error("[STREAM] Failed to send end silence:", error.message)
    }

    const streamDuration = Date.now() - streamStart
    const completionTime = new Date().toISOString()
    console.log(
      `[STREAM-COMPLETE] ${completionTime} - Completed in ${streamDuration}ms, sent ${chunksSuccessfullySent} chunks successfully`,
    )
  }

  /**
   * Enhanced text-to-speech with single streaming method
   */
  const synthesizeAndStreamAudio = async (text, language = "en-IN") => {
    try {
      const ttsStartTime = new Date().toISOString()
      console.log(`[TTS-START] ${ttsStartTime} - Starting TTS streaming for: "${text}"`)

      console.log(`[TTS] Synthesizing: "${text}"`)
      const startTime = Date.now()

      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 3000)

      const response = await fetch("https://api.sarvam.ai/text-to-speech", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "API-Subscription-Key": SARVAM_API_KEY,
          Connection: "keep-alive",
        },
        body: JSON.stringify({
          inputs: [text],
          target_language_code: language,
          speaker: "meera",
          pitch: 0,
          pace: 1.4,
          loudness: 1.0,
          speech_sample_rate: 8000,
          enable_preprocessing: false,
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
      console.log(`[STREAM-START] ${streamStartTime} - Starting streaming to SIP`)

      await streamAudioToCallRealtime(audioBase64)
      
    } catch (error) {
      console.error("[TTS] Error:", error.message)

      // Send a simple beep or tone as fallback
      const fallbackAudio = generateSimpleTone(440, 0.5)
      await streamAudioToCallRealtime(fallbackAudio)
    }
  }

  /**
   * Enhanced Deepgram connection with comprehensive debugging
   */
  const connectToDeepgram = (attemptCount = 0) => {
    console.log(`[STT] Connecting to Deepgram (attempt ${attemptCount + 1})`)

    // C-Zentrix sends slinear16 (16-bit signed linear PCM) at 8kHz
    let params = {
      encoding: "linear16",  // This matches slinear16 from C-Zentrix
      sample_rate: "8000",
      channels: "1",
    }

    if (attemptCount === 0) {
      params = {
        ...params,
        model: "nova-2",
        language: "en",
        interim_results: "true",
        smart_format: "true",
        endpointing: "500",    // Increased for better silence detection
        punctuate: "true",
        diarize: "false",
        multichannel: "false",
        utterance_end_ms: "1000",  // Add utterance end detection
        vad_turnoff: "500"     // Voice activity detection
      }
    } else if (attemptCount === 1) {
      params = {
        ...params,
        model: "base",
        language: "en",
        interim_results: "true",
        endpointing: "300",
      }
    }

    const deepgramUrl = `wss://api.deepgram.com/v1/listen?${new URLSearchParams(params).toString()}`
    console.log(`[STT] Connecting to: ${deepgramUrl}`)

    deepgramWs = new WebSocket(deepgramUrl, {
      headers: {
        Authorization: `Token ${DEEPGRAM_API_KEY}`,
      },
    })

    deepgramWs.on("open", () => {
      const connectTime = Date.now() - deepgramConnectTime
      console.log(`[STT] Connected to Deepgram successfully in ${connectTime}ms`)
      
      // Send a keepalive message every 10 seconds
      const keepAlive = setInterval(() => {
        if (deepgramWs.readyState === WebSocket.OPEN) {
          try {
            deepgramWs.send(JSON.stringify({"type": "KeepAlive"}))
          } catch (error) {
            console.error("[STT] Keepalive error:", error.message)
            clearInterval(keepAlive)
          }
        } else {
          clearInterval(keepAlive)
        }
      }, 10000)
    })

    deepgramWs.on("message", async (data) => {
      try {
        const response = JSON.parse(data)

        if (response.type === "Results") {
          const transcript = response.channel?.alternatives?.[0]?.transcript
          const isFinal = response.is_final
          const confidence = response.channel?.alternatives?.[0]?.confidence || 0

          if (transcript?.trim()) {
            console.log(`[STT] ${isFinal ? 'FINAL' : 'interim'}: "${transcript}" (conf: ${confidence.toFixed(2)})`)
            
            // Only process final results with good confidence to avoid duplicates
            if (isFinal && confidence > 0.5) {
              if (silenceTimer) {
                clearTimeout(silenceTimer)
                silenceTimer = null
              }
              await processUserInput(transcript.trim())
            }
          }
        } else if (response.type === "UtteranceEnd") {
          console.log("[STT] Utterance ended")
          // Only process if we have new content that hasn't been processed
          if (userUtteranceBuffer.trim() && userUtteranceBuffer !== lastProcessedTranscript) {
            console.log(`[STT] Processing utterance buffer: "${userUtteranceBuffer}"`)
            await processUserInput(userUtteranceBuffer)
            userUtteranceBuffer = ""
          }
        } else if (response.type === "Metadata") {
          console.log("[STT] Metadata received:", response)
        } else {
          console.log("[STT] Other message type:", response.type)
        }
      } catch (error) {
        console.error("[STT] Error parsing Deepgram response:", error.message)
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

        const fallbackMessage =
          "I'm having trouble with speech recognition right now, but I can still help you. Please use the keypad to navigate options."
        synthesizeAndStreamAudio(fallbackMessage).catch((err) =>
          console.error("[STT] Fallback message error:", err.message),
        )
      }
    })

    deepgramWs.on("close", (code, reason) => {
      console.log(`[STT] Deepgram connection closed: ${code} - ${reason}`)
      console.log(`[STT] Audio chunks processed: ${deepgramAudioChunkCount}`)
      
      if (deepgramAudioChunkCount === 0) {
        console.error("[STT] NO AUDIO was sent to Deepgram - check audio routing from C-Zentrix!")
      }
    })

    // Override the send method to track audio chunks
    const originalSend = deepgramWs.send.bind(deepgramWs)
    deepgramWs.send = function(data) {
      if (Buffer.isBuffer(data)) {
        deepgramAudioChunkCount++
        if (deepgramAudioChunkCount <= 5 || deepgramAudioChunkCount % 50 === 0) {
          console.log(`[STT] Audio chunk ${deepgramAudioChunkCount} sent to Deepgram (${data.length} bytes)`)
        }
      }
      return originalSend(data)
    }
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

      const messages = [
        {
          role: "system",
          content:
            "You are a helpful AI assistant. Give very concise responses (1-2 sentences max). Be direct and helpful.",
        },
        ...conversationHistory.slice(-4),
        {
          role: "user",
          content: userMessage,
        },
      ]

      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 4000)

      const response = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${OPENAI_API_KEY}`,
        },
        body: JSON.stringify({
          model: "gpt-4o-mini",
          messages: messages,
          max_tokens: 80,
          temperature: 0.5,
          stream: false,
        }),
        signal: controller.signal,
      })

      clearTimeout(timeoutId)

      if (!response.ok) {
        throw new Error(`OpenAI API error: ${response.status}`)
      }

      const data = await response.json()
      const aiResponse = data.choices[0]?.message?.content?.trim()

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
    
    if (isProcessing || !transcript.trim()) return

    // Prevent duplicate processing of same transcript within 3 seconds
    const now = Date.now()
    if (transcript === lastProcessedTranscript && (now - lastProcessedTime) < 3000) {
      console.log(`[PROCESS] Skipping duplicate transcript: "${transcript}"`)
      return
    }

    lastProcessedTranscript = transcript
    lastProcessedTime = now
    
    isProcessing = true
    const totalStart = Date.now()
    console.log(`[PROCESS] Starting processing for: "${transcript}" (ID: ${responseId})`)

    try {
      // Check if this response is still active before proceeding
      if (!isResponseActive(responseId)) {
        console.log(`[PROCESS] Response ${responseId} cancelled - newer request in progress`)
        return
      }

      const quickResponse = getQuickResponse(transcript)

      if (quickResponse && isResponseActive(responseId)) {
        console.log(`[PROCESS] Quick response found: "${quickResponse}"`)
        conversationHistory.push({ role: "user", content: transcript }, { role: "assistant", content: quickResponse })

        await synthesizeAndStreamAudio(quickResponse)
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
        }
      }

      const totalTime = Date.now() - totalStart
      console.log(`[PROCESS] Processing completed in ${totalTime}ms for response ${responseId}`)
    } catch (error) {
      console.error("[PROCESS] Error processing user input:", error.message)
    } finally {
      if (isResponseActive(responseId)) {
        isProcessing = false
      }
    }
  }

  // Handle incoming messages from C-Zentrix
  ws.on("message", async (message) => {
    try {
      const messageStr = message.toString()

      if (!messageStr.startsWith("{")) {
        return
      }

      const data = JSON.parse(messageStr)

      switch (data.event) {
        case "connected":
          console.log("[CZ] Connected - Protocol:", data.protocol, "Version:", data.version)
          break

        case "start":
          console.log("[CZ] Call started")

          streamSid = data.streamSid || data.start?.streamSid
          callSid = data.start?.callSid
          accountSid = data.start?.accountSid

          console.log("[CZ] StreamSID:", streamSid)
          console.log("[CZ] CallSID:", callSid)
          console.log("[CZ] AccountSID:", accountSid)
          console.log("[CZ] Tracks:", JSON.stringify(data.start?.tracks))
          console.log("[CZ] Media Format:", JSON.stringify(data.start?.mediaFormat))

          // Validate tracks configuration - THIS IS CRITICAL
          if (!data.start?.tracks?.includes("inbound")) {
            console.warn("[CZ] WARNING: 'inbound' track not found in tracks array!")
            console.warn("[CZ] This means we won't receive audio from the caller")
            console.warn("[CZ] Current tracks:", data.start?.tracks)
          } else {
            console.log("[CZ] Inbound track confirmed - will receive caller audio")
          }

          if (data.start?.customParameters) {
            console.log("[CZ] Custom Parameters:", data.start.customParameters)
          }

          // Connect to Deepgram for speech recognition
          deepgramConnectTime = Date.now()
          connectToDeepgram(0)

          // Send optimized greeting
          const greeting = "Hi! How can I help you?"
          console.log("[CZ] Sending greeting:", greeting)

          setTimeout(async () => {
            await synthesizeAndStreamAudio(greeting)
          }, 1000) // Increased delay for call stability
          break

        case "media":
          const now = Date.now()
          lastAudioReceived = now
          
          console.log(`[CZ] Media - Track: ${data.media?.track}, Chunk: ${data.media?.chunk}, Timestamp: ${data.media?.timestamp}, Payload: ${data.media?.payload?.length || 0} chars`)
          
          // Only process inbound audio (from caller to bot)
          if (data.media?.track === "inbound") {
            inboundAudioCount++
            if (inboundAudioCount === 1) {
              console.log("[CZ] FIRST inbound audio received from caller!")
              audioReceived = true
            }
            
            if (data.media?.payload && deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
              try {
                const audioBuffer = Buffer.from(data.media.payload, "base64")
                deepgramWs.send(audioBuffer)
                
                if (inboundAudioCount <= 5 || inboundAudioCount % 50 === 0) {
                  console.log(`[STT] Sent inbound audio chunk ${inboundAudioCount} (${audioBuffer.length} bytes) to Deepgram`)
                }
              } catch (error) {
                console.error("[STT] Error processing inbound audio:", error.message)
              }
            } else {
              console.log(`[STT] Cannot process inbound audio - Deepgram state: ${deepgramWs?.readyState || 'null'}`)
            }
          } else if (data.media?.track === "outbound") {
            outboundAudioCount++
            // Don't log every outbound chunk to reduce noise
            if (outboundAudioCount <= 3 || outboundAudioCount % 100 === 0) {
              console.log(`[CZ] Outbound audio chunk ${outboundAudioCount} (bot speaking)`)
            }
          } else {
            console.log(`[CZ] Unknown/empty track: '${data.media?.track}'`)
          }
          
          // Log periodic stats
          if ((inboundAudioCount + outboundAudioCount) % 100 === 0) {
            console.log(`[CZ] Audio stats - Inbound: ${inboundAudioCount}, Outbound: ${outboundAudioCount}`)
          }
          break

        case "stop":
          console.log("[CZ] Call ended")
          console.log(`[CZ] Final audio stats - Inbound: ${inboundAudioCount}, Outbound: ${outboundAudioCount}`)
          console.log(`[STT] Total Deepgram chunks: ${deepgramAudioChunkCount}`)

          if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
            deepgramWs.close()
          }

          if (silenceTimer) {
            clearTimeout(silenceTimer)
          }
          break

        case "dtmf":
          console.log("[CZ] DTMF received:", data.dtmf?.digit)
          break

        case "vad":
          console.log("[CZ] VAD received:", data.vad?.value)
          break

        default:
          console.log(`[CZ] Unknown event: ${data.event}`)
          break
      }
    } catch (error) {
      console.error("[CZ] Error processing message:", error.message)
    }
  })

  // Add connection monitoring
  const monitoringInterval = setInterval(() => {
    if (lastAudioReceived) {
      const timeSinceLastAudio = Date.now() - lastAudioReceived
      if (timeSinceLastAudio > 10000) { // 10 seconds without audio
        console.log(`[CZ] WARNING: No audio received for ${timeSinceLastAudio}ms`)
      }
    }
    
    // Log current status every 30 seconds
    if (Date.now() % 30000 < 5000) {
      console.log(`[STATUS] Audio received: ${audioReceived}, Inbound: ${inboundAudioCount}, Outbound: ${outboundAudioCount}, Deepgram chunks: ${deepgramAudioChunkCount}`)
    }
  }, 5000)

  // Handle connection close
  ws.on("close", () => {
    console.log("[CZ] WebSocket connection closed")
    console.log(`[CZ] Session summary - Audio received: ${audioReceived}, Inbound chunks: ${inboundAudioCount}, Outbound chunks: ${outboundAudioCount}`)

    // Clear monitoring
    clearInterval(monitoringInterval)

    // Cleanup
    if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
      deepgramWs.close()
    }

    if (silenceTimer) {
      clearTimeout(silenceTimer)
    }

    // Reset session state
    streamSid = null
    callSid = null
    accountSid = null
    conversationHistory = []
    isProcessing = false
    userUtteranceBuffer = ""
    audioQueue = []
    isStreaming = false
    lastProcessedTranscript = ""
    lastProcessedTime = 0
    activeResponseId = null
    audioReceived = false
    inboundAudioCount = 0
    outboundAudioCount = 0
    deepgramAudioChunkCount = 0
  })

  // Handle errors
  ws.on("error", (error) => {
    console.error("[CZ] WebSocket error:", error.message)
  })
}

module.exports = {
  setupUnifiedVoiceServer,
}