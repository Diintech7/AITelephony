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
   * FIXED: Stream audio to SanIPPBX in their expected format
   * Based on the logs: chunk duration 20ms, 8kHz sample rate, LINEAR16
   */
  const streamAudioToSanIPPBX = async (audioBase64) => {
    if (!streamId || !callId || !channelId) {
      console.error("[SANPBX] Missing required IDs for streaming")
      return
    }

    try {
      const audioBuffer = Buffer.from(audioBase64, "base64")
      
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
      console.log(
        `[SANPBX-FORMAT] Sending audio -> encoding=${ENCODING}, sample_rate_hz=${SAMPLE_RATE_HZ}, channels=${CHANNELS}, bytes_per_sample=${BYTES_PER_SAMPLE}, chunk_duration_ms=${CHUNK_DURATION_MS}, chunk_size_bytes=${CHUNK_SIZE}`,
      )
      console.log(`[SANPBX] StreamID: ${streamId}, CallID: ${callId}, ChannelID: ${channelId}`)

      let chunksSuccessfullySent = 0

      while (position < audioBuffer.length && ws.readyState === WebSocket.OPEN) {
        const chunk = audioBuffer.slice(position, position + CHUNK_SIZE)

        // Pad smaller chunks with silence if needed
        const paddedChunk = chunk.length < CHUNK_SIZE 
          ? Buffer.concat([chunk, Buffer.alloc(CHUNK_SIZE - chunk.length)]) 
          : chunk

        // Prepare payload
        const payloadBase64 = paddedChunk.toString("base64")

        // Format message exactly like SanIPPBX expects
        const mediaMessage = {
          event: "media",
          payload: payloadBase64,
          chunk: currentChunk,
          chunk_durn_ms: CHUNK_DURATION_MS,
          channelId: channelId,
          callId: callId,
          streamId: streamId,
          callerId: callerIdValue || "",
          callDirection: callDirectionValue || "Outgoing",
          extraParams: "",
          cid: "0",
          did: didValue || "",
          timestamp: new Date().toISOString().slice(0, 19).replace('T', ' ')
        }

        try {
          // Log the exact message being sent, including payload
          console.log(JSON.stringify(mediaMessage, null, 2))
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
            event: "media",
            payload: silenceChunk.toString("base64"),
            chunk: currentChunk,
            chunk_durn_ms: CHUNK_DURATION_MS,
            channelId: channelId,
            callId: callId,
            streamId: streamId,
            callerId: callerIdValue || "",
            callDirection: callDirectionValue || "Outgoing",
            extraParams: "",
            cid: "0",
            did: didValue || "",
            timestamp: new Date().toISOString().slice(0, 19).replace('T', ' ')
          }

          // Log the exact trailing silence message being sent
          console.log(JSON.stringify(silenceMessage, null, 2))
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
   * FIXED: Optimized text-to-speech with Sarvam API for 8kHz output
   */
  const synthesizeAndStreamAudio = async (text, language = "en-IN") => {
    try {
      const ttsStartTime = new Date().toISOString()
      console.log(`[TTS-START] ${ttsStartTime} - Starting TTS for: "${text}"`)

      const startTime = Date.now()

      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 5000)

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

      // Stream audio in SanIPPBX format
      await streamAudioToSanIPPBX(audioBase64)
      
    } catch (error) {
      console.error("[TTS] Error:", error.message)

      // Send simple silence as fallback
      const fallbackAudio = Buffer.alloc(8000).toString("base64") // 1 second of silence
      await streamAudioToSanIPPBX(fallbackAudio)
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
        endpointing: "300",
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
        endpointing: "200",
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
          console.log(JSON.stringify({
            source: "Deepgram",
            type: isFinal ? "final" : "interim",
            transcript,
            confidence
          }, null, 2))

          // forward anything Deepgram believes is speech, even low-confidence, for visibility
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

      const messages = [
        {
          role: "system",
          content: "You are a helpful AI assistant. Give very concise responses (1-2 sentences max). Be direct and helpful.",
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

  // Handle incoming messages from SanIPPBX
  ws.on("message", async (message) => {
    try {
      const messageStr = message.toString()

      if (!messageStr.startsWith("{")) {
        return
      }

      const data = JSON.parse(messageStr)
      console.log(`📨 [SANPBX] Received event: ${data.event}`)

      switch (data.event) {
        case "connected":
          console.log("[SANPBX] Connected")
          console.log("ChannelID:", data.channelId)
          console.log("CallID:", data.callId) 
          console.log("StreamID:", data.streamId)
          console.log("CallerID:", data.callerId)
          console.log("Call Direction:", data.callDirection)
          console.log("DID:", data.did)
          // Cache identifiers if provided
          callerIdValue = data.callerId || callerIdValue
          callDirectionValue = data.callDirection || callDirectionValue
          didValue = data.did || didValue
          break

        case "start":
          console.log("[SANPBX] Call started")
          
          streamId = data.streamId
          callId = data.callId
          channelId = data.channelId

          console.log("[SANPBX] StreamID:", streamId)
          console.log("[SANPBX] CallID:", callId)
          console.log("[SANPBX] ChannelID:", channelId)
          console.log("[SANPBX] Media Format:", JSON.stringify(data.mediaFormat))

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
          } catch (e) {
            console.log('[STT] Using default media format due to parse error:', e.message)
            inputEncoding = 'linear16'
            inputSampleRateHz = 8000
            inputChannels = 1
          }

          // Connect to Deepgram for speech recognition
          connectToDeepgram(0)

          // Send greeting after call is established
          const greeting = "Hi! How can I help you?"
          console.log("[SANPBX] Sending greeting:", greeting)

          setTimeout(async () => {
            await synthesizeAndStreamAudio(greeting)
          }, 1500) // Wait for call to be fully established
          break

        case "answer":
          console.log("[SANPBX] Call answered - ready for media streaming")
          break

        case "media":
          // Forward audio data to Deepgram for transcription
          if (data.payload && deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
            const audioBuffer = Buffer.from(data.payload, "base64")
            deepgramWs.send(audioBuffer)
            
            chunkCounter++
            if (chunkCounter % 50 === 0) {
              console.log(`[SANPBX] Processed ${chunkCounter} audio chunks`)
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
          // Handle DTMF input if needed
          break

        case "transfer-call-response":
          console.log("[SANPBX] Transfer response:", data.message)
          break

        case "hangup-call-response":
          console.log("[SANPBX] Hangup response:", data.message)
          break

        default:
          console.log(`[SANPBX] Unknown event: ${data.event}`)
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