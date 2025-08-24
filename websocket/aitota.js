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

// Test Deepgram API key on startup
const testDeepgramConnection = async () => {
  try {
    const response = await fetch("https://api.deepgram.com/v1/projects", {
      headers: {
        Authorization: `Token ${DEEPGRAM_API_KEY}`,
      },
    })

    if (response.ok) {
      console.log("✅ Deepgram API key is valid")
    } else {
      console.error(`❌ Deepgram API key test failed: ${response.status}`)
      const errorText = await response.text()
      console.error("Error details:", errorText)
    }
  } catch (error) {
    console.error("❌ Deepgram API key test error:", error.message)
  }
}

// Run test on startup
testDeepgramConnection()

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
  let sttFailed = false // Track STT failure state

  /**
   * Alternative streaming method - try different formats
   */
  const streamAudioAlternative = async (audioBase64) => {
    console.log("[STREAM-ALT] Trying high-speed streaming method")

    const audioBuffer = Buffer.from(audioBase64, "base64")

    const CHUNK_SIZE = 320 // 20ms chunks for faster transmission
    const CHUNK_DELAY = 1 // Minimal 1ms delay
    let position = 0

    const chunks = []
    while (position < audioBuffer.length) {
      const chunk = audioBuffer.slice(position, position + CHUNK_SIZE)
      chunks.push(chunk)
      position += CHUNK_SIZE
    }

    const BATCH_SIZE = 10
    for (let i = 0; i < chunks.length; i += BATCH_SIZE) {
      const batch = chunks.slice(i, i + BATCH_SIZE)

      const batchPromises = batch.map((chunk) => {
        const mediaMessage = {
          event: "media",
          streamSid: streamSid,
          media: {
            payload: chunk.toString("base64"),
          },
        }

        return new Promise((resolve, reject) => {
          try {
            ws.send(JSON.stringify(mediaMessage))
            resolve()
          } catch (error) {
            reject(error)
          }
        })
      })

      try {
        await Promise.all(batchPromises)
        // Minimal delay between batches
        if (i + BATCH_SIZE < chunks.length) {
          await new Promise((resolve) => setTimeout(resolve, CHUNK_DELAY))
        }
      } catch (error) {
        console.error("[STREAM-ALT] Batch error:", error.message)
        break
      }
    }

    console.log("[STREAM-ALT] High-speed streaming completed")
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
    return QUICK_RESPONSES[normalized] || null
  }

  /**
   * Optimized text-to-speech with faster streaming
   */
  const synthesizeAndStreamAudio = async (text, language = "en-IN") => {
    try {
      console.log(`[TTS] Synthesizing: "${text}"`)
      const startTime = Date.now()

      // Use fetch with timeout and optimized parameters for C-Zentrix
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
          pace: 1.2, // Slightly faster pace for quicker responses
          loudness: 1.0,
          speech_sample_rate: 8000,
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

      await new Promise((resolve) => setTimeout(resolve, 50)) // Reduced from 100ms

      // Try optimized primary streaming method
      await streamAudioToCall(audioBase64)

      setTimeout(async () => {
        console.log("[TTS] Trying alternative streaming method in case primary failed")
        await streamAudioAlternative(audioBase64)
      }, 200) // Reduced from 500ms
    } catch (error) {
      console.error("[TTS] Error:", error.message)

      // Send a simple beep or tone as fallback
      const fallbackAudio = generateSimpleTone(440, 0.5)
      await streamAudioToCall(fallbackAudio)
    }
  }

  /**
   * Optimized audio streaming with reduced latency for C-Zentrix
   */
  const streamAudioToCall = async (audioBase64) => {
    const audioBuffer = Buffer.from(audioBase64, "base64")

    // C-Zentrix expects specific audio format
    // 8kHz, 16-bit PCM, mono = 160 bytes per 10ms chunk
    const CHUNK_SIZE = 160 // 10ms chunks for 8kHz 16-bit mono
    const CHUNK_DURATION_MS = 2 // Reduced from 10ms to 2ms for faster streaming
    const BATCH_SIZE = 5 // Send multiple chunks in quick succession

    let position = 0
    const streamStart = Date.now()

    console.log(
      `[STREAM] Starting audio stream: ${audioBuffer.length} bytes in ${Math.ceil(audioBuffer.length / CHUNK_SIZE)} chunks`,
    )
    console.log(`[STREAM] StreamSID: ${streamSid}, WS State: ${ws.readyState}`)

    // Send a test message first to verify WebSocket is working
    const testMessage = {
      event: "test",
      streamSid: streamSid,
      timestamp: Date.now(),
    }

    try {
      ws.send(JSON.stringify(testMessage))
      console.log("[STREAM] Test message sent successfully")
    } catch (error) {
      console.error("[STREAM] Failed to send test message:", error.message)
      return
    }

    let chunksSuccessfullySent = 0

    while (position < audioBuffer.length && ws.readyState === WebSocket.OPEN) {
      // Send multiple chunks in a batch to reduce overall delay
      const batchPromises = []

      for (let i = 0; i < BATCH_SIZE && position < audioBuffer.length; i++) {
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

        batchPromises.push(
          new Promise((resolve, reject) => {
            try {
              ws.send(JSON.stringify(mediaMessage))
              chunksSuccessfullySent++
              resolve()
            } catch (error) {
              reject(error)
            }
          }),
        )

        position += CHUNK_SIZE
      }

      // Wait for the batch to complete
      try {
        await Promise.all(batchPromises)

        // Log every 50th chunk to monitor progress
        if (chunksSuccessfullySent % 50 === 0) {
          console.log(`[STREAM] Sent ${chunksSuccessfullySent} chunks`)
        }
      } catch (error) {
        console.error(`[STREAM] Failed to send batch at chunk ${chunksSuccessfullySent}:`, error.message)
        break
      }

      if (position < audioBuffer.length) {
        await new Promise((resolve) => setTimeout(resolve, CHUNK_DURATION_MS))
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
      console.log("[STREAM] End silence sent")
    } catch (error) {
      console.error("[STREAM] Failed to send end silence:", error.message)
    }

    const streamDuration = Date.now() - streamStart
    console.log(`[STREAM] Completed in ${streamDuration}ms, sent ${chunksSuccessfullySent} chunks successfully`)
  }

  /**
   * Connect to Deepgram with optimized settings and fallback
   */
  const connectToDeepgram = (attemptCount = 0) => {
    console.log(`[STT] Connecting to Deepgram (attempt ${attemptCount + 1})`)

    // Start with minimal parameters that always work
    let params = {
      encoding: "linear16",
      sample_rate: "8000",
      channels: "1",
    }

    // Add additional parameters based on attempt
    if (attemptCount === 0) {
      // First attempt: Full feature set
      params = {
        ...params,
        model: "nova-2",
        language: "en",
        interim_results: "true",
        smart_format: "true",
        endpointing: "300",
        punctuate: "true",
      }
    } else if (attemptCount === 1) {
      // Second attempt: Basic features
      params = {
        ...params,
        model: "base",
        language: "en",
        interim_results: "true",
      }
    }
    // Third attempt uses minimal params only

    const deepgramUrl = `wss://api.deepgram.com/v1/listen?${new URLSearchParams(params).toString()}`

    console.log(`[STT] Connecting to URL: ${deepgramUrl}`)
    console.log(`[STT] Using API key: ${DEEPGRAM_API_KEY ? `${DEEPGRAM_API_KEY.substring(0, 8)}...` : "MISSING"}`)

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

        if (transcript?.trim() && confidence > 0.7) {
          // Only process high-confidence results
          if (isFinal) {
            console.log(`[STT] Final transcript: "${transcript}" (${confidence})`)
            userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim()

            // Clear any existing silence timer
            if (silenceTimer) {
              clearTimeout(silenceTimer)
              silenceTimer = null
            }

            // Process immediately for short utterances
            if (userUtteranceBuffer.length < 50) {
              await processUserInput(userUtteranceBuffer)
              userUtteranceBuffer = ""
            } else {
              // Set shorter timer for longer utterances
              silenceTimer = setTimeout(async () => {
                if (userUtteranceBuffer.trim()) {
                  await processUserInput(userUtteranceBuffer)
                  userUtteranceBuffer = ""
                }
              }, 100) // 100ms vs longer delays
            }
          }
        }
      } else if (response.type === "UtteranceEnd") {
        if (userUtteranceBuffer.trim()) {
          console.log(`[STT] Utterance end: "${userUtteranceBuffer}"`)
          await processUserInput(userUtteranceBuffer)
          userUtteranceBuffer = ""
        }
      }
    })

    deepgramWs.on("error", (error) => {
      console.error(`[STT] Deepgram error (attempt ${attemptCount + 1}):`, error.message)

      // Try different connection approaches
      if (attemptCount < 3) {
        console.log(`[STT] Retrying with different parameters...`)
        setTimeout(
          () => {
            connectToDeepgram(attemptCount + 1)
          },
          1000 * (attemptCount + 1),
        ) // Increasing delay
      } else {
        console.error("[STT] All Deepgram connection attempts failed. Check API key and permissions.")
        sttFailed = true

        // Provide feedback to user about STT failure (without await - fire and forget)
        const fallbackMessage =
          "I'm having trouble with speech recognition right now, but I can still help you. Please use the keypad to navigate options."
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
          content:
            "You are a helpful AI assistant. Give very concise responses (1-2 sentences max). Be direct and helpful.",
        },
        ...conversationHistory.slice(-4), // Reduced context for faster processing
        {
          role: "user",
          content: userMessage,
        },
      ]

      // Optimized API call with timeout
      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 4000) // 4s timeout

      const response = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${OPENAI_API_KEY}`,
        },
        body: JSON.stringify({
          model: "gpt-4o-mini",
          messages: messages,
          max_tokens: 80, // Reduced from 150 for faster generation
          temperature: 0.5, // Reduced for more focused responses
          stream: false, // Disable streaming for this use case
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
   * Process user speech input with optimized flow
   */
  const processUserInput = async (transcript) => {
    if (isProcessing || !transcript.trim()) return

    isProcessing = true
    const totalStart = Date.now()

    try {
      // Start AI response and TTS in parallel for quick responses
      const quickResponse = getQuickResponse(transcript)

      if (quickResponse) {
        // Immediate response for common phrases
        conversationHistory.push({ role: "user", content: transcript }, { role: "assistant", content: quickResponse })

        await synthesizeAndStreamAudio(quickResponse)
      } else {
        // Parallel processing: Start AI response generation
        const aiResponsePromise = getAIResponse(transcript)

        // Get AI response
        const aiResponse = await aiResponsePromise

        if (aiResponse) {
          // Add to conversation history
          conversationHistory.push({ role: "user", content: transcript }, { role: "assistant", content: aiResponse })

          // Keep history lean for performance
          if (conversationHistory.length > 6) {
            conversationHistory = conversationHistory.slice(-6)
          }

          // Convert to speech and stream back
          await synthesizeAndStreamAudio(aiResponse)
        }
      }

      const totalTime = Date.now() - totalStart
      console.log(`[TOTAL] Processing completed in ${totalTime}ms`)
    } catch (error) {
      console.error("[PROCESS] Error processing user input:", error.message)
    } finally {
      isProcessing = false
    }
  }

  // Handle incoming messages from C-Zentrix
  ws.on("message", async (message) => {
    try {
      const messageStr = message.toString()

      // Skip non-JSON messages
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

          // Extract call information
          streamSid = data.streamSid || data.start?.streamSid
          callSid = data.start?.callSid
          accountSid = data.start?.accountSid

          console.log("[CZ] StreamSID:", streamSid)
          console.log("[CZ] CallSID:", callSid)
          console.log("[CZ] AccountSID:", accountSid)
          console.log("[CZ] Tracks:", JSON.stringify(data.start?.tracks))
          console.log("[CZ] Media Format:", JSON.stringify(data.start?.mediaFormat))

          // Log custom parameters if present
          if (data.start?.customParameters) {
            console.log("[CZ] Custom Parameters:", data.start.customParameters)
          }

          // Connect to Deepgram for speech recognition
          connectToDeepgram(0)

          // Send optimized greeting with proper timing
          const greeting = "Hi! How can I help you?"
          console.log("[CZ] Sending greeting:", greeting)

          // Wait a moment for call to stabilize before sending audio
          setTimeout(async () => {
            await synthesizeAndStreamAudio(greeting)
          }, 500)
          break

        case "media":
          // Forward audio data to Deepgram for transcription
          if (data.media?.payload && deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
            const audioBuffer = Buffer.from(data.media.payload, "base64")
            deepgramWs.send(audioBuffer)
          } else if (sttFailed) {
            // If STT failed, we can still respond to DTMF or provide menu-driven responses
            console.log("[STT] Audio received but STT unavailable - consider implementing DTMF fallback")
          }
          break

        case "stop":
          console.log("[CZ] Call ended")

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

        default:
          // Reduced logging for performance
          break
      }
    } catch (error) {
      console.error("[CZ] Error processing message:", error.message)
    }
  })

  // Handle connection close
  ws.on("close", () => {
    console.log("[CZ] WebSocket connection closed")

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
  })

  // Handle errors
  ws.on("error", (error) => {
    console.error("[CZ] WebSocket error:", error.message)
  })
}

module.exports = {
  setupUnifiedVoiceServer,
}
