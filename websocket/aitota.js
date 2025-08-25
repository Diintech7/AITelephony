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
  let sttFailed = false // Track STT failure state

  /**
   * Alternative streaming method - try different formats
   */
  const streamAudioAlternative = async (audioBase64) => {
    console.log("[STREAM-ALT] Trying high-speed streaming method")

    const audioBuffer = Buffer.from(audioBase64, "base64")

    const CHUNK_SIZE = 320 // 20ms chunks for faster transmission
    const CHUNK_DELAY = 10 // Increased delay for individual chunk sending
    let position = 0

    const chunks = []
    while (position < audioBuffer.length) {
      const chunk = audioBuffer.slice(position, position + CHUNK_SIZE)
      chunks.push(chunk)
      position += CHUNK_SIZE
    }

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i]

      const mediaMessage = {
        event: "media",
        streamSid: streamSid,
        media: {
          payload: chunk.toString("base64"),
        },
      }

      try {
        ws.send(JSON.stringify(mediaMessage))

        if ((i + 1) % 20 === 0) {
          console.log(`[STREAM-ALT] Sent ${i + 1} chunks`)
        }
      } catch (error) {
        console.error("[STREAM-ALT] Chunk error:", error.message)
        break
      }

      if (i < chunks.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, CHUNK_DELAY))
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
   * Optimized text-to-speech with faster streaming
   */
  const synthesizeAndStreamAudio = async (text, language = "en-IN") => {
    try {
      const ttsStartTime = new Date().toISOString()
      console.log(`[TTS-START] ${ttsStartTime} - Starting TTS streaming for: "${text}"`)

      console.log(`[TTS] Synthesizing: "${text}"`)
      const startTime = Date.now()

      // Use fetch with timeout and optimized parameters for C-Zentrix
      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 3000) // Reduced from 5s to 3s

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
          pace: 1.4, // Increased pace for faster speech
          loudness: 1.0,
          speech_sample_rate: 8000,
          enable_preprocessing: false, // Disable preprocessing for speed
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
      console.log(`[STREAM-START] ${streamStartTime} - Starting immediate streaming to SIP`)

      await streamAudioToCallRealtime(audioBase64)

      setTimeout(async () => {
        console.log("[TTS] Trying alternative streaming method in case primary failed")
        await streamAudioAlternative(audioBase64)
      }, 50) // Reduced from 100ms for faster fallback
    } catch (error) {
      console.error("[TTS] Error:", error.message)

      // Send a simple beep or tone as fallback
      const fallbackAudio = generateSimpleTone(440, 0.5)
      await streamAudioToCallRealtime(fallbackAudio)
    }
  }

  /**
   * Real-time audio streaming - sends each chunk immediately to SIP
   */
  const streamAudioToCallRealtime = async (audioBase64) => {
    const audioBuffer = Buffer.from(audioBase64, "base64")

    // C-Zentrix expects specific audio format
    // 8kHz, 16-bit PCM, mono = 160 bytes per 10ms chunk
    const CHUNK_SIZE = 160 // 10ms chunks for 8kHz 16-bit mono
    const CHUNK_DELAY = 5 // Reduced delay for immediate sending

    let position = 0
    const streamStart = Date.now()
    const streamStartTime = new Date().toISOString()

    console.log(
      `[STREAM-REALTIME] ${streamStartTime} - Starting real-time stream: ${audioBuffer.length} bytes in ${Math.ceil(audioBuffer.length / CHUNK_SIZE)} chunks`,
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

        const chunkTime = new Date().toISOString()
        console.log(`[CHUNK-SENT] ${chunkTime} - Chunk ${chunksSuccessfullySent} sent to SIP immediately`)

        if (chunksSuccessfullySent % 10 === 0) {
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
      const endTime = new Date().toISOString()
      console.log(`[STREAM-END] ${endTime} - End silence sent`)
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
   * Optimized audio streaming with reduced latency for C-Zentrix
   */
  const streamAudioToCall = async (audioBase64) => {
    return await streamAudioToCallRealtime(audioBase64)
  }

  /**
   * Connect to Deepgram with optimized settings for low latency
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
      // First attempt: Ultra-low latency settings
      params = {
        ...params,
        model: "nova-2",
        language: "en",
        interim_results: "true",
        smart_format: "true",
        endpointing: "100", // Reduced from 300ms to 100ms for faster response
        punctuate: "true",
        diarize: "false", // Disable diarization for speed
        multichannel: "false", // Disable multichannel for speed
      }
    } else if (attemptCount === 1) {
      // Second attempt: Basic features with low latency
      params = {
        ...params,
        model: "base",
        language: "en",
        interim_results: "true",
        endpointing: "50", // Even lower for fallback
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

        if (transcript?.trim()) {
          // Lower confidence threshold for faster response
          if (confidence > 0.5) {
            console.log(`[STT] ${isFinal ? 'Final' : 'Interim'} transcript: "${transcript}" (${confidence})`)
            
            if (isFinal) {
              // Clear any existing silence timer
              if (silenceTimer) {
                clearTimeout(silenceTimer)
                silenceTimer = null
              }

              // Process immediately for all final results
              await processUserInput(transcript.trim())
            } else {
              // For interim results, only process if they're substantial and confident
              if (transcript.length > 3 && confidence > 0.8) {
                // Clear any existing silence timer
                if (silenceTimer) {
                  clearTimeout(silenceTimer)
                  silenceTimer = null
                }

                // Process interim results for very confident short phrases
                if (transcript.length < 20) {
                  await processUserInput(transcript.trim())
                }
              }
            }
          }
        }
      } else if (response.type === "UtteranceEnd") {
        // Process any remaining buffer immediately
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
   * Process user speech input with ultra-low latency flow
   */
  const processUserInput = async (transcript) => {
    if (isProcessing || !transcript.trim()) return

    isProcessing = true
    const totalStart = Date.now()
    console.log(`[PROCESS] Starting processing for: "${transcript}"`)

    try {
      // Check for quick responses first (0ms latency)
      const quickResponse = getQuickResponse(transcript)

      if (quickResponse) {
        // Immediate response for common phrases
        console.log(`[PROCESS] Quick response found: "${quickResponse}"`)
        conversationHistory.push({ role: "user", content: transcript }, { role: "assistant", content: quickResponse })

        // Start TTS immediately without waiting
        synthesizeAndStreamAudio(quickResponse).catch(err => 
          console.error("[PROCESS] Quick response TTS error:", err.message)
        )
      } else {
        // For non-quick responses, start AI processing immediately
        console.log(`[PROCESS] Getting AI response for: "${transcript}"`)
        
        // Start AI response generation (don't await here for faster response)
        getAIResponse(transcript).then(async (aiResponse) => {
          if (aiResponse) {
            console.log(`[PROCESS] AI response received: "${aiResponse}"`)
            
            // Add to conversation history
            conversationHistory.push({ role: "user", content: transcript }, { role: "assistant", content: aiResponse })

            // Keep history lean for performance
            if (conversationHistory.length > 6) {
              conversationHistory = conversationHistory.slice(-6)
            }

            // Convert to speech and stream back
            await synthesizeAndStreamAudio(aiResponse)
          }
        }).catch(error => {
          console.error("[PROCESS] AI response error:", error.message)
        })
      }

      const totalTime = Date.now() - totalStart
      console.log(`[PROCESS] Initial processing completed in ${totalTime}ms`)
    } catch (error) {
      console.error("[PROCESS] Error processing user input:", error.message)
    } finally {
      // Don't set isProcessing to false immediately for quick responses
      // Allow parallel processing of multiple inputs
      setTimeout(() => {
        isProcessing = false
      }, 100) // Small delay to prevent rapid-fire processing
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
