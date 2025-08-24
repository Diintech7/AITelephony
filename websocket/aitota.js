const WebSocket = require("ws")
require("dotenv").config()

// Load API keys from environment variables - only the three required services
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

const LANGUAGE_MAPPING = {
  hi: "hi-IN",
  en: "en-IN",
}

const getSarvamLanguage = (defaultLang = "hi") => {
  return LANGUAGE_MAPPING[defaultLang] || "hi-IN"
}

const getDeepgramLanguage = (defaultLang = "hi") => {
  if (defaultLang === "hi") return "hi"
  if (defaultLang === "en") return "en-IN"
  return defaultLang
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
    "male-casual": "amol",
    "female-casual": "anushka",
    "male-young": "arjun",
    "female-young": "diya",
  }

  return voiceMapping[normalized] || "pavithra"
}

const processWithOpenAI = async (
  userMessage,
  conversationHistory,
  systemPrompt = "You are a helpful AI assistant.",
) => {
  const timer = createTimer("LLM_PROCESSING")

  try {
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
        max_tokens: 150,
        temperature: 0.7,
      }),
    })

    if (!response.ok) {
      throw new Error(`OpenAI API error: ${response.status}`)
    }

    const data = await response.json()
    const aiResponse = data.choices[0]?.message?.content?.trim()

    if (aiResponse) {
      console.log(`🕒 [LLM-PROCESSING] ${timer.end()}ms - Response generated`)
      return aiResponse
    }

    throw new Error("No response from OpenAI")
  } catch (error) {
    console.log(`❌ [LLM-PROCESSING] ${timer.end()}ms - Error: ${error.message}`)
    return "I apologize, but I'm having trouble processing your request right now."
  }
}

class SimplifiedSarvamTTSProcessor {
  constructor(language, ws, streamSid) {
    this.language = language
    this.ws = ws
    this.streamSid = streamSid
    this.sarvamLanguage = getSarvamLanguage(language)
    this.voice = getValidSarvamVoice("pavithra")
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
      const audioBase64 = responseData.audios?.[0]

      if (!audioBase64 || this.isInterrupted) {
        if (!this.isInterrupted) {
          console.log(`❌ [TTS-SYNTHESIS] ${timer.end()}ms - No audio data received`)
          throw new Error("No audio data received from Sarvam API")
        }
        return
      }

      console.log(`🕒 [TTS-SYNTHESIS] ${timer.end()}ms - Audio generated, streaming...`)
      await this.streamAudioToTwilio(audioBase64)
    } catch (error) {
      if (!this.isInterrupted) {
        console.log(`❌ [TTS-SYNTHESIS] ${timer.end()}ms - Error: ${error.message}`)
      }
    }
  }

  async streamAudioToTwilio(audioBase64) {
    if (this.isInterrupted) return

    const streamingTimer = createTimer("AUDIO_STREAMING")
    this.currentAudioStreaming = { interrupt: false }

    try {
      const audioBuffer = Buffer.from(audioBase64, "base64")
      const chunkSize = 640 // 40ms chunks for 8kHz (within C-Zentrix recommended range)
      let chunkNumber = 1
      const totalChunks = Math.ceil(audioBuffer.length / chunkSize)

      console.log(`🎵 [AUDIO-STREAMING] Starting C-Zentrix streaming: ${totalChunks} chunks`)

      for (let i = 0; i < audioBuffer.length; i += chunkSize) {
        if (this.isInterrupted || this.currentAudioStreaming.interrupt) {
          console.log("🛑 [AUDIO-STREAMING] Interrupted during streaming")
          return
        }

        const chunk = audioBuffer.slice(i, i + chunkSize)

        const audioPayload = {
          event: "media",
          streamSid: this.streamSid,
          media: {
            payload: chunk.toString("base64"), // C-Zentrix expects base64 slinear16
          },
        }

        this.ws.send(JSON.stringify(audioPayload))
        this.totalAudioBytes += chunk.length
        chunkNumber++

        await new Promise((resolve) => setTimeout(resolve, 40))
      }

      console.log(
        `🕒 [AUDIO-STREAMING] ${streamingTimer.end()}ms - Completed ${totalChunks} chunks (${this.totalAudioBytes} bytes)`,
      )
    } catch (error) {
      console.log(`❌ [AUDIO-STREAMING] ${streamingTimer.end()}ms - Error: ${error.message}`)
    } finally {
      this.currentAudioStreaming = null
    }
  }
}

const setupUnifiedVoiceServer = (wss) => {
  console.log("🎤 [VOICE-AI] Setting up unified voice server")

  // Handle each WebSocket connection
  wss.on("connection", (ws, req) => {
    const clientIP = req.socket.remoteAddress
    console.log(`🔌 [WEBSOCKET] New voice AI connection established from ${clientIP}`)
    console.log(`🔌 [WEBSOCKET] Connection headers:`, req.headers)

    let streamSid = null
    let accountSid = null
    let callSid = null
    const currentLanguage = "hi"
    let conversationHistory = []
    let currentTTS = null
    let isProcessing = false
    let lastProcessedText = ""
    let processingRequestId = 0
    let sequenceNumber = 0

    // Deepgram WebSocket connection
    let deepgramWs = null
    let deepgramReady = false
    let sttTimer = null

    const connectToDeepgram = async () => {
      try {
        const deepgramLanguage = getDeepgramLanguage(currentLanguage)
        console.log(`🎤 [DEEPGRAM] Attempting connection with language: ${deepgramLanguage}`)

        const deepgramUrl = new URL("wss://api.deepgram.com/v1/listen")
        deepgramUrl.searchParams.append("sample_rate", "8000")
        deepgramUrl.searchParams.append("channels", "1")
        deepgramUrl.searchParams.append("encoding", "linear16")
        deepgramUrl.searchParams.append("model", "nova-2")
        deepgramUrl.searchParams.append("language", deepgramLanguage)
        deepgramUrl.searchParams.append("smart_format", "true")
        deepgramUrl.searchParams.append("interim_results", "false")

        console.log(`🎤 [DEEPGRAM] Connection URL: ${deepgramUrl.toString()}`)

        deepgramWs = new WebSocket(deepgramUrl.toString(), {
          headers: { Authorization: `Token ${API_KEYS.deepgram}` },
        })

        deepgramWs.onopen = () => {
          console.log("🎤 [DEEPGRAM] Connection established successfully")
          deepgramReady = true
          console.log("🎤 [DEEPGRAM] Ready to process audio")
        }

        deepgramWs.onmessage = async (event) => {
          console.log(`🎤 [DEEPGRAM] Received message: ${event.data}`)
          const data = JSON.parse(event.data)
          await handleDeepgramResponse(data)
        }

        deepgramWs.onerror = (error) => {
          console.log("❌ [DEEPGRAM] Connection error:", error.message)
          console.log("❌ [DEEPGRAM] Error details:", error)
          deepgramReady = false
          setTimeout(connectToDeepgram, 2000)
        }

        deepgramWs.onclose = (event) => {
          console.log(`🎤 [DEEPGRAM] Connection closed - Code: ${event.code}, Reason: ${event.reason}`)
          deepgramReady = false
        }
      } catch (error) {
        console.log("❌ [DEEPGRAM] Connection failed:", error.message)
        console.log("❌ [DEEPGRAM] Stack trace:", error.stack)
        deepgramReady = false
        setTimeout(connectToDeepgram, 2000)
      }
    }

    const handleDeepgramResponse = async (data) => {
      console.log(`🎤 [DEEPGRAM] Response type: ${data.type}, is_final: ${data.is_final}`)

      if (data.type === "Results" && data.is_final) {
        const transcript = data.channel?.alternatives?.[0]?.transcript
        console.log(`🎤 [DEEPGRAM] Raw transcript: "${transcript}"`)

        if (transcript && transcript.trim()) {
          if (!sttTimer) {
            sttTimer = createTimer("STT_TRANSCRIPTION")
          }

          console.log(`🕒 [STT-TRANSCRIPTION] ${sttTimer.end()}ms - Text: "${transcript.trim()}"`)
          sttTimer = null

          await processUserUtterance(transcript.trim())
        } else {
          console.log(`🎤 [DEEPGRAM] Empty or invalid transcript received`)
        }
      }
    }

    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText || isProcessing) return

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
        console.log("🤖 [USER-UTTERANCE] Processing with OpenAI...")
        const aiResponse = await processWithOpenAI(text, conversationHistory)

        if (processingRequestId === currentRequestId && aiResponse) {
          console.log("🤖 [USER-UTTERANCE] AI Response:", aiResponse)
          console.log("🎤 [USER-UTTERANCE] Starting TTS...")

          currentTTS = new SimplifiedSarvamTTSProcessor(currentLanguage, ws, streamSid)
          await currentTTS.synthesizeAndStream(aiResponse)

          conversationHistory.push({ role: "user", content: text }, { role: "assistant", content: aiResponse })

          // Keep conversation history manageable
          if (conversationHistory.length > 20) {
            conversationHistory = conversationHistory.slice(-16)
          }
        }
      } catch (error) {
        console.log("❌ [USER-UTTERANCE] Processing error:", error.message)
      } finally {
        isProcessing = false
      }
    }

    // WebSocket message handling
    ws.on("message", async (message) => {
      try {
        console.log(
          `📨 [C-ZENTRIX] Raw message received (${message.length} bytes):`,
          message.toString().substring(0, 200) + "...",
        )

        const data = JSON.parse(message)
        console.log(`📨 [C-ZENTRIX] Parsed event: ${data.event}`)
        console.log(`📨 [C-ZENTRIX] Full message structure:`, JSON.stringify(data, null, 2))

        switch (data.event) {
          case "connected":
            console.log("📞 [C-ZENTRIX] Call connected - Protocol:", data.protocol, "Version:", data.version)
            console.log("📞 [C-ZENTRIX] Sending connection acknowledgment")
            break

          case "start":
            streamSid = data.start.streamSid || data.streamSid
            accountSid = data.start.accountSid
            callSid = data.start.callSid
            sequenceNumber = Number.parseInt(data.sequenceNumber) || 0

            console.log("🎵 [C-ZENTRIX] Media stream started:")
            console.log("  - StreamSid:", streamSid)
            console.log("  - AccountSid:", accountSid)
            console.log("  - CallSid:", callSid)
            console.log("  - Tracks:", data.start.tracks)
            console.log("  - Media Format:", data.start.mediaFormat)
            console.log("  - Custom Parameters:", data.start.customParameters)

            if (!streamSid) {
              console.log("❌ [C-ZENTRIX] No streamSid received in start message")
              return
            }

            console.log("🎤 [C-ZENTRIX] Initiating Deepgram connection...")
            await connectToDeepgram()

            console.log("🎤 [FIRST-MESSAGE] Sending welcome audio message to call team...")
            try {
              const welcomeMessage = "Hello! I'm your AI assistant. How can I help you today?"
              const firstMessageTTS = new SimplifiedSarvamTTSProcessor(currentLanguage, ws, streamSid)
              await firstMessageTTS.synthesizeAndStream(welcomeMessage)

              // Add to conversation history
              conversationHistory.push({ role: "assistant", content: welcomeMessage })
              console.log("✅ [FIRST-MESSAGE] Welcome message sent successfully")
            } catch (error) {
              console.log("❌ [FIRST-MESSAGE] Failed to send welcome message:", error.message)
            }
            break

          case "media":
            if (data.media && data.media.payload && data.media.track === "inbound") {
              const audioBuffer = Buffer.from(data.media.payload, "base64")
              console.log(
                `🎵 [C-ZENTRIX] Received audio chunk: ${data.media.chunk}, timestamp: ${data.media.timestamp}, size: ${audioBuffer.length} bytes`,
              )

              console.log(`🎤 [DEEPGRAM] Status - Ready: ${deepgramReady}, WebSocket state: ${deepgramWs?.readyState}`)

              if (deepgramReady && deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
                console.log(`🎤 [DEEPGRAM] Sending ${audioBuffer.length} bytes to Deepgram`)
                deepgramWs.send(audioBuffer)
              } else {
                console.log("⚠️ [DEEPGRAM] Not ready for audio processing")
                console.log(
                  `⚠️ [DEEPGRAM] Ready: ${deepgramReady}, WebSocket exists: ${!!deepgramWs}, State: ${deepgramWs?.readyState}`,
                )

                if (!deepgramReady) {
                  console.log("⚠️ [DEEPGRAM] Attempting reconnection...")
                  await connectToDeepgram()
                }
              }
            } else {
              console.log(`🎵 [C-ZENTRIX] Skipping non-inbound track: ${data.media?.track}`)
            }
            break

          case "dtmf":
            console.log(`📞 [C-ZENTRIX] DTMF detected: ${data.dtmf.digit} on track: ${data.dtmf.track}`)
            break

          case "vad":
            console.log(`🎤 [C-ZENTRIX] VAD event: ${data.vad.value} on track: ${data.vad.track}`)
            break

          case "stop":
            console.log("🛑 [C-ZENTRIX] Media stream stopped")
            console.log("  - AccountSid:", data.stop.accountSid)
            console.log("  - CallSid:", data.stop.callSid)

            if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
              console.log("🎤 [DEEPGRAM] Closing connection due to stream stop")
              deepgramWs.close()
            }
            break

          default:
            console.log("📨 [C-ZENTRIX] Unknown event:", data.event)
            console.log("📨 [C-ZENTRIX] Unknown event data:", JSON.stringify(data, null, 2))
        }
      } catch (error) {
        console.log("❌ [WEBSOCKET] Message parsing error:", error.message)
        console.log("❌ [WEBSOCKET] Error stack:", error.stack)
        console.log("❌ [WEBSOCKET] Raw message:", message.toString())
      }
    })

    ws.on("close", (code, reason) => {
      console.log(`🔌 [WEBSOCKET] Connection closed - Code: ${code}, Reason: ${reason}`)
      if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
        console.log("🎤 [DEEPGRAM] Closing Deepgram connection due to WebSocket close")
        deepgramWs.close()
      }
    })

    ws.on("error", (error) => {
      console.log("❌ [WEBSOCKET] Connection error:", error.message)
      console.log("❌ [WEBSOCKET] Error details:", error)
    })

    console.log("🔌 [WEBSOCKET] Connection handlers set up, waiting for messages...")
  })
}

module.exports = { setupUnifiedVoiceServer }
