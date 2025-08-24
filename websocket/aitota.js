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
      const chunkSize = 320 // 20ms chunks for 8kHz (optimal for Twilio)
      let sequenceNumber = 0
      const totalChunks = Math.ceil(audioBuffer.length / chunkSize)

      console.log(`🎵 [AUDIO-STREAMING] Starting chunked streaming: ${totalChunks} chunks`)

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
            payload: chunk.toString("base64"),
          },
          sequenceNumber: sequenceNumber,
          timestamp: Date.now(),
        }

        this.ws.send(JSON.stringify(audioPayload))
        this.totalAudioBytes += chunk.length
        sequenceNumber++

        await new Promise((resolve) => setTimeout(resolve, 20))
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
    console.log("🔌 [WEBSOCKET] New voice AI connection established")

    let streamSid = null
    const currentLanguage = "hi"
    let conversationHistory = []
    let currentTTS = null
    let isProcessing = false
    let lastProcessedText = ""
    let processingRequestId = 0

    // Deepgram WebSocket connection
    let deepgramWs = null
    let deepgramReady = false
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
        deepgramUrl.searchParams.append("smart_format", "true")
        deepgramUrl.searchParams.append("interim_results", "false")

        deepgramWs = new WebSocket(deepgramUrl.toString(), {
          headers: { Authorization: `Token ${API_KEYS.deepgram}` },
        })

        deepgramWs.onopen = () => {
          console.log("🎤 [DEEPGRAM] Connection established")
          deepgramReady = true
          console.log("🎤 [DEEPGRAM] Ready to process audio")
        }

        deepgramWs.onmessage = async (event) => {
          const data = JSON.parse(event.data)
          await handleDeepgramResponse(data)
        }

        deepgramWs.onerror = (error) => {
          console.log("❌ [DEEPGRAM] Connection error:", error.message)
          deepgramReady = false
          setTimeout(connectToDeepgram, 2000)
        }

        deepgramWs.onclose = () => {
          console.log("🎤 [DEEPGRAM] Connection closed")
          deepgramReady = false
        }
      } catch (error) {
        console.log("❌ [DEEPGRAM] Connection failed:", error.message)
        deepgramReady = false
        setTimeout(connectToDeepgram, 2000)
      }
    }

    const handleDeepgramResponse = async (data) => {
      if (data.type === "Results" && data.is_final) {
        const transcript = data.channel?.alternatives?.[0]?.transcript
        if (transcript && transcript.trim()) {
          if (!sttTimer) {
            sttTimer = createTimer("STT_TRANSCRIPTION")
          }

          console.log(`🕒 [STT-TRANSCRIPTION] ${sttTimer.end()}ms - Text: "${transcript.trim()}"`)
          sttTimer = null

          await processUserUtterance(transcript.trim())
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
        const data = JSON.parse(message)

        switch (data.event) {
          case "connected":
            console.log("📞 [TWILIO] Call connected")
            break

          case "start":
            streamSid = data.start.streamSid
            console.log("🎵 [TWILIO] Media stream started:", streamSid)
            await connectToDeepgram()
            break

          case "media":
            if (data.media && data.media.payload) {
              const audioBuffer = Buffer.from(data.media.payload, "base64")

              if (deepgramReady && deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
                // Send audio directly to Deepgram without additional chunking
                deepgramWs.send(audioBuffer)
              } else if (!deepgramReady) {
                console.log("⚠️ [DEEPGRAM] Not ready, attempting reconnection...")
                await connectToDeepgram()
              }
            }
            break

          case "stop":
            console.log("🛑 [TWILIO] Media stream stopped")
            if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
              deepgramWs.close()
            }
            break

          default:
            console.log("📨 [TWILIO] Unknown event:", data.event)
        }
      } catch (error) {
        console.log("❌ [WEBSOCKET] Message parsing error:", error.message)
      }
    })

    ws.on("close", () => {
      console.log("🔌 [WEBSOCKET] Connection closed")
      if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
        deepgramWs.close()
      }
    })

    ws.on("error", (error) => {
      console.log("❌ [WEBSOCKET] Connection error:", error.message)
    })
  })
}

module.exports = { setupUnifiedVoiceServer }
