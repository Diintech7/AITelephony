const WebSocket = require("ws")
const EventEmitter = require("events")
require("dotenv").config()

const API_KEYS = {
  deepgram: process.env.DEEPGRAM_API_KEY,
  sarvam: process.env.SARVAM_API_KEY,
  openai: process.env.OPENAI_API_KEY,
}
console.log(API_KEYS)
// Validate API keys
if (!API_KEYS.deepgram || !API_KEYS.sarvam || !API_KEYS.openai) {
  console.error("❌ Missing required API keys in environment variables")
  process.exit(1)
}

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

const getSarvamLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang
  return LANGUAGE_MAPPING[lang] || "hi-IN"
}

const getDeepgramLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang
  if (lang === "hi") return "hi"
  if (lang === "en") return "en-IN"
  if (lang === "mr") return "mr"
  return lang
}

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
  return "pavithra" // Default fallback
}

class SipCallSession extends EventEmitter {
  constructor(ws, callSid) {
    super()
    this.ws = ws
    this.callSid = callSid
    this.streamSid = null
    this.isActive = false
    this.audioBuffer = []
    this.conversationHistory = []
    this.detectedLanguage = "en"
    this.createdAt = new Date()

    this.deepgramWs = null
    this.deepgramReady = false
    this.deepgramAudioQueue = []

    console.log(`📞 [SIP-SESSION] New session created: ${this.callSid}`)

    this.connectToDeepgram()
  }

  async connectToDeepgram() {
    try {
      const deepgramLanguage = getDeepgramLanguage(this.detectedLanguage)

      const deepgramUrl = new URL("wss://api.deepgram.com/v1/listen")
      deepgramUrl.searchParams.append("sample_rate", "8000")
      deepgramUrl.searchParams.append("channels", "1")
      deepgramUrl.searchParams.append("encoding", "linear16")
      deepgramUrl.searchParams.append("model", "nova-2")
      deepgramUrl.searchParams.append("language", deepgramLanguage)
      deepgramUrl.searchParams.append("interim_results", "true")
      deepgramUrl.searchParams.append("smart_format", "true")
      deepgramUrl.searchParams.append("endpointing", "300")

      this.deepgramWs = new WebSocket(deepgramUrl.toString(), {
        headers: { Authorization: `Token ${API_KEYS.deepgram}` },
      })

      this.deepgramWs.onopen = () => {
        console.log("🎤 [DEEPGRAM] Connection established")
        this.deepgramReady = true
        console.log("🎤 [DEEPGRAM] Processing queued audio packets:", this.deepgramAudioQueue.length)
        this.deepgramAudioQueue.forEach((buffer) => this.deepgramWs.send(buffer))
        this.deepgramAudioQueue = []
      }

      this.deepgramWs.onmessage = async (event) => {
        const data = JSON.parse(event.data)
        await this.handleDeepgramResponse(data)
      }

      this.deepgramWs.onerror = (error) => {
        console.log("❌ [DEEPGRAM] Connection error:", error.message)
        this.deepgramReady = false
      }

      this.deepgramWs.onclose = () => {
        console.log("🔌 [DEEPGRAM] Connection closed")
        this.deepgramReady = false
      }
    } catch (error) {
      console.error("❌ [DEEPGRAM] Connection setup error:", error.message)
    }
  }

  async handleDeepgramResponse(data) {
    try {
      if (data.channel?.alternatives?.[0]?.transcript) {
        const transcript = data.channel.alternatives[0].transcript
        const confidence = data.channel.alternatives[0].confidence
        const isFinal = data.is_final

        if (isFinal && transcript.trim() && confidence > 0.5) {
          console.log(`🎤 [SIP-STT] Final transcript: ${transcript}`)

          // Detect language if available
          if (data.channel.detected_language) {
            this.detectedLanguage = data.channel.detected_language
          }

          // Process with OpenAI
          await this.processWithOpenAI(transcript)
        }
      }
    } catch (error) {
      console.error("❌ [DEEPGRAM] Response handling error:", error.message)
    }
  }

  async processAudioChunk(audioData) {
    try {
      // Convert base64 µ-law to linear16 for Deepgram
      const audioBuffer = Buffer.from(audioData, "base64")

      if (this.deepgramReady && this.deepgramWs) {
        this.deepgramWs.send(audioBuffer)
      } else {
        // Queue audio if Deepgram not ready
        this.deepgramAudioQueue.push(audioBuffer)
      }
    } catch (error) {
      console.error(`❌ [SIP-STT] Error processing audio:`, error.message)
    }
  }

  async processWithOpenAI(userMessage) {
    try {
      // Add user message to conversation history
      this.conversationHistory.push({
        role: "user",
        content: userMessage,
        timestamp: new Date(),
        language: this.detectedLanguage,
      })

      // Create system prompt with token limit instruction
      const systemPrompt = `${this.getSystemPrompt(this.detectedLanguage)}\n\nIMPORTANT: Keep your responses concise and under 100 tokens. Be brief but helpful.`

      // Prepare messages for OpenAI
      const messages = [
        { role: "system", content: systemPrompt },
        ...this.conversationHistory.slice(-6).map((msg) => ({
          role: msg.role,
          content: msg.content,
        })),
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
          max_tokens: 100,
          temperature: 0.7,
        }),
      })

      if (!response.ok) {
        throw new Error(`OpenAI API error: ${response.status}`)
      }

      const completion = await response.json()
      const aiResponse = completion.choices[0]?.message?.content

      if (aiResponse) {
        console.log(`🤖 [SIP-AI] Response (${this.detectedLanguage}): ${aiResponse}`)

        // Add AI response to conversation history
        this.conversationHistory.push({
          role: "assistant",
          content: aiResponse,
          timestamp: new Date(),
          language: this.detectedLanguage,
        })

        // Convert to speech using Sarvam AI
        await this.convertToSpeech(aiResponse)
      }
    } catch (error) {
      console.error(`❌ [SIP-AI] Error processing with OpenAI:`, error.message)
    }
  }

  async convertToSpeech(text) {
    try {
      const sarvamLanguage = getSarvamLanguage(this.detectedLanguage)
      const voice = getValidSarvamVoice("pavithra")

      const response = await fetch("https://api.sarvam.ai/text-to-speech", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "API-Subscription-Key": API_KEYS.sarvam,
        },
        body: JSON.stringify({
          inputs: [text],
          target_language_code: sarvamLanguage,
          speaker: voice,
          pitch: 0,
          pace: 1.0,
          loudness: 1.0,
          speech_sample_rate: 8000,
          enable_preprocessing: true,
          model: "bulbul:v1",
        }),
      })

      if (!response.ok) {
        throw new Error(`Sarvam API error: ${response.status}`)
      }

      const responseData = await response.json()
      const audioBase64 = responseData.audios?.[0]

      if (audioBase64) {
        // Send audio back to SIP client
        this.sendAudioToClient(audioBase64)
      } else {
        throw new Error("No audio data received from Sarvam API")
      }
    } catch (error) {
      console.error(`❌ [SIP-TTS] Error converting to speech:`, error.message)
      // Fallback: send a simple text response
      this.sendTextToClient("I'm sorry, I'm having trouble with audio processing right now.")
    }
  }

  sendAudioToClient(base64Audio) {
    if (this.ws.readyState === WebSocket.OPEN && this.streamSid) {
      const audioMessage = {
        event: "media",
        streamSid: this.streamSid,
        media: {
          payload: base64Audio,
        },
      }

      this.ws.send(JSON.stringify(audioMessage))
      console.log(`🔊 [SIP-AUDIO] Sent audio response to client`)
    }
  }

  sendTextToClient(text) {
    if (this.ws.readyState === WebSocket.OPEN) {
      const textMessage = {
        event: "text",
        text: text,
        timestamp: new Date().toISOString(),
      }

      this.ws.send(JSON.stringify(textMessage))
      console.log(`📝 [SIP-TEXT] Sent text response: ${text}`)
    }
  }

  getSystemPrompt(language) {
    const prompts = {
      en: "You are a helpful AI assistant for voice calls. Provide concise, natural responses suitable for phone conversations. Keep responses under 50 words.",
      hi: "आप एक सहायक AI असिस्टेंट हैं। संक्षिप्त और प्राकृतिक उत्तर दें जो फोन कॉल के लिए उपयुक्त हों।",
      es: "Eres un asistente de IA útil para llamadas de voz. Proporciona respuestas concisas y naturales adecuadas para conversaciones telefónicas.",
      fr: "Vous êtes un assistant IA utile pour les appels vocaux. Fournissez des réponses concises et naturelles adaptées aux conversations téléphoniques.",
      de: "Sie sind ein hilfreicher KI-Assistent für Sprachanrufe. Geben Sie prägnante, natürliche Antworten, die für Telefongespräche geeignet sind.",
    }

    return prompts[language] || prompts["en"]
  }

  terminate(reason = "normal_termination") {
    console.log(`🛑 [SIP-SESSION] Terminating session ${this.callSid}: ${reason}`)
    this.isActive = false

    if (this.deepgramWs) {
      this.deepgramWs.close()
      this.deepgramWs = null
    }

    this.emit("terminated", { callSid: this.callSid, reason })
  }
}

// Active sessions storage
const activeSessions = new Map()

function setupSipWebSocketServer(wss) {
  console.log("🔧 [SIP-WS] Setting up SIP WebSocket server...")

  wss.on("connection", (ws, req) => {
    console.log("🔗 [SIP-WS] New SIP WebSocket connection established")

    // Send immediate connection acknowledgment
    ws.send(
      JSON.stringify({
        event: "connected",
        protocol: "SIP-WebSocket-v1.0",
        timestamp: new Date().toISOString(),
      }),
    )

    ws.on("message", async (message) => {
      try {
        const data = JSON.parse(message.toString())
        console.log(`📨 [SIP-WS] Received event: ${data.event}`)

        switch (data.event) {
          case "start":
            await handleStart(ws, data)
            break

          case "media":
            await handleMedia(ws, data)
            break

          case "stop":
            await handleStop(ws, data)
            break

          case "dtmf":
            await handleDtmf(ws, data)
            break

          case "mark":
            await handleMark(ws, data)
            break

          case "clear":
            await handleClear(ws, data)
            break

          default:
            console.log(`⚠️ [SIP-WS] Unknown event type: ${data.event}`)
        }
      } catch (error) {
        console.error("❌ [SIP-WS] Error processing message:", error.message)
        ws.send(
          JSON.stringify({
            event: "error",
            message: "Invalid message format",
            timestamp: new Date().toISOString(),
          }),
        )
      }
    })

    ws.on("close", (code, reason) => {
      console.log(`🔗 [SIP-WS] Connection closed: ${code} - ${reason}`)

      // Clean up any active sessions for this connection
      for (const [callSid, session] of activeSessions.entries()) {
        if (session.ws === ws) {
          session.terminate("connection_closed")
          activeSessions.delete(callSid)
        }
      }
    })

    ws.on("error", (error) => {
      console.error("❌ [SIP-WS] WebSocket error:", error.message)
    })
  })

  console.log("✅ [SIP-WS] SIP WebSocket server setup complete")
}

async function handleStart(ws, data) {
  const { callSid, streamSid } = data

  console.log(`🚀 [SIP-START] Starting call session: ${callSid}`)

  // Create new session (no database validation - accept all)
  const session = new SipCallSession(ws, callSid)
  session.streamSid = streamSid
  session.isActive = true

  // Store session
  activeSessions.set(callSid, session)

  // Send acknowledgment
  ws.send(
    JSON.stringify({
      event: "start_ack",
      callSid: callSid,
      streamSid: streamSid,
      status: "accepted",
      message: "Call session started successfully",
      timestamp: new Date().toISOString(),
    }),
  )

  console.log(`✅ [SIP-START] Session started for call: ${callSid}`)
}

async function handleMedia(ws, data) {
  const { streamSid, media } = data

  // Find session by streamSid
  const session = Array.from(activeSessions.values()).find((s) => s.streamSid === streamSid)

  if (!session) {
    console.log(`⚠️ [SIP-MEDIA] No active session found for streamSid: ${streamSid}`)
    return
  }

  if (media && media.payload) {
    // Process audio chunk
    await session.processAudioChunk(media.payload)
  }
}

async function handleStop(ws, data) {
  const { callSid, streamSid } = data

  console.log(`🛑 [SIP-STOP] Stopping call session: ${callSid}`)

  const session = activeSessions.get(callSid)
  if (session) {
    session.terminate("call_ended")
    activeSessions.delete(callSid)

    // Send acknowledgment
    ws.send(
      JSON.stringify({
        event: "stop_ack",
        callSid: callSid,
        streamSid: streamSid,
        status: "terminated",
        timestamp: new Date().toISOString(),
      }),
    )

    console.log(`✅ [SIP-STOP] Session terminated for call: ${callSid}`)
  }
}

async function handleDtmf(ws, data) {
  const { callSid, dtmf } = data

  console.log(`📞 [SIP-DTMF] DTMF received for call ${callSid}: ${dtmf.digit}`)

  const session = activeSessions.get(callSid)
  if (session) {
    // Handle DTMF input (could be used for menu navigation, etc.)
    session.sendTextToClient(`DTMF digit received: ${dtmf.digit}`)
  }
}

async function handleMark(ws, data) {
  const { callSid, mark } = data

  console.log(`🏷️ [SIP-MARK] Mark received for call ${callSid}: ${mark.name}`)

  // Send acknowledgment
  ws.send(
    JSON.stringify({
      event: "mark_ack",
      callSid: callSid,
      mark: mark,
      timestamp: new Date().toISOString(),
    }),
  )
}

async function handleClear(ws, data) {
  const { callSid } = data

  console.log(`🧹 [SIP-CLEAR] Clear received for call: ${callSid}`)

  const session = activeSessions.get(callSid)
  if (session) {
    // Clear any queued audio or reset session state
    session.audioBuffer = []

    ws.send(
      JSON.stringify({
        event: "clear_ack",
        callSid: callSid,
        status: "cleared",
        timestamp: new Date().toISOString(),
      }),
    )
  }
}

// Export the setup function and session management
module.exports = {
  setupSipWebSocketServer,
  activeSessions,
  SipCallSession,
}
