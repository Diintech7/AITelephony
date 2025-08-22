const WebSocket = require("ws")
const EventEmitter = require("events")
require("dotenv").config()

const API_KEYS = {
  deepgram: process.env.DEEPGRAM_API_KEY,
  sarvam: process.env.SARVAM_API_KEY,
  openai: process.env.OPENAI_API_KEY,
}

console.log("🔑 [SANPBX] API Keys loaded:", Object.keys(API_KEYS).filter(key => API_KEYS[key]))

// Validate API keys
if (!API_KEYS.deepgram || !API_KEYS.sarvam || !API_KEYS.openai) {
  console.error("❌ [SANPBX] Missing required API keys in environment variables")
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
  "meera",
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

const getValidSarvamVoice = (voiceSelection = "meera") => {
  const normalized = (voiceSelection || "").toString().trim().toLowerCase()
  if (VALID_SARVAM_VOICES.has(normalized)) {
    return normalized
  }
  return "meera" // Default fallback
}

// -------- Base64 helpers --------
function isProbablyBase64(str) {
  if (typeof str !== "string") return false
  if (str.length < 8) return false
  if (str.length % 4 !== 0) return false
  return /^[A-Za-z0-9+/]+={0,2}$/.test(str)
}

function normalizeBase64String(str) {
  if (typeof str !== "string") return str
  // Strip whitespace
  let s = str.replace(/\s+/g, "")
  if (isProbablyBase64(s)) return s
  // Convert URL-safe base64 to standard
  let urlFixed = s.replace(/-/g, "+").replace(/_/g, "/")
  const pad = urlFixed.length % 4
  if (pad) urlFixed = urlFixed + "=".repeat(4 - pad)
  if (isProbablyBase64(urlFixed)) return urlFixed
  // Fallback: attempt to interpret as binary (latin1) and encode
  try {
    const buf = Buffer.from(str, "binary")
    const b64 = buf.toString("base64")
    return b64
  } catch (_) {
    return str
  }
}

// -------- Audio utils: LINEAR16 (44.1kHz) -> LINEAR16 (8kHz) --------
function resampleLinear16To8kHz(audioBuffer) {
  try {
    // SanIPPBX sends LINEAR16 at 44.1kHz, we need to convert to 8kHz for Deepgram
    const originalSampleRate = 44100
    const targetSampleRate = 8000
    const ratio = targetSampleRate / originalSampleRate
    
    const originalSamples = new Int16Array(audioBuffer.buffer, audioBuffer.byteOffset, audioBuffer.length / 2)
    const targetSampleCount = Math.floor(originalSamples.length * ratio)
    const targetBuffer = new Int16Array(targetSampleCount)
    
    for (let i = 0; i < targetSampleCount; i++) {
      const originalIndex = Math.floor(i / ratio)
      targetBuffer[i] = originalSamples[originalIndex]
    }
    
    return Buffer.from(targetBuffer.buffer)
  } catch (err) {
    console.error("❌ [SANPBX-AUDIO] Resampling error:", err.message)
    return audioBuffer // Return original if resampling fails
  }
}

// -------- Audio utils: LINEAR16 -> µ-law (8kHz mono) --------
function linearPcmSampleToMuLaw(sample) {
  // Clamp to 16-bit signed range
  if (sample > 32767) sample = 32767
  if (sample < -32768) sample = -32768

  const MU = 255

  let sign = 0
  if (sample < 0) {
    sign = 0x80
    sample = -sample
  }

  // Bias for μ-law
  sample = sample + 132
  if (sample > 32635) sample = 32635

  // Determine exponent
  let exponent = 7
  for (let expMask = 0x4000; (sample & expMask) === 0 && exponent > 0; expMask >>= 1) {
    exponent--
  }

  const mantissa = (sample >> (exponent + 3)) & 0x0f
  const muLawByte = ~(sign | (exponent << 4) | mantissa) & 0xff
  return muLawByte
}

function linear16ToMuLawBase64(linear16Base64) {
  try {
    const buffer = Buffer.from(linear16Base64, "base64")
    const sampleCount = buffer.length / 2
    const muLawBuffer = Buffer.alloc(sampleCount)
    
    for (let i = 0; i < sampleCount; i++) {
      const sample = buffer.readInt16LE(i * 2)
      muLawBuffer[i] = linearPcmSampleToMuLaw(sample)
    }
    
    return muLawBuffer.toString("base64")
  } catch (err) {
    console.error("❌ [SANPBX-AUDIO] Linear16 to µ-law conversion error:", err.message)
    return linear16Base64 // Return original if conversion fails
  }
}

class SanPbxCallSession extends EventEmitter {
  constructor(ws, callData) {
    super()
    this.ws = ws
    this.callId = callData.callId
    this.streamId = callData.streamId
    this.channelId = callData.channelId
    this.callerId = callData.callerId
    this.callDirection = callData.callDirection
    this.did = callData.did
    this.isActive = false
    this.isAnswered = false
    this.audioBuffer = []
    this.conversationHistory = []
    this.detectedLanguage = "en"
    this.createdAt = new Date()
    this.mediaFormat = null

    this.deepgramWs = null
    this.deepgramReady = false
    this.deepgramAudioQueue = []

    // Audio packet statistics
    this.audioPacketStats = {
      totalPackets: 0,
      totalBytes: 0,
      averagePacketSize: 0,
      firstPacketTime: null,
      lastPacketTime: null,
      packetSizes: []
    }

    console.log(`📞 [SANPBX-SESSION] New session created: ${this.callId} | Stream: ${this.streamId}`)
    console.log(`📞 [SANPBX-SESSION] Caller: ${this.callerId} | Direction: ${this.callDirection} | DID: ${this.did}`)
  }

  async connectToDeepgram() {
    try {
      const deepgramLanguage = getDeepgramLanguage(this.detectedLanguage)

      const deepgramUrl = new URL("wss://api.deepgram.com/v1/listen")
      deepgramUrl.searchParams.append("sample_rate", "8000")
      deepgramUrl.searchParams.append("channels", "1")
      deepgramUrl.searchParams.append("encoding", "linear16") // SanIPPBX uses LINEAR16
      deepgramUrl.searchParams.append("model", "nova-2")
      deepgramUrl.searchParams.append("language", deepgramLanguage)
      deepgramUrl.searchParams.append("interim_results", "true")
      deepgramUrl.searchParams.append("smart_format", "true")
      deepgramUrl.searchParams.append("endpointing", "300")

      this.deepgramWs = new WebSocket(deepgramUrl.toString(), {
        headers: { Authorization: `Token ${API_KEYS.deepgram}` },
      })

      this.deepgramWs.onopen = () => {
        console.log("🎤 [SANPBX-DEEPGRAM] Connection established")
        this.deepgramReady = true
        console.log("🎤 [SANPBX-DEEPGRAM] Processing queued audio packets:", this.deepgramAudioQueue.length)
        this.deepgramAudioQueue.forEach((buffer) => this.deepgramWs.send(buffer))
        this.deepgramAudioQueue = []
      }

      this.deepgramWs.onmessage = async (event) => {
        const data = JSON.parse(event.data)
        await this.handleDeepgramResponse(data)
      }

      this.deepgramWs.onerror = (error) => {
        console.log("❌ [SANPBX-DEEPGRAM] Connection error:", error.message)
        this.deepgramReady = false
      }

      this.deepgramWs.onclose = () => {
        console.log("🔌 [SANPBX-DEEPGRAM] Connection closed")
        this.deepgramReady = false
      }
    } catch (error) {
      console.error("❌ [SANPBX-DEEPGRAM] Connection setup error:", error.message)
    }
  }

  async handleDeepgramResponse(data) {
    try {
      if (data.channel?.alternatives?.[0]?.transcript) {
        const transcript = data.channel.alternatives[0].transcript
        const confidence = data.channel.alternatives[0].confidence
        const isFinal = data.is_final

        if (isFinal && transcript.trim() && confidence > 0.5) {
          console.log(`🎤 [SANPBX-STT] Final transcript: ${transcript}`)

          // Detect language if available
          if (data.channel.detected_language) {
            this.detectedLanguage = data.channel.detected_language
          }

          // Process with OpenAI
          await this.processWithOpenAI(transcript)
        }
      }
    } catch (error) {
      console.error("❌ [SANPBX-DEEPGRAM] Response handling error:", error.message)
    }
  }

  async processAudioChunk(audioData) {
    try {
      // Log the incoming audio data format for debugging
      if (!this.audioFormatLogged) {
        console.log(`🎵 [SANPBX-AUDIO] Incoming audio format check:`)
        console.log(`   - Data type: ${typeof audioData}`)
        console.log(`   - Data length: ${audioData.length}`)
        console.log(`   - Is base64: ${isProbablyBase64(audioData)}`)
        console.log(`   - Sample data: ${audioData.substring(0, 50)}...`)
        
        // Additional detailed analysis
        if (audioData.length > 0) {
          console.log(`   - First 10 characters: "${audioData.substring(0, 10)}"`)
          console.log(`   - Contains special chars: ${/[^A-Za-z0-9+/=]/.test(audioData)}`)
          console.log(`   - Contains padding: ${audioData.includes('=')}`)
          console.log(`   - Padding count: ${(audioData.match(/=/g) || []).length}`)
        }
        
        this.audioFormatLogged = true
      }

      // Ensure audioData is base64
      const normalizedAudioData = normalizeBase64String(audioData)
      
      // Log normalization details
      if (normalizedAudioData !== audioData && !this.normalizationLogged) {
        console.log(`🔁 [SANPBX-AUDIO] Base64 normalization applied:`)
        console.log(`   - Original: ${audioData.substring(0, 30)}...`)
        console.log(`   - Normalized: ${normalizedAudioData.substring(0, 30)}...`)
        this.normalizationLogged = true
      }
      
      // Update audio packet statistics
      this.updateAudioPacketStats(audioData)
      
      // SanIPPBX sends LINEAR16 at 44.1kHz, convert to 8kHz for Deepgram
      const audioBuffer = Buffer.from(normalizedAudioData, "base64")
      const resampledBuffer = resampleLinear16To8kHz(audioBuffer)

      // Log the processed audio format
      if (!this.processedFormatLogged) {
        console.log(`🎵 [SANPBX-AUDIO] Processed audio format:`)
        console.log(`   - Original buffer size: ${audioBuffer.length} bytes`)
        console.log(`   - Resampled buffer size: ${resampledBuffer.length} bytes`)
        console.log(`   - Resampled base64 length: ${resampledBuffer.toString('base64').length}`)
        console.log(`   - Estimated samples (original): ${Math.floor(audioBuffer.length / 2)}`)
        console.log(`   - Estimated samples (resampled): ${Math.floor(resampledBuffer.length / 2)}`)
        console.log(`   - Estimated duration (original): ${(audioBuffer.length / 2 / 44100 * 1000).toFixed(2)}ms`)
        console.log(`   - Estimated duration (resampled): ${(resampledBuffer.length / 2 / 8000 * 1000).toFixed(2)}ms`)
        this.processedFormatLogged = true
      }

      if (this.deepgramReady && this.deepgramWs) {
        this.deepgramWs.send(resampledBuffer)
      } else {
        // Queue audio if Deepgram not ready
        this.deepgramAudioQueue.push(resampledBuffer)
      }
    } catch (error) {
      console.error(`❌ [SANPBX-STT] Error processing audio:`, error.message)
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
        console.log(`🤖 [SANPBX-AI] Response (${this.detectedLanguage}): ${aiResponse}`)

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
      console.error(`❌ [SANPBX-AI] Error processing with OpenAI:`, error.message)
    }
  }

  async convertToSpeech(text) {
    try {
      const sarvamLanguage = getSarvamLanguage(this.detectedLanguage)
      const voice = getValidSarvamVoice("meera")

      const response = await fetch("https://api.sarvam.ai/text-to-speech", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          // Prefer x-api-key; some tenants also accept API-Subscription-Key
          "x-api-key": API_KEYS.sarvam,
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
          enable_preprocessing: false,
          enable_preprocessing: true,
          model: "bulbul:v1",
        }),
      })

      if (!response.ok) {
        const errorText = await response.text().catch(() => "")
        console.log(`❌ [SANPBX-TTS] Sarvam error ${response.status}: ${errorText}`)
        throw new Error(`Sarvam API error: ${response.status}`)
      }

      const responseData = await response.json()
      const audioBase64 = responseData.audios?.[0]

      if (audioBase64) {
        // Sarvam returns WAV PCM, convert to LINEAR16 base64 for SanIPPBX
        const linear16Base64 = this.convertWavToLinear16Base64(audioBase64) || audioBase64
        this.sendAudioToClient(linear16Base64)
      } else {
        throw new Error("No audio data received from Sarvam API")
      }
    } catch (error) {
      console.error(`❌ [SANPBX-TTS] Error converting to speech:`, error.message)
      // Fallback: send a simple text response
      this.sendTextToClient("I'm sorry, I'm having trouble with audio processing right now.")
    }
  }

  convertWavToLinear16Base64(wavBase64) {
    try {
      const buffer = Buffer.from(wavBase64, "base64")
      if (buffer.length < 44) return null
      
      // Basic WAV header checks
      if (buffer.toString("ascii", 0, 4) !== "RIFF" || buffer.toString("ascii", 8, 12) !== "WAVE") {
        return null
      }

      // Parse fmt chunk
      let offset = 12
      let fmtChunkFound = false
      let dataOffset = -1
      let dataSize = 0

      while (offset + 8 <= buffer.length) {
        const chunkId = buffer.toString("ascii", offset, offset + 4)
        const chunkSize = buffer.readUInt32LE(offset + 4)
        const next = offset + 8 + chunkSize
        
        if (chunkId === "fmt ") {
          fmtChunkFound = true
        } else if (chunkId === "data") {
          dataOffset = offset + 8
          dataSize = chunkSize
        }
        offset = next
      }

      if (!fmtChunkFound || dataOffset < 0 || dataSize <= 0) return null

      // Extract PCM data
      const pcmData = buffer.slice(dataOffset, dataOffset + dataSize)
      return pcmData.toString("base64")
    } catch (err) {
      console.error("❌ [SANPBX-AUDIO] WAV to LINEAR16 conversion error:", err.message)
      return null
    }
  }

  sendAudioToClient(base64Audio) {
    if (this.ws.readyState === WebSocket.OPEN && this.isAnswered) {
      // Ensure the audio is in base64 format
      const normalizedAudio = normalizeBase64String(base64Audio)
      
      // Log the outgoing audio format for debugging
      if (!this.outgoingFormatLogged) {
        console.log(`🎵 [SANPBX-AUDIO] Outgoing audio format:`)
        console.log(`   - Original base64 length: ${base64Audio.length}`)
        console.log(`   - Normalized base64 length: ${normalizedAudio.length}`)
        console.log(`   - Sample data: ${normalizedAudio.substring(0, 50)}...`)
        this.outgoingFormatLogged = true
      }

      const audioMessage = {
        event: "media",
        streamId: this.streamId,
        channelId: this.channelId,
        callId: this.callId,
        media: {
          payload: normalizedAudio,
          format: {
            encoding: "LINEAR16",
            sampleRate: 44100,
            channels: 1
          }
        },
        timestamp: new Date().toISOString()
      }

      this.ws.send(JSON.stringify(audioMessage))
      console.log(`🔊 [SANPBX-AUDIO] Sent audio response to client`)
    }
  }

  sendTextToClient(text) {
    if (this.ws.readyState === WebSocket.OPEN) {
      const textMessage = {
        event: "text",
        streamId: this.streamId,
        channelId: this.channelId,
        callId: this.callId,
        text: text,
        timestamp: new Date().toISOString(),
      }

      this.ws.send(JSON.stringify(textMessage))
      console.log(`📝 [SANPBX-TEXT] Sent text response: ${text}`)
    }
  }

  getSystemPrompt(language) {
    const prompts = {
      en: "You are a helpful AI assistant for voice calls. Provide concise, natural responses suitable for phone conversations. Keep responses under 50 words.",
      hi: "आप एक सहायक AI असिस्टेंट हैं। संक्षिप्त और प्राकृतिक उत्तर दें जो फोन कॉल के लिए उपयुक्त हों।",
      es: "Eres un asistente de IA útil para llamadas de voz. Proporciona respuestas concisas y naturales adecuadas para conversaciones telefónicas.",
      fr: "Vous êtes un assistant IA utile pour les appels vocaux. Fournissez des réponses concisas et naturelles adaptées aux conversations téléphoniques.",
      de: "Sie sind ein hilfreicher KI-Assistent für Sprachanrufe. Geben Sie prägnante, natürliche Antworten, die für Telefongespräche geeignet sind.",
    }

    return prompts[language] || prompts["en"]
  }

  updateAudioPacketStats(audioData) {
    const now = new Date()
    const packetSize = audioData.length
    
    this.audioPacketStats.totalPackets++
    this.audioPacketStats.totalBytes += packetSize
    this.audioPacketStats.packetSizes.push(packetSize)
    
    if (!this.audioPacketStats.firstPacketTime) {
      this.audioPacketStats.firstPacketTime = now
    }
    this.audioPacketStats.lastPacketTime = now
    
    // Keep only last 100 packet sizes for average calculation
    if (this.audioPacketStats.packetSizes.length > 100) {
      this.audioPacketStats.packetSizes.shift()
    }
    
    this.audioPacketStats.averagePacketSize = Math.round(
      this.audioPacketStats.packetSizes.reduce((sum, size) => sum + size, 0) / 
      this.audioPacketStats.packetSizes.length
    )
    
    // Log statistics every 50 packets
    if (this.audioPacketStats.totalPackets % 50 === 0) {
      this.logAudioPacketStats()
    }
  }

  logAudioPacketStats() {
    const duration = this.audioPacketStats.lastPacketTime - this.audioPacketStats.firstPacketTime
    const durationSeconds = duration / 1000
    
    console.log(`📊 [SANPBX-STATS] Audio packet statistics:`)
    console.log(`   - Total packets: ${this.audioPacketStats.totalPackets}`)
    console.log(`   - Total bytes: ${this.audioPacketStats.totalBytes}`)
    console.log(`   - Average packet size: ${this.audioPacketStats.averagePacketSize} bytes`)
    console.log(`   - Duration: ${durationSeconds.toFixed(2)} seconds`)
    console.log(`   - Packet rate: ${(this.audioPacketStats.totalPackets / durationSeconds).toFixed(2)} packets/sec`)
    console.log(`   - Data rate: ${(this.audioPacketStats.totalBytes / durationSeconds).toFixed(2)} bytes/sec`)
    
    // Show packet size distribution
    const sizeRanges = {
      '0-100': 0,
      '101-500': 0,
      '501-1000': 0,
      '1001-2000': 0,
      '2000+': 0
    }
    
    this.audioPacketStats.packetSizes.forEach(size => {
      if (size <= 100) sizeRanges['0-100']++
      else if (size <= 500) sizeRanges['101-500']++
      else if (size <= 1000) sizeRanges['501-1000']++
      else if (size <= 2000) sizeRanges['1001-2000']++
      else sizeRanges['2000+']++
    })
    
    console.log(`   - Packet size distribution:`, sizeRanges)
  }

  terminate(reason = "normal_termination") {
    console.log(`🛑 [SANPBX-SESSION] Terminating session ${this.callId}: ${reason}`)
    
    // Log final audio statistics
    if (this.audioPacketStats.totalPackets > 0) {
      console.log(`📊 [SANPBX-STATS] Final audio statistics for session ${this.callId}:`)
      this.logAudioPacketStats()
    }
    
    this.isActive = false
    this.isAnswered = false

    if (this.deepgramWs) {
      this.deepgramWs.close()
      this.deepgramWs = null
    }

    this.emit("terminated", { callId: this.callId, reason })
  }
}

// Active sessions storage
const activeSessions = new Map()

function setupSanPbxWebSocketServer(wss) {
  console.log("🔧 [SANPBX-WS] Setting up SanIPPBX WebSocket server...")

  wss.on("connection", (ws, req) => {
    console.log("🔗 [SANPBX-WS] New SanIPPBX WebSocket connection established")

    // Send immediate connection acknowledgment with first message
    ws.send(
      JSON.stringify({
        event: "connected",
        protocol: "SanIPPBX-WebSocket-v1.0",
        message: "SanIPPBX WebSocket server is ready to handle calls",
        status: "ready",
        timestamp: new Date().toISOString(),
      }),
    )
    
    console.log("📝 [SANPBX-WS] Sent first connection message to client")

    ws.on("message", async (message) => {
      try {
        const data = JSON.parse(message.toString())
        console.log(`📨 [SANPBX-WS] Received event: ${data.event}`)

        switch (data.event) {
          case "connected":
            await handleConnected(ws, data)
            break

          case "start":
            await handleStart(ws, data)
            break

          case "answer":
            await handleAnswer(ws, data)
            break

          case "media":
            await handleMedia(ws, data)
            break

          case "dtmf":
            await handleDtmf(ws, data)
            break

          case "stop":
            await handleStop(ws, data)
            break

          case "transfer-call":
            await handleTransferCall(ws, data)
            break

          case "hangup-call":
            await handleHangupCall(ws, data)
            break

          default:
            console.log(`⚠️ [SANPBX-WS] Unknown event type: ${data.event}`)
        }
      } catch (error) {
        console.error("❌ [SANPBX-WS] Error processing message:", error.message)
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
      console.log(`🔗 [SANPBX-WS] Connection closed: ${code} - ${reason}`)

      // Clean up any active sessions for this connection
      for (const [callId, session] of activeSessions.entries()) {
        if (session.ws === ws) {
          session.terminate("connection_closed")
          activeSessions.delete(callId)
        }
      }
    })

    ws.on("error", (error) => {
      console.error("❌ [SANPBX-WS] WebSocket error:", error.message)
    })
  })

  console.log("✅ [SANPBX-WS] SanIPPBX WebSocket server setup complete")
}

async function handleConnected(ws, data) {
  console.log(`🔗 [SANPBX-CONNECTED] Call connected: ${data.callId} | Stream: ${data.streamId}`)
  console.log(`📞 [SANPBX-CONNECTED] Caller: ${data.callerId} | Direction: ${data.callDirection} | DID: ${data.did}`)
  
  // Store initial call data for session creation
  ws.pendingCallData = {
    callId: data.callId,
    streamId: data.streamId,
    channelId: data.channelId,
    callerId: data.callerId,
    callDirection: data.callDirection,
    did: data.did,
    extraParams: data.extraParams
  }
}

async function handleStart(ws, data) {
  const callId = data.callId
  const streamId = data.streamId
  const mediaFormat = data.mediaFormat

  console.log(`🚀 [SANPBX-START] Starting call session: ${callId} | Stream: ${streamId}`)
  console.log(`🎵 [SANPBX-START] Media format:`, mediaFormat)

  // Create new session
  const session = new SanPbxCallSession(ws, {
    callId,
    streamId,
    channelId: data.channelId,
    callerId: data.callerId,
    callDirection: data.callDirection,
    did: data.did
  })
  
  session.mediaFormat = mediaFormat
  session.isActive = true

  // Store session
  activeSessions.set(callId, session)

  // Connect to Deepgram
  await session.connectToDeepgram()

  console.log(`✅ [SANPBX-START] Session started. CallId: ${callId}`)
}

async function handleAnswer(ws, data) {
  const callId = data.callId
  const session = activeSessions.get(callId)

  if (!session) {
    console.log(`⚠️ [SANPBX-ANSWER] No active session found for callId: ${callId}`)
    return
  }

  console.log(`📞 [SANPBX-ANSWER] Call answered: ${callId}`)
  session.isAnswered = true

  // Send initial greeting with enhanced message
  try {
    const greetingMessage = "Hello! Welcome to our AI assistant.?"
    console.log(`🎤 [SANPBX-ANSWER] Sending first message: ${greetingMessage}`)
    await session.convertToSpeech(greetingMessage)
  } catch (error) {
    console.error("❌ [SANPBX-ANSWER] Error sending greeting:", error.message)
    // Fallback: send text message if TTS fails
    session.sendTextToClient("Hello! Welcome to our AI assistant. How can I help you today?")
  }
}

async function handleMedia(ws, data) {
  const { streamId, media } = data
  const callId = data.callId

  // Find session by callId
  const session = activeSessions.get(callId)

  if (!session) {
    console.log(`⚠️ [SANPBX-MEDIA] No active session found for callId: ${callId}`)
    return
  }

  if (media && media.payload) {
    // Log the incoming media format for debugging
    if (!session.mediaFormatLogged) {
      console.log(`🎵 [SANPBX-MEDIA] Incoming media format:`)
      console.log(`   - Media encoding: ${media.format?.encoding || 'unknown'}`)
      console.log(`   - Sample rate: ${media.format?.sampleRate || 'unknown'}`)
      console.log(`   - Channels: ${media.format?.channels || 'unknown'}`)
      console.log(`   - Payload type: ${typeof media.payload}`)
      console.log(`   - Payload length: ${media.payload.length}`)
      
      // Additional media packet analysis
      console.log(`   - Full media object keys: ${Object.keys(media).join(', ')}`)
      console.log(`   - Format object keys: ${media.format ? Object.keys(media.format).join(', ') : 'none'}`)
      
      // Check for additional metadata
      if (media.metadata) {
        console.log(`   - Metadata: ${JSON.stringify(media.metadata)}`)
      }
      if (media.timestamp) {
        console.log(`   - Timestamp: ${media.timestamp}`)
      }
      if (media.sequence) {
        console.log(`   - Sequence: ${media.sequence}`)
      }
      
      session.mediaFormatLogged = true
    }

    // Ensure payload is base64 for downstream processing
    const normalized = normalizeBase64String(media.payload)
    if (normalized !== media.payload) {
      console.log("🔁 [SANPBX-MEDIA] Normalized incoming payload to base64")
    }
    await session.processAudioChunk(normalized)
  }
}

async function handleDtmf(ws, data) {
  const { callId, digit, dtmfDurationMs } = data

  console.log(`📞 [SANPBX-DTMF] DTMF received. callId: ${callId}, digit: ${digit}, duration: ${dtmfDurationMs}ms`)

  const session = activeSessions.get(callId)
  if (session) {
    // Handle DTMF input (could be used for menu navigation, etc.)
    session.sendTextToClient(`DTMF digit received: ${digit}`)
  }
}

async function handleStop(ws, data) {
  const callId = data.callId
  const disconnectedBy = data.disconnectedBy

  console.log(`🛑 [SANPBX-STOP] Stopping call session. callId: ${callId}, disconnected by: ${disconnectedBy}`)

  const session = activeSessions.get(callId)
  if (session) {
    session.terminate("call_ended")
    activeSessions.delete(callId)

    console.log(`✅ [SANPBX-STOP] Session terminated. CallId: ${callId}`)
  }
}

async function handleTransferCall(ws, data) {
  const { callId, transferTo, streamId, channelId } = data

  console.log(`🔄 [SANPBX-TRANSFER] Transfer request. callId: ${callId}, transferTo: ${transferTo}`)

  // Send transfer response (success)
  ws.send(
    JSON.stringify({
      event: "transfer-call-response",
      status: true,
      message: "Redirect successful",
      data: {},
      status_code: 200,
      channelId: channelId,
      callId: callId,
      streamId: streamId,
      timestamp: new Date().toISOString(),
    }),
  )

  // Terminate the session
  const session = activeSessions.get(callId)
  if (session) {
    session.terminate("call_transferred")
    activeSessions.delete(callId)
  }
}

async function handleHangupCall(ws, data) {
  const { callId, streamId, channel } = data

  console.log(`📞 [SANPBX-HANGUP] Hangup request. callId: ${callId}`)

  // Send hangup response (success)
  ws.send(
    JSON.stringify({
      event: "hangup-call-response",
      status: true,
      message: "Channel Hungup",
      data: {},
      status_code: 200,
      channelId: channel,
      callId: callId,
      streamId: streamId,
      timestamp: new Date().toISOString(),
    }),
  )

  // Terminate the session
  const session = activeSessions.get(callId)
  if (session) {
    session.terminate("call_hungup")
    activeSessions.delete(callId)
  }
}

// Export the setup function and session management
module.exports = {
  setupSanPbxWebSocketServer,
  activeSessions,
  SanPbxCallSession,
}
