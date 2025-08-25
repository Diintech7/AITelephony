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
const setupSanPbxWebSocketServer = (ws) => {
  console.log("Setting up SanIPPBX voice server connection")

  // Session state
  let streamId = null
  let callId = null
  let channelId = null

  // Real-time audio streaming (44.1kHz)
  const streamAudioToCallRealtime = async (audioBase64) => {
    const audioBuffer = Buffer.from(audioBase64, "base64")

    // SanIPPBX expects LINEAR16 44.1kHz PCM, mono
    const CHUNK_SIZE = 882 // 10ms @ 44100Hz 16-bit mono
    const CHUNK_DELAY = 10

    let position = 0
    console.log(`[STREAM-REALTIME] Start: ${audioBuffer.length} bytes in ${Math.ceil(audioBuffer.length / CHUNK_SIZE)} chunks`)
    console.log(`[STREAM] streamId: ${streamId}, WS State: ${ws.readyState}`)

    while (position < audioBuffer.length && ws.readyState === WebSocket.OPEN) {
      const chunk = audioBuffer.slice(position, position + CHUNK_SIZE)

      const paddedChunk =
        chunk.length < CHUNK_SIZE ? Buffer.concat([chunk, Buffer.alloc(CHUNK_SIZE - chunk.length)]) : chunk

      const mediaMessage = {
        event: "media",
        streamId: streamId,
        media: { payload: paddedChunk.toString("base64") },
      }

      try {
        ws.send(JSON.stringify(mediaMessage))
      } catch (err) {
        console.error(`[STREAM] Failed to send chunk: ${err.message}`)
        break
      }

      position += CHUNK_SIZE
      if (position < audioBuffer.length) {
        await new Promise((resolve) => setTimeout(resolve, CHUNK_DELAY))
      }
    }

    console.log(`[STREAM-COMPLETE] Sent audio successfully.`)
  }

  // Incoming events
  ws.on("message", async (message) => {
    const data = JSON.parse(message.toString())
    switch (data.event) {
      case "connected":
        console.log("[SANPBX] Connected:", data)
        break
      case "start":
        console.log("[SANPBX] Call started")
        streamId = data.streamId
        callId = data.callId
        channelId = data.channelId
        console.log("[SANPBX] streamId:", streamId)
        console.log("[SANPBX] callId:", callId)
        console.log("[SANPBX] channelId:", channelId)
        console.log("[SANPBX] Media Format:", JSON.stringify(data.mediaFormat))

        // Play greeting
        const greeting = "Hello, this is AI speaking from SanIPPBX."
        setTimeout(async () => {
          await synthesizeAndStreamAudio(greeting, "en-IN")
        }, 1000)
        break
      case "answer":
        console.log("[SANPBX] Call answered")
        break
      case "stop":
        console.log("[SANPBX] Call stopped")
        break
      case "dtmf":
        console.log("[SANPBX] DTMF pressed:", data.digit)
        break
      default:
        break
    }
  })
}


module.exports = {
  setupSanPbxWebSocketServer,
}