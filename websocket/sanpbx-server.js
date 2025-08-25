/* eslint-disable no-console */
const WebSocket = require("ws")
const { WebSocket: DGWebSocket } = require("ws")
const fetch = require("node-fetch")
const fs = require("fs")

// ENV
const ELEVEN_API_KEY = process.env.ELEVEN_API_KEY
const ELEVEN_VOICE_ID = process.env.ELEVEN_VOICE_ID || "kdmDKE6EkgrWrrykO9Qt"
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY
const OPENAI_API_KEY = process.env.OPENAI_API_KEY

if (!ELEVEN_API_KEY || !DEEPGRAM_API_KEY || !OPENAI_API_KEY) {
  console.error("❌ Missing API keys in .env (ElevenLabs / Deepgram / OpenAI)")
  process.exit(1)
}

const ts = () => new Date().toISOString()
const wait = (ms) => new Promise((res) => setTimeout(res, ms))

// -------- Enhanced Base64 Validation --------
function validateBase64Audio(base64String, label = "AUDIO") {
  try {
    if (!base64String || typeof base64String !== 'string') {
      console.log(`[${label}] ❌ Invalid base64 string: ${typeof base64String}`)
      return false
    }
    
    const buffer = Buffer.from(base64String, 'base64')
    if (buffer.length === 0) {
      console.log(`[${label}] ❌ Empty buffer from base64`)
      return false
    }
    
    if (buffer.length % 2 !== 0) {
      console.log(`[${label}] ⚠️  Odd buffer length: ${buffer.length} bytes`)
    }
    
    console.log(`[${label}] ✅ Valid base64: ${base64String.length} chars -> ${buffer.length} bytes`)
    
    const samples = []
    for (let i = 0; i < Math.min(8, buffer.length - 1); i += 2) {
      samples.push(buffer.readInt16LE(i))
    }
    console.log(`[${label}] First samples: [${samples.join(', ')}]`)
    
    return true
  } catch (err) {
    console.log(`[${label}] ❌ Base64 validation error: ${err.message}`)
    return false
  }
}

function logFullData(label, data) {
  console.log(`\n=== ${label} ===`)
  console.log(JSON.stringify(data, null, 2))
  console.log(`=== END ${label} ===\n`)
}

function logAudioSample(label, buffer, maxBytes = 32) {
  if (!buffer || buffer.length === 0) {
    console.log(`[${label}] Empty buffer`)
    return
  }
  
  const sample = buffer.slice(0, Math.min(maxBytes, buffer.length))
  const hex = sample.toString('hex').match(/.{1,2}/g).join(' ')
  const decimal = Array.from(sample).join(', ')
  
  console.log(`[${label}] First ${sample.length} bytes:`)
  console.log(`  HEX: ${hex}`)
  console.log(`  DEC: ${decimal}`)
  
  if (buffer.length >= 2) {
    const int16samples = []
    for (let i = 0; i < Math.min(8, buffer.length - 1); i += 2) {
      int16samples.push(buffer.readInt16LE(i))
    }
    console.log(`  INT16LE: [${int16samples.join(', ')}]`)
  }
}

// -------- MAIN SanPBX Handler --------
function setupSanPbxWebSocketServer(ws) {
  console.log("\n🚀 Setting up SanIPPBX voice server connection")

  let streamId = null
  let mediaFormat = { encoding: "LINEAR16", sampleRate: 8000, channels: 1 }
  let dgWs = null
  let callAnswered = false
  let isProcessing = false
  let incomingMediaCount = 0
  let hasReceivedAudio = false

  ws.on("message", async (raw) => {
    let data
    try {
      data = JSON.parse(raw.toString())
    } catch (parseError) {
      console.error("[SANPBX] ❌ Failed to parse message:", parseError.message)
      return
    }
    
    console.log(`\n📨 [SANPBX] Received event: ${data.event}`)
    
    switch (data.event) {
      case "connected":
        logFullData("CONNECTED", data)
        break
        
      case "start":
        logFullData("START", data)
        streamId = data.streamId
        if (data.mediaFormat) {
          mediaFormat = {
            encoding: data.mediaFormat.encoding || "LINEAR16",
            sampleRate: data.mediaFormat.sampleRate || 8000,
            channels: data.mediaFormat.channels || 1
          }
        }
        console.log(`[SANPBX] 🆔 Stream ID: ${streamId}`)
        console.log(`[SANPBX] 🎵 Media Format:`, mediaFormat)
        break
        
      case "answer":
        logFullData("ANSWER", data)
        callAnswered = true
        break

      case "media": {
        incomingMediaCount++

        // 🆕 Always log full payload in the same format SIP sends
        console.log(JSON.stringify(data, null, 2))

        const b64 = data?.payload
        if (incomingMediaCount <= 3 && b64 && b64.length > 0) {
          const buffer = Buffer.from(b64, "base64")
          console.log(`[MEDIA-${incomingMediaCount}] ✅ Payload: ${b64.length} chars -> ${buffer.length} bytes`)
          logAudioSample(`MEDIA-${incomingMediaCount}`, buffer, 16)
          if (!hasReceivedAudio) {
            hasReceivedAudio = true
            console.log("🎉 [MEDIA] First audio received!")
          }
        } else if (incomingMediaCount === 4) {
          console.log(`[MEDIA] ... (received ${incomingMediaCount}+ packets)`)
        }
        break
      }
      
      case "dtmf":
        console.log(`📱 [SANPBX] DTMF: ${data.digit}`)
        break
        
      case "stop":
        console.log("🛑 [SANPBX] Call stopped")
        break
        
      default:
        console.log(`❓ [SANPBX] Unknown event: ${data.event}`)
        break
    }
  })
  
  ws.on("error", (error) => {
    console.error("[SANPBX] ❌ WebSocket error:", error.message)
  })
  
  ws.on("close", (code, reason) => {
    console.log(`[SANPBX] 🔌 WebSocket connection closed: ${code} - ${reason}`)
  })
}

module.exports = { setupSanPbxWebSocketServer }
