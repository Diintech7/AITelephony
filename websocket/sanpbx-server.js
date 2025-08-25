/* eslint-disable no-console */
const WebSocket = require("ws")
const { WebSocket: DGWebSocket } = require("ws")
const fetch = require("node-fetch")

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

// -------- PCM utils --------
function bytesPer10ms(rate, channels = 1) {
  return Math.round(rate / 100) * 2 * channels
}

// -------- STREAM audio to PBX --------
async function streamAudioToCallRealtime({ ws, streamId, pcmBuffer, sampleRate, channels }) {
  const chunkBytes = bytesPer10ms(sampleRate, channels)
  const totalChunks = Math.ceil(pcmBuffer.length / chunkBytes)
  console.log(`[STREAM-START] ${ts()} - total=${totalChunks}, chunkBytes=${chunkBytes}`)

  for (let pos = 0, sent = 0; pos < pcmBuffer.length && ws.readyState === WebSocket.OPEN; pos += chunkBytes) {
    const slice = pcmBuffer.slice(pos, pos + chunkBytes)
    const padded = slice.length < chunkBytes ? Buffer.concat([slice, Buffer.alloc(chunkBytes - slice.length)]) : slice
    ws.send(JSON.stringify({ event: "media", streamId, media: { payload: padded.toString("base64") } }))
    sent++
    if (sent % 20 === 0 || sent === totalChunks) console.log(`[STREAM] ${ts()} - Sent ${sent}/${totalChunks}`)
    if (sent < totalChunks) await wait(10)
  }
  console.log(`[STREAM-COMPLETE] ${ts()} - done`)
}

// -------- ElevenLabs TTS --------
async function synthesizeAndStreamAudio({ text, ws, streamId, sampleRate, channels }) {
  console.log(`[TTS-START] ${ts()} - ElevenLabs: "${text}"`)

  // Map PBX sampleRate → ElevenLabs output_format
  const formatMap = {
    8000: "pcm_8000",
    16000: "pcm_16000",
    22050: "pcm_22050",
    24000: "pcm_24000",
    44100: "pcm_44100",
    44000: "pcm_44000", // PBX sometimes reports 44000
  }
  const outputFormat = formatMap[sampleRate] || "pcm_44100"

  try {
    const resp = await fetch(
      `https://api.elevenlabs.io/v1/text-to-speech/${ELEVEN_VOICE_ID}`,
      {
        method: "POST",
        headers: {
          "xi-api-key": ELEVEN_API_KEY,
          Accept: "audio/pcm",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          text,
          output_format: outputFormat,
        }),
      }
    )
    if (!resp.ok) throw new Error(`ElevenLabs API ${resp.status}: ${await resp.text()}`)

    const audioBuf = Buffer.from(await resp.arrayBuffer()) // already PCM LINEAR16
    console.log(`[TTS] ElevenLabs returned ${audioBuf.length} bytes PCM (${outputFormat})`)

    await streamAudioToCallRealtime({
      ws,
      streamId,
      pcmBuffer: audioBuf,
      sampleRate,
      channels,
    })
  } catch (err) {
    console.error(`[TTS] ElevenLabs error: ${err.message}`)
  }
}

// -------- OpenAI GPT --------
async function getAiResponse(prompt) {
  try {
    const resp = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: "You are a helpful voice assistant." },
          { role: "user", content: prompt },
        ],
      }),
    })
    if (!resp.ok) throw new Error(`OpenAI ${resp.status}: ${await resp.text()}`)
    const data = await resp.json()
    return data.choices[0].message.content
  } catch (err) {
    console.error(`[AI] Error: ${err.message}`)
    return "Sorry, I had a problem understanding."
  }
}

// -------- SanPBX Handler --------
function setupSanPbxWebSocketServer(ws) {
  console.log("Setting up SanIPPBX voice server connection")

  let streamId = null
  let mediaFormat = { encoding: "LINEAR16", sampleRate: 44100, channels: 1 }
  let dgWs = null

  // connect Deepgram once call starts
  const connectDeepgram = (sampleRate) => {
    dgWs = new DGWebSocket(
      `wss://api.deepgram.com/v1/listen?encoding=linear16&sample_rate=${sampleRate}&channels=1&model=nova-2&interim_results=false&smart_format=true`,
      { headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` } }
    )
    dgWs.on("open", () => console.log("[STT] Connected to Deepgram"))
    dgWs.on("message", async (msg) => {
      const data = JSON.parse(msg.toString())
      const transcript = data.channel?.alternatives?.[0]?.transcript
      if (transcript) {
        console.log(`[STT] Transcript: "${transcript}"`)
        const reply = await getAiResponse(transcript)
        console.log(`[AI] Response: "${reply}"`)
        await synthesizeAndStreamAudio({
          text: reply,
          ws,
          streamId,
          sampleRate: mediaFormat.sampleRate,
          channels: mediaFormat.channels,
        })
      }
    })
    dgWs.on("close", () => console.log("[STT] Deepgram closed"))
    dgWs.on("error", (e) => console.error("[STT] Error:", e.message))
  }

  ws.on("message", async (raw) => {
    let data
    try {
      data = JSON.parse(raw.toString())
    } catch {
      return
    }
    switch (data.event) {
      case "connected":
        console.log("[SANPBX] Connected:", data)
        break
      case "start":
        console.log("[SANPBX] Call started")
        streamId = data.streamId
        mediaFormat = data.mediaFormat || mediaFormat
        console.log("[SANPBX] streamId:", streamId)
        console.log("[SANPBX] Media Format:", mediaFormat)
        connectDeepgram(mediaFormat.sampleRate)
        setTimeout(async () => {
          await synthesizeAndStreamAudio({
            text: "Hi! This is a test greeting from the AI assistant. How can I help you today?",
            ws,
            streamId,
            sampleRate: mediaFormat.sampleRate,
            channels: mediaFormat.channels,
          })
        }, 500)
        break
        case "answer": 
          console.log("[SANPBX] Call answered")
        
          // ✅ pull identifiers from the incoming message
          const { callId, channelId, streamId } = data
        
          // ✅ Send ACK back to PBX so it bridges media
          const ack = {
            event: "answer",
            callId,
            channelId,
            streamId,
          }
          try {
            ws.send(JSON.stringify(ack))
            console.log("[SANPBX] Sent answer ACK back to PBX")
          } catch (err) {
            console.error("[SANPBX] Failed to send answer ACK:", err.message)
          }
        
          // Now it’s safe to synthesize & stream
          const greeting = "Hi! This is a test greeting from the AI assistant. How can I help you today?"
          await synthesizeAndStreamAudio({
            text: greeting,
            language: DEFAULT_LANG,
            voice: DEFAULT_VOICE,
            ws,
            streamId,
            callId,
            channelId,
            sampleRate: mediaFormat.sampleRate,
            channels: mediaFormat.channels,
          })
          break
        
        
        
        
      case "media": {
        const b64 = data?.media?.payload
        if (b64 && dgWs && dgWs.readyState === WebSocket.OPEN) {
          dgWs.send(Buffer.from(b64, "base64"))
        }
        break
      }
      case "dtmf":
        console.log(`[SANPBX] DTMF: ${data.digit}`)
        break
      case "stop":
        console.log("[SANPBX] Call stopped")
        if (dgWs) try { dgWs.close() } catch {}
        ws.close()
        break
      default:
        break
    }
  })
}

module.exports = { setupSanPbxWebSocketServer }
