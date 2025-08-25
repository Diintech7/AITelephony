/* eslint-disable no-console */
const WebSocket = require("ws")

// ENV vars
const ELEVEN_API_KEY = process.env.ELEVEN_API_KEY
const ELEVEN_VOICE_ID = process.env.ELEVEN_VOICE_ID || "JBFqnCBsd6RMkjVDRZzb" // pick your ElevenLabs voice

if (!ELEVEN_API_KEY) {
  console.error("❌ ELEVEN_API_KEY is missing from environment variables")
  process.exit(1)
}

const ts = () => new Date().toISOString()

/**
 * Extract PCM16 mono data from WAV buffer (RIFF format).
 * If already PCM (no RIFF), returns the original buffer.
 */
function base64ToPcm16(b64) {
  const buf = Buffer.from(b64, "base64")

  // Check for "RIFF"
  if (buf.slice(0, 4).toString("ascii") !== "RIFF") {
    return buf
  }

  let offset = 12 // skip RIFF header
  while (offset + 8 <= buf.length) {
    const chunkId = buf.slice(offset, offset + 4).toString("ascii")
    const chunkSize = buf.readUInt32LE(offset + 4)
    const chunkStart = offset + 8
    const chunkEnd = chunkStart + chunkSize

    if (chunkId === "data") {
      return buf.slice(chunkStart, chunkEnd)
    }
    offset = chunkEnd
  }

  console.warn(`[WAV] ${ts()} - 'data' chunk not found`)
  return buf
}

/** Sleep helper */
const wait = (ms) => new Promise((res) => setTimeout(res, ms))

/**
 * Compute PCM16 bytes per 10ms chunk.
 */
function bytesPer10ms(sampleRate, channels = 1) {
  const samples = Math.round(sampleRate / 100)
  return samples * 2 * (channels || 1)
}

/**
 * Stream PCM16 audio back to SanIPPBX in base64 chunks.
 */
async function streamAudioToCallRealtime({ ws, streamId, pcmBuffer, sampleRate, channels }) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    console.warn(`[STREAM] ${ts()} - WS not open, cannot stream`)
    return
  }
  if (!streamId) {
    console.warn(`[STREAM] ${ts()} - Missing streamId, cannot stream`)
    return
  }

  const chunkBytes = bytesPer10ms(sampleRate, channels)
  const totalChunks = Math.ceil(pcmBuffer.length / chunkBytes)

  console.log(
    `[STREAM-START] ${ts()} - sampleRate=${sampleRate}, channels=${channels}, chunkBytes=${chunkBytes}, totalChunks=${totalChunks}`
  )

  let sent = 0
  for (let pos = 0; pos < pcmBuffer.length && ws.readyState === WebSocket.OPEN; pos += chunkBytes) {
    const slice = pcmBuffer.slice(pos, pos + chunkBytes)
    const padded =
      slice.length < chunkBytes ? Buffer.concat([slice, Buffer.alloc(chunkBytes - slice.length)]) : slice

    const msg = {
      event: "media",
      streamId,
      media: { payload: padded.toString("base64") },
    }

    try {
      ws.send(JSON.stringify(msg))
    } catch (err) {
      console.error(`[STREAM] ${ts()} - Send error: ${err.message}`)
      break
    }

    sent++
    if (sent % 20 === 0 || sent === totalChunks) {
      console.log(`[STREAM] ${ts()} - Sent ${sent}/${totalChunks} chunks`)
    }

    if (sent < totalChunks) await wait(10) // pace ~10ms
  }

  console.log(`[STREAM-COMPLETE] ${ts()} - Finished; chunksSent=${sent}/${totalChunks}`)
}

/**
 * ElevenLabs TTS → PCM16 → stream to PBX.
 */
async function synthesizeAndStreamAudio({ text, ws, streamId, sampleRate, channels }) {
  console.log(`[TTS-START] ${ts()} - ElevenLabs: "${text}"`)

  try {
    const resp = await fetch(
      `https://api.elevenlabs.io/v1/text-to-speech/${ELEVEN_VOICE_ID}`,
      {
        method: "POST",
        headers: {
          "xi-api-key": ELEVEN_API_KEY,
          "Accept": "audio/wav", // PCM16 WAV
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          text,
          voice_settings: {
            stability: 0.5,
            similarity_boost: 0.8,
          },
        }),
      }
    )

    if (!resp.ok) {
      const errText = await resp.text()
      throw new Error(`ElevenLabs API ${resp.status}: ${errText}`)
    }

    const arrayBuf = await resp.arrayBuffer()
    const audioBuf = Buffer.from(arrayBuf)

    // Extract PCM16 from WAV
    const pcm = base64ToPcm16(audioBuf.toString("base64"))
    console.log(`[TTS] ${ts()} - Got ElevenLabs PCM: ${pcm.length} bytes`)

    await streamAudioToCallRealtime({
      ws,
      streamId,
      pcmBuffer: pcm,
      sampleRate: sampleRate || 44100,
      channels: channels || 1,
    })
  } catch (err) {
    console.error(`[TTS] ElevenLabs error: ${err.message}`)
  }
}

/**
 * Main SanIPPBX handler
 */
function setupSanPbxWebSocketServer(ws) {
  console.log("Setting up SanIPPBX voice server connection")

  let streamId = null
  let callId = null
  let channelId = null
  let mediaFormat = { encoding: "PCM", sampleRate: 44100, channels: 1 }

  ws.on("close", () => {
    console.log(`[SANPBX] ${ts()} - WS closed`)
  })

  ws.on("error", (err) => {
    console.error(`[SANPBX] ${ts()} - WS error: ${err.message}`)
  })

  ws.on("message", async (raw) => {
    let data
    try {
      data = JSON.parse(raw.toString())
    } catch (err) {
      console.error(`[SANPBX] ${ts()} - Bad JSON: ${err.message}`)
      return
    }

    switch (data.event) {
      case "connected":
        console.log("[SANPBX] Connected:", data)
        break

      case "start":
        console.log("[SANPBX] Call started")
        streamId = data.streamId
        callId = data.callId
        channelId = data.channelId
        mediaFormat = {
          encoding: data.mediaFormat?.encoding || "PCM",
          sampleRate: Number(data.mediaFormat?.sampleRate) || 44100,
          channels: Number(data.mediaFormat?.channels) || 1,
        }

        console.log("[SANPBX] streamId:", streamId)
        console.log("[SANPBX] callId:", callId)
        console.log("[SANPBX] channelId:", channelId)
        console.log("[SANPBX] Media Format:", mediaFormat)

        // Play greeting
        setTimeout(async () => {
          await synthesizeAndStreamAudio({
            text: "Hi! How can I help you?",
            ws,
            streamId,
            sampleRate: mediaFormat.sampleRate,
            channels: mediaFormat.channels,
          })
        }, 500)
        break

      case "answer":
        console.log("[SANPBX] Call answered")
        break

      case "dtmf":
        console.log(`[SANPBX] DTMF pressed: ${data.digit}`)
        await synthesizeAndStreamAudio({
          text: `You pressed ${data.digit}.`,
          ws,
          streamId,
          sampleRate: mediaFormat.sampleRate,
          channels: mediaFormat.channels,
        })
        break

      case "stop":
        console.log("[SANPBX] Call stopped")
        try {
          ws.close()
        } catch {}
        break

      default:
        console.log(`[SANPBX] Unhandled event: ${data.event}`)
        break
    }
  })
}

module.exports = { setupSanPbxWebSocketServer }
