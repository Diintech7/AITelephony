/* eslint-disable no-console */
const WebSocket = require("ws")

/**
 * SanIPPBX WebSocket handler (per-connection).
 * Exported function expects a *single* ws connection object.
 *
 * Events (from PBX):
 * - connected
 * - start { mediaFormat: { encoding, sampleRate, channels } }
 * - answer
 * - dtmf
 * - stop
 *
 * Events (to PBX):
 * - media { streamId, media: { payload: <base64 PCM16> } }
 */

const DEFAULT_VOICE = process.env.DEFAULT_VOICE || "meera"
const DEFAULT_LANG = process.env.DEFAULT_LANG || "en-IN"

/** ---------- Utilities ---------- */

const ts = () => new Date().toISOString()

function safeJsonParse(bufOrStr) {
  try {
    const str = Buffer.isBuffer(bufOrStr) ? bufOrStr.toString() : String(bufOrStr)
    return JSON.parse(str)
  } catch (err) {
    console.error(`[JSON] ${ts()} - Failed to parse JSON: ${err.message}`)
    return null
  }
}

/**
 * Minimal WAV (PCM) parser -> returns raw PCM16 mono/stereo Buffer
 * Accepts base64 WAV or base64 raw PCM. If WAV, extracts "data" chunk.
 */
function base64ToPcm16(bufferB64) {
  const buf = Buffer.from(bufferB64, "base64")

  // If not RIFF, assume it's already raw PCM
  if (buf.slice(0, 4).toString("ascii") !== "RIFF") {
    return buf
  }

  // RIFF WAV layout: "RIFF" + size + "WAVE" + chunks
  let offset = 12 // skip RIFF header
  while (offset + 8 <= buf.length) {
    const chunkId = buf.slice(offset, offset + 4).toString("ascii")
    const chunkSize = buf.readUInt32LE(offset + 4)
    const chunkDataStart = offset + 8
    const chunkDataEnd = chunkDataStart + chunkSize

    if (chunkDataEnd > buf.length) break

    if (chunkId === "data") {
      return buf.slice(chunkDataStart, chunkDataEnd)
    }

    offset = chunkDataEnd
  }

  console.warn(`[WAV] ${ts()} - 'data' chunk not found; returning original buffer`)
  return buf
}

/** Sleep helper */
const wait = (ms) => new Promise((res) => setTimeout(res, ms))

/**
 * Compute bytes per 10ms chunk for PCM16
 *   samplesPer10ms = round(sampleRate / 100)
 *   bytes = samples * 2 (16-bit) * channels
 */
function bytesPer10ms(sampleRate, channels = 1) {
  const samples = Math.round(sampleRate / 100)
  return samples * 2 * (channels || 1)
}

/** ---------- TTS Pipeline (Sarvam) ---------- */
/**
 * Synthesize TTS via Sarvam (returns base64 WAV or base64 PCM depending on API),
 * then convert to raw PCM16 and stream to PBX in real time.
 */
async function synthesizeAndStreamAudio({
  text,
  language,
  voice,
  ws,
  streamId,
  sampleRate,
  channels,
}) {
  console.log(`[TTS-START] ${ts()} - Text: "${text}"`)
  try {
    const startAt = Date.now()
    // Node 18+ has global fetch
    const resp = await fetch("https://api.sarvam.ai/text-to-speech", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "API-Subscription-Key": process.env.SARVAM_API_KEY,
      },
      body: JSON.stringify({
        inputs: [text],
        target_language_code: language || DEFAULT_LANG,
        speaker: voice || DEFAULT_VOICE,
        pitch: 0,
        pace: 1.0,
        loudness: 1.0,
        speech_sample_rate: sampleRate, // match PBX
        enable_preprocessing: false,
        model: "bulbul:v1",
      }),
    })

    if (!resp.ok) {
      const errText = await resp.text()
      throw new Error(`Sarvam API ${resp.status}: ${errText}`)
    }

    const data = await resp.json()
    const audioBase64 = data?.audios?.[0]
    if (!audioBase64) {
      throw new Error("Sarvam returned no audio")
    }

    console.log(
      `[TTS] ${ts()} - Received audio, length(base64)=${audioBase64.length}, elapsed=${Date.now() - startAt}ms`,
    )

    const pcm = base64ToPcm16(audioBase64)
    console.log(`[TTS] ${ts()} - PCM bytes: ${pcm.length}`)

    await streamAudioToCallRealtime({
      ws,
      streamId,
      pcmBuffer: pcm,
      sampleRate,
      channels,
    })
  } catch (err) {
    console.error(`[TTS] ${ts()} - Error: ${err.message}`)
  }
}

/** ---------- Streaming back to PBX ---------- */
/**
 * Streams raw PCM16 to PBX as base64 in 10ms chunks, respecting sampleRate/channels.
 * Builds: { event: "media", streamId, media: { payload: <base64> } }
 */
async function streamAudioToCallRealtime({ ws, streamId, pcmBuffer, sampleRate, channels }) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    console.warn(`[STREAM] ${ts()} - WS not open; cannot stream`)
    return
  }
  if (!streamId) {
    console.warn(`[STREAM] ${ts()} - Missing streamId; cannot stream`)
    return
  }

  const chunkBytes = bytesPer10ms(sampleRate, channels)
  const totalChunks = Math.ceil(pcmBuffer.length / chunkBytes)
  console.log(
    `[STREAM-START] ${ts()} - sampleRate=${sampleRate}, channels=${channels}, chunkBytes=${chunkBytes}, totalChunks=${totalChunks}`,
  )
  console.log(`[STREAM] ${ts()} - streamId=${streamId}, wsState=${ws.readyState}`)

  let sent = 0
  for (let pos = 0; pos < pcmBuffer.length && ws.readyState === WebSocket.OPEN; pos += chunkBytes) {
    const slice = pcmBuffer.slice(pos, pos + chunkBytes)
    const padded =
      slice.length < chunkBytes ? Buffer.concat([slice, Buffer.alloc(chunkBytes - slice.length)]) : slice

    const payloadB64 = padded.toString("base64")
    const msg = {
      event: "media",
      streamId: streamId,
      media: { payload: payloadB64 },
    }

    try {
      ws.send(JSON.stringify(msg))
    } catch (err) {
      console.error(`[STREAM] ${ts()} - Send error at chunk ${sent + 1}: ${err.message}`)
      break
    }

    sent++
    if (sent % 20 === 0 || sent === totalChunks) {
      console.log(`[STREAM] ${ts()} - Sent ${sent}/${totalChunks} chunks`)
    }

    // Pace at ~10ms per chunk
    if (sent < totalChunks) await wait(10)
  }

  console.log(`[STREAM-COMPLETE] ${ts()} - Finished; chunksSent=${sent}/${totalChunks}`)
}

/** ---------- Main exported connection handler ---------- */

function setupSanPbxWebSocketServer(ws) {
  console.log("Setting up SanIPPBX voice server connection")

  // Per-call/session state
  let streamId = null
  let callId = null
  let channelId = null
  let mediaFormat = { encoding: "PCM", sampleRate: 24000, channels: 1 } // default/fallback

  // Optional: ping keepalive (PBX usually drives the session, but harmless)
  let pingTimer = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.ping()
      } catch {}
    }
  }, 30000)

  ws.on("close", (code, reason) => {
    clearInterval(pingTimer)
    console.log(`[SANPBX] ${ts()} - WS closed code=${code} reason=${reason}`)
  })

  ws.on("error", (err) => {
    console.error(`[SANPBX] ${ts()} - WS error: ${err.message}`)
  })
  function normalizeSampleRate(rate) {
    const allowed = [8000, 16000, 22050, 24000]
    return allowed.includes(rate) ? rate : 8000 // fallback
  }
  
  ws.on("message", async (raw) => {
    const data = safeJsonParse(raw)
    if (!data || !data.event) return

    switch (data.event) {
      case "connected": {
        console.log("[SANPBX] Connected:", data)
        // Sometimes PBX sends IDs here as well; keep note but wait for 'start' to lock.
        break
      }

      case "start": {
        console.log("[SANPBX] Call started")
        streamId = data.streamId
        callId = data.callId
        channelId = data.channelId

        // PBX can report different spellings/values; normalize
        const mf = data.mediaFormat || {}
        mediaFormat.encoding = mf.encoding || "PCM"
        mediaFormat.sampleRate = normalizeSampleRate(Number(mf.sampleRate) || 24000)
        mediaFormat.channels = Number(mf.channels) || 1

        console.log("[SANPBX] streamId:", streamId)
        console.log("[SANPBX] callId:", callId)
        console.log("[SANPBX] channelId:", channelId)
        console.log("[SANPBX] Media Format:", JSON.stringify(mediaFormat))

        // === Greeting (audible) ===
        const greeting = "Hi! How can I help you?"
        // small delay to let PBX finish start/answer handshake
        setTimeout(async () => {
          await synthesizeAndStreamAudio({
            text: greeting,
            language: DEFAULT_LANG,
            voice: DEFAULT_VOICE,
            ws,
            streamId,
            sampleRate: mediaFormat.sampleRate,
            channels: mediaFormat.channels,
          })
        }, 600)
        break
      }

      case "answer": {
        console.log("[SANPBX] Call answered")
        break
      }

      case "dtmf": {
        console.log(`[SANPBX] DTMF: digit=${data.digit}, durationMs=${data.dtmfDurationMs}`)
        // Example: speak back the digit
        const say = `You pressed ${data.digit}.`
        await synthesizeAndStreamAudio({
          text: say,
          language: DEFAULT_LANG,
          voice: DEFAULT_VOICE,
          ws,
          streamId,
          sampleRate: mediaFormat.sampleRate,
          channels: mediaFormat.channels,
        })
        break
      }

      case "stop": {
        console.log("[SANPBX] Call stopped")
        try {
          ws.close()
        } catch {}
        break
      }

      // If PBX ever sends upstream audio media (rare), you can handle here:
      case "media": {
        // Incoming audio from PBX (base64 PCM). Typically used for STT passthrough if needed.
        // const b64 = data?.media?.payload
        // console.log(`[SANPBX] <- media (${b64?.length || 0} chars)`)
        break
      }

      // Transfer / hangup responses, etc., can be logged:
      case "transfer-call-response":
      case "hangup-call-response": {
        console.log(`[SANPBX] ${data.event}:`, data)
        break
      }

      default: {
        console.log(`[SANPBX] Unhandled event: ${data.event}`, data)
        break
      }
    }
  })
}

module.exports = { setupSanPbxWebSocketServer }
