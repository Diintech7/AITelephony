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

// -------- Logging utilities --------
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

// -------- Audio Format Conversion --------
function resampleAudio(buffer, fromRate, toRate) {
  if (fromRate === toRate) return buffer

  const ratio = fromRate / toRate
  const newLength = Math.floor(buffer.length / 2 / ratio) * 2
  const result = Buffer.alloc(newLength)

  for (let i = 0; i < newLength; i += 2) {
    const sourceIndex = Math.floor((i / 2) * ratio) * 2
    if (sourceIndex < buffer.length - 1) {
      result[i] = buffer[sourceIndex]
      result[i + 1] = buffer[sourceIndex + 1]
    }
  }

  console.log(`[AUDIO-RESAMPLE] ${fromRate}Hz -> ${toRate}Hz: ${buffer.length} -> ${newLength} bytes`)
  logAudioSample("RESAMPLED", result, 16)
  return result
}

// -------- PCM utils --------
function bytesPer10ms(rate, channels = 1) {
  const bytes = Math.round(rate / 100) * 2 * channels
  console.log(`[PCM-CALC] Rate: ${rate}Hz, Channels: ${channels}, Bytes per 10ms: ${bytes}`)
  return bytes
}

// -------- STREAM audio to PBX --------
async function streamAudioToCallRealtime({ ws, streamId, pcmBuffer, sampleRate, channels }) {
  const chunkBytes = bytesPer10ms(sampleRate, channels)
  const totalChunks = Math.ceil(pcmBuffer.length / chunkBytes)
  
  console.log(`\n[STREAM-START] ${ts()}`)
  console.log(`  Total chunks: ${totalChunks}`)
  console.log(`  Chunk size: ${chunkBytes} bytes`)
  console.log(`  Sample rate: ${sampleRate}Hz`)
  console.log(`  Channels: ${channels}`)
  console.log(`  Buffer size: ${pcmBuffer.length} bytes`)
  console.log(`  Expected duration: ${(pcmBuffer.length / (sampleRate * 2 * channels)).toFixed(2)}s`)
  
  logAudioSample("STREAM-INPUT", pcmBuffer, 32)

  for (let pos = 0, sent = 0; pos < pcmBuffer.length && ws.readyState === WebSocket.OPEN; pos += chunkBytes) {
    const slice = pcmBuffer.slice(pos, pos + chunkBytes)
    const padded = slice.length < chunkBytes ? Buffer.concat([slice, Buffer.alloc(chunkBytes - slice.length)]) : slice
    
    const mediaEvent = {
      event: "media",
      streamId,
      media: { 
        payload: padded.toString("base64")
      }
    }
    
    // Log first few chunks in detail
    if (sent < 3) {
      console.log(`\n[CHUNK-${sent}] Size: ${slice.length}/${chunkBytes} bytes`)
      logAudioSample(`CHUNK-${sent}`, slice, 16)
      console.log(`[CHUNK-${sent}] Base64 length: ${mediaEvent.media.payload.length}`)
      console.log(`[CHUNK-${sent}] Media event:`)
      console.log(JSON.stringify(mediaEvent, null, 2))
    }
    
    try {
      ws.send(JSON.stringify(mediaEvent))
      sent++
    } catch (err) {
      console.error(`[STREAM-ERROR] Failed to send chunk ${sent}: ${err.message}`)
      break
    }
    
    if (sent % 20 === 0 || sent === totalChunks) {
      console.log(`[STREAM-PROGRESS] ${ts()} - Sent ${sent}/${totalChunks}`)
    }
    
    if (sent < totalChunks) await wait(10)
  }
  console.log(`[STREAM-COMPLETE] ${ts()} - Sent ${sent}/${totalChunks} chunks\n`)
}

// -------- ElevenLabs TTS --------
async function synthesizeAndStreamAudio({ text, ws, streamId, sampleRate, channels }) {
  console.log(`\n[TTS-START] ${ts()}`)
  console.log(`  Text: "${text}"`)
  console.log(`  Target: ${sampleRate}Hz, ${channels} channels`)

  try {
    const outputFormat = "pcm_44100"
    console.log(`[TTS] Requesting ElevenLabs format: ${outputFormat}`)
    
    const requestBody = {
      text,
      output_format: outputFormat,
      voice_settings: {
        stability: 0.5,
        similarity_boost: 0.8,
        style: 0.0,
        use_speaker_boost: true
      }
    }
    
    console.log("[TTS] Request body:", JSON.stringify(requestBody, null, 2))
    
    const resp = await fetch(
      `https://api.elevenlabs.io/v1/text-to-speech/${ELEVEN_VOICE_ID}`,
      {
        method: "POST",
        headers: {
          "xi-api-key": ELEVEN_API_KEY,
          Accept: "audio/pcm",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
      }
    )
    
    console.log(`[TTS] Response status: ${resp.status}`)
    console.log(`[TTS] Response headers:`, Object.fromEntries(resp.headers))
    
    if (!resp.ok) throw new Error(`ElevenLabs API ${resp.status}: ${await resp.text()}`)

    let audioBuf = Buffer.from(await resp.arrayBuffer())
    console.log(`[TTS] ElevenLabs returned ${audioBuf.length} bytes`)
    
    if (audioBuf.length === 0) {
      console.error("[TTS] ERROR: Empty audio buffer from ElevenLabs")
      return
    }

    logAudioSample("TTS-ORIGINAL", audioBuf, 32)

    // Check for valid PCM data
    const maxSample = Math.max(...Array.from({length: Math.min(100, audioBuf.length/2)}, (_, i) => Math.abs(audioBuf.readInt16LE(i * 2))))
    console.log(`[TTS] Max sample value: ${maxSample} (should be > 100 for audible content)`)

    // Save original audio for debugging
    try {
      fs.writeFileSync(`/tmp/debug_original_${Date.now()}.pcm`, audioBuf)
      console.log("[TTS] Saved original audio to /tmp/debug_original_*.pcm")
    } catch (err) {
      console.log("[TTS] Could not save debug file:", err.message)
    }

    // Resample if needed
    if (sampleRate !== 44100) {
      console.log(`[TTS] Resampling from 44100Hz to ${sampleRate}Hz`)
      audioBuf = resampleAudio(audioBuf, 44100, sampleRate)
      
      // Save resampled audio
      try {
        fs.writeFileSync(`/tmp/debug_resampled_${Date.now()}.pcm`, audioBuf)
        console.log("[TTS] Saved resampled audio to /tmp/debug_resampled_*.pcm")
      } catch (err) {
        console.log("[TTS] Could not save resampled debug file:", err.message)
      }
    }

    await streamAudioToCallRealtime({
      ws,
      streamId,
      pcmBuffer: audioBuf,
      sampleRate,
      channels,
    })
  } catch (err) {
    console.error(`[TTS] ElevenLabs error: ${err.message}`)
    console.error(`[TTS] Stack trace:`, err.stack)
  }
}

// Test audio generation
async function testAudioGeneration(sampleRate) {
  console.log(`\n[TEST-AUDIO] Generating test tone for ${sampleRate}Hz`)
  
  const duration = 2.0 // 2 seconds for easier detection
  const frequency = 440 // A4 note
  const samples = Math.floor(sampleRate * duration)
  const buffer = Buffer.alloc(samples * 2)
  
  for (let i = 0; i < samples; i++) {
    const sample = Math.sin(2 * Math.PI * frequency * i / sampleRate) * 16384
    buffer.writeInt16LE(Math.round(sample), i * 2)
  }
  
  console.log(`[TEST-AUDIO] Generated ${buffer.length} bytes (${duration}s @ ${frequency}Hz)`)
  logAudioSample("TEST-AUDIO", buffer, 32)
  
  // Save test audio
  try {
    fs.writeFileSync(`/tmp/debug_test_${Date.now()}.pcm`, buffer)
    console.log("[TEST-AUDIO] Saved test audio to /tmp/debug_test_*.pcm")
  } catch (err) {
    console.log("[TEST-AUDIO] Could not save test file:", err.message)
  }
  
  return buffer
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
          { role: "system", content: "You are a helpful voice assistant. Keep responses very short, under 15 words." },
          { role: "user", content: prompt },
        ],
      }),
    })
    if (!resp.ok) throw new Error(`OpenAI ${resp.status}: ${await resp.text()}`)
    const data = await resp.json()
    return data.choices[0].message.content
  } catch (err) {
    console.error(`[AI] Error: ${err.message}`)
    return "I understand."
  }
}

// -------- SanPBX Handler --------
function setupSanPbxWebSocketServer(ws) {
  console.log("\n🚀 Setting up SanIPPBX voice server connection")

  let streamId = null
  let mediaFormat = { encoding: "LINEAR16", sampleRate: 44100, channels: 1 }
  let dgWs = null
  let callAnswered = false
  let isProcessing = false
  let incomingMediaCount = 0

  const connectDeepgram = (sampleRate, encoding) => {
    const dgEncoding = 'linear16'
    console.log(`\n[STT-SETUP] Connecting to Deepgram`)
    console.log(`  Encoding: ${dgEncoding} (from PBX: ${encoding})`)
    console.log(`  Sample rate: ${sampleRate}Hz`)
    
    dgWs = new DGWebSocket(
      `wss://api.deepgram.com/v1/listen?encoding=${dgEncoding}&sample_rate=${sampleRate}&channels=1&model=nova-2&interim_results=false&smart_format=true&endpointing=300`,
      { headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` } }
    )
    
    dgWs.on("open", () => {
      console.log("[STT] ✅ Connected to Deepgram successfully")
    })
    
    dgWs.on("message", async (msg) => {
      try {
        const data = JSON.parse(msg.toString())
        console.log("[STT] Raw Deepgram response:", JSON.stringify(data, null, 2))
        
        const transcript = data.channel?.alternatives?.[0]?.transcript
        
        if (transcript && transcript.trim() && !isProcessing) {
          console.log(`\n🎤 [STT] Transcript: "${transcript}"`)
          isProcessing = true
          
          try {
            if (transcript.toLowerCase().includes('test audio')) {
              console.log("[DEBUG] 🔊 Test audio command detected")
              const testBuffer = await testAudioGeneration(mediaFormat.sampleRate)
              await streamAudioToCallRealtime({
                ws,
                streamId,
                pcmBuffer: testBuffer,
                sampleRate: mediaFormat.sampleRate,
                channels: mediaFormat.channels,
              })
            } else {
              const reply = await getAiResponse(transcript)
              console.log(`🤖 [AI] Response: "${reply}"`)
              
              await synthesizeAndStreamAudio({
                text: reply,
                ws,
                streamId,
                sampleRate: mediaFormat.sampleRate,
                channels: mediaFormat.channels,
              })
            }
          } finally {
            isProcessing = false
          }
        }
      } catch (error) {
        console.error("[STT] ❌ Error processing message:", error.message)
      }
    })
    
    dgWs.on("close", (code, reason) => {
      console.log(`[STT] 🔌 Deepgram closed: ${code} - ${reason}`)
    })
    
    dgWs.on("error", (e) => {
      console.error("[STT] ❌ Deepgram error:", e.message)
    })
  }

  ws.on("message", async (raw) => {
    let data
    try {
      data = JSON.parse(raw.toString())
    } catch (parseError) {
      console.error("[SANPBX] ❌ Failed to parse message:", parseError.message)
      console.error("[SANPBX] Raw message:", raw.toString().substring(0, 200))
      return
    }
    
    console.log(`\n📨 [SANPBX] Received event: ${data.event}`)
    
    switch (data.event) {
      case "connected":
        console.log("🔗 [SANPBX] Connected event:")
        logFullData("CONNECTED", data)
        break
        
      case "start":
        console.log("▶️ [SANPBX] Call started")
        logFullData("START", data)
        
        streamId = data.streamId
        
        if (data.mediaFormat) {
          mediaFormat = {
            encoding: data.mediaFormat.encoding || "LINEAR16",
            sampleRate: data.mediaFormat.sampleRate || 44100,
            channels: data.mediaFormat.channels || 1
          }
        }
        
        console.log(`[SANPBX] 🆔 Stream ID: ${streamId}`)
        console.log(`[SANPBX] 🎵 Media Format:`, mediaFormat)
        
        connectDeepgram(mediaFormat.sampleRate, mediaFormat.encoding)
        break
        
      case "answer":
        console.log("📞 [SANPBX] Call answered")
        logFullData("ANSWER", data)
        
        callAnswered = true
        
        const { callId, channelId, streamId: ansStreamId } = data
        
        if (ansStreamId) {
          streamId = ansStreamId
          console.log(`[SANPBX] 🆔 Updated Stream ID: ${streamId}`)
        }
        
        // Send ACK
        const ack = {
          event: "answer",
          callId,
          channelId,
          streamId,
        }
        
        console.log("[SANPBX] 📤 Sending ACK:")
        logFullData("ACK", ack)
        
        try {
          ws.send(JSON.stringify(ack))
          console.log("[SANPBX] ✅ Sent answer ACK back to PBX")
        } catch (err) {
          console.error("[SANPBX] ❌ Failed to send answer ACK:", err.message)
        }

        // Enhanced greeting sequence
        setTimeout(async () => {
          if (!isProcessing && callAnswered) {
            isProcessing = true
            
            try {
              console.log("\n🔊 [DEBUG] Starting audio test sequence...")
              
              // Step 1: Send test tone
              console.log("1️⃣ Sending test tone...")
              const testBuffer = await testAudioGeneration(mediaFormat.sampleRate)
              await streamAudioToCallRealtime({
                ws,
                streamId,
                pcmBuffer: testBuffer,
                sampleRate: mediaFormat.sampleRate,
                channels: mediaFormat.channels,
              })
              
              // Step 2: Wait and send greeting
              console.log("2️⃣ Waiting 3 seconds before greeting...")
              setTimeout(async () => {
                const greeting = "Hello! This is your AI assistant. Can you hear this message?"
                console.log(`3️⃣ Sending greeting: "${greeting}"`)
                await synthesizeAndStreamAudio({
                  text: greeting,
                  ws,
                  streamId,
                  sampleRate: mediaFormat.sampleRate,
                  channels: mediaFormat.channels,
                })
                isProcessing = false
              }, 3000)
              
            } catch (err) {
              console.error("[DEBUG] ❌ Error in test sequence:", err.message)
              isProcessing = false
            }
          }
        }, 2000)
        break

      case "media": {
        incomingMediaCount++
        const b64 = data?.media?.payload
        
        if (incomingMediaCount <= 5) {
          console.log(`\n🎵 [MEDIA-${incomingMediaCount}] Received media packet`)
          if (b64) {
            const buffer = Buffer.from(b64, "base64")
            console.log(`[MEDIA-${incomingMediaCount}] Payload size: ${buffer.length} bytes`)
            logAudioSample(`MEDIA-${incomingMediaCount}`, buffer, 16)
          } else {
            console.log(`[MEDIA-${incomingMediaCount}] No payload in media packet`)
          }
        } else if (incomingMediaCount === 6) {
          console.log(`[MEDIA] ... (suppressing further media logs, received ${incomingMediaCount}+ packets)`)
        }
        
        if (b64 && dgWs && dgWs.readyState === WebSocket.OPEN && callAnswered) {
          try {
            dgWs.send(Buffer.from(b64, "base64"))
          } catch (err) {
            console.error("[STT] ❌ Error sending audio to Deepgram:", err.message)
          }
        }
        break
      }
      
      case "dtmf":
        console.log(`📱 [SANPBX] DTMF: ${data.digit}`)
        logFullData("DTMF", data)
        
        // Enhanced DTMF testing
        if (!isProcessing) {
          isProcessing = true
          console.log(`[DEBUG] 🔊 DTMF ${data.digit} pressed - sending test audio`)
          
          const testBuffer = await testAudioGeneration(mediaFormat.sampleRate)
          await streamAudioToCallRealtime({
            ws,
            streamId,
            pcmBuffer: testBuffer,
            sampleRate: mediaFormat.sampleRate,
            channels: mediaFormat.channels,
          })
          isProcessing = false
        }
        break
        
      case "stop":
        console.log("🛑 [SANPBX] Call stopped")
        logFullData("STOP", data)
        
        callAnswered = false
        isProcessing = false
        incomingMediaCount = 0
        
        if (dgWs) {
          try { dgWs.close() } catch {}
        }
        try { ws.close() } catch {}
        break
        
      default:
        console.log(`❓ [SANPBX] Unknown event: ${data.event}`)
        logFullData("UNKNOWN", data)
        break
    }
  })
  
  ws.on("error", (error) => {
    console.error("[SANPBX] ❌ WebSocket error:", error.message)
  })
  
  ws.on("close", (code, reason) => {
    console.log(`[SANPBX] 🔌 WebSocket connection closed: ${code} - ${reason}`)
    callAnswered = false
    isProcessing = false
    incomingMediaCount = 0
    
    if (dgWs) {
      try { dgWs.close() } catch {}
    }
  })
}

module.exports = { setupSanPbxWebSocketServer }