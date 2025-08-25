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

// -------- Audio Format Conversion --------
function resampleAudio(buffer, fromRate, toRate) {
  if (fromRate === toRate) return buffer

  const ratio = fromRate / toRate
  const newLength = Math.floor(buffer.length / 2 / ratio) * 2 // Ensure even number (16-bit samples)
  const result = Buffer.alloc(newLength)

  for (let i = 0; i < newLength; i += 2) {
    const sourceIndex = Math.floor((i / 2) * ratio) * 2
    if (sourceIndex < buffer.length - 1) {
      result[i] = buffer[sourceIndex]
      result[i + 1] = buffer[sourceIndex + 1]
    }
  }

  console.log(`[AUDIO] Resampled from ${fromRate}Hz to ${toRate}Hz: ${buffer.length} -> ${newLength} bytes`)
  return result
}

// -------- PCM utils --------
function bytesPer10ms(rate, channels = 1) {
  return Math.round(rate / 100) * 2 * channels
}

// -------- STREAM audio to PBX --------
async function streamAudioToCallRealtime({ ws, streamId, pcmBuffer, sampleRate, channels }) {
  const chunkBytes = bytesPer10ms(sampleRate, channels)
  const totalChunks = Math.ceil(pcmBuffer.length / chunkBytes)
  
  console.log(`[STREAM-START] ${ts()} - total=${totalChunks}, chunkBytes=${chunkBytes}, sampleRate=${sampleRate}`)
  console.log(`[STREAM-DEBUG] Buffer size: ${pcmBuffer.length} bytes, Expected duration: ${(pcmBuffer.length / (sampleRate * 2)).toFixed(2)}s`)

  for (let pos = 0, sent = 0; pos < pcmBuffer.length && ws.readyState === WebSocket.OPEN; pos += chunkBytes) {
    const slice = pcmBuffer.slice(pos, pos + chunkBytes)
    const padded = slice.length < chunkBytes ? Buffer.concat([slice, Buffer.alloc(chunkBytes - slice.length)]) : slice
    
    // Debug first few chunks
    if (sent < 3) {
      console.log(`[STREAM-DEBUG] Chunk ${sent}: ${slice.length} bytes, first 8 bytes: [${Array.from(slice.slice(0, 8)).join(', ')}]`)
    }
    
    const mediaEvent = {
      event: "answer",
      streamId,
      media: { 
        payload: padded.toString("base64")
      }
    }
    
    ws.send(JSON.stringify(mediaEvent))
    sent++
    
    if (sent % 20 === 0 || sent === totalChunks) {
      console.log(`[STREAM] ${ts()} - Sent ${sent}/${totalChunks}`)
    }
    
    if (sent < totalChunks) await wait(10)
  }
  console.log(`[STREAM-COMPLETE] ${ts()} - done`)
}

// -------- ElevenLabs TTS --------
async function synthesizeAndStreamAudio({ text, ws, streamId, sampleRate, channels }) {
  console.log(`[TTS-START] ${ts()} - ElevenLabs: "${text}" (target: ${sampleRate}Hz)`)

  try {
    // Always request 44100 from ElevenLabs, then resample if needed
    const outputFormat = "pcm_44100"
    
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
          voice_settings: {
            stability: 0.5,
            similarity_boost: 0.8,
            style: 0.0,
            use_speaker_boost: true
          }
        }),
      }
    )
    
    if (!resp.ok) throw new Error(`ElevenLabs API ${resp.status}: ${await resp.text()}`)

    let audioBuf = Buffer.from(await resp.arrayBuffer())
    console.log(`[TTS] ElevenLabs returned ${audioBuf.length} bytes PCM (${outputFormat})`)
    
    // Check if audio buffer has content
    if (audioBuf.length === 0) {
      console.error("[TTS] ERROR: Empty audio buffer from ElevenLabs")
      return
    }

    // Sample the audio data to check if it's not silent
    const samplePoints = [0, Math.floor(audioBuf.length/4), Math.floor(audioBuf.length/2), Math.floor(audioBuf.length*3/4)]
    const samples = samplePoints.map(i => audioBuf.readInt16LE(i)).filter(s => Math.abs(s) > 100)
    console.log(`[TTS-DEBUG] Audio samples at key points: [${samplePoints.map(i => audioBuf.readInt16LE(i)).join(', ')}]`)
    console.log(`[TTS-DEBUG] Non-silent samples: ${samples.length}/${samplePoints.length}`)

    // Resample if needed
    if (sampleRate !== 44100) {
      console.log(`[TTS] Resampling from 44100Hz to ${sampleRate}Hz`)
      audioBuf = resampleAudio(audioBuf, 44100, sampleRate)
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
  }
}

// Test audio generation function
async function testAudioGeneration(sampleRate) {
  console.log(`[TEST-AUDIO] Generating test tone for ${sampleRate}Hz`)
  
  // Generate a 1-second 440Hz sine wave
  const duration = 1.0 // seconds
  const frequency = 440 // Hz
  const samples = Math.floor(sampleRate * duration)
  const buffer = Buffer.alloc(samples * 2) // 16-bit samples
  
  for (let i = 0; i < samples; i++) {
    const sample = Math.sin(2 * Math.PI * frequency * i / sampleRate) * 16384 // Half volume
    buffer.writeInt16LE(sample, i * 2)
  }
  
  console.log(`[TEST-AUDIO] Generated ${buffer.length} bytes of test audio`)
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
          { role: "system", content: "You are a helpful voice assistant. Keep responses very short and conversational, under 20 words." },
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
  let callAnswered = false
  let isProcessing = false
  let testAudioSent = false

  // connect Deepgram once call starts
  const connectDeepgram = (sampleRate, encoding) => {
    const dgEncoding = 'linear16'
    console.log(`[STT] Connecting to Deepgram with ${dgEncoding}, ${sampleRate}Hz`)
    
    dgWs = new DGWebSocket(
      `wss://api.deepgram.com/v1/listen?encoding=${dgEncoding}&sample_rate=${sampleRate}&channels=1&model=nova-2&interim_results=false&smart_format=true&endpointing=300`,
      { headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` } }
    )
    
    dgWs.on("open", () => {
      console.log("[STT] Connected to Deepgram successfully")
    })
    
    dgWs.on("message", async (msg) => {
      try {
        const data = JSON.parse(msg.toString())
        const transcript = data.channel?.alternatives?.[0]?.transcript
        
        if (transcript && transcript.trim() && !isProcessing) {
          console.log(`[STT] Transcript: "${transcript}"`)
          isProcessing = true
          
          try {
            // Special commands for testing
            if (transcript.toLowerCase().includes('test audio')) {
              console.log("[DEBUG] Test audio command detected")
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
              console.log(`[AI] Response: "${reply}"`)
              
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
        console.error("[STT] Error processing message:", error.message)
      }
    })
    
    dgWs.on("close", (code, reason) => {
      console.log(`[STT] Deepgram closed: ${code} ${reason}`)
    })
    
    dgWs.on("error", (e) => {
      console.error("[STT] Deepgram error:", e.message)
    })
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
        
        if (data.mediaFormat) {
          mediaFormat = {
            encoding: data.mediaFormat.encoding || "LINEAR16",
            sampleRate: data.mediaFormat.sampleRate || 44100,
            channels: data.mediaFormat.channels || 1
          }
        }
        
        console.log("[SANPBX] streamId:", streamId)
        console.log("[SANPBX] Media Format:", mediaFormat)
        
        connectDeepgram(mediaFormat.sampleRate, mediaFormat.encoding)
        break
        
      case "answer":
        console.log("[SANPBX] Call answered")
        callAnswered = true
        
        const { callId, channelId, streamId: ansStreamId } = data
        
        if (ansStreamId) {
          streamId = ansStreamId
        }
        
        // Send ACK
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

        // Send test audio first, then greeting
        setTimeout(async () => {
          if (!isProcessing && callAnswered && !testAudioSent) {
            testAudioSent = true
            isProcessing = true
            
            try {
              console.log("[DEBUG] Sending test tone first...")
              const testBuffer = await testAudioGeneration(mediaFormat.sampleRate)
              await streamAudioToCallRealtime({
                ws,
                streamId,
                pcmBuffer: testBuffer,
                sampleRate: mediaFormat.sampleRate,
                channels: mediaFormat.channels,
              })
              
              // Wait a bit, then send greeting
              setTimeout(async () => {
                const greeting = "Hello! Can you hear me? Say test audio to hear a tone."
                await synthesizeAndStreamAudio({
                  text: greeting,
                  ws,
                  streamId,
                  sampleRate: mediaFormat.sampleRate,
                  channels: mediaFormat.channels,
                })
                isProcessing = false
              }, 2000)
              
            } catch (err) {
              console.error("[DEBUG] Error in test sequence:", err.message)
              isProcessing = false
            }
          }
        }, 1500)
        break

      case "media": {
        const b64 = data?.media?.payload
        if (b64 && dgWs && dgWs.readyState === WebSocket.OPEN && callAnswered) {
          try {
            dgWs.send(Buffer.from(b64, "base64"))
          } catch (err) {
            console.error("[STT] Error sending audio to Deepgram:", err.message)
          }
        }
        break
      }
      
      case "dtmf":
        console.log(`[SANPBX] DTMF: ${data.digit}`)
        // Send test audio on DTMF press
        if (data.digit === '1' && !isProcessing) {
          console.log("[DEBUG] DTMF 1 pressed - sending test audio")
          isProcessing = true
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
        console.log("[SANPBX] Call stopped")
        callAnswered = false
        isProcessing = false
        testAudioSent = false
        
        if (dgWs) {
          try { dgWs.close() } catch {}
        }
        try { ws.close() } catch {}
        break
        
      default:
        console.log(`[SANPBX] Unknown event: ${data.event}`)
        break
    }
  })
  
  ws.on("error", (error) => {
    console.error("[SANPBX] WebSocket error:", error.message)
  })
  
  ws.on("close", () => {
    console.log("[SANPBX] WebSocket connection closed")
    callAnswered = false
    isProcessing = false
    testAudioSent = false
    
    if (dgWs) {
      try { dgWs.close() } catch {}
    }
  })
}

module.exports = { setupSanPbxWebSocketServer }