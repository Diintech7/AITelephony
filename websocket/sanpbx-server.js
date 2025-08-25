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
    
    // Check if it's valid base64
    const buffer = Buffer.from(base64String, 'base64')
    if (buffer.length === 0) {
      console.log(`[${label}] ❌ Empty buffer from base64`)
      return false
    }
    
    // Check if buffer length is even (for 16-bit PCM)
    if (buffer.length % 2 !== 0) {
      console.log(`[${label}] ⚠️  Odd buffer length: ${buffer.length} bytes`)
    }
    
    console.log(`[${label}] ✅ Valid base64: ${base64String.length} chars -> ${buffer.length} bytes`)
    
    // Sample first few values
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

// -------- CRITICAL: Enhanced Base64 Audio Streaming --------
async function streamAudioToCallRealtime({ ws, streamId, pcmBuffer, sampleRate, channels }) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    console.error("[STREAM-ERROR] ❌ WebSocket not open")
    return 0
  }

  if (!streamId) {
    console.error("[STREAM-ERROR] ❌ No streamId provided")
    return 0
  }

  if (!pcmBuffer || pcmBuffer.length === 0) {
    console.error("[STREAM-ERROR] ❌ Empty PCM buffer")
    return 0
  }

  const chunkBytes = bytesPer10ms(sampleRate, channels)
  const totalChunks = Math.ceil(pcmBuffer.length / chunkBytes)
  let sentCount = 0
  
  console.log(`\n[STREAM-START] ${ts()}`)
  console.log(`  📊 Total chunks: ${totalChunks}`)
  console.log(`  📦 Chunk size: ${chunkBytes} bytes`)
  console.log(`  🎵 Sample rate: ${sampleRate}Hz`)
  console.log(`  🔊 Channels: ${channels}`)
  console.log(`  💾 Buffer size: ${pcmBuffer.length} bytes`)
  console.log(`  ⏱️  Expected duration: ${(pcmBuffer.length / (sampleRate * 2 * channels)).toFixed(2)}s`)
  
  logAudioSample("STREAM-INPUT", pcmBuffer, 32)

  for (let pos = 0; pos < pcmBuffer.length && ws.readyState === WebSocket.OPEN; pos += chunkBytes) {
    const slice = pcmBuffer.slice(pos, pos + chunkBytes)
    
    // Ensure chunk is exactly the right size (pad with zeros if needed)
    const paddedSlice = slice.length < chunkBytes ? 
      Buffer.concat([slice, Buffer.alloc(chunkBytes - slice.length, 0)]) : 
      slice

    // CRITICAL: Convert to base64 and validate
    const base64Payload = paddedSlice.toString("base64")
    
    if (!validateBase64Audio(base64Payload, `CHUNK-${sentCount}`)) {
      console.error(`[STREAM-ERROR] ❌ Invalid base64 for chunk ${sentCount}`)
      continue
    }
    
    const mediaEvent = {
      event: "media",
      streamId: streamId,
      media: { 
        payload: base64Payload
      }
    }
    
    // Enhanced logging for first few chunks
    if (sentCount < 5) {
      console.log(`\n[CHUNK-${sentCount}] 📦 Processing chunk`)
      console.log(`  Raw size: ${slice.length}/${chunkBytes} bytes`)
      console.log(`  Padded size: ${paddedSlice.length} bytes`)
      console.log(`  Base64 length: ${base64Payload.length} characters`)
      console.log(`  StreamId: ${streamId}`)
      
      logAudioSample(`CHUNK-${sentCount}`, paddedSlice, 16)
      
      // Log the exact JSON being sent
      console.log(`[CHUNK-${sentCount}] 📤 Media event JSON:`)
      console.log(JSON.stringify(mediaEvent, null, 2))
      
      // Verify the base64 can be decoded back
      const testDecode = Buffer.from(base64Payload, 'base64')
      console.log(`[CHUNK-${sentCount}] 🔍 Base64 decode test: ${testDecode.length} bytes`)
    }
    
    try {
      const jsonString = JSON.stringify(mediaEvent)
      console.log(`[CHUNK-${sentCount}] 📡 Sending ${jsonString.length} bytes to WebSocket`)
      
      ws.send(jsonString)
      sentCount++
      
      console.log(`[CHUNK-${sentCount-1}] ✅ Sent successfully`)
      
    } catch (err) {
      console.error(`[STREAM-ERROR] ❌ Failed to send chunk ${sentCount}: ${err.message}`)
      console.error(`[STREAM-ERROR] WebSocket state: ${ws.readyState}`)
      break
    }
    
    // Progress logging
    if (sentCount % 20 === 0 || sentCount === totalChunks) {
      console.log(`[STREAM-PROGRESS] ${ts()} - 📊 Sent ${sentCount}/${totalChunks} chunks`)
    }
    
    // Critical timing: 10ms delay between chunks
    if (sentCount < totalChunks) {
      await wait(10)
    }
  }
  
  console.log(`[STREAM-COMPLETE] ${ts()} - 🎯 Final count: ${sentCount}/${totalChunks} chunks sent`)
  
  // Final validation
  if (sentCount === totalChunks) {
    console.log(`[STREAM-COMPLETE] ✅ All chunks sent successfully`)
  } else {
    console.log(`[STREAM-COMPLETE] ⚠️  Incomplete: ${sentCount}/${totalChunks} chunks sent`)
  }
  
  return sentCount
}

// -------- Enhanced Test Audio Generation --------
async function testAudioGeneration(sampleRate, duration = 4.0) {
  console.log(`\n[TEST-AUDIO] 🎵 Generating test tone for ${sampleRate}Hz`)
  
  const frequency = 800 // 800Hz tone - easily audible
  const samples = Math.floor(sampleRate * duration)
  const buffer = Buffer.alloc(samples * 2)
  
  console.log(`[TEST-AUDIO] 📊 Generating ${samples} samples for ${duration}s`)
  
  for (let i = 0; i < samples; i++) {
    // Generate a clear sine wave with good amplitude
    const amplitude = 16000 // Strong signal
    const sample = Math.sin(2 * Math.PI * frequency * i / sampleRate) * amplitude
    
    // Add envelope to prevent clicks (fade in/out)
    const fadeLength = Math.floor(sampleRate * 0.1) // 100ms fade
    let envelope = 1.0
    
    if (i < fadeLength) {
      envelope = i / fadeLength
    } else if (i > samples - fadeLength) {
      envelope = (samples - i) / fadeLength
    }
    
    const finalSample = Math.round(sample * envelope)
    buffer.writeInt16LE(finalSample, i * 2)
  }
  
  console.log(`[TEST-AUDIO] ✅ Generated ${buffer.length} bytes (${duration}s @ ${frequency}Hz)`)
  logAudioSample("TEST-AUDIO", buffer, 32)
  
  // Validate the test audio
  const testBase64 = buffer.toString('base64')
  validateBase64Audio(testBase64, "TEST-AUDIO-BASE64")
  
  // Save test audio for debugging
  try {
    const filename = `/tmp/debug_test_${sampleRate}hz_${Date.now()}.pcm`
    fs.writeFileSync(filename, buffer)
    console.log(`[TEST-AUDIO] 💾 Saved test audio to ${filename}`)
  } catch (err) {
    console.log(`[TEST-AUDIO] ⚠️  Could not save test file: ${err.message}`)
  }
  
  return buffer
}

// -------- Enhanced ElevenLabs TTS --------
async function synthesizeAndStreamAudio({ text, ws, streamId, sampleRate, channels }) {
  console.log(`\n[TTS-START] ${ts()}`)
  console.log(`  📝 Text: "${text}"`)
  console.log(`  🎵 Target: ${sampleRate}Hz, ${channels} channels`)

  if (!text || text.trim().length === 0) {
    console.error("[TTS] ❌ Empty text provided")
    return
  }

  try {
    // Choose appropriate format based on sample rate
    let outputFormat, elevenlabsRate
    if (sampleRate <= 8000) {
      outputFormat = "pcm_16000"
      elevenlabsRate = 16000
    } else if (sampleRate <= 22050) {
      outputFormat = "pcm_22050"
      elevenlabsRate = 22050
    } else {
      outputFormat = "pcm_44100"
      elevenlabsRate = 44100
    }
    
    console.log(`[TTS] 🎧 Using ElevenLabs format: ${outputFormat} (${elevenlabsRate}Hz)`)
    
    const requestBody = {
      text: text.trim(),
      output_format: outputFormat,
      voice_settings: {
        stability: 0.7,
        similarity_boost: 0.8,
        style: 0.2,
        use_speaker_boost: true
      }
    }
    
    console.log("[TTS] 📤 Request body:", JSON.stringify(requestBody, null, 2))
    
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
    
    console.log(`[TTS] 📥 Response status: ${resp.status}`)
    console.log(`[TTS] 📋 Response headers:`, Object.fromEntries(resp.headers))
    
    if (!resp.ok) {
      const errorText = await resp.text()
      throw new Error(`ElevenLabs API ${resp.status}: ${errorText}`)
    }

    let audioBuf = Buffer.from(await resp.arrayBuffer())
    console.log(`[TTS] 📦 ElevenLabs returned ${audioBuf.length} bytes`)
    
    if (audioBuf.length === 0) {
      console.error("[TTS] ❌ Empty audio buffer from ElevenLabs")
      return
    }

    logAudioSample("TTS-ORIGINAL", audioBuf, 32)

    // Validate audio quality
    const maxSample = Math.max(...Array.from({length: Math.min(100, audioBuf.length/2)}, (_, i) => Math.abs(audioBuf.readInt16LE(i * 2))))
    console.log(`[TTS] 🔊 Max sample value: ${maxSample} (should be > 1000 for good audio)`)

    if (maxSample < 100) {
      console.warn("[TTS] ⚠️  Audio seems very quiet or silent")
    }

    // Save original for debugging
    try {
      const filename = `/tmp/debug_tts_original_${elevenlabsRate}hz_${Date.now()}.pcm`
      fs.writeFileSync(filename, audioBuf)
      console.log(`[TTS] 💾 Saved original to ${filename}`)
    } catch (err) {
      console.log(`[TTS] ⚠️  Could not save debug file: ${err.message}`)
    }

    // Resample if needed
    if (sampleRate !== elevenlabsRate) {
      console.log(`[TTS] 🔄 Resampling from ${elevenlabsRate}Hz to ${sampleRate}Hz`)
      audioBuf = resampleAudio(audioBuf, elevenlabsRate, sampleRate)
      
      try {
        const filename = `/tmp/debug_tts_resampled_${sampleRate}hz_${Date.now()}.pcm`
        fs.writeFileSync(filename, audioBuf)
        console.log(`[TTS] 💾 Saved resampled to ${filename}`)
      } catch (err) {
        console.log(`[TTS] ⚠️  Could not save resampled file: ${err.message}`)
      }
    }

    // Validate final buffer before streaming
    const finalBase64 = audioBuf.toString('base64')
    if (!validateBase64Audio(finalBase64, "TTS-FINAL")) {
      console.error("[TTS] ❌ Final audio buffer failed base64 validation")
      return
    }

    console.log(`[TTS] 🚀 Starting audio stream...`)
    
    const sentChunks = await streamAudioToCallRealtime({
      ws,
      streamId,
      pcmBuffer: audioBuf,
      sampleRate,
      channels,
    })

    console.log(`[TTS] ✅ Successfully sent ${sentChunks} audio chunks`)
    
    if (sentChunks === 0) {
      console.error("[TTS] ❌ No chunks were sent!")
    }
    
  } catch (err) {
    console.error(`[TTS] ❌ ElevenLabs error: ${err.message}`)
    console.error(`[TTS] Stack trace:`, err.stack)
  }
}

// -------- OpenAI GPT --------
async function getAiResponse(prompt) {
  try {
    console.log(`[AI] 🤖 Processing prompt: "${prompt}"`)
    
    const resp = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { 
            role: "system", 
            content: "You are a helpful voice assistant for phone calls. Keep responses short and natural, under 25 words. Be friendly and conversational."
          },
          { role: "user", content: prompt },
        ],
        max_tokens: 60,
        temperature: 0.7
      }),
    })
    
    if (!resp.ok) {
      const errorText = await resp.text()
      throw new Error(`OpenAI ${resp.status}: ${errorText}`)
    }
    
    const data = await resp.json()
    const response = data.choices[0].message.content.trim()
    console.log(`[AI] 💬 Generated response: "${response}"`)
    return response
    
  } catch (err) {
    console.error(`[AI] ❌ Error: ${err.message}`)
    return "I understand. How can I help you?"
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

  const connectDeepgram = (sampleRate, encoding) => {
    const dgEncoding = 'linear16'
    console.log(`\n[STT-SETUP] 🎤 Connecting to Deepgram`)
    console.log(`  Encoding: ${dgEncoding} (from PBX: ${encoding})`)
    console.log(`  Sample rate: ${sampleRate}Hz`)
    
    if (dgWs) {
      try { dgWs.close() } catch {}
    }
    
    dgWs = new DGWebSocket(
      `wss://api.deepgram.com/v1/listen?encoding=${dgEncoding}&sample_rate=${sampleRate}&channels=1&model=nova-2&interim_results=false&smart_format=true&endpointing=300&utterance_end_ms=1000&punctuate=true`,
      { headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` } }
    )
    
    dgWs.on("open", () => {
      console.log("[STT] ✅ Connected to Deepgram successfully")
    })
    
    dgWs.on("message", async (msg) => {
      try {
        const data = JSON.parse(msg.toString())
        
        // Only log non-empty responses
        if (data.channel?.alternatives?.[0]?.transcript) {
          console.log("[STT] 📥 Deepgram response:", JSON.stringify(data, null, 2))
        }
        
        const transcript = data.channel?.alternatives?.[0]?.transcript?.trim()
        
        if (transcript && transcript.length > 0 && !isProcessing) {
          console.log(`\n🎤 [STT] Transcript: "${transcript}"`)
          isProcessing = true
          
          try {
            const lowerTranscript = transcript.toLowerCase()
            
            if (lowerTranscript.includes('test') || lowerTranscript.includes('tone')) {
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
            // Longer delay to prevent overlap
            setTimeout(() => {
              isProcessing = false
            }, 2000)
          }
        }
      } catch (error) {
        console.error("[STT] ❌ Error processing message:", error.message)
        isProcessing = false
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
      return
    }
    
    console.log(`\n📨 [SANPBX] Received event: ${data.event}`)
    
    switch (data.event) {
      case "connected":
        console.log("🔗 [SANPBX] Connected event")
        logFullData("CONNECTED", data)
        break
        
      case "start":
        console.log("▶️ [SANPBX] Call started")
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
        
        // Send ACK immediately
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
          console.log("[SANPBX] ✅ Sent answer ACK")
        } catch (err) {
          console.error("[SANPBX] ❌ Failed to send ACK:", err.message)
        }

        // Start greeting sequence
        setTimeout(async () => {
          if (!isProcessing && callAnswered && streamId) {
            isProcessing = true
            
            try {
              console.log("\n🔊 [DEBUG] Starting audio sequence...")
              
              // Send a strong test tone first
              console.log("1️⃣ Sending test tone...")
              const testBuffer = await testAudioGeneration(mediaFormat.sampleRate, 3.0)
              const testSent = await streamAudioToCallRealtime({
                ws,
                streamId,
                pcmBuffer: testBuffer,
                sampleRate: mediaFormat.sampleRate,
                channels: mediaFormat.channels,
              })
              
              console.log(`[DEBUG] ✅ Test tone: ${testSent} chunks sent`)
              
              // Wait then send greeting
              setTimeout(async () => {
                console.log("2️⃣ Sending greeting...")
                const greeting = "Hello! This is your AI assistant. I'm sending audio now. Can you hear me clearly?"
                await synthesizeAndStreamAudio({
                  text: greeting,
                  ws,
                  streamId,
                  sampleRate: mediaFormat.sampleRate,
                  channels: mediaFormat.channels,
                })
                
                setTimeout(() => {
                  isProcessing = false
                  console.log("✅ [DEBUG] Audio sequence complete")
                }, 2000)
              }, 3000)
              
            } catch (err) {
              console.error("[DEBUG] ❌ Error in audio sequence:", err.message)
              isProcessing = false
            }
          }
        }, 2000)
        break

      case "media": {
        incomingMediaCount++
        const b64 = data?.media?.payload
        
        if (incomingMediaCount <= 3) {
          console.log(`\n🎵 [MEDIA-${incomingMediaCount}] Received media packet`)
          if (b64 && b64.length > 0) {
            const buffer = Buffer.from(b64, "base64")
            console.log(`[MEDIA-${incomingMediaCount}] ✅ Payload: ${b64.length} chars -> ${buffer.length} bytes`)
            logAudioSample(`MEDIA-${incomingMediaCount}`, buffer, 16)
            
            if (!hasReceivedAudio) {
              hasReceivedAudio = true
              console.log("🎉 [MEDIA] First audio received!")
            }
          } else {
            console.log(`[MEDIA-${incomingMediaCount}] ❌ No payload`)
          }
        } else if (incomingMediaCount === 4) {
          console.log(`[MEDIA] ... (received ${incomingMediaCount}+ packets)`)
        }
        
        // Forward to Deepgram if we have audio
        if (b64 && b64.length > 0 && dgWs && dgWs.readyState === WebSocket.OPEN && callAnswered) {
          try {
            const audioBuffer = Buffer.from(b64, "base64")
            if (audioBuffer.length > 0) {
              dgWs.send(audioBuffer)
            }
          } catch (err) {
            console.error("[STT] ❌ Error forwarding to Deepgram:", err.message)
          }
        }
        break
      }
      
      case "dtmf":
        console.log(`📱 [SANPBX] DTMF: ${data.digit}`)
        
        if (!isProcessing) {
          isProcessing = true
          console.log(`[DEBUG] 🔊 DTMF ${data.digit} - immediate audio response`)
          
          setTimeout(async () => {
            try {
              // Send test tone
              const testBuffer = await testAudioGeneration(mediaFormat.sampleRate, 2.0)
              await streamAudioToCallRealtime({
                ws,
                streamId,
                pcmBuffer: testBuffer,
                sampleRate: mediaFormat.sampleRate,
                channels: mediaFormat.channels,
              })
              
              // Send voice response
              const dtmfResponse = `You pressed ${data.digit}. Audio test complete.`
              await synthesizeAndStreamAudio({
                text: dtmfResponse,
                ws,
                streamId,
                sampleRate: mediaFormat.sampleRate,
                channels: mediaFormat.channels,
              })
            } catch (err) {
              console.error("[DEBUG] ❌ DTMF response error:", err.message)
            } finally {
              setTimeout(() => {
                isProcessing = false
              }, 1000)
            }
          }, 200)
        }
        break
        
      case "stop":
        console.log("🛑 [SANPBX] Call stopped")
        
        callAnswered = false
        isProcessing = false
        incomingMediaCount = 0
        hasReceivedAudio = false
        
        if (dgWs) {
          try { dgWs.close() } catch {}
        }
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
    callAnswered = false
    isProcessing = false
    incomingMediaCount = 0
    hasReceivedAudio = false
    
    if (dgWs) {
      try { dgWs.close() } catch {}
    }
  })
}

module.exports = { setupSanPbxWebSocketServer }