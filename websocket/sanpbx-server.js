const WebSocket = require("ws")
const EventEmitter = require("events")

// AI Service Integrations
const { createClient } = require("@deepgram/sdk")
const OpenAI = require("openai")

class SanPBXWebSocketServer extends EventEmitter {
  constructor() {
    super()
    this.activeCalls = new Map()
    this.deepgramClient = null
    this.openaiClient = null
    this.sarvamConfig = null

    this.initializeAIServices()
  }

  initializeAIServices() {
    // Initialize Deepgram for Speech-to-Text
    if (process.env.DEEPGRAM_API_KEY) {
      this.deepgramClient = createClient(process.env.DEEPGRAM_API_KEY)
      console.log("✅ [SANPBX] Deepgram client initialized")
    } else {
      console.warn("⚠️ [SANPBX] DEEPGRAM_API_KEY not found")
    }

    // Initialize OpenAI for Conversation AI
    if (process.env.OPENAI_API_KEY) {
      this.openaiClient = new OpenAI({
        apiKey: process.env.OPENAI_API_KEY,
      })
      console.log("✅ [SANPBX] OpenAI client initialized")
    } else {
      console.warn("⚠️ [SANPBX] OPENAI_API_KEY not found")
    }

    // Initialize Sarvam configuration
    if (process.env.SARVAM_API_KEY) {
      this.sarvamConfig = {
        apiKey: process.env.SARVAM_API_KEY,
        baseUrl: "https://api.sarvam.ai/text-to-speech",
      }
      console.log("✅ [SANPBX] Sarvam configuration initialized")
    } else {
      console.warn("⚠️ [SANPBX] SARVAM_API_KEY not found")
    }
  }

  setupWebSocketServer(wss) {
    wss.on("connection", (ws, req) => {
      console.log("🔗 [SANPBX] New WebSocket connection established")

      // Initialize connection state
      ws.callState = {
        callId: null,
        streamId: null,
        channelId: null,
        isActive: false,
        conversationHistory: [],
        deepgramConnection: null,
        audioBuffer: [],
        lastActivity: Date.now(),
      }

      // Handle incoming messages
      ws.on("message", async (data) => {
        try {
          await this.handleMessage(ws, data)
        } catch (error) {
          console.error("❌ [SANPBX] Error handling message:", error)
          this.sendError(ws, "Message processing failed", error.message)
        }
      })

      // Handle connection close
      ws.on("close", (code, reason) => {
        console.log(`🔗 [SANPBX] Connection closed: ${code} - ${reason}`)
        this.cleanupConnection(ws)
      })

      // Handle connection errors
      ws.on("error", (error) => {
        console.error("❌ [SANPBX] WebSocket error:", error)
        this.cleanupConnection(ws)
      })

      // Send initial connection acknowledgment
      this.sendMessage(ws, {
        event: "connection_ready",
        message: "SanPBX WebSocket server ready",
        timestamp: new Date().toISOString(),
        capabilities: {
          deepgram: !!this.deepgramClient,
          openai: !!this.openaiClient,
          sarvam: !!this.sarvamConfig,
        },
      })
    })

    console.log("✅ [SANPBX] WebSocket server setup complete")
  }

  async handleMessage(ws, data) {
    let message

    try {
      // Handle both JSON and binary data
      if (Buffer.isBuffer(data)) {
        // Binary audio data - convert to base64
        const base64Audio = data.toString("base64")
        await this.processAudioData(ws, base64Audio)
        return
      }

      message = JSON.parse(data.toString())
    } catch (error) {
      console.error("❌ [SANPBX] Invalid message format:", error)
      return
    }

    const { event } = message
    console.log(`📨 [SANPBX] Received event: ${event}`)

    switch (event) {
      case "connected":
        await this.handleConnected(ws, message)
        break
      case "start":
        await this.handleStart(ws, message)
        break
      case "answer":
        await this.handleAnswer(ws, message)
        break
      case "media":
        await this.handleMedia(ws, message)
        break
      case "dtmf":
        await this.handleDTMF(ws, message)
        break
      case "stop":
        await this.handleStop(ws, message)
        break
      case "transfer-call":
        await this.handleTransferCall(ws, message)
        break
      case "hangup-call":
        await this.handleHangupCall(ws, message)
        break
      default:
        console.warn(`⚠️ [SANPBX] Unknown event: ${event}`)
    }
  }

  async handleConnected(ws, message) {
    const { callId, streamId, channelId, callerId, callDirection } = message

    ws.callState.callId = callId
    ws.callState.streamId = streamId
    ws.callState.channelId = channelId
    ws.callState.isActive = true
    ws.callState.lastActivity = Date.now()

    // Store active call
    this.activeCalls.set(streamId, {
      ws,
      callId,
      channelId,
      callerId,
      callDirection,
      startTime: new Date(),
    })

    console.log(`📞 [SANPBX] Call connected - ID: ${callId}, Stream: ${streamId}`)

    // Initialize conversation context
    ws.callState.conversationHistory = [
      {
        role: "system",
        content: `You are an AI assistant handling a ${callDirection.toLowerCase()} call. Be helpful, concise, and professional. Keep responses brief for voice conversation.`,
      },
    ]

    this.sendMessage(ws, {
      event: "connected_ack",
      callId,
      streamId,
      message: "Call connection acknowledged",
      timestamp: new Date().toISOString(),
    })
  }

  async handleStart(ws, message) {
    const { mediaFormat } = message
    console.log(`🎵 [SANPBX] Media stream started:`, mediaFormat)

    // Initialize Deepgram connection for real-time transcription
    if (this.deepgramClient) {
      try {
        const deepgramLive = this.deepgramClient.listen.live({
          model: "nova-2",
          language: "en-US",
          smart_format: true,
          interim_results: true,
          endpointing: 300,
          utterance_end_ms: 1000,
        })

        // Handle transcription results
        deepgramLive.on("Results", async (data) => {
          const transcript = data.channel?.alternatives?.[0]?.transcript
          if (transcript && data.is_final) {
            console.log(`🎤 [SANPBX] Transcript: ${transcript}`)
            await this.processTranscript(ws, transcript)
          }
        })

        deepgramLive.on("error", (error) => {
          console.error("❌ [SANPBX] Deepgram error:", error)
        })

        ws.callState.deepgramConnection = deepgramLive
        console.log("✅ [SANPBX] Deepgram live connection established")
      } catch (error) {
        console.error("❌ [SANPBX] Failed to initialize Deepgram:", error)
      }
    }

    this.sendMessage(ws, {
      event: "start_ack",
      callId: ws.callState.callId,
      streamId: ws.callState.streamId,
      message: "Media stream ready",
      timestamp: new Date().toISOString(),
    })
  }

  async handleAnswer(ws, message) {
    console.log(`📞 [SANPBX] Call answered - Stream: ${ws.callState.streamId}`)

    // Send welcome message
    const welcomeMessage = "Hello! I'm your AI assistant. How can I help you today?"
    await this.generateAndSendAudio(ws, welcomeMessage)

    this.sendMessage(ws, {
      event: "answer_ack",
      callId: ws.callState.callId,
      streamId: ws.callState.streamId,
      message: "Call answered, AI assistant ready",
      timestamp: new Date().toISOString(),
    })
  }

  async handleMedia(ws, message) {
    const { media } = message
    if (!media || !media.payload) return

    // Process base64 audio data
    await this.processAudioData(ws, media.payload)
  }

  async processAudioData(ws, base64Audio) {
    if (!ws.callState.deepgramConnection) return

    try {
      // Convert base64 to buffer and send to Deepgram
      const audioBuffer = Buffer.from(base64Audio, "base64")
      ws.callState.deepgramConnection.send(audioBuffer)
      ws.callState.lastActivity = Date.now()
    } catch (error) {
      console.error("❌ [SANPBX] Error processing audio:", error)
    }
  }

  async processTranscript(ws, transcript) {
    if (!transcript.trim()) return

    console.log(`💬 [SANPBX] Processing transcript: ${transcript}`)

    // Add user message to conversation history
    ws.callState.conversationHistory.push({
      role: "user",
      content: transcript,
    })

    // Generate AI response using OpenAI
    if (this.openaiClient) {
      try {
        const completion = await this.openaiClient.chat.completions.create({
          model: "gpt-4o-mini",
          messages: ws.callState.conversationHistory,
          max_tokens: 150,
          temperature: 0.7,
        })

        const aiResponse = completion.choices[0]?.message?.content
        if (aiResponse) {
          console.log(`🤖 [SANPBX] AI Response: ${aiResponse}`)

          // Add AI response to conversation history
          ws.callState.conversationHistory.push({
            role: "assistant",
            content: aiResponse,
          })

          // Generate and send audio response
          await this.generateAndSendAudio(ws, aiResponse)
        }
      } catch (error) {
        console.error("❌ [SANPBX] OpenAI error:", error)
        await this.generateAndSendAudio(ws, "I'm sorry, I'm having trouble processing that right now.")
      }
    }
  }

  async generateAndSendAudio(ws, text) {
    if (!this.sarvamConfig) {
      console.warn("⚠️ [SANPBX] Sarvam not configured, skipping TTS")
      return
    }

    try {
      console.log(`🔊 [SANPBX] Generating audio for: ${text}`)

      const response = await fetch(this.sarvamConfig.baseUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "API-Subscription-Key": this.sarvamConfig.apiKey,
        },
        body: JSON.stringify({
          inputs: [text],
          target_language_code: "en-IN",
          speaker: "meera",
          pitch: 0,
          pace: 1.0,
          loudness: 1.0,
          speech_sample_rate: 44100,
          enable_preprocessing: true,
          model: "bulbul:v1",
        }),
      })

      if (!response.ok) {
        throw new Error(`Sarvam API error: ${response.status}`)
      }

      const audioData = await response.arrayBuffer()
      const base64Audio = Buffer.from(audioData).toString("base64")

      // Send audio back to SanPBX
      this.sendMessage(ws, {
        event: "media",
        streamId: ws.callState.streamId,
        media: {
          contentType: "audio/wav",
          payload: base64Audio,
        },
        timestamp: new Date().toISOString(),
      })

      console.log("✅ [SANPBX] Audio sent successfully")
    } catch (error) {
      console.error("❌ [SANPBX] TTS generation failed:", error)
    }
  }

  async handleDTMF(ws, message) {
    const { digit, dtmfDurationMs } = message
    console.log(`📱 [SANPBX] DTMF received: ${digit} (${dtmfDurationMs}ms)`)

    // Handle DTMF commands
    switch (digit) {
      case "0":
        await this.generateAndSendAudio(ws, "You pressed zero. How can I assist you?")
        break
      case "1":
        await this.generateAndSendAudio(ws, "You pressed one. Connecting you to support.")
        break
      case "*":
        await this.generateAndSendAudio(ws, "You pressed star. Returning to main menu.")
        break
      case "#":
        await this.generateAndSendAudio(ws, "You pressed hash. Thank you for calling.")
        break
      default:
        await this.generateAndSendAudio(ws, `You pressed ${digit}.`)
    }

    this.sendMessage(ws, {
      event: "dtmf_ack",
      digit,
      callId: ws.callState.callId,
      streamId: ws.callState.streamId,
      timestamp: new Date().toISOString(),
    })
  }

  async handleTransferCall(ws, message) {
    const { transferTo, streamId } = message
    console.log(`📞 [SANPBX] Transfer call request - Stream: ${streamId}, Transfer to: ${transferTo}`)

    // Acknowledge transfer request
    this.sendMessage(ws, {
      event: "transfer-call-response",
      status: true,
      message: "Transfer request acknowledged",
      data: { transferTo },
      status_code: 200,
      streamId,
      callId: ws.callState.callId,
      timestamp: new Date().toISOString(),
    })

    // Generate farewell message
    await this.generateAndSendAudio(ws, "Transferring your call now. Please hold.")
  }

  async handleHangupCall(ws, message) {
    const { streamId } = message
    console.log(`📞 [SANPBX] Hangup call request - Stream: ${streamId}`)

    // Acknowledge hangup request
    this.sendMessage(ws, {
      event: "hangup-call-response",
      status: true,
      message: "Call hangup acknowledged",
      data: {},
      status_code: 200,
      streamId,
      callId: ws.callState.callId,
      timestamp: new Date().toISOString(),
    })

    // Cleanup connection
    this.cleanupConnection(ws)
  }

  async handleStop(ws, message) {
    const { disconnectedBy } = message
    console.log(`📞 [SANPBX] Call stopped - Disconnected by: ${disconnectedBy}`)

    this.cleanupConnection(ws)

    this.sendMessage(ws, {
      event: "stop_ack",
      callId: ws.callState.callId,
      streamId: ws.callState.streamId,
      message: "Call termination acknowledged",
      timestamp: new Date().toISOString(),
    })
  }

  cleanupConnection(ws) {
    if (ws.callState) {
      // Close Deepgram connection
      if (ws.callState.deepgramConnection) {
        try {
          ws.callState.deepgramConnection.finish()
        } catch (error) {
          console.error("❌ [SANPBX] Error closing Deepgram connection:", error)
        }
      }

      // Remove from active calls
      if (ws.callState.streamId) {
        this.activeCalls.delete(ws.callState.streamId)
      }

      // Reset call state
      ws.callState.isActive = false
    }

    console.log("🧹 [SANPBX] Connection cleanup completed")
  }

  sendMessage(ws, message) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message))
    }
  }

  sendError(ws, error, details) {
    this.sendMessage(ws, {
      event: "error",
      error,
      details,
      timestamp: new Date().toISOString(),
    })
  }

  // Get active calls statistics
  getStats() {
    return {
      activeCalls: this.activeCalls.size,
      calls: Array.from(this.activeCalls.values()).map((call) => ({
        callId: call.callId,
        channelId: call.channelId,
        callerId: call.callerId,
        callDirection: call.callDirection,
        duration: Date.now() - call.startTime.getTime(),
      })),
    }
  }
}

// Export setup function
function setupSanPbxWebSocketServer(wss) {
  const sanpbxServer = new SanPBXWebSocketServer()
  sanpbxServer.setupWebSocketServer(wss)
  return sanpbxServer
}

module.exports = {
  setupSanPbxWebSocketServer,
  SanPBXWebSocketServer,
}
