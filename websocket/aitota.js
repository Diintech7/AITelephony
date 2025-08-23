const WebSocket = require("ws")
require("dotenv").config()
const mongoose = require("mongoose")
const Agent = require("../models/Agent")
const CallLog = require("../models/CallLog")
const fs = require("fs")
const path = require("path")

// Load API keys from environment variables
const API_KEYS = {
  deepgram: process.env.DEEPGRAM_API_KEY,
  sarvam: process.env.SARVAM_API_KEY,
  openai: process.env.OPENAI_API_KEY,
}

// Validate API keys
if (!API_KEYS.deepgram || !API_KEYS.sarvam || !API_KEYS.openai) {
  console.error("❌ Missing required API keys in environment variables")
  process.exit(1)
}

const fetch = globalThis.fetch || require("node-fetch")

// Performance timing helper
const createTimer = (label) => {
  const start = Date.now()
  return {
    start,
    end: () => Date.now() - start,
    checkpoint: (checkpointName) => Date.now() - start,
  }
}

// Simple language detection
const detectLanguage = (text) => {
  if (!text) return "en"
  const hindiPatterns = /[\u0900-\u097F]/
  if (hindiPatterns.test(text)) return "hi"
  return "en"
}

// Utility function to decode base64 extra data
const decodeExtraData = (extraBase64) => {
  try {
    if (!extraBase64) return null
    const decodedString = Buffer.from(extraBase64, "base64").toString("utf-8")
    const fixedString = decodedString
      .replace(/="([^"]*?)"/g, ':"$1"')
      .replace(/=([^",}\s]+)/g, ':"$1"')
      .replace(/,\s*}/g, "}")
      .replace(/,\s*]/g, "]")
    const parsedData = JSON.parse(fixedString)
    return parsedData
  } catch (error) {
    return null
  }
}

// Utility function to decode czdata (base64 JSON)
const decodeCzdata = (czdataBase64) => {
  try {
    if (!czdataBase64) return null;
    const decoded = Buffer.from(czdataBase64, "base64").toString("utf-8");
    return JSON.parse(decoded);
  } catch (e) {
    return null;
  }
};

// Allowed lead statuses
const ALLOWED_LEAD_STATUSES = new Set([
  'vvi', 'maybe', 'enrolled', 'junk_lead', 'not_required', 'enrolled_other', 
  'decline', 'not_eligible', 'wrong_number', 'hot_followup', 'cold_followup', 
  'schedule', 'not_connected'
]);

const normalizeLeadStatus = (value, fallback = 'maybe') => {
  if (!value || typeof value !== 'string') return fallback;
  const normalized = value.trim().toLowerCase();
  return ALLOWED_LEAD_STATUSES.has(normalized) ? normalized : fallback;
};

// Simplified Call Logger
class CallLogger {
  constructor(clientId, mobile = null, callDirection = "inbound") {
    this.clientId = clientId
    this.mobile = mobile
    this.callDirection = callDirection
    this.callStartTime = new Date()
    this.transcripts = []
    this.responses = []
    this.totalDuration = 0
    this.callLogId = null
    this.streamSid = null
    this.callSid = null
    this.accountSid = null
    this.ws = null
  }

  async createInitialCallLog(agentId = null) {
    try {
      const callLogData = {
        clientId: this.clientId,
        agentId: agentId,
        mobile: this.mobile,
        time: this.callStartTime,
        transcript: "",
        duration: 0,
        leadStatus: 'not_connected',
        streamSid: this.streamSid,
        callSid: this.callSid,
        metadata: {
          callDirection: this.callDirection,
          isActive: true,
          lastUpdated: new Date()
        }
      }

      const callLog = new CallLog(callLogData)
      const savedLog = await callLog.save()
      this.callLogId = savedLog._id
      return savedLog
    } catch (error) {
      console.log(`❌ [CALL-LOG] Error: ${error.message}`)
      throw error
    }
  }

  logUserTranscript(transcript, language) {
    this.transcripts.push({
      type: "user",
      text: transcript,
      language: language,
      timestamp: new Date()
    })
  }

  logAIResponse(response, language) {
    this.responses.push({
      type: "ai",
      text: response,
      language: language,
      timestamp: new Date()
    })
  }

  generateFullTranscript() {
    const allEntries = [...this.transcripts, ...this.responses].sort(
      (a, b) => new Date(a.timestamp) - new Date(b.timestamp)
    )

    return allEntries
      .map((entry) => {
        const speaker = entry.type === "user" ? "User" : "AI"
        return `${speaker} (${entry.language}): ${entry.text}`
      })
      .join("\n")
  }

  async saveToDatabase(leadStatusInput = 'maybe') {
    try {
      const callEndTime = new Date()
      this.totalDuration = Math.round((callEndTime - this.callStartTime) / 1000)

      if (this.callLogId) {
        const finalUpdateData = {
          transcript: this.generateFullTranscript(),
          duration: this.totalDuration,
          leadStatus: normalizeLeadStatus(leadStatusInput),
          'metadata.callEndTime': callEndTime,
          'metadata.isActive': false,
          'metadata.lastUpdated': callEndTime
        }

        return await CallLog.findByIdAndUpdate(this.callLogId, finalUpdateData, { new: true })
      } else {
        const callLogData = {
          clientId: this.clientId,
          mobile: this.mobile,
          time: this.callStartTime,
          transcript: this.generateFullTranscript(),
          duration: this.totalDuration,
          leadStatus: normalizeLeadStatus(leadStatusInput),
          streamSid: this.streamSid,
          callSid: this.callSid,
          metadata: {
            callDirection: this.callDirection,
            isActive: false
          }
        }

        const callLog = new CallLog(callLogData)
        return await callLog.save()
      }
    } catch (error) {
      console.log(`❌ [CALL-LOG-SAVE] Error: ${error.message}`)
      throw error
    }
  }

  getStats() {
    return {
      duration: this.totalDuration,
      userMessages: this.transcripts.length,
      aiResponses: this.responses.length,
      startTime: this.callStartTime,
      callDirection: this.callDirection
    }
  }
}

// Streaming OpenAI Processor
class StreamingOpenAIProcessor {
  constructor(agentConfig, userName = null) {
    this.agentConfig = agentConfig
    this.userName = userName
  }

  async processWithStreaming(userMessage, conversationHistory, detectedLanguage, ws, streamSid, callLogger) {
    try {
      const basePrompt = this.agentConfig.systemPrompt || "You are a helpful AI assistant."
      const firstMessage = (this.agentConfig.firstMessage || "").trim()
      const knowledgeBlock = firstMessage ? `FirstGreeting: "${firstMessage}"\n` : ""

      const policyBlock = [
        "Answer strictly using the information provided above.",
        "If the user asks for address, phone, timings, or other specifics, check the System Prompt or FirstGreeting.",
        "If the information is not present, reply briefly that you don't have that information.",
        "Always end your answer with a short, relevant follow-up question to keep the conversation going.",
        "Keep the entire reply under 100 tokens."
      ].join(" ")

      const systemPrompt = `System Prompt:\n${basePrompt}\n\n${knowledgeBlock}${policyBlock}`

      const personalizationMessage = this.userName && this.userName.trim()
        ? { role: "system", content: `The user's name is ${this.userName.trim()}. Address them by name naturally when appropriate.` }
        : null

      const messages = [
        { role: "system", content: systemPrompt },
        ...(personalizationMessage ? [personalizationMessage] : []),
        ...conversationHistory.slice(-6),
        { role: "user", content: userMessage }
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
          max_tokens: 120,
          temperature: 0.3,
          stream: true
        }),
      })

      if (!response.ok) {
        throw new Error(`OpenAI API error: ${response.status}`)
      }

      const reader = response.body.getReader()
      const decoder = new TextDecoder()
      let fullResponse = ""
      let isFirstChunk = true

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        const chunk = decoder.decode(value)
        const lines = chunk.split('\n')

        for (const line of lines) {
          if (line.startsWith('data: ')) {
            const data = line.slice(6)
            if (data === '[DONE]') break

            try {
              const parsed = JSON.parse(data)
              const content = parsed.choices?.[0]?.delta?.content
              
              if (content) {
                if (isFirstChunk) {
                  isFirstChunk = false
                  // Log the start of AI response
                  if (callLogger) {
                    callLogger.logAIResponse(content, detectedLanguage)
                  }
                } else {
                  // Append to existing response
                  if (callLogger && callLogger.responses.length > 0) {
                    const lastResponse = callLogger.responses[callLogger.responses.length - 1]
                    lastResponse.text += content
                  }
                }
                fullResponse += content
              }
            } catch (e) {
              // Skip invalid JSON chunks
            }
          }
        }
      }

      // Ensure follow-up question
      if (fullResponse && !/[?]\s*$/.test(fullResponse)) {
        const followUps = {
          hi: "क्या मैं और किसी बात में आपकी मदद कर सकता/सकती हूँ?",
          en: "Is there anything else I can help you with?"
        }
        const fu = followUps[detectedLanguage] || followUps.en
        fullResponse = `${fullResponse} ${fu}`.trim()
        
        // Update the last response
        if (callLogger && callLogger.responses.length > 0) {
          const lastResponse = callLogger.responses[callLogger.responses.length - 1]
          lastResponse.text = fullResponse
        }
      }

      return fullResponse
    } catch (error) {
      console.log(`❌ [OPENAI-STREAMING] Error: ${error.message}`)
      return null
    }
  }
}

// Simplified TTS Processor
class SimplifiedTTSProcessor {
  constructor(language, ws, streamSid) {
    this.language = language
    this.ws = ws
    this.streamSid = streamSid
    this.isInterrupted = false
    this.currentAudioStreaming = null
  }

  interrupt() {
    this.isInterrupted = true
    if (this.currentAudioStreaming) {
      this.currentAudioStreaming.interrupt = true
      setTimeout(() => {
        if (this.currentAudioStreaming) {
          this.currentAudioStreaming = null
        }
      }, 100)
    }
  }

  async synthesizeAndStream(text) {
    if (this.isInterrupted) return

    try {
      const response = await fetch("https://api.sarvam.ai/text-to-speech", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "API-Subscription-Key": API_KEYS.sarvam,
        },
        body: JSON.stringify({
          inputs: [text],
          target_language_code: this.language === "hi" ? "hi-IN" : "en-IN",
          speaker: this.language === "hi" ? "pavithra" : "manisha",
          pitch: 0.5,
          pace: 1.0,
          loudness: 1.0,
          speech_sample_rate: 8000,
          enable_preprocessing: false,
          model: "bulbul:v1",
          output_audio_codec: "linear16",
        }),
      })

      if (!response.ok || this.isInterrupted) {
        if (!this.isInterrupted) {
          throw new Error(`Sarvam API error: ${response.status}`)
        }
        return
      }

      const responseData = await response.json()
      const audioBase64 = responseData.audios?.[0]

      if (!audioBase64 || this.isInterrupted) {
        if (!this.isInterrupted) {
          throw new Error("No audio data received from Sarvam API")
        }
        return
      }

      if (!this.isInterrupted) {
        await this.streamLinear16AudioToSIP(audioBase64)
      }
    } catch (error) {
      if (!this.isInterrupted) {
        console.log(`❌ [TTS] Error: ${error.message}`)
        throw error
      }
    }
  }

  async streamLinear16AudioToSIP(audioBase64) {
    if (this.isInterrupted) return

    if (this.currentAudioStreaming && !this.currentAudioStreaming.interrupt) {
      this.currentAudioStreaming.interrupt = true
      await new Promise(resolve => setTimeout(resolve, 100))
    }

    const audioBuffer = Buffer.from(audioBase64, "base64")
    const streamingSession = { interrupt: false, streamId: Date.now() }
    this.currentAudioStreaming = streamingSession

    const SAMPLE_RATE = 8000
    const BYTES_PER_SAMPLE = 2
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000
    const OPTIMAL_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS)

    let position = 0
    let chunkIndex = 0

    while (position < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
      const remaining = audioBuffer.length - position
      const chunkSize = Math.min(OPTIMAL_CHUNK_SIZE, remaining)
      const chunk = audioBuffer.slice(position, position + chunkSize)

      const mediaMessage = {
        event: "media",
        streamSid: this.streamSid,
        media: {
          payload: chunk.toString("base64"),
        },
      }

      if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted && !streamingSession.interrupt) {
        try {
          this.ws.send(JSON.stringify(mediaMessage))
        } catch (error) {
          console.log(`❌ [TTS-STREAM] Error sending chunk: ${error.message}`)
          break
        }
      } else {
        break
      }

      if (position + chunkSize < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
        const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS)
        const delayMs = Math.max(chunkDurationMs - 2, 10)
        await new Promise((resolve) => setTimeout(resolve, delayMs))
      }

      position += chunkSize
      chunkIndex++
    }

    if (this.currentAudioStreaming === streamingSession) {
      this.currentAudioStreaming = null
    }
  }
}

// Agent lookup function
const findAgentForCall = async (callData) => {
  try {
    const { accountSid, callDirection, extraData } = callData

    let agent = null

    if (callDirection === "inbound") {
      if (!accountSid) {
        throw new Error("Missing accountSid for inbound call")
      }

      agent = await Agent.findOne({ 
        accountSid, 
        isActive: true 
      }).lean()
      
      if (!agent) {
        throw new Error(`No active agent found for accountSid: ${accountSid}`)
      }
    } else if (callDirection === "outbound") {
      if (!extraData?.CallVaId) {
        throw new Error("Missing CallVaId in extraData for outbound call")
      }

      const callVaId = extraData.CallVaId
      
      agent = await Agent.findOne({ 
        callerId: callVaId, 
        isActive: true 
      }).lean()
      
      if (!agent) {
        throw new Error(`No active agent found for callerId: ${callVaId}`)
      }
    } else {
      throw new Error(`Unknown call direction: ${callDirection}`)
    }

    return agent
  } catch (error) {
    console.log(`❌ [AGENT-LOOKUP] Error: ${error.message}`)
    throw error
  }
}

// Main WebSocket server setup
const setupUnifiedVoiceServer = (wss) => {
  wss.on("connection", (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`)
    const urlParams = Object.fromEntries(url.searchParams.entries())

    // Session state
    let streamSid = null
    let conversationHistory = []
    let isProcessing = false
    let userUtteranceBuffer = ""
    let lastProcessedText = ""
    let currentTTS = null
    let currentLanguage = undefined
    let processingRequestId = 0
    let callLogger = null
    let callDirection = "inbound"
    let agentConfig = null
    let userName = null

    // Deepgram WebSocket connection
    let deepgramWs = null
    let deepgramReady = false
    let deepgramAudioQueue = []
    let sttTimer = null

    const connectToDeepgram = async () => {
      try {
        const deepgramLanguage = currentLanguage === "hi" ? "hi" : "en-IN"

        const deepgramUrl = new URL("wss://api.deepgram.com/v1/listen")
        deepgramUrl.searchParams.append("sample_rate", "8000")
        deepgramUrl.searchParams.append("channels", "1")
        deepgramUrl.searchParams.append("encoding", "linear16")
        deepgramUrl.searchParams.append("model", "nova-2")
        deepgramUrl.searchParams.append("language", deepgramLanguage)
        deepgramUrl.searchParams.append("interim_results", "true")
        deepgramUrl.searchParams.append("smart_format", "true")
        deepgramUrl.searchParams.append("endpointing", "300")

        deepgramWs = new WebSocket(deepgramUrl.toString(), {
          headers: { Authorization: `Token ${API_KEYS.deepgram}` },
        })

        deepgramWs.onopen = () => {
          console.log("🎤 [DEEPGRAM] Connection established")
          deepgramReady = true
          deepgramAudioQueue.forEach((buffer) => deepgramWs.send(buffer))
          deepgramAudioQueue = []
        }

        deepgramWs.onmessage = async (event) => {
          const data = JSON.parse(event.data)
          await handleDeepgramResponse(data)
        }

        deepgramWs.onerror = (error) => {
          console.log("❌ [DEEPGRAM] Connection error:", error.message)
          deepgramReady = false
        }

        deepgramWs.onclose = () => {
          console.log("🔌 [DEEPGRAM] Connection closed")
          deepgramReady = false
        }
      } catch (error) {
        // Silent error handling
      }
    }

    const handleDeepgramResponse = async (data) => {
      if (data.type === "Results") {
        if (!sttTimer) {
          sttTimer = createTimer("STT_TRANSCRIPTION")
        }

        const transcript = data.channel?.alternatives?.[0]?.transcript
        const is_final = data.is_final

        if (transcript?.trim()) {
          if (currentTTS && isProcessing) {
            currentTTS.interrupt()
            isProcessing = false
            processingRequestId++
          }

          if (is_final) {
            console.log(`🕒 [STT] ${sttTimer.end()}ms - Text: "${transcript.trim()}"`)
            sttTimer = null

            userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim()

            if (callLogger && transcript.trim()) {
              const detectedLang = detectLanguage(transcript.trim())
              callLogger.logUserTranscript(transcript.trim(), detectedLang)
            }

            await processUserUtterance(userUtteranceBuffer)
            userUtteranceBuffer = ""
          }
        }
      } else if (data.type === "UtteranceEnd") {
        if (sttTimer) {
          console.log(`🕒 [STT] ${sttTimer.end()}ms - Text: "${userUtteranceBuffer.trim()}"`)
          sttTimer = null
        }

        if (userUtteranceBuffer.trim()) {
          if (callLogger && userUtteranceBuffer.trim()) {
            const detectedLang = detectLanguage(userUtteranceBuffer.trim())
            callLogger.logUserTranscript(userUtteranceBuffer.trim(), detectedLang)
          }

          await processUserUtterance(userUtteranceBuffer)
          userUtteranceBuffer = ""
        }
      }
    }

    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText) return

      console.log("🗣️ [USER] Text:", text.trim())

      if (currentTTS) {
        currentTTS.interrupt()
      }

      isProcessing = true
      lastProcessedText = text
      const currentRequestId = ++processingRequestId

      try {
        const detectedLanguage = detectLanguage(text)
        console.log("🌐 [USER] Language:", detectedLanguage)

        if (detectedLanguage !== currentLanguage) {
          currentLanguage = detectedLanguage
        }

        const openaiProcessor = new StreamingOpenAIProcessor(agentConfig, userName)
        const aiResponse = await openaiProcessor.processWithStreaming(
          text,
          conversationHistory,
          detectedLanguage,
          ws,
          streamSid,
          callLogger
        )

        if (processingRequestId === currentRequestId && aiResponse) {
          console.log("🤖 [AI] Response:", aiResponse)
          
          currentTTS = new SimplifiedTTSProcessor(detectedLanguage, ws, streamSid)
          await currentTTS.synthesizeAndStream(aiResponse)

          conversationHistory.push(
            { role: "user", content: text },
            { role: "assistant", content: aiResponse }
          )

          if (conversationHistory.length > 10) {
            conversationHistory = conversationHistory.slice(-10)
          }
        }
      } catch (error) {
        console.log("❌ [USER] Error:", error.message)
      } finally {
        if (processingRequestId === currentRequestId) {
          isProcessing = false
        }
      }
    }

    ws.on("message", async (message) => {
      try {
        const messageStr = message.toString()

        if (messageStr === "EOS" || messageStr === "BOS" || !messageStr.startsWith("{")) {
          return
        }

        const data = JSON.parse(messageStr)

        switch (data.event) {
          case "connected":
            console.log("🔗 [SIP] WebSocket connected")
            break

          case "start": {
            streamSid = data.streamSid || data.start?.streamSid
            const accountSid = data.start?.accountSid

            let mobile = null;
            let callerId = null;
            let customParams = {};
            let czdataDecoded = null;
            
            if (urlParams.czdata) {
              czdataDecoded = decodeCzdata(urlParams.czdata);
              if (czdataDecoded) {
                customParams = czdataDecoded;
                userName = (
                  czdataDecoded.name ||
                  czdataDecoded.Name ||
                  czdataDecoded.full_name ||
                  czdataDecoded.fullName ||
                  czdataDecoded.customer_name ||
                  czdataDecoded.customerName ||
                  czdataDecoded.CustomerName ||
                  czdataDecoded.candidate_name ||
                  czdataDecoded.contactName ||
                  null
                );
              }
            }

            if (data.start?.from) {
              mobile = data.start.from;
            } else if (urlParams.caller_id) {
              mobile = urlParams.caller_id;
            } else if (data.start?.extraData?.CallCli) {
              mobile = data.start.extraData.CallCli;
            }

            let extraData = null;
            if (data.start?.extraData) {
              extraData = decodeExtraData(data.start.extraData);
            } else if (urlParams.extra) {
              extraData = decodeExtraData(urlParams.extra);
            }

            if (extraData?.CallCli) {
              mobile = extraData.CallCli;
            }
            if (extraData?.CallVaId) {
              callerId = extraData.CallVaId;
            }
            if (!userName && extraData) {
              userName = (
                extraData.name ||
                extraData.Name ||
                extraData.full_name ||
                extraData.fullName ||
                extraData.customer_name ||
                extraData.customerName ||
                extraData.CustomerName ||
                extraData.candidate_name ||
                extraData.candidateName ||
                null
              );
            }

            if (!userName && urlParams.name) {
              userName = urlParams.name;
            }

            if (extraData && extraData.CallDirection === "OutDial") {
              callDirection = "outbound";
            } else if (urlParams.direction === "OutDial") {
              callDirection = "outbound";
              if (!extraData && urlParams.extra) {
                extraData = decodeExtraData(urlParams.extra);
              }
            } else {
              callDirection = "inbound";
            }

            try {
              agentConfig = await findAgentForCall({
                accountSid,
                callDirection,
                extraData,
              })

              if (!agentConfig) {
                ws.send(
                  JSON.stringify({
                    event: "error",
                    message: `No agent found for ${callDirection} call`,
                  }),
                )
                ws.close()
                return
              }
            } catch (err) {
              ws.send(
                JSON.stringify({
                  event: "error",
                  message: err.message,
                }),
              )
              ws.close()
              return
            }

            currentLanguage = agentConfig.language || "en"

            callLogger = new CallLogger(
              agentConfig.clientId || accountSid,
              mobile,
              callDirection
            );
            callLogger.customParams = customParams;
            callLogger.callerId = callerId;
            callLogger.streamSid = streamSid;
            callLogger.callSid = data.start?.callSid || data.start?.CallSid || data.callSid || data.CallSid;
            callLogger.accountSid = accountSid;
            callLogger.ws = ws;

            try {
              await callLogger.createInitialCallLog(agentConfig._id);
            } catch (error) {
              console.log("❌ [CALL-SETUP] Failed to create initial call log:", error.message)
            }

            await connectToDeepgram()

            let greeting = agentConfig.firstMessage || "Hello! How can I help you today?"
            if (userName && userName.trim()) {
              const base = agentConfig.firstMessage || "How can I help you today?"
              greeting = `Hello ${userName.trim()}! ${base}`
            }

            if (callLogger) {
              callLogger.logAIResponse(greeting, currentLanguage)
            }

            currentTTS = new SimplifiedTTSProcessor(currentLanguage, ws, streamSid)
            await currentTTS.synthesizeAndStream(greeting)
            break
          }

          case "media":
            if (data.media?.payload) {
              const audioBuffer = Buffer.from(data.media.payload, "base64")
              
              if (deepgramWs && deepgramReady && deepgramWs.readyState === WebSocket.OPEN) {
                deepgramWs.send(audioBuffer)
              } else {
                deepgramAudioQueue.push(audioBuffer)
              }
            }
            break

          case "stop":
            console.log("🛑 [SIP] Call ended")
            
            if (callLogger) {
              try {
                await callLogger.saveToDatabase("maybe")
              } catch (error) {
                console.log("❌ [SIP] Error saving call log:", error.message)
              }
            }

            if (deepgramWs?.readyState === WebSocket.OPEN) {
              deepgramWs.close()
            }
            break

          default:
            break
        }
      } catch (error) {
        // Silent error handling
      }
    })

    ws.on("close", async () => {
      console.log("🔌 [SIP] WebSocket closed")
      
      if (callLogger) {
        try {
          await callLogger.saveToDatabase("maybe")
        } catch (error) {
          console.log("❌ [SIP] Error saving call log:", error.message)
        }
      }

      if (deepgramWs?.readyState === WebSocket.OPEN) {
        deepgramWs.close()
      }
      
      // Reset state
      streamSid = null
      conversationHistory = []
      isProcessing = false
      userUtteranceBuffer = ""
      lastProcessedText = ""
      deepgramReady = false
      deepgramAudioQueue = []
      currentTTS = null
      currentLanguage = undefined
      processingRequestId = 0
      callLogger = null
      callDirection = "inbound"
      agentConfig = null
      sttTimer = null
    })

    ws.on("error", (error) => {
      console.log("❌ [SIP] WebSocket error:", error.message)
    })
  })
}

module.exports = { 
  setupUnifiedVoiceServer
}