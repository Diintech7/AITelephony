const WebSocket = require("ws")
require("dotenv").config()

// Import franc with fallback for different versions
let franc;
try {
  franc = require("franc").franc;
  if (!franc) {
    franc = require("franc");
  }
} catch (error) {
  franc = () => 'und';
}

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

// Enhanced language mappings with Marathi support
const LANGUAGE_MAPPING = {
  hi: "hi-IN",
  en: "en-IN",
  bn: "bn-IN",
  te: "te-IN",
  ta: "ta-IN",
  mr: "mr-IN",
  gu: "gu-IN",
  kn: "kn-IN",
  ml: "ml-IN",
  pa: "pa-IN",
  or: "or-IN",
  as: "as-IN",
  ur: "ur-IN",
}

// Enhanced Franc language code mapping to our supported languages
const FRANC_TO_SUPPORTED = {
  'hin': 'hi',
  'eng': 'en',
  'ben': 'bn',
  'tel': 'te',
  'tam': 'ta',
  'mar': 'mr',
  'guj': 'gu',
  'kan': 'kn',
  'mal': 'ml',
  'pan': 'pa',
  'ori': 'or',
  'asm': 'as',
  'urd': 'ur',
  'src': 'en',
  'und': 'en',
  'lat': 'en',
  'sco': 'en',
  'fra': 'en',
  'deu': 'en',
  'nld': 'en',
  'spa': 'en',
  'ita': 'en',
  'por': 'en',
}

const getSarvamLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang
  return LANGUAGE_MAPPING[lang] || "hi-IN"
}

const getDeepgramLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang
  if (lang === "hi") return "hi"
  if (lang === "en") return "en-IN"
  if (lang === "mr") return "mr"
  return lang
}

// Valid Sarvam voice options
const VALID_SARVAM_VOICES = ["meera", "anushka", "arvind", "amol", "maya", "anushka", "abhilash", "manisha", "vidya", "arya", "karun", "hitesh"]

const getValidSarvamVoice = (voiceSelection = "anushka") => {
  if (VALID_SARVAM_VOICES.includes(voiceSelection)) {
    return voiceSelection
  }

  const voiceMapping = {
    "male-professional": "arvind",
    "female-professional": "anushka",
    "male-friendly": "amol",
    "female-friendly": "maya",
    neutral: "anushka",
    default: "anushka",
  }

  return voiceMapping[voiceSelection] || "anushka"
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

// Enhanced language detection with better fallback logic
const detectLanguageWithFranc = (text, fallbackLanguage = "en") => {
  try {
    const cleanText = text.trim()
    
    if (cleanText.length < 10) {
      const englishPatterns = /^(what|how|why|when|where|who|can|do|does|did|is|are|am|was|were|have|has|had|will|would|could|should|may|might|hello|hi|hey|yes|no|ok|okay|thank|thanks|please|sorry|our|your|my|name|help)\b/i
      const hindiPatterns = /[\u0900-\u097F]/
      const englishWords = /^[a-zA-Z\s\?\!\.\,\'\"]+$/
      
      if (hindiPatterns.test(cleanText)) {
        return "hi"
      } else if (englishPatterns.test(cleanText) || englishWords.test(cleanText)) {
        return "en"
      } else {
        return fallbackLanguage
      }
    }

    if (typeof franc !== 'function') {
      return fallbackLanguage
    }

    const detected = franc(cleanText)

    if (detected === 'und' || !detected) {
      const hindiPatterns = /[\u0900-\u097F]/
      if (hindiPatterns.test(cleanText)) {
        return "hi"
      }
      
      const latinScript = /^[a-zA-Z\s\?\!\.\,\'\"0-9\-$$$$]+$/
      if (latinScript.test(cleanText)) {
        return "en"
      }
      
      return fallbackLanguage
    }

    const mappedLang = FRANC_TO_SUPPORTED[detected]
    
    if (mappedLang) {
      return mappedLang
    } else {
      const hindiPatterns = /[\u0900-\u097F]/
      if (hindiPatterns.test(cleanText)) {
        return "hi"
      }
      
      const tamilScript = /[\u0B80-\u0BFF]/
      const teluguScript = /[\u0C00-\u0C7F]/
      const kannadaScript = /[\u0C80-\u0CFF]/
      const malayalamScript = /[\u0D00-\u0D7F]/
      const gujaratiScript = /[\u0A80-\u0AFF]/
      const bengaliScript = /[\u0980-\u09FF]/
      
      if (tamilScript.test(cleanText)) return "ta"
      if (teluguScript.test(cleanText)) return "te"
      if (kannadaScript.test(cleanText)) return "kn"
      if (malayalamScript.test(cleanText)) return "ml"
      if (gujaratiScript.test(cleanText)) return "gu"
      if (bengaliScript.test(cleanText)) return "bn"
      
      const latinScript = /^[a-zA-Z\s\?\!\.\,\'\"0-9\-$$$$]+$/
      if (latinScript.test(cleanText)) {
        return "en"
      }
      
      return fallbackLanguage
    }
    
  } catch (error) {
    return fallbackLanguage
  }
}

// OpenAI processing with streaming
const processWithOpenAIStream = async (
  userMessage,
  conversationHistory,
  detectedLanguage,
  agentConfig,
  onChunk,
) => {
  const timer = createTimer("LLM_PROCESSING")

  try {
    let systemPrompt = agentConfig?.systemPrompt || "You are a helpful AI assistant."

    if (Buffer.byteLength(systemPrompt, "utf8") > 150) {
      let truncated = systemPrompt
      while (Buffer.byteLength(truncated, "utf8") > 150) {
        truncated = truncated.slice(0, -1)
      }
      systemPrompt = truncated
    }

    const messages = [
      { role: "system", content: systemPrompt },
      ...conversationHistory.slice(-6),
      { role: "user", content: userMessage },
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
        max_tokens: 100,
        temperature: 0.3,
        stream: true, // Enable streaming
      }),
    })

    if (!response.ok) {
      console.log(`❌ [LLM-PROCESSING] ${timer.end()}ms - Error: ${response.status}`)
      return null
    }

    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let fullResponse = ""
    let buffer = ""

    while (true) {
      const { done, value } = await reader.read()
      if (done) break

      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop() // Keep incomplete line in buffer

      for (const line of lines) {
        if (line.startsWith('data: ')) {
          const data = line.slice(6)
          if (data === '[DONE]') {
            console.log(`🕒 [LLM-PROCESSING] ${timer.end()}ms - Stream completed`)
            return fullResponse
          }

          try {
            const parsed = JSON.parse(data)
            const chunk = parsed.choices[0]?.delta?.content
            if (chunk) {
              fullResponse += chunk
              onChunk(chunk) // Send chunk immediately to TTS
            }
          } catch (e) {
            // Ignore parsing errors for incomplete chunks
          }
        }
      }
    }

    return fullResponse
  } catch (error) {
    console.log(`❌ [LLM-PROCESSING] ${timer.end()}ms - Error: ${error.message}`)
    return null
  }
}

// Simplified TTS processor using Sarvam WebSocket with reduced latency
class SimplifiedSarvamTTSProcessor {
  constructor(language, ws, streamSid) {
    this.language = language
    this.ws = ws // Main SIP WebSocket (for sending audio out)
    this.streamSid = streamSid
    this.sarvamLanguage = getSarvamLanguage(language)
    this.voice = getValidSarvamVoice(ws.sessionAgentConfig?.voiceSelection || "anushka")
    this.sarvamWs = null // Sarvam TTS WebSocket
    this.sarvamWsConnected = false
    this.audioQueue = [] // Queue for audio buffers received from Sarvam
    this.isStreamingToSIP = false // Flag to prevent multiple streaming loops
    this.totalAudioBytes = 0
    this.currentText = "" // Current text being processed
  }

  async connectSarvamWs() {
    if (this.sarvamWsConnected && this.sarvamWs?.readyState === WebSocket.OPEN) {
      return true // Already connected
    }

    const timer = createTimer("SARVAM_WS_CONNECT");
    try {
      const sarvamUrl = new URL("wss://api.sarvam.ai/text-to-speech/ws");
      sarvamUrl.searchParams.append("model", "bulbul:v2"); 

      // Pass API key as a subprotocol
      this.sarvamWs = new WebSocket(sarvamUrl.toString(), [`api-subscription-key.${API_KEYS.sarvam}`]);

      return new Promise((resolve, reject) => {
        let configAcknowledged = false;
        let connectionTimeout;

        this.sarvamWs.onopen = () => {
          this.sarvamWsConnected = true;
          console.log(`🕒 [SARVAM-WS-CONNECT] ${timer.end()}ms - Sarvam TTS WebSocket connected.`);

          // Set connection timeout
          connectionTimeout = setTimeout(() => {
            if (!configAcknowledged) {
              console.error("Sarvam WS config timeout");
              reject(new Error("Config acknowledgment timeout"));
            }
          }, 5000);

          // Send simplified config message
          const configMessage = {
            type: "config",
            data: {
              target_language_code: this.sarvamLanguage,
              speaker: this.voice,
              speech_sample_rate: 8000,
            },
          };
          this.sarvamWs.send(JSON.stringify(configMessage));
          console.log("Sarvam TTS config sent.");
        };

        this.sarvamWs.onmessage = async (event) => {
          try {
            const response = JSON.parse(event.data);
            console.log("Sarvam WS message received:", response.type);
            
            if (response.type === "audio" && response.data?.audio) {
              const audioBuffer = Buffer.from(response.data.audio, "base64");
              this.audioQueue.push(audioBuffer);
              this.totalAudioBytes += audioBuffer.length;
              if (!this.isStreamingToSIP) {
                this.startStreamingToSIP();
              }
            } else if (response.type === "error") {
              console.error(`❌ Sarvam TTS WS Error: ${response.data?.message || response.message} (Code: ${response.data?.code || response.code})`);
              clearTimeout(connectionTimeout);
              reject(new Error(`Sarvam error: ${response.data?.message || response.message}`));
            } else if (response.type === "config_ack") {
              console.log("✅ Sarvam TTS config acknowledged");
              configAcknowledged = true;
              clearTimeout(connectionTimeout);
              resolve(true);
            } else if (response.type === "ready") {
              console.log("✅ Sarvam TTS ready for text input");
              if (!configAcknowledged) {
                configAcknowledged = true;
                clearTimeout(connectionTimeout);
                resolve(true);
              }
            }
          } catch (parseError) {
            console.error("Error parsing Sarvam WS message:", parseError);
          }
        };

        this.sarvamWs.onerror = (error) => {
          this.sarvamWsConnected = false;
          clearTimeout(connectionTimeout);
          console.error(`❌ [SARVAM-WS-CONNECT] ${timer.end()}ms - Sarvam TTS WebSocket error:`, error.message);
          reject(error);
        };

        this.sarvamWs.onclose = () => {
          this.sarvamWsConnected = false;
          clearTimeout(connectionTimeout);
          console.log(`🕒 [SARVAM-WS-CONNECT] ${timer.end()}ms - Sarvam TTS WebSocket closed.`);
        };
      });
    } catch (error) {
      console.error(`❌ [SARVAM-WS-CONNECT] ${timer.end()}ms - Failed to connect to Sarvam TTS WebSocket: ${error.message}`);
      return false;
    }
  }

  // Process text chunks as they arrive from LLM
  async processTextChunk(chunk) {
    this.currentText += chunk;
    
    // Send text to Sarvam when we have enough content or hit punctuation
    if (this.currentText.length >= 20 || /[.!?।]/.test(chunk)) {
      await this.synthesizeText(this.currentText);
      this.currentText = "";
    }
  }

  // Finalize any remaining text
  async finalizeText() {
    if (this.currentText.trim()) {
      await this.synthesizeText(this.currentText);
      this.currentText = "";
    }
  }

  async synthesizeText(text) {
    if (!text.trim()) return;

    const timer = createTimer("TTS_SYNTHESIS_WS");

    try {
      const connected = await this.connectSarvamWs();
      if (!connected) {
        console.log("Sarvam WS not connected, aborting synthesis.");
        return;
      }

      // Send text message with correct format
      const textMessage = {
        type: "text",
        data: text.trim(),
      };
      this.sarvamWs.send(JSON.stringify(textMessage));
      
      // Send flush message to signal end of text
      const flushMessage = {
        type: "flush"
      };
      this.sarvamWs.send(JSON.stringify(flushMessage));
      
      console.log(`🕒 [TTS-SYNTHESIS-WS] ${timer.end()}ms - Text and flush sent to Sarvam WS: "${text.trim()}"`);
    } catch (error) {
      console.error(`❌ [TTS-SYNTHESIS-WS] ${timer.end()}ms - Error sending text to Sarvam WS: ${error.message}`);
    }
  }

  async startStreamingToSIP() {
    if (this.isStreamingToSIP) {
      return; // Already streaming
    }
    this.isStreamingToSIP = true;
    console.log("Starting streaming audio from Sarvam to SIP...");

    const SAMPLE_RATE = 8000;
    const BYTES_PER_SAMPLE = 2; // linear16 is 16-bit, so 2 bytes
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000;
    const OPTIMAL_CHUNK_SIZE = Math.floor(20 * BYTES_PER_MS); // Reduced to 20ms chunks for lower latency

    while (this.audioQueue.length > 0) {
      const audioBuffer = this.audioQueue.shift(); // Get the next audio chunk

      let position = 0;
      while (position < audioBuffer.length) {
        const remaining = audioBuffer.length - position;
        const chunkSize = Math.min(OPTIMAL_CHUNK_SIZE, remaining);
        const chunk = audioBuffer.slice(position, position + chunkSize);

        const mediaMessage = {
          event: "media",
          streamSid: this.streamSid,
          media: {
            payload: chunk.toString("base64"),
          },
        };

        if (this.ws.readyState === WebSocket.OPEN) {
          try {
            this.ws.send(JSON.stringify(mediaMessage));
          } catch (error) {
            console.error("Error sending media to SIP WS:", error.message);
            this.isStreamingToSIP = false;
            return;
          }
        } else {
          console.log("SIP WS not open, stopping streaming.");
          this.isStreamingToSIP = false;
          return;
        }

        // Reduced delay for lower latency
        if (position + chunkSize < audioBuffer.length) {
          const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS);
          const delayMs = Math.max(chunkDurationMs - 1, 5); // Reduced delay
          await new Promise((resolve) => setTimeout(resolve, delayMs));
        }

        position += chunkSize;
      }
    }
    this.isStreamingToSIP = false;
    console.log("Stopped streaming audio from Sarvam to SIP.");
  }

  getStats() {
    return {
      totalAudioBytes: this.totalAudioBytes,
    };
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
    let currentTTS = null // This will be the persistent TTS processor instance
    let currentLanguage = "en" // Default language
    let agentConfig = {
      systemPrompt: "You are a helpful AI assistant.",
      firstMessage: "Hello! How can I help you today?",
      voiceSelection: "anushka"
    }

    // Deepgram WebSocket connection
    let deepgramWs = null
    let deepgramReady = false
    let deepgramAudioQueue = []
    let sttTimer = null

    const connectToDeepgram = async () => {
      try {
        const deepgramLanguage = getDeepgramLanguage(currentLanguage)

        const deepgramUrl = new URL("wss://api.deepgram.com/v1/listen")
        deepgramUrl.searchParams.append("sample_rate", "8000")
        deepgramUrl.searchParams.append("channels", "1")
        deepgramUrl.searchParams.append("encoding", "linear16")
        deepgramUrl.searchParams.append("model", "nova-2")
        deepgramUrl.searchParams.append("language", deepgramLanguage)
        deepgramUrl.searchParams.append("interim_results", "true")
        deepgramUrl.searchParams.append("smart_format", "true")
        deepgramUrl.searchParams.append("endpointing", "200") // Reduced for faster response

        deepgramWs = new WebSocket(deepgramUrl.toString(), {
          headers: { Authorization: `Token ${API_KEYS.deepgram}` },
        })

        deepgramWs.onopen = () => {
          deepgramReady = true
          deepgramAudioQueue.forEach((buffer) => deepgramWs.send(buffer))
          deepgramAudioQueue = []
        }

        deepgramWs.onmessage = async (event) => {
          const data = JSON.parse(event.data)
          await handleDeepgramResponse(data)
        }

        deepgramWs.onerror = (error) => {
          deepgramReady = false
        }

        deepgramWs.onclose = () => {
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
          if (is_final) {
            console.log(`🕒 [STT-TRANSCRIPTION] ${sttTimer.end()}ms - Text: "${transcript.trim()}"`)
            sttTimer = null

            userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim()

            await processUserUtterance(userUtteranceBuffer)
            userUtteranceBuffer = ""
          }
        }
      } else if (data.type === "UtteranceEnd") {
        if (sttTimer) {
          console.log(`🕒 [STT-TRANSCRIPTION] ${sttTimer.end()}ms - Text: "${userUtteranceBuffer.trim()}"`)
          sttTimer = null
        }

        if (userUtteranceBuffer.trim()) {
          await processUserUtterance(userUtteranceBuffer)
          userUtteranceBuffer = ""
        }
      }
    }

    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText) return

      isProcessing = true
      lastProcessedText = text

      try {
        const detectedLanguage = detectLanguageWithFranc(text, currentLanguage || "en")

        if (detectedLanguage !== currentLanguage) {
          currentLanguage = detectedLanguage
          // If language changes, reset TTS processor with new language
          if (currentTTS) {
            currentTTS = new SimplifiedSarvamTTSProcessor(currentLanguage, ws, streamSid)
          }
        }

        // Use the existing currentTTS instance or create new one
        if (!currentTTS) {
          currentTTS = new SimplifiedSarvamTTSProcessor(currentLanguage, ws, streamSid)
        }

        // Process with streaming LLM and send chunks directly to TTS
        const response = await processWithOpenAIStream(
          text,
          conversationHistory,
          detectedLanguage,
          agentConfig,
          async (chunk) => {
            // Send each chunk immediately to TTS for lower latency
            await currentTTS.processTextChunk(chunk)
          }
        )

        if (response) {
          // Finalize any remaining text
          await currentTTS.finalizeText()

          conversationHistory.push(
            { role: "user", content: text },
            { role: "assistant", content: response }
          )

          if (conversationHistory.length > 10) {
            conversationHistory = conversationHistory.slice(-10)
          }
        }
      } catch (error) {
        console.error("Error in processUserUtterance:", error)
      } finally {
        isProcessing = false
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
            break

          case "start": {
            streamSid = data.streamSid || data.start?.streamSid

            // Initialize TTS processor
            currentTTS = new SimplifiedSarvamTTSProcessor(currentLanguage, ws, streamSid)

            await connectToDeepgram()

            const greeting = agentConfig.firstMessage || "Hello! How can I help you today?"

            // Synthesize greeting via TTS processor
            await currentTTS.synthesizeText(greeting)
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
      currentLanguage = "en"
      agentConfig = {
        systemPrompt: "You are a helpful AI assistant.",
        firstMessage: "Hello! How can I help you today?",
        voiceSelection: "anushka"
      }
      sttTimer = null
    })

    ws.on("error", (error) => {
      // Silent error handling
    })
  })
}

module.exports = { setupUnifiedVoiceServer }
