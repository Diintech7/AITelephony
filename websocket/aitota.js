const WebSocket = require("ws");
require("dotenv").config();

// Load API keys from environment variables
const API_KEYS = {
  deepgram: process.env.DEEPGRAM_API_KEY,
  sarvam: process.env.SARVAM_API_KEY,
  openai: process.env.OPENAI_API_KEY,
};

// Validate API keys
if (!API_KEYS.deepgram || !API_KEYS.sarvam || !API_KEYS.openai) {
  console.error("❌ Missing required API keys in environment variables");
  process.exit(1);
}

const fetch = globalThis.fetch || require("node-fetch");

// Performance timing helper
const createTimer = (label) => {
  const start = Date.now();
  return {
    start,
    end: () => Date.now() - start,
    checkpoint: (checkpointName) => Date.now() - start,
  };
};

// Language mappings
const LANGUAGE_MAPPING = {
  hi: "hi-IN", en: "en-IN", bn: "bn-IN", te: "te-IN", ta: "ta-IN",
  mr: "mr-IN", gu: "gu-IN", kn: "kn-IN", ml: "ml-IN", pa: "pa-IN",
  or: "or-IN", as: "as-IN", ur: "ur-IN",
};

const getSarvamLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang;
  return LANGUAGE_MAPPING[lang] || "hi-IN";
};

const getDeepgramLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang;
  if (lang === "hi") return "hi";
  if (lang === "en") return "en-IN";
  return lang;
};

// Valid Sarvam voice options
const VALID_SARVAM_VOICES = ["meera", "pavithra", "arvind", "amol", "maya", "anushka"];

const getValidSarvamVoice = (voiceSelection = "pavithra") => {
  if (VALID_SARVAM_VOICES.includes(voiceSelection)) {
    return voiceSelection;
  }
  
  const voiceMapping = {
    "male-professional": "arvind",
    "female-professional": "anushka",
    "male-friendly": "amol",
    "female-friendly": "maya",
    neutral: "pavithra",
    default: "pavithra",
  };
  
  return voiceMapping[voiceSelection] || "pavithra";
};

// Basic configuration
const DEFAULT_CONFIG = {
  agentName: "हिंदी सहायक",
  language: "hi",
  voiceSelection: "anushka",
  firstMessage: "नमस्कार! एआई तोता में संपर्क करने के लिए धन्यवाद। बताइए, मैं आपकी किस प्रकार मदद कर सकता हूँ?",
  personality: "friendly",
  category: "customer service",
  contextMemory: "customer service conversation in Hindi",
};

// Optimized OpenAI streaming with phrase-based chunking
const processWithOpenAIStreaming = async (userMessage, conversationHistory, onPhrase, onComplete) => {
  const timer = createTimer("OPENAI_STREAMING");
  
  try {
    const systemPrompt = `You are Aitota, a polite, emotionally intelligent AI customer care executive. You speak fluently in English and Hindi. Use natural, conversational language with warmth and empathy. Keep responses short—just 1–2 lines. End each message with a friendly follow-up question to keep the conversation going. When speaking Hindi, use Devanagari script (e.g., नमस्ते, कैसे मदद कर सकता हूँ?). Your goal is to make customers feel heard, supported, and valued.

💬 Example Conversations (2 English + 2 Hindi)
---
🗨️ English Example 1
👤: I forgot my password.
🤖: No worries, I can help reset it. Should I send the reset link to your email now?
---
🗨️ English Example 2
👤: How can I track my order?
🤖: I'll check it for you—could you share your order ID please?
---
🗨️ Hindi Example 1
👤: मेरा रिचार्ज नहीं हुआ है।
🤖: क्षमा कीजिए, मैं तुरंत जाँच करता हूँ। क्या आप अपना मोबाइल नंबर बता सकते हैं?
---
🗨️ Hindi Example 2
👤: मुझे नया पता जोड़ना है।
🤖: बिल्कुल, कृपया नया पता बताइए। क्या आप इसे डिलीवरी एड्रेस भी बनाना चाहेंगे?

Language: ${DEFAULT_CONFIG.language}`;

    const messages = [
      { role: "system", content: systemPrompt },
      ...conversationHistory.slice(-6),
      { role: "user", content: userMessage }
    ];

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEYS.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages,
        max_tokens: 50,
        temperature: 0.3,
        stream: true,
      }),
    });

    if (!response.ok) {
      console.error(`❌ [OPENAI] Error: ${response.status}`);
      return null;
    }

    let fullResponse = "";
    let phraseBuffer = "";
    let isFirstPhrase = true;

    const reader = response.body.getReader();
    const decoder = new TextDecoder();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      const chunk = decoder.decode(value, { stream: true });
      const lines = chunk.split('\n').filter(line => line.trim());

      for (const line of lines) {
        if (line.startsWith('data: ')) {
          const data = line.slice(6);
          
          if (data === '[DONE]') {
            if (phraseBuffer.trim()) {
              onPhrase(phraseBuffer.trim());
              fullResponse += phraseBuffer;
            }
            break;
          }

          try {
            const parsed = JSON.parse(data);
            const content = parsed.choices?.[0]?.delta?.content;
            
            if (content) {
              phraseBuffer += content;
              
              if (shouldSendPhrase(phraseBuffer)) {
                const phrase = phraseBuffer.trim();
                if (phrase.length > 0) {
                  if (isFirstPhrase) {
                    console.log(`⚡ [OPENAI] First phrase (${timer.checkpoint('first_phrase')}ms)`);
                    isFirstPhrase = false;
                  }
                  onPhrase(phrase);
                  fullResponse += phrase;
                  phraseBuffer = "";
                }
              }
            }
          } catch (e) {
            // Skip malformed JSON
          }
        }
      }
    }

    console.log(`🤖 [OPENAI] Complete: "${fullResponse}" (${timer.end()}ms)`);
    onComplete(fullResponse);
    return fullResponse;

  } catch (error) {
    console.error(`❌ [OPENAI] Error: ${error.message}`);
    return null;
  }
};

// Smart phrase detection for better chunking
const shouldSendPhrase = (buffer) => {
  const trimmed = buffer.trim();
  
  // Complete sentences
  if (/[.!?।]$/.test(trimmed)) return true;
  
  // Meaningful phrases with natural breaks
  if (trimmed.length >= 8 && /[,;।]\s*$/.test(trimmed)) return true;
  
  // Longer phrases (prevent too much buffering)
  if (trimmed.length >= 25 && /\s/.test(trimmed)) return true;
  
  return false;
};

// WebSocket-based Sarvam TTS Processor - ULTRA FAST VERSION
class WebSocketSarvamTTSProcessor {
  constructor(language, sipWs, streamSid) {
    this.language = language;
    this.sipWs = sipWs;
    this.streamSid = streamSid;
    this.sarvamLanguage = getSarvamLanguage(language);
    this.voice = getValidSarvamVoice(DEFAULT_CONFIG.voiceSelection);
    
    // WebSocket connection to Sarvam
    this.sarvamWs = null;
    this.isConnected = false;
    this.isConfigured = false;
    
    // Queue for phrases while connecting
    this.phraseQueue = [];
    this.isProcessing = false;
    
    // Audio streaming stats
    this.totalChunks = 0;
    this.totalAudioBytes = 0;
    this.connectionTimer = createTimer("SARVAM_WS_CONNECTION");
    
    // Initialize connection immediately
    this.connectToSarvam();
  }

  async connectToSarvam() {
    try {
      console.log("🔌 [SARVAM-WS] Connecting to streaming TTS...");
      
      // Construct WebSocket URL for Sarvam streaming TTS
      const sarvamWsUrl = "wss://api.sarvam.ai/text-to-speech/stream";
      
      this.sarvamWs = new WebSocket(sarvamWsUrl, {
        headers: {
          'API-Subscription-Key': API_KEYS.sarvam,
        }
      });

      this.sarvamWs.onopen = async () => {
        this.isConnected = true;
        console.log(`✅ [SARVAM-WS] Connected in ${this.connectionTimer.end()}ms`);
        
        // Send configuration immediately
        await this.configureSarvamConnection();
        
        // Process queued phrases
        this.processQueue();
      };

      this.sarvamWs.onmessage = (event) => {
        this.handleSarvamMessage(event);
      };

      this.sarvamWs.onerror = (error) => {
        console.error("❌ [SARVAM-WS] Connection error:", error);
        this.isConnected = false;
      };

      this.sarvamWs.onclose = (event) => {
        console.log(`🔌 [SARVAM-WS] Connection closed: ${event.code} - ${event.reason}`);
        this.isConnected = false;
        this.isConfigured = false;
      };

    } catch (error) {
      console.error("❌ [SARVAM-WS] Setup error:", error.message);
    }
  }

  async configureSarvamConnection() {
    if (!this.isConnected || this.isConfigured) return;

    const configMessage = {
      type: "config",
      data: {
        speaker: this.voice,
        target_language_code: this.sarvamLanguage,
        pitch: 0,
        pace: 1.2, // Slightly faster for real-time feel
        min_buffer_size: 20, // Process smaller chunks faster
        max_chunk_length: 100, // Shorter chunks for streaming
        output_audio_codec: "wav", // WAV for better SIP compatibility
        output_audio_bitrate: "64k" // Lower bitrate for real-time
      }
    };

    try {
      this.sarvamWs.send(JSON.stringify(configMessage));
      this.isConfigured = true;
      console.log("⚙️ [SARVAM-WS] Configuration sent");
    } catch (error) {
      console.error("❌ [SARVAM-WS] Config error:", error.message);
    }
  }

  handleSarvamMessage(event) {
    try {
      const message = JSON.parse(event.data);
      
      switch (message.type) {
        case 'audio':
          this.handleAudioChunk(message.data);
          break;
          
        case 'error':
          console.error("❌ [SARVAM-WS] Server error:", message.error);
          break;
          
        case 'status':
          console.log("📊 [SARVAM-WS] Status:", message.status);
          break;
          
        default:
          console.log("❓ [SARVAM-WS] Unknown message type:", message.type);
      }
    } catch (error) {
      console.error("❌ [SARVAM-WS] Message parse error:", error.message);
    }
  }

  async handleAudioChunk(audioData) {
    if (!audioData?.audio) return;

    const timer = createTimer("AUDIO_CHUNK_PROCESSING");
    
    try {
      // Stream audio chunk immediately to SIP
      await this.streamAudioChunkToSIP(audioData.audio);
      
      // Update stats
      const audioBuffer = Buffer.from(audioData.audio, "base64");
      this.totalAudioBytes += audioBuffer.length;
      this.totalChunks++;
      
      console.log(`🎵 [SARVAM-WS] Chunk ${this.totalChunks}: ${audioBuffer.length} bytes (${timer.end()}ms)`);
      
    } catch (error) {
      console.error("❌ [SARVAM-WS] Audio chunk error:", error.message);
    }
  }

  async streamAudioChunkToSIP(audioBase64) {
    const audioBuffer = Buffer.from(audioBase64, "base64");
    
    // SIP-optimized chunking parameters
    const SAMPLE_RATE = 8000;
    const BYTES_PER_SAMPLE = 2;
    const CHUNK_DURATION_MS = 20; // 20ms chunks for SIP
    const CHUNK_SIZE = Math.floor((SAMPLE_RATE * BYTES_PER_SAMPLE * CHUNK_DURATION_MS) / 1000);
    
    let position = 0;
    let chunkIndex = 0;
    
    while (position < audioBuffer.length) {
      const remainingBytes = audioBuffer.length - position;
      const currentChunkSize = Math.min(CHUNK_SIZE, remainingBytes);
      
      if (currentChunkSize > 0) {
        const chunk = audioBuffer.slice(position, position + currentChunkSize);
        
        // Send to SIP immediately
        const mediaMessage = {
          event: "media",
          streamSid: this.streamSid,
          media: {
            payload: chunk.toString("base64")
          }
        };

        if (this.sipWs.readyState === WebSocket.OPEN) {
          this.sipWs.send(JSON.stringify(mediaMessage));
        }
        
        position += currentChunkSize;
        chunkIndex++;
        
        // Minimal delay between chunks (just network buffer)
        if (position < audioBuffer.length) {
          await new Promise(resolve => setTimeout(resolve, 2));
        }
      } else {
        break;
      }
    }
  }

  addPhrase(phrase) {
    if (!phrase.trim()) return;
    
    this.phraseQueue.push(phrase.trim());
    
    if (this.isConnected && this.isConfigured) {
      this.processQueue();
    }
  }

  async processQueue() {
    if (this.isProcessing || this.phraseQueue.length === 0) return;
    if (!this.isConnected || !this.isConfigured) return;

    this.isProcessing = true;

    try {
      // Process all queued phrases as one batch for efficiency
      const textBatch = this.phraseQueue.join(" ");
      this.phraseQueue = [];
      
      if (textBatch.trim()) {
        const timer = createTimer("SARVAM_WS_TEXT_PROCESSING");
        
        console.log(`📤 [SARVAM-WS] Sending: "${textBatch}"`);
        
        const textMessage = {
          type: "text",
          data: {
            text: textBatch
          }
        };

        this.sarvamWs.send(JSON.stringify(textMessage));
        console.log(`⚡ [SARVAM-WS] Text sent in ${timer.end()}ms`);
      }
    } catch (error) {
      console.error(`❌ [SARVAM-WS] Process queue error: ${error.message}`);
    } finally {
      this.isProcessing = false;
      
      // Check if more phrases were added during processing
      if (this.phraseQueue.length > 0) {
        setTimeout(() => this.processQueue(), 50); // Faster retry
      }
    }
  }

  complete() {
    // Process any remaining phrases
    if (this.phraseQueue.length > 0) {
      this.processQueue();
    }
    
    // Send flush message to ensure all text is processed
    if (this.isConnected && this.isConfigured) {
      try {
        const flushMessage = { type: "flush" };
        this.sarvamWs.send(JSON.stringify(flushMessage));
        console.log("🔄 [SARVAM-WS] Flush sent");
      } catch (error) {
        console.error("❌ [SARVAM-WS] Flush error:", error.message);
      }
    }
    
    // Log final stats
    console.log(`📊 [SARVAM-WS-STATS] Total: ${this.totalChunks} chunks, ${this.totalAudioBytes} bytes`);
  }

  // Close connection when done
  close() {
    if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.OPEN) {
      this.sarvamWs.close();
      console.log("🔌 [SARVAM-WS] Connection closed");
    }
  }

  getStats() {
    return {
      totalChunks: this.totalChunks,
      totalAudioBytes: this.totalAudioBytes,
      avgBytesPerChunk: this.totalChunks > 0 ? Math.round(this.totalAudioBytes / this.totalChunks) : 0,
      isConnected: this.isConnected,
      isConfigured: this.isConfigured,
      queueLength: this.phraseQueue.length
    };
  }
}

// Main WebSocket server setup with WebSocket TTS
const setupUnifiedVoiceServer = (wss) => {
  console.log("🚀 [ULTRA-FAST] Voice Server with WebSocket TTS started");

  wss.on("connection", (ws, req) => {
    console.log("🔗 [CONNECTION] New ultra-fast WebSocket connection");

    // Session state
    let streamSid = null;
    let conversationHistory = [];
    let isProcessing = false;
    let userUtteranceBuffer = "";
    let lastProcessedText = "";
    let webSocketTTS = null;

    // Deepgram WebSocket connection
    let deepgramWs = null;
    let deepgramReady = false;
    let deepgramAudioQueue = [];

    // Optimized Deepgram connection
    const connectToDeepgram = async () => {
      try {
        console.log("🔌 [DEEPGRAM] Connecting...");
        const deepgramLanguage = getDeepgramLanguage(DEFAULT_CONFIG.language);
        
        const deepgramUrl = new URL("wss://api.deepgram.com/v1/listen");
        deepgramUrl.searchParams.append("sample_rate", "8000");
        deepgramUrl.searchParams.append("channels", "1");
        deepgramUrl.searchParams.append("encoding", "linear16");
        deepgramUrl.searchParams.append("model", "nova-2");
        deepgramUrl.searchParams.append("language", deepgramLanguage);
        deepgramUrl.searchParams.append("interim_results", "true");
        deepgramUrl.searchParams.append("smart_format", "true");
        deepgramUrl.searchParams.append("endpointing", "200"); // Even faster endpointing

        deepgramWs = new WebSocket(deepgramUrl.toString(), {
          headers: { Authorization: `Token ${API_KEYS.deepgram}` },
        });

        deepgramWs.onopen = () => {
          deepgramReady = true;
          console.log("✅ [DEEPGRAM] Connected");
          
          // Send buffered audio
          deepgramAudioQueue.forEach(buffer => deepgramWs.send(buffer));
          deepgramAudioQueue = [];
        };

        deepgramWs.onmessage = async (event) => {
          const data = JSON.parse(event.data);
          await handleDeepgramResponse(data);
        };

        deepgramWs.onerror = (error) => {
          console.error("❌ [DEEPGRAM] Error:", error);
          deepgramReady = false;
        };

        deepgramWs.onclose = () => {
          console.log("🔌 [DEEPGRAM] Connection closed");
          deepgramReady = false;
        };

      } catch (error) {
        console.error("❌ [DEEPGRAM] Setup error:", error.message);
      }
    };

    // Handle Deepgram responses
    const handleDeepgramResponse = async (data) => {
      if (data.type === "Results") {
        const transcript = data.channel?.alternatives?.[0]?.transcript;
        const is_final = data.is_final;
        
        if (transcript?.trim()) {
          if (is_final) {
            userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim();
            await processUserUtterance(userUtteranceBuffer);
            userUtteranceBuffer = "";
          }
        }
      } else if (data.type === "UtteranceEnd") {
        if (userUtteranceBuffer.trim()) {
          await processUserUtterance(userUtteranceBuffer);
          userUtteranceBuffer = "";
        }
      }
    };

    // Ultra-fast utterance processing with WebSocket TTS
    const processUserUtterance = async (text) => {
      if (!text.trim() || isProcessing || text === lastProcessedText) return;

      isProcessing = true;
      lastProcessedText = text;
      const timer = createTimer("ULTRA_FAST_PROCESSING");

      try {
        console.log(`🎤 [USER] Processing: "${text}"`);

        // Initialize WebSocket TTS processor
        webSocketTTS = new WebSocketSarvamTTSProcessor(DEFAULT_CONFIG.language, ws, streamSid);

        // Process with OpenAI streaming + WebSocket TTS
        const response = await processWithOpenAIStreaming(
          text,
          conversationHistory,
          (phrase) => {
            // Send phrase immediately to WebSocket TTS
            console.log(`📤 [PHRASE] "${phrase}"`);
            webSocketTTS.addPhrase(phrase);
          },
          (fullResponse) => {
            // Complete processing
            console.log(`✅ [COMPLETE] "${fullResponse}"`);
            webSocketTTS.complete();
            
            // Log stats
            const stats = webSocketTTS.getStats();
            console.log(`📊 [WS-TTS-STATS] ${stats.totalChunks} chunks, Connected: ${stats.isConnected}, Queue: ${stats.queueLength}`);
            
            // Update conversation history
            conversationHistory.push(
              { role: "user", content: text },
              { role: "assistant", content: fullResponse }
            );

            // Keep last 10 messages for context
            if (conversationHistory.length > 10) {
              conversationHistory = conversationHistory.slice(-10);
            }
          }
        );

        console.log(`⚡ [ULTRA-FAST] Total processing time: ${timer.end()}ms`);

      } catch (error) {
        console.error(`❌ [PROCESSING] Error: ${error.message}`);
      } finally {
        isProcessing = false;
      }
    };

    // Ultra-fast initial greeting
    const sendInitialGreeting = async () => {
      console.log("👋 [GREETING] Sending WebSocket TTS greeting");
      const tts = new WebSocketSarvamTTSProcessor(DEFAULT_CONFIG.language, ws, streamSid);
      
      // Add greeting and immediately complete
      tts.addPhrase(DEFAULT_CONFIG.firstMessage);
      tts.complete();
      
      // Log connection stats after greeting
      setTimeout(() => {
        const stats = tts.getStats();
        console.log(`📊 [GREETING-STATS] Connected: ${stats.isConnected}, Configured: ${stats.isConfigured}`);
      }, 1000);
    };

    // WebSocket message handling
    ws.on("message", async (message) => {
      try {
        const data = JSON.parse(message.toString());

        switch (data.event) {
          case "connected":
            console.log(`🔗 [ULTRA-FAST] Connected - Protocol: ${data.protocol}`);
            break;

          case "start":
            streamSid = data.streamSid || data.start?.streamSid;
            console.log(`🎯 [ULTRA-FAST] Stream started - StreamSid: ${streamSid}`);
            
            await connectToDeepgram();
            await sendInitialGreeting();
            break;

          case "media":
            if (data.media?.payload) {
              const audioBuffer = Buffer.from(data.media.payload, "base64");
              
              if (deepgramWs && deepgramReady && deepgramWs.readyState === WebSocket.OPEN) {
                deepgramWs.send(audioBuffer);
              } else {
                deepgramAudioQueue.push(audioBuffer);
              }
            }
            break;

          case "stop":
            console.log(`📞 [ULTRA-FAST] Stream stopped`);
            if (deepgramWs?.readyState === WebSocket.OPEN) {
              deepgramWs.close();
            }
            if (webSocketTTS) {
              webSocketTTS.close();
            }
            break;

          default:
            console.log(`❓ [ULTRA-FAST] Unknown event: ${data.event}`);
        }
      } catch (error) {
        console.error(`❌ [ULTRA-FAST] Message error: ${error.message}`);
      }
    });

    // Connection cleanup
    ws.on("close", () => {
      console.log("🔗 [ULTRA-FAST] Connection closed");
      
      if (deepgramWs?.readyState === WebSocket.OPEN) {
        deepgramWs.close();
      }

      if (webSocketTTS) {
        webSocketTTS.close();
      }

      // Reset state
      streamSid = null;
      conversationHistory = [];
      isProcessing = false;
      userUtteranceBuffer = "";
      lastProcessedText = "";
      deepgramReady = false;
      deepgramAudioQueue = [];
      webSocketTTS = null;
    });

    ws.on("error", (error) => {
      console.error(`❌ [ULTRA-FAST] WebSocket error: ${error.message}`);
    });
  });
};

module.exports = { setupUnifiedVoiceServer };