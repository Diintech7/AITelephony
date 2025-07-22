const WebSocket = require("ws");
const Agent = require("../models/Agent"); // Adjust path as needed
const ApiKey = require("../models/ApiKey"); // Adjust path as needed

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
const VALID_SARVAM_VOICES = ["meera", "pavithra", "arvind", "amol", "maya", "abhilash", "anushka", "maitreyi", "diya", "neel", "misha", "vian", "arjun", "manisha", "vidya", "arya", "karun", "hitesh"];

const getValidSarvamVoice = (voiceSelection = "pavithra") => {
  if (VALID_SARVAM_VOICES.includes(voiceSelection)) {
    return voiceSelection;
  }
  
  const voiceMapping = {
    "male-professional": "arvind",
    "female-professional": "pavithra",
    "male-friendly": "amol",
    "female-friendly": "maya",
    neutral: "pavithra",
    default: "pavithra",
  };
  
  return voiceMapping[voiceSelection] || "pavithra";
};

// Database helper functions
const getAgentByAccountSid = async (accountSid) => {
  try {
    console.log(`🔍 [DB] Looking for agent with accountSid: ${accountSid}`);
    
    const agent = await Agent.findOne({ accountSid: accountSid });
    
    if (!agent) {
      console.log(`❌ [DB] No agent found for accountSid: ${accountSid}`);
      return null;
    }
    
    console.log(`✅ [DB] Found agent: ${agent.agentName} (${agent.tenantId})`);
    return agent;
  } catch (error) {
    console.error(`❌ [DB] Error fetching agent: ${error.message}`);
    return null;
  }
};

const getApiKeys = async (tenantId) => {
  try {
    console.log(`🔑 [DB] Fetching API keys for tenant: ${tenantId}`);
    
    const apiKeys = await ApiKey.find({ tenantId: tenantId, isActive: true });
    
    if (!apiKeys || apiKeys.length === 0) {
      console.log(`❌ [DB] No active API keys found for tenant: ${tenantId}`);
      return {};
    }
    
    console.log(`📋 [DB] Found ${apiKeys.length} API key(s) for tenant ${tenantId}:`);
    apiKeys.forEach((apiKey, index) => {
      console.log(`   ${index + 1}. Provider: ${apiKey.provider}, Active: ${apiKey.isActive}, Created: ${apiKey.createdAt || 'N/A'}`);
    });
    
    const keys = {};
    const successfulKeys = [];
    const failedKeys = [];
    
    for (const apiKey of apiKeys) {
      try {
        const decryptedKey = apiKey.getDecryptedKey();
        keys[apiKey.provider] = decryptedKey;
        successfulKeys.push(apiKey.provider);
        console.log(`✅ [DB] Successfully loaded ${apiKey.provider} API key`);
        
        if (decryptedKey && decryptedKey.length > 12) {
          const maskedKey = decryptedKey.substring(0, 8) + '...' + decryptedKey.substring(decryptedKey.length - 4);
          console.log(`   🔐 [DB] ${apiKey.provider} key: ${maskedKey}`);
        } else {
          console.log(`   🔐 [DB] ${apiKey.provider} key: [SHORT_KEY]`);
        }
        
      } catch (error) {
        failedKeys.push(apiKey.provider);
        console.error(`❌ [DB] Failed to decrypt ${apiKey.provider} key: ${error.message}`);
      }
    }
    
    console.log(`📊 [DB] API Keys Summary for tenant ${tenantId}:`);
    console.log(`   ✅ Successfully loaded: ${successfulKeys.length} keys [${successfulKeys.join(', ')}]`);
    if (failedKeys.length > 0) {
      console.log(`   ❌ Failed to load: ${failedKeys.length} keys [${failedKeys.join(', ')}]`);
    }
    console.log(`   📦 Total keys in response object: ${Object.keys(keys).length}`);
    
    if (Object.keys(keys).length > 0) {
      console.log(`🗝️  [DB] Available API providers: ${Object.keys(keys).join(', ')}`);
    }
    
    return keys;
  } catch (error) {
    console.error(`❌ [DB] Error fetching API keys: ${error.message}`);
    console.error(`❌ [DB] Error stack: ${error.stack}`);
    return {};
  }
};

// Enhanced loadAgentConfiguration function with API key logging
const loadAgentConfiguration = async (accountSid) => {
  try {
    console.log(`🔧 [CONFIG] Loading configuration for accountSid: ${accountSid}`);
    
    agentConfig = await getAgentByAccountSid(accountSid);
    if (!agentConfig) {
      console.error(`❌ [CONFIG] No agent found for accountSid: ${accountSid}`);
      return false;
    }
    
    apiKeys = await getApiKeys(agentConfig.tenantId);
    
    const requiredKeys = ['openai', 'deepgram', 'sarvam'];
    const availableKeys = Object.keys(apiKeys);
    const missingKeys = requiredKeys.filter(key => !apiKeys[key]);
    
    console.log(`🔍 [CONFIG] API Keys validation for tenant ${agentConfig.tenantId}:`);
    console.log(`   📋 Required keys: ${requiredKeys.join(', ')}`);
    console.log(`   ✅ Available keys: ${availableKeys.join(', ')}`);
    
    if (missingKeys.length > 0) {
      console.error(`   ❌ Missing keys: ${missingKeys.join(', ')}`);
      console.error(`❌ [CONFIG] Missing required API keys for tenant: ${agentConfig.tenantId}`);
      return false;
    }
    
    console.log(`   ✅ All required API keys are available!`);
    console.log(`✅ [CONFIG] Configuration loaded successfully`);
    console.log(`🤖 [AGENT] Name: ${agentConfig.agentName}, Language: ${agentConfig.language}, Voice: ${agentConfig.voiceSelection}`);
    
    return true;
  } catch (error) {
    console.error(`❌ [CONFIG] Error loading configuration: ${error.message}`);
    return false;
  }
};

// Optimized OpenAI streaming with phrase-based chunking
const processWithOpenAIStreaming = async (userMessage, conversationHistory, systemPrompt, apiKeys, onPhrase, onComplete) => {
  const timer = createTimer("OPENAI_STREAMING");
  
  try {
    if (!apiKeys.openai) {
      throw new Error("OpenAI API key not found");
    }

    const messages = [
      { role: "system", content: systemPrompt },
      ...conversationHistory.slice(-6),
      { role: "user", content: userMessage }
    ];

    console.log(`🤖 [OPENAI] Sending request with ${messages.length} messages`);

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKeys.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages,
        max_tokens: 150, // Increased for better responses
        temperature: 0.3,
        stream: true,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`❌ [OPENAI] Error: ${response.status} - ${errorText}`);
      throw new Error(`OpenAI API Error: ${response.status}`);
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
            console.warn(`⚠️ [OPENAI] Skipping malformed JSON: ${data.substring(0, 50)}...`);
          }
        }
      }
    }

    console.log(`🤖 [OPENAI] Complete: "${fullResponse}" (${timer.end()}ms)`);
    onComplete(fullResponse);
    return fullResponse;

  } catch (error) {
    console.error(`❌ [OPENAI] Error: ${error.message}`);
    // Return fallback response to avoid breaking the flow
    const fallbackResponse = "I'm here to help. Could you please repeat that?";
    onPhrase(fallbackResponse);
    onComplete(fallbackResponse);
    return fallbackResponse;
  }
};

// Smart phrase detection for better chunking
const shouldSendPhrase = (buffer) => {
  const trimmed = buffer.trim();
  
  if (/[.!?।]$/.test(trimmed)) return true;
  if (trimmed.length >= 8 && /[,;।]\s*$/.test(trimmed)) return true;
  if (trimmed.length >= 25 && /\s/.test(trimmed)) return true;
  
  return false;
};

// Enhanced WebSocket-based Sarvam TTS Processor with better error handling
class WebSocketSarvamTTSProcessor {
  constructor(language, ws, streamSid, apiKeys, voiceSelection) {
    this.language = language;
    this.ws = ws;
    this.streamSid = streamSid;
    this.apiKeys = apiKeys;
    this.queue = [];
    this.isProcessing = false;
    this.sarvamLanguage = getSarvamLanguage(language);
    this.voice = getValidSarvamVoice(voiceSelection);
    
    this.sentenceBuffer = "";
    this.processingTimeout = 150; // Slightly increased timeout
    this.sentenceTimer = null;
    
    this.totalChunks = 0;
    this.totalAudioBytes = 0;
    
    // WebSocket connection to Sarvam
    this.sarvamWs = null;
    this.sarvamReady = false;
    this.audioQueue = [];
    this.pendingTexts = [];
    this.connectionAttempts = 0;
    this.maxRetries = 3;
    
    this.initializeSarvamWebSocket();
  }

  async initializeSarvamWebSocket() {
    try {
      if (!this.apiKeys.sarvam) {
        throw new Error("Sarvam API key not found");
      }

      console.log(`🔌 [SARVAM-WS] Connecting to Sarvam WebSocket (attempt ${this.connectionAttempts + 1})...`);
      
      const sarvamUrl = 'wss://api.sarvam.ai/text-to-speech-streaming';
      
      this.sarvamWs = new WebSocket(sarvamUrl, {
        headers: {
          'API-Subscription-Key': this.apiKeys.sarvam,
        }
      });

      // Set connection timeout
      const connectionTimeout = setTimeout(() => {
        if (this.sarvamWs.readyState === WebSocket.CONNECTING) {
          console.error(`❌ [SARVAM-WS] Connection timeout`);
          this.sarvamWs.close();
          this.handleConnectionFailure();
        }
      }, 10000); // 10 second timeout

      this.sarvamWs.onopen = () => {
        clearTimeout(connectionTimeout);
        console.log(`✅ [SARVAM-WS] Connected to Sarvam WebSocket`);
        this.sarvamReady = true;
        this.connectionAttempts = 0; // Reset on successful connection
        
        // Send configuration message first
        const configMessage = {
          type: "config",
          data: {
            speaker: this.voice,
            target_language_code: this.sarvamLanguage,
            pitch: 0,
            pace: 1.0,
            min_buffer_size: 40,
            max_chunk_length: 200,
            output_audio_codec: "mp3",
            output_audio_bitrate: "64k"
          }
        };
        
        if (this.sarvamWs.readyState === WebSocket.OPEN) {
          this.sarvamWs.send(JSON.stringify(configMessage));
          console.log(`🔧 [SARVAM-WS] Configuration sent: ${this.voice}, ${this.sarvamLanguage}`);
          
          // Process any pending texts
          this.pendingTexts.forEach(text => this.sendTextToSarvam(text));
          this.pendingTexts = [];
        }
      };

      this.sarvamWs.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          this.handleSarvamMessage(message);
        } catch (error) {
          console.error(`❌ [SARVAM-WS] Message parse error: ${error.message}`);
        }
      };

      this.sarvamWs.onerror = (error) => {
        clearTimeout(connectionTimeout);
        console.error(`❌ [SARVAM-WS] WebSocket error:`, error);
        this.sarvamReady = false;
        this.handleConnectionFailure();
      };

      this.sarvamWs.onclose = (event) => {
        clearTimeout(connectionTimeout);
        console.log(`🔌 [SARVAM-WS] Connection closed: ${event.code} - ${event.reason}`);
        this.sarvamReady = false;
        
        if (event.code !== 1000 && this.connectionAttempts < this.maxRetries) {
          this.handleConnectionFailure();
        }
      };

    } catch (error) {
      console.error(`❌ [SARVAM-WS] Initialization error: ${error.message}`);
      this.handleConnectionFailure();
    }
  }

  handleConnectionFailure() {
    this.connectionAttempts++;
    
    if (this.connectionAttempts < this.maxRetries) {
      const retryDelay = Math.min(1000 * Math.pow(2, this.connectionAttempts), 5000);
      console.log(`🔄 [SARVAM-WS] Retrying connection in ${retryDelay}ms...`);
      
      setTimeout(() => {
        this.initializeSarvamWebSocket();
      }, retryDelay);
    } else {
      console.error(`❌ [SARVAM-WS] Max retry attempts reached. TTS will not work.`);
    }
  }

  handleSarvamMessage(message) {
    if (message.type === "audio" && message.data?.audio) {
      const audioBase64 = message.data.audio;
      console.log(`🎵 [SARVAM-WS] Received audio chunk: ${audioBase64.length} characters`);
      
      // Stream the audio to the client immediately
      this.streamAudioToClient(audioBase64);
      
      this.totalChunks++;
      const audioBuffer = Buffer.from(audioBase64, "base64");
      this.totalAudioBytes += audioBuffer.length;
    } else if (message.type === "error") {
      console.error(`❌ [SARVAM-WS] Error from Sarvam: ${message.message}`);
    } else if (message.type === "config") {
      console.log(`✅ [SARVAM-WS] Configuration acknowledged`);
    } else {
      console.log(`📨 [SARVAM-WS] Received message: ${message.type}`);
    }
  }

  sendTextToSarvam(text) {
    if (!text.trim()) return;
    
    if (this.sarvamWs && this.sarvamReady && this.sarvamWs.readyState === WebSocket.OPEN) {
      const textMessage = {
        type: "text",
        data: {
          text: text
        }
      };
      
      try {
        this.sarvamWs.send(JSON.stringify(textMessage));
        console.log(`📤 [SARVAM-WS] Sent text: "${text}"`);
      } catch (error) {
        console.error(`❌ [SARVAM-WS] Error sending text: ${error.message}`);
        this.pendingTexts.push(text);
      }
    } else {
      console.log(`⏳ [SARVAM-WS] Queuing text (not ready): "${text}"`);
      this.pendingTexts.push(text);
    }
  }

  async streamAudioToClient(audioBase64) {
    try {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        console.warn(`⚠️ [STREAM-CLIENT] WebSocket not ready, skipping audio chunk`);
        return;
      }

      const audioBuffer = Buffer.from(audioBase64, "base64");
      
      const SAMPLE_RATE = 8000;
      const BYTES_PER_SAMPLE = 2;
      const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000;
      
      const MIN_CHUNK_SIZE = Math.floor(20 * BYTES_PER_MS); // Reduced minimum
      const MAX_CHUNK_SIZE = Math.floor(100 * BYTES_PER_MS);
      const OPTIMAL_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS);
      
      const alignToSample = (size) => Math.floor(size / 2) * 2;
      
      const minChunk = alignToSample(MIN_CHUNK_SIZE);
      const maxChunk = alignToSample(MAX_CHUNK_SIZE);
      const optimalChunk = alignToSample(OPTIMAL_CHUNK_SIZE);
      
      console.log(`📦 [SARVAM-STREAM] Streaming ${audioBuffer.length} bytes to client`);
      
      let position = 0;
      let chunkIndex = 0;
      
      while (position < audioBuffer.length && this.ws.readyState === WebSocket.OPEN) {
        const remaining = audioBuffer.length - position;
        let chunkSize;
        
        if (remaining <= maxChunk) {
          chunkSize = remaining >= minChunk ? remaining : minChunk;
        } else {
          chunkSize = optimalChunk;
        }
        
        chunkSize = Math.min(chunkSize, remaining);
        const chunk = audioBuffer.slice(position, position + chunkSize);
        
        if (chunk.length >= minChunk) {
          const durationMs = (chunk.length / BYTES_PER_MS).toFixed(1);
          
          console.log(`📤 [CLIENT-STREAM] Chunk ${chunkIndex + 1}: ${chunk.length} bytes (${durationMs}ms)`);
          
          const mediaMessage = {
            event: "media",
            streamSid: this.streamSid,
            media: {
              payload: chunk.toString("base64")
            }
          };

          try {
            this.ws.send(JSON.stringify(mediaMessage));
          } catch (error) {
            console.error(`❌ [CLIENT-STREAM] Error sending chunk: ${error.message}`);
            break;
          }
          
          const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS);
          const networkBufferMs = 2;
          const delayMs = Math.max(chunkDurationMs - networkBufferMs, 5); // Reduced minimum delay
          
          if (position + chunkSize < audioBuffer.length) {
            await new Promise(resolve => setTimeout(resolve, delayMs));
          }
          
          chunkIndex++;
        }
        
        position += chunkSize;
      }
      
    } catch (error) {
      console.error(`❌ [STREAM-CLIENT] Error: ${error.message}`);
    }
  }

  addPhrase(phrase) {
    if (!phrase.trim()) return;
    
    this.sentenceBuffer += (this.sentenceBuffer ? " " : "") + phrase.trim();
    
    if (this.hasCompleteSentence(this.sentenceBuffer)) {
      this.processCompleteSentences();
    } else {
      this.scheduleProcessing();
    }
  }

  hasCompleteSentence(text) {
    return /[.!?।॥]/.test(text);
  }

  extractCompleteSentences(text) {
    const sentences = text.split(/([.!?।॥])/).filter(s => s.trim());
    
    let completeSentences = "";
    let remainingText = "";
    
    for (let i = 0; i < sentences.length; i += 2) {
      const sentence = sentences[i];
      const punctuation = sentences[i + 1];
      
      if (punctuation) {
        completeSentences += sentence + punctuation + " ";
      } else {
        remainingText = sentence;
      }
    }
    
    return {
      complete: completeSentences.trim(),
      remaining: remainingText.trim()
    };
  }

  processCompleteSentences() {
    if (this.sentenceTimer) {
      clearTimeout(this.sentenceTimer);
      this.sentenceTimer = null;
    }

    const { complete, remaining } = this.extractCompleteSentences(this.sentenceBuffer);
    
    if (complete) {
      this.sendTextToSarvam(complete);
      this.sentenceBuffer = remaining;
    }
  }

  scheduleProcessing() {
    if (this.sentenceTimer) clearTimeout(this.sentenceTimer);
    
    this.sentenceTimer = setTimeout(() => {
      if (this.sentenceBuffer.trim()) {
        this.sendTextToSarvam(this.sentenceBuffer.trim());
        this.sentenceBuffer = "";
      }
    }, this.processingTimeout);
  }

  complete() {
    // Clear any pending timeout
    if (this.sentenceTimer) {
      clearTimeout(this.sentenceTimer);
      this.sentenceTimer = null;
    }
    
    // Send any remaining text
    if (this.sentenceBuffer.trim()) {
      this.sendTextToSarvam(this.sentenceBuffer.trim());
      this.sentenceBuffer = "";
    }
    
    // Send flush message to Sarvam to process remaining buffer
    if (this.sarvamWs && this.sarvamReady && this.sarvamWs.readyState === WebSocket.OPEN) {
      try {
        const flushMessage = { type: "flush" };
        this.sarvamWs.send(JSON.stringify(flushMessage));
        console.log(`🔄 [SARVAM-WS] Sent flush message`);
      } catch (error) {
        console.error(`❌ [SARVAM-WS] Error sending flush: ${error.message}`);
      }
    }
    
    console.log(`📊 [SARVAM-WS-STATS] Total: ${this.totalChunks} chunks, ${this.totalAudioBytes} bytes`);
  }

  close() {
    if (this.sentenceTimer) {
      clearTimeout(this.sentenceTimer);
      this.sentenceTimer = null;
    }
    
    if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.OPEN) {
      this.sarvamWs.close(1000, "Normal closure");
      console.log(`🔌 [SARVAM-WS] Closed connection`);
    }
  }

  getStats() {
    return {
      totalChunks: this.totalChunks,
      totalAudioBytes: this.totalAudioBytes,
      avgBytesPerChunk: this.totalChunks > 0 ? Math.round(this.totalAudioBytes / this.totalChunks) : 0,
      isReady: this.sarvamReady,
      connectionAttempts: this.connectionAttempts
    };
  }
}

// Enhanced function to stream pre-recorded audio from database
const streamPreRecordedAudio = async (audioBase64, ws, streamSid) => {
  const timer = createTimer("PRERECORDED_AUDIO_STREAM");
  
  try {
    if (!audioBase64) {
      console.log(`❌ [PRERECORDED] No audio data provided`);
      return;
    }

    if (!ws || ws.readyState !== WebSocket.OPEN) {
      console.error(`❌ [PRERECORDED] WebSocket not ready`);
      return;
    }

    console.log(`🎵 [PRERECORDED] Streaming stored audio`);
    
    const audioBuffer = Buffer.from(audioBase64, "base64");
    
    const SAMPLE_RATE = 8000;
    const BYTES_PER_SAMPLE = 2;
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000;
    
    const OPTIMAL_CHUNK_SIZE = Math.floor(20 * BYTES_PER_MS);
    const alignToSample = (size) => Math.floor(size / 2) * 2;
    const optimalChunk = alignToSample(OPTIMAL_CHUNK_SIZE);
    
    console.log(`📦 [PRERECORDED] Streaming ${audioBuffer.length} bytes`);
    
    let position = 0;
    let chunkIndex = 0;
    
    while (position < audioBuffer.length && ws.readyState === WebSocket.OPEN) {
      const remaining = audioBuffer.length - position;
      const chunkSize = Math.min(optimalChunk, remaining);
      const chunk = audioBuffer.slice(position, position + chunkSize);
      
      const durationMs = (chunk.length / BYTES_PER_MS).toFixed(1);
      console.log(`📤 [PRERECORDED] Chunk ${chunkIndex + 1}: ${chunk.length} bytes (${durationMs}ms)`);
      
      const mediaMessage = {
        event: "media",
        streamSid: streamSid,
        media: {
          payload: chunk.toString("base64")
        }
      };

      try {
        ws.send(JSON.stringify(mediaMessage));
      } catch (error) {
        console.error(`❌ [PRERECORDED] Error sending chunk: ${error.message}`);
        break;
      }
      
      const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS);
      const delayMs = Math.max(chunkDurationMs - 2, 5);
      
      if (position + chunkSize < audioBuffer.length) {
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
      
      chunkIndex++;
      position += chunkSize;
    }
    
    console.log(`✅ [PRERECORDED] Completed streaming ${chunkIndex} chunks in ${timer.end()}ms`);
    
  } catch (error) {
    console.error(`❌ [PRERECORDED] Error streaming audio: ${error.message}`);
  }
};

// Main WebSocket server setup with improved error handling
const setupUnifiedVoiceServer = (wss) => {
  console.log("🚀 [DB-INTEGRATED-WS] Voice Server started with WebSocket Sarvam TTS");

  wss.on("connection", (ws, req) => {
    console.log("🔗 [CONNECTION] New database-integrated WebSocket connection");

    // Session state
    let streamSid = null;
    let conversationHistory = [];
    let isProcessing = false;
    let userUtteranceBuffer = "";
    let lastProcessedText = "";
    let optimizedTTS = null;
    let agentConfig = null;
    let apiKeys = {};
    let sessionActive = false;

    // Deepgram WebSocket connection
    let deepgramWs = null;
    let deepgramReady = false;
    let deepgramAudioQueue = [];
    let deepgramReconnectAttempts = 0;
    const MAX_DEEPGRAM_RETRIES = 3;

    // Enhanced Deepgram connection with retry logic
    const connectToDeepgram = async () => {
      try {
        if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
          return;
        }

        console.log("🔌 [DEEPGRAM] Connecting...");
        const deepgramLanguage = getDeepgramLanguage(agentConfig.language);
        
        const deepgramUrl = new URL("wss://api.deepgram.com/v1/listen");
        deepgramUrl.searchParams.append("sample_rate", "8000");
        deepgramUrl.searchParams.append("channels", "1");
        deepgramUrl.searchParams.append("encoding", "linear16");
        deepgramUrl.searchParams.append("model", "nova-2");
        deepgramUrl.searchParams.append("language", deepgramLanguage);
        deepgramUrl.searchParams.append("interim_results", "true");
        deepgramUrl.searchParams.append("smart_format", "true");
        deepgramUrl.searchParams.append("endpointing", "300");
        deepgramUrl.searchParams.append("utterance_end_ms", "1000");

        deepgramWs = new WebSocket(deepgramUrl.toString(), {
          headers: { Authorization: `Token ${apiKeys.deepgram}` },
        });

        // Set connection timeout
        const connectionTimeout = setTimeout(() => {
          if (deepgramWs.readyState === WebSocket.CONNECTING) {
            console.error("❌ [DEEPGRAM] Connection timeout");
            deepgramWs.close();
            handleDeepgramConnectionFailure();
          }
        }, 10000);

        deepgramWs.onopen = () => {
          clearTimeout(connectionTimeout);
          deepgramReady = true;
          deepgramReconnectAttempts = 0;
          console.log("✅ [DEEPGRAM] Connected");
          
          // Process any queued audio
          deepgramAudioQueue.forEach(buffer => {
            if (deepgramWs.readyState === WebSocket.OPEN) {
              deepgramWs.send(buffer);
            }
          });
          deepgramAudioQueue = [];
        };

        deepgramWs.onmessage = async (event) => {
          const data = JSON.parse(event.data);
          await handleDeepgramResponse(data);
        };

        deepgramWs.onerror = (error) => {
          clearTimeout(connectionTimeout);
          console.error("❌ [DEEPGRAM] Error:", error);
          deepgramReady = false;
          handleDeepgramConnectionFailure();
        };

        deepgramWs.onclose = (event) => {
          clearTimeout(connectionTimeout);
          console.log(`🔌 [DEEPGRAM] Connection closed: ${event.code} - ${event.reason}`);
          deepgramReady = false;
          
          if (event.code !== 1000 && sessionActive) {
            handleDeepgramConnectionFailure();
          }
        };

      } catch (error) {
        console.error("❌ [DEEPGRAM] Setup error:", error.message);
        handleDeepgramConnectionFailure();
      }
    };

    const handleDeepgramConnectionFailure = () => {
      deepgramReconnectAttempts++;
      
      if (deepgramReconnectAttempts < MAX_DEEPGRAM_RETRIES && sessionActive) {
        const retryDelay = Math.min(1000 * Math.pow(2, deepgramReconnectAttempts), 5000);
        console.log(`🔄 [DEEPGRAM] Retrying connection in ${retryDelay}ms...`);
        
        setTimeout(() => {
          connectToDeepgram();
        }, retryDelay);
      } else {
        console.error("❌ [DEEPGRAM] Max retry attempts reached. Speech recognition may not work.");
      }
    };

    // Enhanced Deepgram response handling
    const handleDeepgramResponse = async (data) => {
      try {
        if (data.type === "Results") {
          const transcript = data.channel?.alternatives?.[0]?.transcript;
          const is_final = data.is_final;
          
          if (transcript?.trim()) {
            console.log(`🎤 [DEEPGRAM] ${is_final ? "Final" : "Interim"}: "${transcript}"`);
            
            if (is_final) {
              userUtteranceBuffer += (userUtteranceBuffer ? " " : "") + transcript.trim();
              await processUserUtterance(userUtteranceBuffer);
              userUtteranceBuffer = "";
            } else {
              // For interim results, we can update the buffer but wait for final confirmation
              userUtteranceBuffer = transcript.trim();
            }
          }
        } else if (data.type === "UtteranceEnd") {
          console.log("🔚 [DEEPGRAM] Utterance end detected");
          if (userUtteranceBuffer.trim()) {
            await processUserUtterance(userUtteranceBuffer);
            userUtteranceBuffer = "";
          }
        } else if (data.type === "Metadata") {
          console.log(`ℹ️ [DEEPGRAM] Metadata: ${JSON.stringify(data)}`);
        }
      } catch (error) {
        console.error(`❌ [DEEPGRAM] Response handling error: ${error.message}`);
      }
    };

    // Enhanced utterance processing with better state management
    const processUserUtterance = async (text) => {
      if (!text.trim() || isProcessing || !sessionActive) return;

      // Skip if the text is too similar to the last processed one
      if (lastProcessedText && text.toLowerCase() === lastProcessedText.toLowerCase()) {
        console.log("⏭️ [PROCESSING] Skipping duplicate utterance");
        return;
      }

      isProcessing = true;
      lastProcessedText = text;
      const timer = createTimer("UTTERANCE_PROCESSING");

      try {
        console.log(`🎤 [USER] Processing: "${text}"`);

        // Initialize TTS processor if not already done
        if (!optimizedTTS) {
          optimizedTTS = new WebSocketSarvamTTSProcessor(
            agentConfig.language, 
            ws, 
            streamSid, 
            apiKeys, 
            agentConfig.voiceSelection
          );
        }

        const response = await processWithOpenAIStreaming(
          text,
          conversationHistory,
          agentConfig.systemPrompt,
          apiKeys,
          (phrase) => {
            console.log(`📤 [PHRASE] "${phrase}"`);
            optimizedTTS.addPhrase(phrase);
          },
          (fullResponse) => {
            console.log(`✅ [COMPLETE] "${fullResponse}"`);
            optimizedTTS.complete();
            
            const stats = optimizedTTS.getStats();
            console.log(`📊 [TTS-WS-STATS] ${stats.totalChunks} chunks, ${stats.avgBytesPerChunk} avg bytes/chunk`);
            
            // Only add to history if the response was successful
            if (fullResponse && !fullResponse.includes("I'm here to help")) {
              conversationHistory.push(
                { role: "user", content: text },
                { role: "assistant", content: fullResponse }
              );

              if (conversationHistory.length > 10) {
                conversationHistory = conversationHistory.slice(-10);
              }
            }
          }
        );

        console.log(`⚡ [TOTAL] Processing time: ${timer.end()}ms`);

      } catch (error) {
        console.error(`❌ [PROCESSING] Error: ${error.message}`);
      } finally {
        isProcessing = false;
      }
    };

    // Enhanced initial greeting with fallback
    const sendInitialGreeting = async () => {
      console.log("👋 [GREETING] Sending initial greeting");
      
      try {
        if (agentConfig.audioBytes) {
          console.log("🎵 [GREETING] Using stored audio from database");
          await streamPreRecordedAudio(agentConfig.audioBytes, ws, streamSid);
        } else if (agentConfig.firstMessage) {
          console.log("🎵 [GREETING] Using WebSocket TTS for first message");
          optimizedTTS = new WebSocketSarvamTTSProcessor(
            agentConfig.language, 
            ws, 
            streamSid, 
            apiKeys, 
            agentConfig.voiceSelection
          );
          
          // Wait briefly for WebSocket to initialize
          await new Promise(resolve => setTimeout(resolve, 300));
          
          optimizedTTS.addPhrase(agentConfig.firstMessage);
          optimizedTTS.complete();
          
          // Add to conversation history
          conversationHistory.push(
            { role: "assistant", content: agentConfig.firstMessage }
          );
        }
      } catch (error) {
        console.error(`❌ [GREETING] Error: ${error.message}`);
      }
    };

    // WebSocket message handling with better error handling
    ws.on("message", async (message) => {
      try {
        const data = JSON.parse(message.toString());
        console.log(`📨 [MESSAGE] Received event: ${data.event}`);

        switch (data.event) {
          case "connected":
            console.log(`🔗 [CONNECTED] Protocol: ${data.protocol}`);
            break;

          case "start":
            sessionActive = true;
            streamSid = data.streamSid || data.start?.streamSid;
            const accountSid = data.start?.accountSid || data.accountSid;
            
            console.log(`🎯 [START] Stream started - StreamSid: ${streamSid}, AccountSid: ${accountSid}`);
            
            // Load configuration
            const configLoaded = await loadAgentConfiguration(accountSid);
            if (!configLoaded) {
              console.error(`❌ [CONFIG] Failed to load configuration`);
              ws.close();
              return;
            }
            
            // Connect to Deepgram and send greeting
            await connectToDeepgram();
            await sendInitialGreeting();
            break;

          case "media":
            if (data.media?.payload) {
              const audioBuffer = Buffer.from(data.media.payload, "base64");
              
              if (deepgramWs && deepgramReady && deepgramWs.readyState === WebSocket.OPEN) {
                try {
                  deepgramWs.send(audioBuffer);
                } catch (error) {
                  console.error(`❌ [DEEPGRAM] Error sending audio: ${error.message}`);
                  deepgramAudioQueue.push(audioBuffer);
                }
              } else {
                deepgramAudioQueue.push(audioBuffer);
              }
            }
            break;

          case "stop":
            console.log(`📞 [STOP] Stream stopped`);
            sessionActive = false;
            if (deepgramWs?.readyState === WebSocket.OPEN) {
              deepgramWs.close();
            }
            if (optimizedTTS) {
              optimizedTTS.close();
            }
            break;

          case "mark":
            console.log(`📍 [MARK] Received mark: ${data.mark?.name}`);
            break;

          default:
            console.log(`❓ [UNKNOWN] Unknown event: ${data.event}`);
        }
      } catch (error) {
        console.error(`❌ [MESSAGE-HANDLER] Error: ${error.message}`);
      }
    });

    // Connection cleanup
    ws.on("close", () => {
      console.log("🔗 [CLOSE] Connection closed");
      sessionActive = false;
      
      if (deepgramWs?.readyState === WebSocket.OPEN) {
        deepgramWs.close();
      }

      if (optimizedTTS) {
        optimizedTTS.close();
      }

      // Reset state
      streamSid = null;
      conversationHistory = [];
      isProcessing = false;
      userUtteranceBuffer = "";
      lastProcessedText = "";
      deepgramReady = false;
      deepgramAudioQueue = [];
      optimizedTTS = null;
      agentConfig = null;
      apiKeys = {};
    });

    ws.on("error", (error) => {
      console.error(`❌ [ERROR] WebSocket error: ${error.message}`);
    });
  });
};

module.exports = { setupUnifiedVoiceServer };