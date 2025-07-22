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
    
    // Find agent by accountSid (you might need to adjust the field name based on your schema)
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
    
    const keys = {};
    for (const apiKey of apiKeys) {
      try {
        const decryptedKey = apiKey.getDecryptedKey();
        keys[apiKey.provider] = decryptedKey;
        console.log(`✅ [DB] Loaded ${apiKey.provider} API key`);
      } catch (error) {
        console.error(`❌ [DB] Failed to decrypt ${apiKey.provider} key: ${error.message}`);
      }
    }
    
    return keys;
  } catch (error) {
    console.error(`❌ [DB] Error fetching API keys: ${error.message}`);
    return {};
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

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKeys.openai}`,
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
  
  if (/[.!?।]$/.test(trimmed)) return true;
  if (trimmed.length >= 8 && /[,;।]\s*$/.test(trimmed)) return true;
  if (trimmed.length >= 25 && /\s/.test(trimmed)) return true;
  
  return false;
};

// Enhanced TTS processor with sentence-based optimization and SIP streaming
class OptimizedSarvamTTSProcessor {
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
    this.processingTimeout = 100;
    this.sentenceTimer = null;
    
    this.totalChunks = 0;
    this.totalAudioBytes = 0;
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
      this.queue.push(complete);
      this.sentenceBuffer = remaining;
      this.processQueue();
    }
  }

  scheduleProcessing() {
    if (this.sentenceTimer) clearTimeout(this.sentenceTimer);
    
    this.sentenceTimer = setTimeout(() => {
      if (this.sentenceBuffer.trim()) {
        this.queue.push(this.sentenceBuffer.trim());
        this.sentenceBuffer = "";
        this.processQueue();
      }
    }, this.processingTimeout);
  }

  async processQueue() {
    if (this.isProcessing || this.queue.length === 0) return;

    this.isProcessing = true;
    const textToProcess = this.queue.shift();

    try {
      await this.synthesizeAndStream(textToProcess);
    } catch (error) {
      console.error(`❌ [SARVAM-TTS] Error: ${error.message}`);
    } finally {
      this.isProcessing = false;
      
      if (this.queue.length > 0) {
        setTimeout(() => this.processQueue(), 10);
      }
    }
  }

  async synthesizeAndStream(text) {
    const timer = createTimer("SARVAM_TTS_SENTENCE");
    
    try {
      if (!this.apiKeys.sarvam) {
        throw new Error("Sarvam API key not found");
      }

      console.log(`🎵 [SARVAM-TTS] Synthesizing: "${text}" (${this.sarvamLanguage})`);

      const response = await fetch("https://api.sarvam.ai/text-to-speech", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "API-Subscription-Key": this.apiKeys.sarvam,
        },
        body: JSON.stringify({
          inputs: [text],
          target_language_code: this.sarvamLanguage,
          speaker: this.voice,
          pitch: 0,
          pace: 1.0,
          loudness: 1.0,
          speech_sample_rate: 8000,
          enable_preprocessing: false,
          model: "bulbul:v1",
        }),
      });

      if (!response.ok) {
        throw new Error(`Sarvam API error: ${response.status} - ${response.statusText}`);
      }

      const responseData = await response.json();
      const audioBase64 = responseData.audios?.[0];
      
      if (!audioBase64) {
        throw new Error("No audio data received from Sarvam API");
      }

      console.log(`⚡ [SARVAM-TTS] Synthesis completed in ${timer.end()}ms`);
      
      await this.streamAudioOptimizedForSIP(audioBase64);
      
      const audioBuffer = Buffer.from(audioBase64, "base64");
      this.totalAudioBytes += audioBuffer.length;
      this.totalChunks++;
      
    } catch (error) {
      console.error(`❌ [SARVAM-TTS] Synthesis error: ${error.message}`);
      throw error;
    }
  }

  async streamAudioOptimizedForSIP(audioBase64) {
    const audioBuffer = Buffer.from(audioBase64, "base64");
    
    const SAMPLE_RATE = 8000;
    const BYTES_PER_SAMPLE = 2;
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000;
    
    const MIN_CHUNK_SIZE = Math.floor(20 * BYTES_PER_MS);
    const MAX_CHUNK_SIZE = Math.floor(100 * BYTES_PER_MS);
    const OPTIMAL_CHUNK_SIZE = Math.floor(20 * BYTES_PER_MS);
    
    const alignToSample = (size) => Math.floor(size / 2) * 2;
    
    const minChunk = alignToSample(MIN_CHUNK_SIZE);
    const maxChunk = alignToSample(MAX_CHUNK_SIZE);
    const optimalChunk = alignToSample(OPTIMAL_CHUNK_SIZE);
    
    console.log(`📦 [SARVAM-SIP] Streaming ${audioBuffer.length} bytes`);
    
    let position = 0;
    let chunkIndex = 0;
    
    while (position < audioBuffer.length) {
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
        
        console.log(`📤 [SARVAM-SIP] Chunk ${chunkIndex + 1}: ${chunk.length} bytes (${durationMs}ms)`);
        
        const mediaMessage = {
          event: "media",
          streamSid: this.streamSid,
          media: {
            payload: chunk.toString("base64")
          }
        };

        if (this.ws.readyState === WebSocket.OPEN) {
          this.ws.send(JSON.stringify(mediaMessage));
        }
        
        const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS);
        const networkBufferMs = 2;
        const delayMs = Math.max(chunkDurationMs - networkBufferMs, 10);
        
        if (position + chunkSize < audioBuffer.length) {
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
        
        chunkIndex++;
      }
      
      position += chunkSize;
    }
    
    console.log(`✅ [SARVAM-SIP] Completed streaming ${chunkIndex} chunks`);
  }

  complete() {
    if (this.sentenceBuffer.trim()) {
      this.queue.push(this.sentenceBuffer.trim());
      this.sentenceBuffer = "";
    }
    
    if (this.queue.length > 0) {
      this.processQueue();
    }
    
    console.log(`📊 [SARVAM-STATS] Total: ${this.totalChunks} sentences, ${this.totalAudioBytes} bytes`);
  }

  getStats() {
    return {
      totalChunks: this.totalChunks,
      totalAudioBytes: this.totalAudioBytes,
      avgBytesPerChunk: this.totalChunks > 0 ? Math.round(this.totalAudioBytes / this.totalChunks) : 0
    };
  }
}

// Function to stream pre-recorded audio from database - FIXED VERSION
const streamPreRecordedAudio = async (audioBase64, ws, streamSid) => {
  const timer = createTimer("PRERECORDED_AUDIO_STREAM");
  
  try {
    if (!audioBase64) {
      console.log(`❌ [PRERECORDED] No audio data provided`);
      return;
    }

    console.log(`🎵 [PRERECORDED] Streaming stored audio`);
    
    const audioBuffer = Buffer.from(audioBase64, "base64");
    
    const SAMPLE_RATE = 8000;
    const BYTES_PER_SAMPLE = 2;
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000;
    
    // Smaller chunks for smoother streaming (10ms instead of 20ms)
    const OPTIMAL_CHUNK_SIZE = Math.floor(10 * BYTES_PER_MS);
    const alignToSample = (size) => Math.floor(size / 2) * 2;
    const optimalChunk = alignToSample(OPTIMAL_CHUNK_SIZE);
    
    console.log(`📦 [PRERECORDED] Streaming ${audioBuffer.length} bytes in ${optimalChunk}-byte chunks`);
    
    let position = 0;
    let chunkIndex = 0;
    
    // Send all chunks rapidly without artificial delays
    while (position < audioBuffer.length) {
      const remaining = audioBuffer.length - position;
      const chunkSize = Math.min(optimalChunk, remaining);
      const chunk = audioBuffer.slice(position, position + chunkSize);
      
      const mediaMessage = {
        event: "media",
        streamSid: streamSid,
        media: {
          payload: chunk.toString("base64")
        }
      };

      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(mediaMessage));
      }
      
      chunkIndex++;
      position += chunkSize;
      
      // Minimal delay only to prevent overwhelming the WebSocket (1ms)
      if (position < audioBuffer.length) {
        await new Promise(resolve => setTimeout(resolve, 1));
      }
    }
    
    console.log(`✅ [PRERECORDED] Completed streaming ${chunkIndex} chunks in ${timer.end()}ms`);
    
  } catch (error) {
    console.error(`❌ [PRERECORDED] Error streaming audio: ${error.message}`);
  }
};

// Alternative: Send entire audio as one message (if your system supports it)
const streamPreRecordedAudioAsSingleMessage = async (audioBase64, ws, streamSid) => {
  const timer = createTimer("PRERECORDED_AUDIO_SINGLE");
  
  try {
    if (!audioBase64) {
      console.log(`❌ [PRERECORDED] No audio data provided`);
      return;
    }

    console.log(`🎵 [PRERECORDED] Streaming stored audio as single message`);
    
    const mediaMessage = {
      event: "media",
      streamSid: streamSid,
      media: {
        payload: audioBase64
      }
    };

    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(mediaMessage));
      console.log(`✅ [PRERECORDED] Sent complete audio in ${timer.end()}ms`);
    }
    
  } catch (error) {
    console.error(`❌ [PRERECORDED] Error streaming audio: ${error.message}`);
  }
};

// Updated sendInitialGreeting function
const sendInitialGreeting = async () => {
  console.log("👋 [GREETING] Sending initial greeting from database");
  
  if (agentConfig.audioBytes) {
    console.log("🎵 [GREETING] Using stored audio from database");
    
    // Try sending as single message first (recommended for greetings)
    try {
      await streamPreRecordedAudioAsSingleMessage(agentConfig.audioBytes, ws, streamSid);
    } catch (error) {
      console.log("🎵 [GREETING] Single message failed, falling back to chunked streaming");
      await streamPreRecordedAudio(agentConfig.audioBytes, ws, streamSid);
    }
  } else {
    console.log("🎵 [GREETING] No stored audio, using TTS for first message");
    const tts = new OptimizedSarvamTTSProcessor(
      agentConfig.language, 
      ws, 
      streamSid, 
      apiKeys, 
      agentConfig.voiceSelection
    );
    await tts.synthesizeAndStream(agentConfig.firstMessage);
  }
};

// Main WebSocket server setup
const setupUnifiedVoiceServer = (wss) => {
  console.log("🚀 [DB-INTEGRATED] Voice Server started");

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

    // Deepgram WebSocket connection
    let deepgramWs = null;
    let deepgramReady = false;
    let deepgramAudioQueue = [];

    // Load agent configuration and API keys
    const loadAgentConfiguration = async (accountSid) => {
      try {
        console.log(`🔧 [CONFIG] Loading configuration for accountSid: ${accountSid}`);
        
        // Get agent configuration
        agentConfig = await getAgentByAccountSid(accountSid);
        if (!agentConfig) {
          console.error(`❌ [CONFIG] No agent found for accountSid: ${accountSid}`);
          return false;
        }
        
        // Get API keys for this tenant
        apiKeys = await getApiKeys(agentConfig.tenantId);
        if (!apiKeys.openai || !apiKeys.deepgram || !apiKeys.sarvam) {
          console.error(`❌ [CONFIG] Missing required API keys for tenant: ${agentConfig.tenantId}`);
          return false;
        }
        
        console.log(`✅ [CONFIG] Configuration loaded successfully`);
        console.log(`🤖 [AGENT] Name: ${agentConfig.agentName}, Language: ${agentConfig.language}, Voice: ${agentConfig.voiceSelection}`);
        
        return true;
      } catch (error) {
        console.error(`❌ [CONFIG] Error loading configuration: ${error.message}`);
        return false;
      }
    };

    // Optimized Deepgram connection
    const connectToDeepgram = async () => {
      try {
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

        deepgramWs = new WebSocket(deepgramUrl.toString(), {
          headers: { Authorization: `Token ${apiKeys.deepgram}` },
        });

        deepgramWs.onopen = () => {
          deepgramReady = true;
          console.log("✅ [DEEPGRAM] Connected");
          
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

    // Optimized utterance processing
    const processUserUtterance = async (text) => {
      if (!text.trim() || isProcessing || text === lastProcessedText) return;

      isProcessing = true;
      lastProcessedText = text;
      const timer = createTimer("UTTERANCE_PROCESSING");

      try {
        console.log(`🎤 [USER] Processing: "${text}"`);

        optimizedTTS = new OptimizedSarvamTTSProcessor(
          agentConfig.language, 
          ws, 
          streamSid, 
          apiKeys, 
          agentConfig.voiceSelection
        );

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
            console.log(`📊 [TTS-STATS] ${stats.totalChunks} chunks, ${stats.avgBytesPerChunk} avg bytes/chunk`);
            
            conversationHistory.push(
              { role: "user", content: text },
              { role: "assistant", content: fullResponse }
            );

            if (conversationHistory.length > 10) {
              conversationHistory = conversationHistory.slice(-10);
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

    // Send initial greeting using stored audioBytes
    const sendInitialGreeting = async () => {
      console.log("👋 [GREETING] Sending initial greeting from database");
      
      if (agentConfig.audioBytes) {
        console.log("🎵 [GREETING] Using stored audio from database");
        await streamPreRecordedAudio(agentConfig.audioBytes, ws, streamSid);
      } else {
        console.log("🎵 [GREETING] No stored audio, using TTS for first message");
        const tts = new OptimizedSarvamTTSProcessor(
          agentConfig.language, 
          ws, 
          streamSid, 
          apiKeys, 
          agentConfig.voiceSelection
        );
        await tts.synthesizeAndStream(agentConfig.firstMessage);
      }
    };

    // WebSocket message handling
    ws.on("message", async (message) => {
      try {
        const data = JSON.parse(message.toString());

        switch (data.event) {
          case "connected":
            console.log(`🔗 [DB-INTEGRATED] Connected - Protocol: ${data.protocol}`);
            break;

          case "start":
            streamSid = data.streamSid || data.start?.streamSid;
            const accountSid = data.start?.accountSid || data.accountSid;
            
            console.log(`🎯 [DB-INTEGRATED] Stream started - StreamSid: ${streamSid}, AccountSid: ${accountSid}`);
            
            // Load configuration based on accountSid
            const configLoaded = await loadAgentConfiguration(accountSid);
            if (!configLoaded) {
              console.error(`❌ [DB-INTEGRATED] Failed to load configuration, closing connection`);
              ws.close();
              return;
            }
            
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
            console.log(`📞 [DB-INTEGRATED] Stream stopped`);
            if (deepgramWs?.readyState === WebSocket.OPEN) {
              deepgramWs.close();
            }
            break;

          default:
            console.log(`❓ [DB-INTEGRATED] Unknown event: ${data.event}`);
        }
      } catch (error) {
        console.error(`❌ [DB-INTEGRATED] Message error: ${error.message}`);
      }
    });

    // Connection cleanup
    ws.on("close", () => {
      console.log("🔗 [DB-INTEGRATED] Connection closed");
      
      if (deepgramWs?.readyState === WebSocket.OPEN) {
        deepgramWs.close();
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
      console.error(`❌ [DB-INTEGRATED] WebSocket error: ${error.message}`);
    });
  });
};

module.exports = { setupUnifiedVoiceServer };