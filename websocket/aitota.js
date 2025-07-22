const WebSocket = require("ws");
require("dotenv").config();

// Load API keys from environment variables
const API_KEYS = {
  deepgram: process.env.DEEPGRAM_API_KEY,
  lmnt: process.env.LMNT_API_KEY, // Changed from sarvam to lmnt
  openai: process.env.OPENAI_API_KEY,
};

// Validate API keys
if (!API_KEYS.deepgram || !API_KEYS.lmnt || !API_KEYS.openai) {
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

// Language mappings for Deepgram
const DEEPGRAM_LANGUAGE_MAPPING = {
  hi: "hi", en: "en-IN", bn: "bn", te: "te", ta: "ta",
  mr: "mr", gu: "gu", kn: "kn", ml: "ml", pa: "pa",
  or: "or", as: "as", ur: "ur",
};

const getDeepgramLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang;
  return DEEPGRAM_LANGUAGE_MAPPING[lang] || "hi";
};

// LMNT voice configurations
const LMNT_VOICES = {
  // English voices
  en: {
    default: "lily", // Default English voice
    options: ["lily", "daniel", "sarah", "alex", "maya", "josh"]
  },
  // Hindi and other languages - LMNT supports multilingual voices
  hi: {
    default: "lily", // LMNT voices are multilingual
    options: ["lily", "daniel", "sarah"]
  },
  // For other languages, we'll use the same voices as they're multilingual
  default: {
    default: "lily",
    options: ["lily", "daniel", "sarah", "alex"]
  }
};

const getLMNTVoice = (language = "en", voicePreference = "default") => {
  const lang = language.toLowerCase();
  const voiceConfig = LMNT_VOICES[lang] || LMNT_VOICES.default;
  
  if (voicePreference === "default" || voicePreference === "neutral") {
    return voiceConfig.default;
  }
  
  // Voice preference mapping
  const voiceMapping = {
    "male-professional": "daniel",
    "female-professional": "lily",
    "male-friendly": "alex",
    "female-friendly": "sarah",
    "energetic": "josh",
    "calm": "maya"
  };
  
  const selectedVoice = voiceMapping[voicePreference] || voiceConfig.default;
  
  // Ensure the voice is available for the language
  if (voiceConfig.options.includes(selectedVoice)) {
    return selectedVoice;
  }
  
  return voiceConfig.default;
};

// Language detection using OpenAI
const detectLanguageWithOpenAI = async (text) => {
  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEYS.openai}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content: `You are a language detection expert. Analyze the given text and return ONLY the 2-letter language code (hi, en, bn, te, ta, mr, gu, kn, ml, pa, or, as, ur). 

Examples:
- "Hello, how are you?" → en
- "नमस्ते, आप कैसे हैं?" → hi
- "আপনি কেমন আছেন?" → bn
- "நீங்கள் எப்படி இருக்கிறீர்கள்?" → ta

Return only the language code, nothing else.`
          },
          {
            role: "user",
            content: text
          }
        ],
        max_tokens: 10,
        temperature: 0.1,
      }),
    });

    if (!response.ok) {
      throw new Error(`Language detection failed: ${response.status}`);
    }

    const data = await response.json();
    const detectedLang = data.choices[0]?.message?.content?.trim().toLowerCase();
    
    // Validate detected language
    const validLanguages = Object.keys(DEEPGRAM_LANGUAGE_MAPPING);
    if (validLanguages.includes(detectedLang)) {
      console.log(`🔍 [LANG-DETECT] Detected: "${detectedLang}" from text: "${text.substring(0, 50)}..."`);
      return detectedLang;
    }
    
    console.log(`⚠️ [LANG-DETECT] Invalid language "${detectedLang}", defaulting to "hi"`);
    return "hi"; // Default fallback
    
  } catch (error) {
    console.error(`❌ [LANG-DETECT] Error: ${error.message}`);
    return "hi"; // Default fallback
  }
};

// Basic configuration
const DEFAULT_CONFIG = {
  agentName: "हिंदी सहायक",
  language: "hi",
  voiceSelection: "default",
  firstMessage: "नमस्कार! एआई तोता में संपर्क करने के लिए धन्यवाद। बताइए, मैं आपकी किस प्रकार मदद कर सकता हूँ?",
  personality: "friendly",
  category: "customer service",
  contextMemory: "customer service conversation in Hindi",
};

// Optimized OpenAI streaming with phrase-based chunking and language detection
const processWithOpenAIStreaming = async (userMessage, conversationHistory, detectedLanguage, onPhrase, onComplete, onInterrupt) => {
  const timer = createTimer("OPENAI_STREAMING");
  
  try {
    // Dynamic system prompt based on detected language
    const getSystemPrompt = (lang) => {
      const prompts = {
        hi: "आप एआई तोता हैं, एक विनम्र और भावनात्मक रूप से बुद्धिमान AI ग्राहक सेवा कार्यकारी। आप हिंदी में धाराप्रवाह बोलते हैं। प्राकृतिक, बातचीत की भाषा का प्रयोग करें जो गर्मजोशी और सहानुभूति से भरी हो। जवाब छोटे रखें—केवल 1-2 लाइन। ग्राहकों को सुना, समर्थित और मूल्यवान महसूस कराना आपका लक्ष्य है।",
        
        en: "You are Aitota, a polite, emotionally intelligent AI customer care executive. You speak fluently in English. Use natural, conversational language with warmth and empathy. Keep responses short—just 1–2 lines. Your goal is to make customers feel heard, supported, and valued.",
        
        bn: "আপনি আইতোতা, একজন ভদ্র এবং আবেগপ্রবণভাবে বুদ্ধিমান AI গ্রাহক সেবা কর্মকর্তা। আপনি বাংলায় সাবলীলভাবে কথা বলেন। উষ্ণতা এবং সহানুভূতি সহ প্রাকৃতিক, কথোপকথনমূলক ভাষা ব্যবহার করুন।",
        
        te: "మీరు ఐతోతా, మర్యాదపూర్వక, భావోద్వేగంతో తెలివైన AI కస్టమర్ కేర్ ఎగ్జిక్యూటివ్. మీరు తెలుగులో సరళంగా మాట్లాడుతారు। వెచ్చదనం మరియు సానుభూతితో సహజమైన, సంభాషణా భాషను ఉపయోగించండి।",
        
        ta: "நீங்கள் ஐதோதா, ஒரு கண்ணியமான, உணர்வுபூர்வமாக புத்திசாலித்தனமான AI வாடிக்கையாளர் சேவை நிர்வாகி. நீங்கள் தமிழில் சரளமாக பேசுகிறீர்கள். அன்பு மற்றும் அனுதாபத்துடன் இயற்கையான, உரையாடல் மொழியைப் பயன்படுத்துங்கள்।"
      };
      
      return prompts[lang] || prompts.en;
    };

    const systemPrompt = getSystemPrompt(detectedLanguage);

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
        max_tokens: 80,
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
    let isInterrupted = false;

    const reader = response.body.getReader();
    const decoder = new TextDecoder();

    // Check for interruption periodically
    const checkInterruption = () => {
      return onInterrupt && onInterrupt();
    };

    while (true) {
      // Check for interruption
      if (checkInterruption()) {
        isInterrupted = true;
        console.log(`⚠️ [OPENAI] Stream interrupted by new user input`);
        reader.cancel();
        break;
      }

      const { done, value } = await reader.read();
      if (done) break;

      const chunk = decoder.decode(value, { stream: true });
      const lines = chunk.split('\n').filter(line => line.trim());

      for (const line of lines) {
        if (line.startsWith('data: ')) {
          const data = line.slice(6);
          
          if (data === '[DONE]') {
            if (phraseBuffer.trim() && !isInterrupted) {
              onPhrase(phraseBuffer.trim(), detectedLanguage);
              fullResponse += phraseBuffer;
            }
            break;
          }

          try {
            const parsed = JSON.parse(data);
            const content = parsed.choices?.[0]?.delta?.content;
            
            if (content) {
              phraseBuffer += content;
              
              // Check for interruption before sending phrase
              if (checkInterruption()) {
                isInterrupted = true;
                break;
              }
              
              if (shouldSendPhrase(phraseBuffer)) {
                const phrase = phraseBuffer.trim();
                if (phrase.length > 0 && !isInterrupted) {
                  if (isFirstPhrase) {
                    console.log(`⚡ [OPENAI] First phrase (${timer.checkpoint('first_phrase')}ms)`);
                    isFirstPhrase = false;
                  }
                  onPhrase(phrase, detectedLanguage);
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
      
      if (isInterrupted) break;
    }

    if (!isInterrupted) {
      console.log(`🤖 [OPENAI] Complete: "${fullResponse}" (${timer.end()}ms)`);
      onComplete(fullResponse);
    } else {
      console.log(`🤖 [OPENAI] Interrupted after ${timer.end()}ms`);
    }
    
    return isInterrupted ? null : fullResponse;

  } catch (error) {
    console.error(`❌ [OPENAI] Error: ${error.message}`);
    return null;
  }
};

// Smart phrase detection for better chunking
const shouldSendPhrase = (buffer) => {
  const trimmed = buffer.trim();
  
  // Complete sentences
  if (/[.!?।॥।]$/.test(trimmed)) return true;
  
  // Meaningful phrases with natural breaks
  if (trimmed.length >= 8 && /[,;।]\s*$/.test(trimmed)) return true;
  
  // Longer phrases (prevent too much buffering)
  if (trimmed.length >= 25 && /\s/.test(trimmed)) return true;
  
  return false;
};

// Enhanced LMNT TTS processor with proper WebSocket streaming
class OptimizedLMNTTTSProcessor {
  constructor(language, ws, streamSid) {
    this.language = language;
    this.ws = ws;
    this.streamSid = streamSid;
    this.queue = [];
    this.isProcessing = false;
    this.voice = getLMNTVoice(language, DEFAULT_CONFIG.voiceSelection);
    
    // Interruption handling
    this.isInterrupted = false;
    this.currentAudioStreaming = null;
    
    // LMNT Session connection (using sessions API, not raw WebSocket)
    this.lmntSession = null;
    this.lmntReady = false;
    
    // Sentence-based processing settings
    this.sentenceBuffer = "";
    this.processingTimeout = 50;
    this.sentenceTimer = null;
    
    // Audio streaming stats
    this.totalChunks = 0;
    this.totalAudioBytes = 0;
    
    // Initialize LMNT session
    this.connectToLMNT();
  }

  // Connect to LMNT using sessions API (not raw WebSocket)
  async connectToLMNT() {
    try {
      console.log(`🔌 [LMNT] Creating session with voice: ${this.voice}`);
      
      // Create LMNT session using HTTP API first
      const sessionResponse = await fetch('https://api.lmnt.com/v1/speech/sessions', {
        method: 'POST',
        headers: {
          'X-API-Key': API_KEYS.lmnt,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          voice: this.voice,
          format: 'pcm_16000' // 16kHz PCM for good quality
        })
      });

      if (!sessionResponse.ok) {
        throw new Error(`Session creation failed: ${sessionResponse.status}`);
      }

      const sessionData = await sessionResponse.json();
      const sessionId = sessionData.session_id;
      
      // Connect to WebSocket using the session ID
      const wsUrl = `wss://api.lmnt.com/v1/speech/sessions/${sessionId}/stream`;
      
      this.lmntSession = new WebSocket(wsUrl, {
        headers: {
          'X-API-Key': API_KEYS.lmnt
        }
      });

      this.lmntSession.onopen = () => {
        this.lmntReady = true;
        console.log(`✅ [LMNT] Session connected: ${sessionId}`);
      };

      this.lmntSession.onmessage = (event) => {
        this.handleLMNTMessage(event.data);
      };

      this.lmntSession.onerror = (error) => {
        console.error(`❌ [LMNT] WebSocket error:`, error);
        this.lmntReady = false;
      };

      this.lmntSession.onclose = (code, reason) => {
        console.log(`🔌 [LMNT] Connection closed: ${code} - ${reason}`);
        this.lmntReady = false;
        
        // Attempt to reconnect if not interrupted
        if (!this.isInterrupted) {
          setTimeout(() => this.connectToLMNT(), 1000);
        }
      };

    } catch (error) {
      console.error(`❌ [LMNT] Connection error: ${error.message}`);
    }
  }

  // Handle LMNT WebSocket messages
  handleLMNTMessage(data) {
    try {
      // LMNT sends binary audio data directly for streaming sessions
      if (data instanceof Buffer || data instanceof ArrayBuffer) {
        this.handleAudioData(data);
        return;
      }

      // Handle JSON messages
      const message = JSON.parse(data);
      
      switch (message.type) {
        case 'audio':
          if (message.audio) {
            const audioBuffer = Buffer.from(message.audio, 'base64');
            this.handleAudioData(audioBuffer);
          }
          break;
        case 'done':
          console.log(`✅ [LMNT] Synthesis completed`);
          this.totalChunks++;
          break;
        case 'error':
          console.error(`❌ [LMNT] Synthesis error:`, message.error);
          break;
        default:
          console.log(`📦 [LMNT] Unknown message type: ${message.type}`);
      }
    } catch (error) {
      // If it's not JSON, treat as binary audio data
      if (data instanceof Buffer || data instanceof ArrayBuffer) {
        this.handleAudioData(data);
      } else {
        console.error(`❌ [LMNT] Message parsing error: ${error.message}`);
      }
    }
  }

  // Handle audio data from LMNT
  async handleAudioData(audioData) {
    if (this.isInterrupted) return;
    
    const audioBuffer = Buffer.isBuffer(audioData) ? audioData : Buffer.from(audioData);
    
    // Stream audio immediately to reduce latency
    await this.streamAudioChunk(audioBuffer);
    
    this.totalAudioBytes += audioBuffer.length;
  }

  // Method to interrupt current processing
  interrupt() {
    console.log(`⚠️ [LMNT-TTS] Interrupting current processing`);
    this.isInterrupted = true;
    
    // Clear queue and buffer
    this.queue = [];
    this.sentenceBuffer = "";
    
    // Clear any pending timeout
    if (this.sentenceTimer) {
      clearTimeout(this.sentenceTimer);
      this.sentenceTimer = null;
    }
    
    // Stop current audio streaming if active
    if (this.currentAudioStreaming) {
      this.currentAudioStreaming.interrupt = true;
    }
    
    console.log(`🛑 [LMNT-TTS] Processing interrupted and cleaned up`);
  }

  // Reset for new processing
  reset(newLanguage) {
    this.interrupt();
    
    // Update language settings
    if (newLanguage && newLanguage !== this.language) {
      this.language = newLanguage;
      this.voice = getLMNTVoice(newLanguage, DEFAULT_CONFIG.voiceSelection);
      console.log(`🔄 [LMNT-TTS] Language updated to: ${newLanguage}, Voice: ${this.voice}`);
      
      // Reconnect with new voice if needed
      if (this.lmntSession) {
        this.lmntSession.close();
        setTimeout(() => this.connectToLMNT(), 500);
      }
    }
    
    // Reset state
    this.isInterrupted = false;
    this.isProcessing = false;
    this.totalChunks = 0;
    this.totalAudioBytes = 0;
  }

  addPhrase(phrase, detectedLanguage) {
    if (!phrase.trim() || this.isInterrupted) return;
    
    // Update language if different from current
    if (detectedLanguage && detectedLanguage !== this.language) {
      console.log(`🔄 [LMNT-TTS] Language change detected: ${this.language} → ${detectedLanguage}`);
      this.reset(detectedLanguage);
      return;
    }
    
    this.sentenceBuffer += (this.sentenceBuffer ? " " : "") + phrase.trim();
    
    if (this.hasCompleteSentence(this.sentenceBuffer)) {
      this.processCompleteSentences();
    } else {
      this.scheduleProcessing();
    }
  }

  hasCompleteSentence(text) {
    return /[.!?।॥।]/.test(text);
  }

  extractCompleteSentences(text) {
    const sentences = text.split(/([.!?।॥।])/).filter(s => s.trim());
    
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
    if (this.isInterrupted) return;
    
    if (this.sentenceTimer) {
      clearTimeout(this.sentenceTimer);
      this.sentenceTimer = null;
    }

    const { complete, remaining } = this.extractCompleteSentences(this.sentenceBuffer);
    
    if (complete && !this.isInterrupted) {
      this.queue.push(complete);
      this.sentenceBuffer = remaining;
      this.processQueue();
    }
  }

  scheduleProcessing() {
    if (this.isInterrupted) return;
    
    if (this.sentenceTimer) clearTimeout(this.sentenceTimer);
    
    this.sentenceTimer = setTimeout(() => {
      if (this.sentenceBuffer.trim() && !this.isInterrupted) {
        this.queue.push(this.sentenceBuffer.trim());
        this.sentenceBuffer = "";
        this.processQueue();
      }
    }, this.processingTimeout);
  }

  async processQueue() {
    if (this.isProcessing || this.queue.length === 0 || this.isInterrupted) return;

    this.isProcessing = true;
    const textToProcess = this.queue.shift();

    try {
      if (!this.isInterrupted) {
        await this.synthesizeWithLMNT(textToProcess);
      }
    } catch (error) {
      if (!this.isInterrupted) {
        console.error(`❌ [LMNT-TTS] Error: ${error.message}`);
      }
    } finally {
      this.isProcessing = false;
      
      // Process next item in queue if not interrupted
      if (this.queue.length > 0 && !this.isInterrupted) {
        setTimeout(() => this.processQueue(), 10);
      }
    }
  }

  async synthesizeWithLMNT(text) {
    if (this.isInterrupted || !this.lmntReady) return;
    
    const timer = createTimer("LMNT_TTS_SENTENCE");
    
    try {
      console.log(`🎵 [LMNT] Synthesizing: "${text}" (${this.voice})`);

      // Send text to LMNT session using appendText method
      const appendMessage = {
        type: 'append_text',
        text: text
      };

      if (this.lmntSession && this.lmntSession.readyState === WebSocket.OPEN) {
        this.lmntSession.send(JSON.stringify(appendMessage));
        
        // Send flush to get audio back immediately
        const flushMessage = { type: 'flush' };
        this.lmntSession.send(JSON.stringify(flushMessage));
        
        console.log(`📤 [LMNT] Text sent and flushed`);
      } else {
        console.error(`❌ [LMNT] Session not ready`);
      }
      
    } catch (error) {
      if (!this.isInterrupted) {
        console.error(`❌ [LMNT] Synthesis error: ${error.message}`);
        throw error;
      }
    }
  }

  async streamAudioChunk(audioBuffer) {
    if (this.isInterrupted || !audioBuffer || audioBuffer.length === 0) return;
    
    // Convert 16kHz PCM to 8kHz for SIP compatibility
    const convertedBuffer = this.convertTo8kHz(audioBuffer);
    
    // Stream in smaller chunks for lower latency
    const CHUNK_SIZE = 160; // 20ms of 8kHz audio
    let position = 0;
    
    while (position < convertedBuffer.length && !this.isInterrupted) {
      const chunk = convertedBuffer.slice(position, position + CHUNK_SIZE);
      
      const mediaMessage = {
        event: "media",
        streamSid: this.streamSid,
        media: {
          payload: chunk.toString("base64")
        }
      };

      if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted) {
        this.ws.send(JSON.stringify(mediaMessage));
      }
      
      position += CHUNK_SIZE;
      
      // Small delay between chunks for smooth audio
      if (position < convertedBuffer.length) {
        await new Promise(resolve => setTimeout(resolve, 20));
      }
    }
  }

  // Convert 16kHz PCM to 8kHz for SIP compatibility
  convertTo8kHz(buffer) {
    // Simple downsampling: take every other sample
    const samples16 = buffer.length / 2;
    const samples8 = Math.floor(samples16 / 2);
    const result = Buffer.alloc(samples8 * 2);
    
    for (let i = 0; i < samples8; i++) {
      const sourceIndex = i * 4; // Every other 16-bit sample
      result[i * 2] = buffer[sourceIndex];
      result[i * 2 + 1] = buffer[sourceIndex + 1];
    }
    
    return result;
  }

  complete() {
    if (this.isInterrupted) return;
    
    if (this.sentenceBuffer.trim()) {
      this.queue.push(this.sentenceBuffer.trim());
      this.sentenceBuffer = "";
    }
    
    if (this.queue.length > 0) {
      this.processQueue();
    }
    
    // Send finish to close LMNT session gracefully
    if (this.lmntSession && this.lmntSession.readyState === WebSocket.OPEN) {
      const finishMessage = { type: 'finish' };
      this.lmntSession.send(JSON.stringify(finishMessage));
    }
    
    console.log(`📊 [LMNT-STATS] Total: ${this.totalChunks} sentences, ${this.totalAudioBytes} bytes`);
  }

  getStats() {
    return {
      totalChunks: this.totalChunks,
      totalAudioBytes: this.totalAudioBytes,
      avgBytesPerChunk: this.totalChunks > 0 ? Math.round(this.totalAudioBytes / this.totalChunks) : 0,
      isConnected: this.lmntReady
    };
  }

  // Cleanup when connection ends
  cleanup() {
    this.interrupt();
    
    if (this.lmntSession) {
      // Send finish before closing
      if (this.lmntSession.readyState === WebSocket.OPEN) {
        const finishMessage = { type: 'finish' };
        this.lmntSession.send(JSON.stringify(finishMessage));
      }
      
      this.lmntSession.close();
      this.lmntSession = null;
    }
    
    console.log(`🧹 [LMNT-TTS] Cleanup completed`);
  }
}

// Main WebSocket server setup
const setupUnifiedVoiceServer = (wss) => {
  console.log("🚀 [ENHANCED] Voice Server started with LMNT WebSocket TTS and language detection");

  wss.on("connection", (ws, req) => {
    console.log("🔗 [CONNECTION] New enhanced WebSocket connection with LMNT");

    // Session state
    let streamSid = null;
    let conversationHistory = [];
    let isProcessing = false;
    let userUtteranceBuffer = "";
    let lastProcessedText = "";
    let optimizedTTS = null;
    let currentLanguage = DEFAULT_CONFIG.language;
    let processingRequestId = 0; // To track processing requests

    // Deepgram WebSocket connection
    let deepgramWs = null;
    let deepgramReady = false;
    let deepgramAudioQueue = [];

    // Optimized Deepgram connection
    const connectToDeepgram = async () => {
      try {
        console.log("🔌 [DEEPGRAM] Connecting...");
        const deepgramLanguage = getDeepgramLanguage(currentLanguage);
        
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
          headers: { Authorization: `Token ${API_KEYS.deepgram}` },
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

    // Handle Deepgram responses with interruption logic
    const handleDeepgramResponse = async (data) => {
      if (data.type === "Results") {
        const transcript = data.channel?.alternatives?.[0]?.transcript;
        const is_final = data.is_final;
        
        if (transcript?.trim()) {
          // Interrupt current TTS if new speech detected
          if (optimizedTTS && (isProcessing || optimizedTTS.isProcessing)) {
            console.log(`🛑 [INTERRUPT] New speech detected, interrupting current response`);
            optimizedTTS.interrupt();
            isProcessing = false;
            processingRequestId++; // Invalidate current processing
          }
          
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

    // Enhanced utterance processing with language detection and interruption handling
    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText) return;

      // Interrupt any ongoing processing
      if (optimizedTTS) {
        optimizedTTS.interrupt();
      }
      
      isProcessing = true;
      lastProcessedText = text;
      const currentRequestId = ++processingRequestId;
      const timer = createTimer("UTTERANCE_PROCESSING");

      try {
        console.log(`🎤 [USER] Processing: "${text}"`);

        // Step 1: Detect language using OpenAI
        const detectedLanguage = await detectLanguageWithOpenAI(text);
        
        // Step 2: Update current language and initialize TTS processor
        if (detectedLanguage !== currentLanguage) {
          console.log(`🌍 [LANGUAGE] Changed: ${currentLanguage} → ${detectedLanguage}`);
          currentLanguage = detectedLanguage;
        }

        // Create new TTS processor with detected language
        optimizedTTS = new OptimizedLMNTTTSProcessor(detectedLanguage, ws, streamSid);

        // Step 3: Check for interruption function
        const checkInterruption = () => {
          return processingRequestId !== currentRequestId;
        };

        // Step 4: Process with OpenAI streaming
        const response = await processWithOpenAIStreaming(
          text,
          conversationHistory,
          detectedLanguage,
          (phrase, lang) => {
            // Handle phrase chunks - only if not interrupted
            if (processingRequestId === currentRequestId && !checkInterruption()) {
              console.log(`📤 [PHRASE] "${phrase}" (${lang})`);
              optimizedTTS.addPhrase(phrase, lang);
            }
          },
          (fullResponse) => {
            // Handle completion - only if not interrupted
            if (processingRequestId === currentRequestId && !checkInterruption()) {
              console.log(`✅ [COMPLETE] "${fullResponse}"`);
              optimizedTTS.complete();
              
              const stats = optimizedTTS.getStats();
              console.log(`📊 [TTS-STATS] ${stats.totalChunks} chunks, ${stats.avgBytesPerChunk} avg bytes/chunk`);
              
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
          },
          checkInterruption
        );

        console.log(`⚡ [TOTAL] Processing time: ${timer.end()}ms`);

      } catch (error) {
        console.error(`❌ [PROCESSING] Error: ${error.message}`);
      } finally {
        if (processingRequestId === currentRequestId) {
          isProcessing = false;
        }
      }
    };

    // Optimized initial greeting with language detection
    const sendInitialGreeting = async () => {
      console.log("👋 [GREETING] Sending initial greeting");
      const tts = new OptimizedLMNTTTSProcessor(currentLanguage, ws, streamSid);
      await tts.synthesizeWithLMNT(DEFAULT_CONFIG.firstMessage);
    };

    // WebSocket message handling
    ws.on("message", async (message) => {
      try {
        const data = JSON.parse(message.toString());

        switch (data.event) {
          case "connected":
            console.log(`🔗 [ENHANCED] Connected - Protocol: ${data.protocol}`);
            break;

          case "start":
            streamSid = data.streamSid || data.start?.streamSid;
            console.log(`🎯 [ENHANCED] Stream started - StreamSid: ${streamSid}`);
            
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
            console.log(`📞 [ENHANCED] Stream stopped`);
            if (deepgramWs?.readyState === WebSocket.OPEN) {
              deepgramWs.close();
            }
            if (optimizedTTS) {
              optimizedTTS.cleanup();
            }
            break;

          default:
            console.log(`❓ [ENHANCED] Unknown event: ${data.event}`);
        }
      } catch (error) {
        console.error(`❌ [ENHANCED] Message error: ${error.message}`);
      }
    });

    // Connection cleanup
    ws.on("close", () => {
      console.log("🔗 [ENHANCED] Connection closed");
      
      if (deepgramWs?.readyState === WebSocket.OPEN) {
        deepgramWs.close();
      }

      if (optimizedTTS) {
        optimizedTTS.cleanup();
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
      currentLanguage = DEFAULT_CONFIG.language;
      processingRequestId = 0;
    });

    ws.on("error", (error) => {
      console.error(`❌ [ENHANCED] WebSocket error: ${error.message}`);
    });
  });
};

module.exports = { setupUnifiedVoiceServer };