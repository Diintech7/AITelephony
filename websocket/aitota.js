const WebSocket = require("ws");
require("dotenv").config();

// Load API keys from environment variables
const API_KEYS = {
  deepgram: process.env.DEEPGRAM_API_KEY,
  lmnt: process.env.LMNT_API_KEY,
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

// Language mappings for LMNT (using ISO 639-1 two-letter codes)
const LANGUAGE_MAPPING = {
  hi: "hi",
  en: "en",
  bn: "bn",
  te: "te",
  ta: "ta",
  mr: "mr",
  gu: "gu",
  kn: "kn",
  ml: "ml",
  pa: "pa",
  or: "or",
  as: "as",
  ur: "ur",
};

const getLMNTLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang;
  return LANGUAGE_MAPPING[lang] || "hi";
};

const getDeepgramLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang;
  if (lang === "hi") return "hi";
  if (lang === "en") return "en-IN";
  return lang;
};

// Valid LMNT voice options (replace with actual LMNT voice IDs from your account)
const VALID_LMNT_VOICES = ["elowen", "morgan"]; // Update with actual LMNT voice IDs

const getValidLMNTVoice = (voiceSelection = "elowen") => {
  if (VALID_LMNT_VOICES.includes(voiceSelection)) {
    return voiceSelection;
  }
  
  const voiceMapping = {
    "male-professional": "morgan",
    "female-professional": "elowen",
    "male-friendly": "morgan",
    "female-friendly": "elowen",
    neutral: "elowen",
    default: "elowen",
  };
  
  return voiceMapping[voiceSelection] || "elowen";
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
Return only the language code, nothing else.`,
          },
          {
            role: "user",
            content: text,
          },
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
    
    const validLanguages = Object.keys(LANGUAGE_MAPPING);
    if (validLanguages.includes(detectedLang)) {
      console.log(`🔍 [LANG-DETECT] Detected: "${detectedLang}" from text: "${text.substring(0, 50)}..."`);
      return detectedLang;
    }
    
    console.log(`⚠️ [LANG-DETECT] Invalid language "${detectedLang}", defaulting to "hi"`);
    return "hi";
  } catch (error) {
    console.error(`❌ [LANG-DETECT] Error: ${error.message}`);
    return "hi";
  }
};

// Basic configuration
const DEFAULT_CONFIG = {
  agentName: "हिंदी सहायक",
  language: "hi",
  voiceSelection: "elowen",
  firstMessage: "नमस्कार! एआई तोता में संपर्क करने के लिए धन्यवाद। बताइए, मैं आपकी किस प्रकार मदद कर सकता हूँ?",
  personality: "friendly",
  category: "customer service",
  contextMemory: "customer service conversation in Hindi",
};

// Optimized OpenAI streaming
const processWithOpenAIStreaming = async (userMessage, conversationHistory, detectedLanguage, onPhrase, onComplete, onInterrupt) => {
  const timer = createTimer("OPENAI_STREAMING");
  
  try {
    const getSystemPrompt = (lang) => {
      const prompts = {
        hi: "आप एआई तोता हैं, एक विनम्र और भावनात्मक रूप से बुद्धिमान AI ग्राहक सेवा कार्यकारी। आप हिंदी में धाराप्रवाह बोलते हैं। प्राकृतिक, बातचीत की भाषा का प्रयोग करें जो गर्मजोशी और सहानुभूति से भरी हो। जवाब छोटे रखें—केवल 1-2 लाइन। ग्राहकों को सुना, समर्थित और मूल्यवान महसूस कराना आपका लक्ष्य है।",
        en: "You are Aitota, a polite, emotionally intelligent AI customer care executive. You speak fluently in English. Use natural, conversational language with warmth and empathy. Keep responses short—just 1–2 lines. Your goal is to make customers feel heard, supported, and valued.",
        bn: "আপনি আইতোতা, একজন ভদ্র এবং আবেগপ্রবণভাবে বুদ্ধিমান AI গ্রাহক সেবা কর্মকর্তা। আপনি বাংলায় সাবলীলভাবে কথা বলেন। উষ্ণতা এবং সহানুভূতি সহ প্রাকৃতিক, কথোপকথনমূলক ভাষা ব্যবহার করুন।",
        te: "మీరు ఐతోతా, మర్యాదపూర్వక, భావోద్వేగంతో తెలివైన AI కస్టమర్ కేర్ ఎగ్జిక్యూటివ్. మీరు తెలుగులో సరళంగా మాట్లాడుతారు। వెచ్చదనం మరియు సానుభూతితో సహజమైన, సంభాషణా భాషను ఉపయోగించండి।",
        ta: "நீங்கள் ஐதோதா, ஒரு கண்ணியமான, உணர்வுபூர்வமாக புத்திசாலித்தனமான AI வாடிக்கையாளர் சேவை நிர்வாகி. நீங்கள் தமிழில் சரளமாக பேசுகிறீர்கள். அன்பு மற்றும் அனுதாபத்துடன் இயற்கையான, உரையாடல் மொழியைப் பயன்படுத்துங்கள்।",
      };
      return prompts[lang] || prompts.en;
    };

    const systemPrompt = getSystemPrompt(detectedLanguage);
    const messages = [
      { role: "system", content: systemPrompt },
      ...conversationHistory.slice(-6),
      { role: "user", content: userMessage },
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

    const checkInterruption = () => {
      return onInterrupt && onInterrupt();
    };

    while (true) {
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
  if (/[.!?।॥।]$/.test(trimmed)) return true;
  if (trimmed.length >= 8 && /[,;।]\s*$/.test(trimmed)) return true;
  if (trimmed.length >= 25 && /\s/.test(trimmed)) return true;
  return false;
};

// Enhanced TTS processor with LMNT WebSocket
class OptimizedLMNTTTSProcessor {
  constructor(language, ws, streamSid) {
    this.language = language;
    this.ws = ws;
    this.streamSid = streamSid;
    this.queue = [];
    this.isProcessing = false;
    this.lmntLanguage = getLMNTLanguage(language);
    this.voice = getValidLMNTVoice(DEFAULT_CONFIG.voiceSelection);
    
    // Interruption handling
    this.isInterrupted = false;
    this.currentAudioStreaming = null;
    
    // Sentence-based processing settings
    this.sentenceBuffer = "";
    this.processingTimeout = 200; // Increased to allow more buffering
    
    // Audio streaming stats
    this.totalChunks = 0;
    this.totalAudioBytes = 0;
    
    // LMNT WebSocket
    this.lmntWs = null;
    this.lmntReady = false;
    this.audioBuffer = [];
    
    // Initialize LMNT WebSocket connection
    this.connectToLMNT();
  }

  async connectToLMNT() {
    try {
      console.log("🔌 [LMNT] Connecting to WebSocket...");
      const lmntUrl = "wss://api.lmnt.com/v1/ai/speech/stream";
      
      this.lmntWs = new WebSocket(lmntUrl, {
        headers: { "X-API-Key": API_KEYS.lmnt },
      });

      this.lmntWs.onopen = () => {
        console.log("✅ [LMNT] WebSocket Connected");
        this.lmntReady = true;
        
        // Send initial configuration
        this.lmntWs.send(JSON.stringify({
          "X-API-Key": API_KEYS.lmnt,
          voice: this.voice,
          format: "ulaw",
          sample_rate: 8000,
          return_extras: false,
          language: this.lmntLanguage,
        }));
        
        // Process any queued text
        if (this.queue.length > 0) {
          this.processQueue();
        }
      };

      this.lmntWs.onmessage = async (event) => {
        if (this.isInterrupted) return;
        
        if (typeof event.data === "string") {
          try {
            const data = JSON.parse(event.data);
            if (data.error) {
              console.error(`❌ [LMNT] Error: ${data.error}`);
              this.lmntReady = false;
              this.lmntWs.close();
              // Attempt reconnection
              setTimeout(() => this.connectToLMNT(), 1000);
            } else if (data.buffer_empty) {
              console.log(`📤 [LMNT] Buffer empty, synthesis complete for current text`);
            }
          } catch (e) {
            console.error(`❌ [LMNT] Invalid JSON: ${e.message}`);
          }
        } else if (event.data instanceof Buffer) {
          console.log(`📥 [LMNT] Received audio chunk: ${event.data.length} bytes`);
          this.audioBuffer.push(event.data);
          await this.streamAudioOptimizedForSIP(event.data);
        }
      };

      this.lmntWs.onerror = (error) => {
        console.error(`❌ [LMNT] WebSocket Error: ${error.message}`);
        this.lmntReady = false;
        // Attempt reconnection
        setTimeout(() => this.connectToLMNT(), 1000);
      };

      this.lmntWs.onclose = () => {
        console.log("🔌 [LMNT] WebSocket Connection closed");
        this.lmntReady = false;
        // Attempt reconnection
        setTimeout(() => this.connectToLMNT(), 1000);
      };
    } catch (error) {
      console.error(`❌ [LMNT] Setup error: ${error.message}`);
      setTimeout(() => this.connectToLMNT(), 1000);
    }
  }

  interrupt() {
    console.log(`⚠️ [LMNT-TTS] Interrupting current processing`);
    this.isInterrupted = true;
    
    this.queue = [];
    this.sentenceBuffer = "";
    
    if (this.currentAudioStreaming) {
      this.currentAudioStreaming.interrupt = true;
    }
    
    if (this.lmntWs?.readyState === WebSocket.OPEN) {
      this.lmntWs.send(JSON.stringify({ flush: true })); // Flush before closing
      this.lmntWs.close();
    }
    
    console.log(`🛑 [LMNT-TTS] Processing interrupted and cleaned up`);
    this.isInterrupted = false; // Reset for next use
  }

  reset(newLanguage) {
    this.interrupt();
    
    if (newLanguage) {
      this.language = newLanguage;
      this.lmntLanguage = getLMNTLanguage(newLanguage);
      console.log(`🔄 [LMNT-TTS] Language updated to: ${this.lmntLanguage}`);
    }
    
    this.isProcessing = false;
    this.totalChunks = 0;
    this.totalAudioBytes = 0;
    this.audioBuffer = [];
    
    this.connectToLMNT();
  }

  addPhrase(phrase, detectedLanguage) {
    if (!phrase.trim() || this.isInterrupted) return;
    
    if (detectedLanguage && detectedLanguage !== this.language) {
      console.log(`🔄 [LMNT-TTS] Language change detected: ${this.language} → ${detectedLanguage}`);
      this.reset(detectedLanguage);
    }
    
    this.sentenceBuffer += (this.sentenceBuffer ? " " : "") + phrase.trim();
    
    if (this.hasCompleteSentence(this.sentenceBuffer) || this.sentenceBuffer.length > 50) {
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
      remaining: remainingText.trim(),
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
    if (this.isProcessing || this.queue.length === 0 || this.isInterrupted || !this.lmntReady) return;

    this.isProcessing = true;
    const textToProcess = this.queue.shift();

    try {
      if (!this.isInterrupted) {
        await this.synthesizeAndStream(textToProcess);
      }
    } catch (error) {
      if (!this.isInterrupted) {
        console.error(`❌ [LMNT-TTS] Error: ${error.message}`);
      }
    } finally {
      this.isProcessing = false;
      
      if (this.queue.length > 0 && !this.isInterrupted) {
        setTimeout(() => this.processQueue(), 10);
      } else if (!this.isInterrupted && this.lmntWs?.readyState === WebSocket.OPEN) {
        this.lmntWs.send(JSON.stringify({ flush: true }));
      }
    }
  }

  async synthesizeAndStream(text) {
    if (this.isInterrupted || !this.lmntWs || this.lmntWs.readyState !== WebSocket.OPEN) {
      console.log(`⚠️ [LMNT-TTS] Cannot synthesize: WebSocket not ready`);
      return;
    }
    
    const timer = createTimer("LMNT_TTS_SENTENCE");
    
    try {
      console.log(`🎵 [LMNT-TTS] Synthesizing: "${text}" (${this.lmntLanguage})`);
      
      this.lmntWs.send(JSON.stringify({ text }));
      this.lmntWs.send(JSON.stringify({ flush: true })); // Ensure immediate synthesis
      
      console.log(`⚡ [LMNT-TTS] Synthesis initiated in ${timer.end()}ms`);
      
    } catch (error) {
      if (!this.isInterrupted) {
        console.error(`❌ [LMNT-TTS] Synthesis error: ${error.message}`);
        throw error;
      }
    }
  }

  async streamAudioOptimizedForSIP(audioBuffer) {
    if (this.isInterrupted) return;
    
    const streamingSession = { interrupt: false };
    this.currentAudioStreaming = streamingSession;
    
    const SAMPLE_RATE = 8000;
    const BYTES_PER_SAMPLE = 1;
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000;
    const OPTIMAL_CHUNK_SIZE = Math.floor(20 * BYTES_PER_MS); // Reduced chunk size for smoother streaming
    
    console.log(`📦 [LMNT-SIP] Streaming ${audioBuffer.length} bytes`);
    
    let position = 0;
    let chunkIndex = 0;
    
    while (position < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
      const remaining = audioBuffer.length - position;
      const chunkSize = Math.min(OPTIMAL_CHUNK_SIZE, remaining);
      const chunk = audioBuffer.slice(position, position + chunkSize);
      
      console.log(`📤 [LMNT-SIP] Chunk ${chunkIndex + 1}: ${chunk.length} bytes`);
      
      const mediaMessage = {
        event: "media",
        streamSid: this.streamSid,
        media: {
          payload: chunk.toString("base64"),
        },
      };

      if (this.ws.readyState === WebSocket.OPEN && !this.isInterrupted) {
        this.ws.send(JSON.stringify(mediaMessage));
      }
      
      if (position + chunkSize < audioBuffer.length && !this.isInterrupted) {
        const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS);
        const delayMs = Math.max(chunkDurationMs - 1, 5); // Reduced delay for smoother streaming
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
      
      position += chunkSize;
      chunkIndex++;
    }
    
    if (this.isInterrupted || streamingSession.interrupt) {
      console.log(`🛑 [LMNT-SIP] Audio streaming interrupted at chunk ${chunkIndex}`);
    } else {
      console.log(`✅ [LMNT-SIP] Completed streaming ${chunkIndex} chunks`);
    }
    
    this.totalAudioBytes += audioBuffer.length;
    this.totalChunks++;
    this.currentAudioStreaming = null;
  }

  complete() {
    if (this.isInterrupted || !this.lmntWs || this.lmntWs.readyState !== WebSocket.OPEN) return;
    
    if (this.sentenceBuffer.trim()) {
      this.queue.push(this.sentenceBuffer.trim());
      this.sentenceBuffer = "";
    }
    
    if (this.queue.length > 0) {
      this.processQueue();
    }
    
    this.lmntWs.send(JSON.stringify({ flush: true }));
    
    console.log(`📊 [LMNT-STATS] Total: ${this.totalChunks} sentences, ${this.totalAudioBytes} bytes`);
  }

  getStats() {
    return {
      totalChunks: this.totalChunks,
      totalAudioBytes: this.totalAudioBytes,
      avgBytesPerChunk: this.totalChunks > 0 ? Math.round(this.totalAudioBytes / this.totalChunks) : 0,
    };
  }
}

// Main WebSocket server setup
const setupUnifiedVoiceServer = (wss) => {
  console.log("🚀 [ENHANCED] Voice Server started with language detection and interruption handling");

  wss.on("connection", (ws, req) => {
    console.log("🔗 [CONNECTION] New enhanced WebSocket connection");

    let streamSid = null;
    let conversationHistory = [];
    let isProcessing = false;
    let userUtteranceBuffer = "";
    let lastProcessedText = "";
    let optimizedTTS = null;
    let currentLanguage = DEFAULT_CONFIG.language;
    let processingRequestId = 0;

    let deepgramWs = null;
    let deepgramReady = false;
    let deepgramAudioQueue = [];

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

    const handleDeepgramResponse = async (data) => {
      if (data.type === "Results") {
        const transcript = data.channel?.alternatives?.[0]?.transcript;
        const is_final = data.is_final;
        
        if (transcript?.trim()) {
          if (optimizedTTS && (isProcessing || optimizedTTS.isProcessing)) {
            console.log(`🛑 [INTERRUPT] New speech detected, interrupting current response`);
            optimizedTTS.interrupt();
            isProcessing = false;
            processingRequestId++;
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

    const processUserUtterance = async (text) => {
      if (!text.trim() || text === lastProcessedText) return;

      if (optimizedTTS) {
        optimizedTTS.interrupt();
      }
      
      isProcessing = true;
      lastProcessedText = text;
      const currentRequestId = ++processingRequestId;
      const timer = createTimer("UTTERANCE_PROCESSING");

      try {
        console.log(`🎤 [USER] Processing: "${text}"`);

        const detectedLanguage = await detectLanguageWithOpenAI(text);
        
        if (detectedLanguage !== currentLanguage) {
          console.log(`🌍 [LANGUAGE] Changed: ${currentLanguage} → ${detectedLanguage}`);
          currentLanguage = detectedLanguage;
        }

        optimizedTTS = new OptimizedLMNTTTSProcessor(detectedLanguage, ws, streamSid);

        const checkInterruption = () => {
          return processingRequestId !== currentRequestId;
        };

        const response = await processWithOpenAIStreaming(
          text,
          conversationHistory,
          detectedLanguage,
          (phrase, lang) => {
            if (processingRequestId === currentRequestId && !checkInterruption()) {
              console.log(`📤 [PHRASE] "${phrase}" (${lang})`);
              optimizedTTS.addPhrase(phrase, lang);
            }
          },
          (fullResponse) => {
            if (processingRequestId === currentRequestId && !checkInterruption()) {
              console.log(`✅ [COMPLETE] "${fullResponse}"`);
              optimizedTTS.complete();
              
              const stats = optimizedTTS.getStats();
              console.log(`📊 [TTS-STATS] ${stats.totalChunks} chunks, ${stats.avgBytesPerChunk} avg bytes/chunk`);
              
              conversationHistory.push(
                { role: "user", content: text },
                { role: "assistant", content: fullResponse },
              );

              if (conversationHistory.length > 10) {
                conversationHistory = conversationHistory.slice(-10);
              }
            }
          },
          checkInterruption,
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

    const sendInitialGreeting = async () => {
      console.log("👋 [GREETING] Sending initial greeting");
      const tts = new OptimizedLMNTTTSProcessor(currentLanguage, ws, streamSid);
      
      // Wait for WebSocket to be ready
      await new Promise(resolve => {
        const checkReady = () => {
          if (tts.lmntReady) {
            resolve();
          } else {
            setTimeout(checkReady, 100);
          }
        };
        checkReady();
      });
      
      await tts.synthesizeAndStream(DEFAULT_CONFIG.firstMessage);
      tts.complete(); // Ensure greeting is flushed
    };

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
              optimizedTTS.interrupt();
            }
            break;

          default:
            console.log(`❓ [ENHANCED] Unknown event: ${data.event}`);
        }
      } catch (error) {
        console.error(`❌ [ENHANCED] Message error: ${error.message}`);
      }
    });

    ws.on("close", () => {
      console.log("🔗 [ENHANCED] Connection closed");
      
      if (deepgramWs?.readyState === WebSocket.OPEN) {
        deepgramWs.close();
      }
      if (optimizedTTS) {
        optimizedTTS.interrupt();
      }

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