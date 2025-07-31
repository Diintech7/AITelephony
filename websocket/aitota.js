const WebSocket = require("ws");
require("dotenv").config();
const mongoose = require('mongoose');
const Agent = require('../models/Agent');
const CallLog = require('../models/CallLog');

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

// Enhanced language mappings with Marathi support
const LANGUAGE_MAPPING = {
  hi: "hi-IN", 
  en: "en-IN", 
  bn: "bn-IN", 
  te: "te-IN", 
  ta: "ta-IN",
  mr: "mr-IN", // Marathi added
  gu: "gu-IN", 
  kn: "kn-IN", 
  ml: "ml-IN", 
  pa: "pa-IN",
  or: "or-IN", 
  as: "as-IN", 
  ur: "ur-IN",
};

const getSarvamLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang;
  return LANGUAGE_MAPPING[lang] || "hi-IN";
};

const getDeepgramLanguage = (detectedLang, defaultLang = "hi") => {
  const lang = detectedLang?.toLowerCase() || defaultLang;
  if (lang === "hi") return "hi";
  if (lang === "en") return "en-IN";
  if (lang === "mr") return "mr"; // Marathi support for Deepgram
  return lang;
};

// Valid Sarvam voice options
const VALID_SARVAM_VOICES = ["meera", "pavithra", "arvind", "amol", "maya"];

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

// Enhanced language detection with Marathi support
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
- "तुम्ही कसे आहात?" → mr
- "તમે કેમ છો?" → gu

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
    const validLanguages = Object.keys(LANGUAGE_MAPPING);
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

// Call logging utility class
class CallLogger {
  constructor(clientId, mobile = null) {
    this.clientId = clientId;
    this.mobile = mobile;
    this.callStartTime = new Date();
    this.transcripts = [];
    this.responses = [];
    this.totalDuration = 0;
  }

  // Log user transcript from Deepgram
  logUserTranscript(transcript, language, timestamp = new Date()) {
    const entry = {
      type: 'user',
      text: transcript,
      language: language,
      timestamp: timestamp,
      source: 'deepgram'
    };
    
    this.transcripts.push(entry);
    console.log(`📝 [CALL-LOG] User: "${transcript}" (${language})`);
  }

  // Log AI response from Sarvam
  logAIResponse(response, language, timestamp = new Date()) {
    const entry = {
      type: 'ai',
      text: response,
      language: language,
      timestamp: timestamp,
      source: 'sarvam'
    };
    
    this.responses.push(entry);
    console.log(`🤖 [CALL-LOG] AI: "${response}" (${language})`);
  }

  // Generate full transcript combining user and AI messages
  generateFullTranscript() {
    const allEntries = [...this.transcripts, ...this.responses]
      .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    
    return allEntries.map(entry => {
      const speaker = entry.type === 'user' ? 'User' : 'AI';
      const time = entry.timestamp.toISOString();
      return `[${time}] ${speaker} (${entry.language}): ${entry.text}`;
    }).join('\n');
  }

  // Save call log to database
  async saveToDatabase(leadStatus = 'medium') {
    try {
      const callEndTime = new Date();
      this.totalDuration = Math.round((callEndTime - this.callStartTime) / 1000); // Duration in seconds

      const callLogData = {
        clientId: this.clientId,
        mobile: this.mobile,
        time: this.callStartTime,
        transcript: this.generateFullTranscript(),
        duration: this.totalDuration,
        leadStatus: leadStatus,
        // Additional metadata
        metadata: {
          userTranscriptCount: this.transcripts.length,
          aiResponseCount: this.responses.length,
          languages: [...new Set([...this.transcripts, ...this.responses].map(entry => entry.language))],
          callEndTime: callEndTime
        }
      };

      const callLog = new CallLog(callLogData);
      const savedLog = await callLog.save();
      
      console.log(`💾 [CALL-LOG] Saved to DB - ID: ${savedLog._id}, Duration: ${this.totalDuration}s`);
      console.log(`📊 [CALL-LOG] Stats - User messages: ${this.transcripts.length}, AI responses: ${this.responses.length}`);
      
      return savedLog;
    } catch (error) {
      console.error(`❌ [CALL-LOG] Database save error: ${error.message}`);
      throw error;
    }
  }

  // Get call statistics
  getStats() {
    return {
      duration: this.totalDuration,
      userMessages: this.transcripts.length,
      aiResponses: this.responses.length,
      languages: [...new Set([...this.transcripts, ...this.responses].map(entry => entry.language))],
      startTime: this.callStartTime
    };
  }
}

// Optimized OpenAI streaming with phrase-based chunking and language detection
const processWithOpenAIStreaming = async (userMessage, conversationHistory, detectedLanguage, onPhrase, onComplete, onInterrupt, callLogger) => {
  const timer = createTimer("OPENAI_STREAMING");
  
  try {
    // Enhanced system prompt with Marathi support
    const getSystemPrompt = (lang) => {
      const prompts = {
        hi: "आप एआई तोता हैं, एक विनम्र और भावनात्मक रूप से बुद्धिमान AI ग्राहक सेवा कार्यकारी। आप हिंदी में धाराप्रवाह बोलते हैं। प्राकृतिक, बातचीत की भाषा का प्रयोग करें जो गर्मजोशी और सहानुभूति से भरी हो। जवाब छोटे रखें—केवल 1-2 लाइन। ग्राहकों को सुना, समर्थित और मूल्यवान महसूस कराना आपका लक्ष्य है।",
        
        en: "You are Aitota, a polite, emotionally intelligent AI customer care executive. You speak fluently in English. Use natural, conversational language with warmth and empathy. Keep responses short—just 1–2 lines. Your goal is to make customers feel heard, supported, and valued.",
        
        bn: "আপনি আইতোতা, একজন ভদ্র এবং আবেগপ্রবণভাবে বুদ্ধিমান AI গ্রাহক সেবা কর্মকর্তা। আপনি বাংলায় সাবলীলভাবে কথা বলেন। উষ্ণতা এবং সহানুভূতি সহ প্রাকৃতিক, কথোপকথনমূলক ভাষা ব্যবহার করুন।",
        
        te: "మీరు ఐతోతా, మర్యాదపూర్వక, భావోద్వేగంతో తెలివైన AI కస్టమర్ కేర్ ఎగ్జిక్యూటివ్. మీరు తెలుగులో సరళంగా మాట్లాడుతారు। వెచ్చదనం మరియు సానుభూతితో సహజమైన, సంభాషణా భాషను ఉపయోగించండి।",
        
        ta: "நீங்கள் ஐதோதா, ஒரு கண்ணியமான, உணர்வுபூர்வமாக புத்திசாலித்தனமான AI வாடிக்கையாளர் சேவை நிர்வாகி. நீங்கள் தமிழில் சரளமாக பேசுகிறீர்கள். அன்பு மற்றும் அனுதாபத்துடன் இயற்கையான, உரையாடல் மொழியைப் பயன்படுத்துங்கள்।",
        
        mr: "तुम्ही एआयतोता आहात, एक नम्र आणि भावनिकदृष्ट्या बुद्धिमान AI ग्राहक सेवा कार्यकारी. तुम्ही मराठीत अस्खलितपणे बोलता. उबदारपणा आणि सहानुभूतीसह नैसर्गिक, संभाषणात्मक भाषा वापरा. उत्तरे लहान ठेवा—फक्त 1-2 ओळी. ग्राहकांना ऐकले, समर्थित आणि मूल्यवान वाटण्याचे तुमचे ध्येय आहे।"
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
      
      // Log AI response to call logger
      if (callLogger && fullResponse.trim()) {
        callLogger.logAIResponse(fullResponse.trim(), detectedLanguage);
      }
      
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

// Enhanced TTS processor with call logging
class OptimizedSarvamTTSProcessor {
  constructor(language, ws, streamSid, callLogger = null) {
    this.language = language;
    this.ws = ws;
    this.streamSid = streamSid;
    this.callLogger = callLogger;
    this.queue = [];
    this.isProcessing = false;
    this.sarvamLanguage = getSarvamLanguage(language);
    this.voice = getValidSarvamVoice(ws.sessionAgentConfig?.voiceSelection || "pavithra");
    
    // Interruption handling
    this.isInterrupted = false;
    this.currentAudioStreaming = null;
    
    // Sentence-based processing settings
    this.sentenceBuffer = "";
    this.processingTimeout = 100;
    this.sentenceTimer = null;
    
    // Audio streaming stats
    this.totalChunks = 0;
    this.totalAudioBytes = 0;
  }

  // Method to interrupt current processing
  interrupt() {
    console.log(`⚠️ [SARVAM-TTS] Interrupting current processing`);
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
    
    console.log(`🛑 [SARVAM-TTS] Processing interrupted and cleaned up`);
  }

  // Reset for new processing
  reset(newLanguage) {
    this.interrupt();
    
    // Update language settings
    if (newLanguage) {
      this.language = newLanguage;
      this.sarvamLanguage = getSarvamLanguage(newLanguage);
      console.log(`🔄 [SARVAM-TTS] Language updated to: ${this.sarvamLanguage}`);
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
      console.log(`🔄 [SARVAM-TTS] Language change detected: ${this.language} → ${detectedLanguage}`);
      this.language = detectedLanguage;
      this.sarvamLanguage = getSarvamLanguage(detectedLanguage);
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
        await this.synthesizeAndStream(textToProcess);
      }
    } catch (error) {
      if (!this.isInterrupted) {
        console.error(`❌ [SARVAM-TTS] Error: ${error.message}`);
      }
    } finally {
      this.isProcessing = false;
      
      // Process next item in queue if not interrupted
      if (this.queue.length > 0 && !this.isInterrupted) {
        setTimeout(() => this.processQueue(), 10);
      }
    }
  }

  async synthesizeAndStream(text) {
    if (this.isInterrupted) return;
    
    const timer = createTimer("SARVAM_TTS_SENTENCE");
    
    try {
      console.log(`🎵 [SARVAM-TTS] Synthesizing: "${text}" (${this.sarvamLanguage})`);

      const response = await fetch("https://api.sarvam.ai/text-to-speech", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "API-Subscription-Key": API_KEYS.sarvam,
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

      if (!response.ok || this.isInterrupted) {
        if (this.isInterrupted) return;
        throw new Error(`Sarvam API error: ${response.status} - ${response.statusText}`);
      }

      const responseData = await response.json();
      const audioBase64 = responseData.audios?.[0];
      
      if (!audioBase64 || this.isInterrupted) {
        if (!this.isInterrupted) {
          throw new Error("No audio data received from Sarvam API");
        }
        return;
      }

      console.log(`⚡ [SARVAM-TTS] Synthesis completed in ${timer.end()}ms`);
      
      // Stream audio if not interrupted
      if (!this.isInterrupted) {
        await this.streamAudioOptimizedForSIP(audioBase64);
        
        const audioBuffer = Buffer.from(audioBase64, "base64");
        this.totalAudioBytes += audioBuffer.length;
        this.totalChunks++;
      }
      
    } catch (error) {
      if (!this.isInterrupted) {
        console.error(`❌ [SARVAM-TTS] Synthesis error: ${error.message}`);
        throw error;
      }
    }
  }

  async streamAudioOptimizedForSIP(audioBase64) {
    if (this.isInterrupted) return;
    
    const audioBuffer = Buffer.from(audioBase64, "base64");
    const streamingSession = { interrupt: false };
    this.currentAudioStreaming = streamingSession;
    
    // SIP audio specifications
    const SAMPLE_RATE = 8000;
    const BYTES_PER_SAMPLE = 2;
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000;
    const OPTIMAL_CHUNK_SIZE = Math.floor(40 * BYTES_PER_MS);
    
    console.log(`📦 [SARVAM-SIP] Streaming ${audioBuffer.length} bytes`);
    
    let position = 0;
    let chunkIndex = 0;
    
    while (position < audioBuffer.length && !this.isInterrupted && !streamingSession.interrupt) {
      const remaining = audioBuffer.length - position;
      const chunkSize = Math.min(OPTIMAL_CHUNK_SIZE, remaining);
      const chunk = audioBuffer.slice(position, position + chunkSize);
      
      console.log(`📤 [SARVAM-SIP] Chunk ${chunkIndex + 1}: ${chunk.length} bytes`);
      
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
      
      // Delay between chunks
      if (position + chunkSize < audioBuffer.length && !this.isInterrupted) {
        const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS);
        const delayMs = Math.max(chunkDurationMs - 2, 10);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
      
      position += chunkSize;
      chunkIndex++;
    }
    
    if (this.isInterrupted || streamingSession.interrupt) {
      console.log(`🛑 [SARVAM-SIP] Audio streaming interrupted at chunk ${chunkIndex}`);
    } else {
      console.log(`✅ [SARVAM-SIP] Completed streaming ${chunkIndex} chunks`);
    }
    
    this.currentAudioStreaming = null;
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

// Main WebSocket server setup with enhanced call logging
const setupUnifiedVoiceServer = (wss) => {
  console.log("🚀 [ENHANCED] Voice Server started with call logging and Marathi support");

  wss.on("connection", (ws, req) => {
    console.log("🔗 [CONNECTION] New enhanced WebSocket connection");
    
    // ===== ENHANCED LOGGING - Connection Details =====
    const connectionDetails = {
      timestamp: new Date().toISOString(),
      clientIP: req.socket.remoteAddress,
      userAgent: req.headers["user-agent"],
      origin: req.headers.origin,
      host: req.headers.host,
      protocol: req.headers["sec-websocket-protocol"],
      extensions: req.headers["sec-websocket-extensions"],
      key: req.headers["sec-websocket-key"],
      version: req.headers["sec-websocket-version"],
      url: req.url,
      method: req.method,
      headers: req.headers
    };
    
    console.log(`📋 [WS-CONNECTION] Full Connection Details:`);
    console.log(`   🕐 Timestamp: ${connectionDetails.timestamp}`);
    console.log(`   🌐 Client IP: ${connectionDetails.clientIP}`);
    console.log(`   🔗 URL: ${connectionDetails.url}`);
    console.log(`   📱 User Agent: ${connectionDetails.userAgent}`);
    console.log(`   🏠 Origin: ${connectionDetails.origin}`);
    console.log(`   🖥️  Host: ${connectionDetails.host}`);
    console.log(`   📄 Method: ${connectionDetails.method}`);
    console.log(`   🔧 Protocol: ${connectionDetails.protocol}`);
    console.log(`   📦 Extensions: ${connectionDetails.extensions}`);
    console.log(`   🔑 WebSocket Key: ${connectionDetails.key}`);
    console.log(`   📊 WebSocket Version: ${connectionDetails.version}`);
    console.log(`   📋 All Headers:`, JSON.stringify(connectionDetails.headers, null, 2));
    console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);

    // Session state
    let streamSid = null;
    let conversationHistory = [];
    let isProcessing = false;
    let userUtteranceBuffer = "";
    let lastProcessedText = "";
    let optimizedTTS = null;
    let currentLanguage = undefined;
    let processingRequestId = 0;
    let callLogger = null; // Call logger instance

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

    // Handle Deepgram responses with call logging
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
            
            // Log the final transcript to call logger
            if (callLogger && transcript.trim()) {
              const detectedLang = await detectLanguageWithOpenAI(transcript.trim());
              callLogger.logUserTranscript(transcript.trim(), detectedLang);
            }
            
            await processUserUtterance(userUtteranceBuffer);
            userUtteranceBuffer = "";
          }
        }
      } else if (data.type === "UtteranceEnd") {
        if (userUtteranceBuffer.trim()) {
          // Log the utterance end transcript
          if (callLogger && userUtteranceBuffer.trim()) {
            const detectedLang = await detectLanguageWithOpenAI(userUtteranceBuffer.trim());
            callLogger.logUserTranscript(userUtteranceBuffer.trim(), detectedLang);
          }
          
          await processUserUtterance(userUtteranceBuffer);
          userUtteranceBuffer = "";
        }
      }
    };

    // Enhanced utterance processing with call logging
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
        optimizedTTS = new OptimizedSarvamTTSProcessor(detectedLanguage, ws, streamSid, callLogger);

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
          checkInterruption,
          callLogger // Pass call logger to OpenAI processing
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

    // ===== ENHANCED LOGGING - WebSocket Message Handling =====
    ws.on("message", async (message) => {
      try {
        // Log raw message details
        console.log(`\n📨 [WS-INCOMING] New Message Received:`);
        console.log(`   🕐 Timestamp: ${new Date().toISOString()}`);
        console.log(`   📏 Message Size: ${message.length} bytes`);
        console.log(`   📦 Message Type: ${typeof message}`);
        console.log(`   🔤 Message Buffer Type: ${Buffer.isBuffer(message) ? 'Buffer' : 'String'}`);
        
        // Try to parse as JSON and log both raw and parsed data
        let data;
        let rawMessageString = message.toString();
        
        console.log(`   📄 Raw Message (first 500 chars): ${rawMessageString.substring(0, 500)}${rawMessageString.length > 500 ? '...' : ''}`);
        
        try {
          data = JSON.parse(rawMessageString);
          console.log(`   ✅ Successfully parsed JSON`);
          console.log(`   📋 Parsed Data:`, JSON.stringify(data, null, 2));
        } catch (parseError) {
          console.log(`   ❌ JSON Parse Error: ${parseError.message}`);
          console.log(`   🔤 Raw String: "${rawMessageString}"`);
          return;
        }

        // Log event-specific details
        console.log(`   🎯 Event Type: "${data.event}"`);
        
        switch (data.event) {
          case "connected":
            console.log(`   🔗 [EVENT-CONNECTED] Protocol: ${data.protocol}`);
            console.log(`   📋 Full Connected Data:`, JSON.stringify(data, null, 2));
            break;

          case "start": {
            streamSid = data.streamSid || data.start?.streamSid;
            const accountSid = data.start?.accountSid;
            const mobile = data.start?.from || null; // Extract mobile number from call data
            
            console.log(`   🎯 [EVENT-START] Stream Details:`);
            console.log(`      📡 StreamSid: ${streamSid}`);
            console.log(`      🏢 AccountSid: ${accountSid}`);
            console.log(`      📱 Mobile: ${mobile}`);
            console.log(`      📋 Complete Start Data:`, JSON.stringify(data, null, 2));

            // Fetch agent config from DB using accountSid (MANDATORY)
            let agentConfig = null;
            if (accountSid) {
              try {
                console.log(`   🔍 [DB-QUERY] Searching for agent with accountSid: ${accountSid}`);
                agentConfig = await Agent.findOne({ accountSid }).lean();
                if (!agentConfig) {
                  console.log(`   ❌ [DB-ERROR] No agent found for accountSid: ${accountSid}`);
                  ws.send(JSON.stringify({ event: 'error', message: `No agent found for accountSid: ${accountSid}` }));
                  ws.close();
                  return;
                } else {
                  console.log(`   ✅ [DB-SUCCESS] Agent found:`, JSON.stringify(agentConfig, null, 2));
                }
                
              } catch (err) {
                console.log(`   ❌ [DB-ERROR] Database error for accountSid: ${accountSid}`, err);
                ws.send(JSON.stringify({ event: 'error', message: `DB error for accountSid: ${accountSid}` }));
                ws.close();
                return;
              }
            } else {
              console.log(`   ❌ [VALIDATION-ERROR] Missing accountSid in start event`);
              ws.send(JSON.stringify({ event: 'error', message: 'Missing accountSid in start event' }));
              ws.close();
              return;
            }
            
            ws.sessionAgentConfig = agentConfig;
            currentLanguage = agentConfig.language || 'hi';
            console.log(`   🌍 [LANGUAGE] Set to: ${currentLanguage}`);

            // Initialize call logger
            callLogger = new CallLogger(agentConfig.clientId || accountSid, mobile);
            console.log(`   📝 [CALL-LOG] Initialized for client: ${agentConfig.clientId}, mobile: ${mobile}`);

            await connectToDeepgram();
            
            // Use agent's firstMessage for greeting and log it
            const greeting = agentConfig.firstMessage || "Hello! How can I help you today?";
            console.log(`   👋 [GREETING] "${greeting}"`);
            
            // Log the initial greeting
            if (callLogger) {
              callLogger.logAIResponse(greeting, currentLanguage);
            }
            
            const tts = new OptimizedSarvamTTSProcessor(currentLanguage, ws, streamSid, callLogger);
            await tts.synthesizeAndStream(greeting);
            break;
          }

          case "media":
            const mediaPayload = data.media?.payload;
            const mediaTimestamp = data.media?.timestamp;
            
            console.log(`   🎵 [EVENT-MEDIA] Media Details:`);
            console.log(`      📡 StreamSid: ${data.streamSid}`);
            console.log(`      🕐 Timestamp: ${mediaTimestamp}`);
            console.log(`      📦 Payload Length: ${mediaPayload ? mediaPayload.length : 0} chars`);
            console.log(`      🔤 Payload Sample (first 100 chars): ${mediaPayload ? mediaPayload.substring(0, 100) + '...' : 'null'}`);
            
            if (mediaPayload) {
              const audioBuffer = Buffer.from(mediaPayload, "base64");
              console.log(`      📊 Audio Buffer Size: ${audioBuffer.length} bytes`);
              
              if (deepgramWs && deepgramReady && deepgramWs.readyState === WebSocket.OPEN) {
                console.log(`      ✅ [DEEPGRAM] Sending audio to Deepgram`);
                deepgramWs.send(audioBuffer);
              } else {
                console.log(`      ⏳ [DEEPGRAM] Queueing audio (not ready yet)`);
                deepgramAudioQueue.push(audioBuffer);
              }
            } else {
              console.log(`      ❌ [MEDIA-ERROR] No payload in media event`);
            }
            break;

          case "stop":
            console.log(`   📞 [EVENT-STOP] Stream stopped`);
            console.log(`   📋 Stop Data:`, JSON.stringify(data, null, 2));
            
            // Save call log to database before closing
            if (callLogger) {
              try {
                const savedLog = await callLogger.saveToDatabase('medium'); // Default lead status
                console.log(`   💾 [CALL-LOG] Final save completed - ID: ${savedLog._id}`);
                
                // Print call statistics
                const stats = callLogger.getStats();
                console.log(`   📊 [CALL-STATS] Duration: ${stats.duration}s, User: ${stats.userMessages}, AI: ${stats.aiResponses}, Languages: ${stats.languages.join(', ')}`);
              } catch (error) {
                console.error(`   ❌ [CALL-LOG] Failed to save final log: ${error.message}`);
              }
            }
            
            if (deepgramWs?.readyState === WebSocket.OPEN) {
              deepgramWs.close();
            }
            break;

          case "mark":
            console.log(`   🏷️  [EVENT-MARK] Mark Details:`);
            console.log(`   📋 Mark Data:`, JSON.stringify(data, null, 2));
            break;

          case "dtmf":
            console.log(`   📱 [EVENT-DTMF] DTMF Details:`);
            console.log(`      🔢 Digit: ${data.dtmf?.digit}`);
            console.log(`   📋 DTMF Data:`, JSON.stringify(data, null, 2));
            break;

          default:
            console.log(`   ❓ [EVENT-UNKNOWN] Unknown event: ${data.event}`);
            console.log(`   📋 Unknown Event Data:`, JSON.stringify(data, null, 2));
        }
        
        console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
        
      } catch (error) {
        console.error(`❌ [WS-MESSAGE-ERROR] Error processing message: ${error.message}`);
        console.error(`❌ [WS-MESSAGE-ERROR] Stack trace:`, error.stack);
        console.error(`❌ [WS-MESSAGE-ERROR] Raw message:`, message.toString());
      }
    });

    // Enhanced connection cleanup with call logging
    ws.on("close", async (code, reason) => {
      console.log(`\n🔗 [WS-CLOSE] Connection Closed:`);
      console.log(`   🕐 Timestamp: ${new Date().toISOString()}`);
      console.log(`   🔢 Close Code: ${code}`);
      console.log(`   📝 Close Reason: ${reason || 'No reason provided'}`);
      console.log(`   📊 Connection Duration: ${Date.now() - (ws.connectedAt?.getTime() || Date.now())}ms`);
      
      // Save call log before cleanup if not already saved
      if (callLogger) {
        try {
          const savedLog = await callLogger.saveToDatabase('not_connected'); // Status for unexpected disconnection
          console.log(`   💾 [CALL-LOG] Emergency save completed - ID: ${savedLog._id}`);
        } catch (error) {
          console.error(`   ❌ [CALL-LOG] Emergency save failed: ${error.message}`);
        }
      }
      
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
      currentLanguage = undefined;
      processingRequestId = 0;
      callLogger = null;
      
      console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
    });

    ws.on("error", (error) => {
      console.log(`\n❌ [WS-ERROR] WebSocket Error:`);
      console.log(`   🕐 Timestamp: ${new Date().toISOString()}`);
      console.log(`   📝 Error Message: ${error.message}`);
      console.log(`   📋 Error Stack:`, error.stack);
      console.log(`   🔧 Error Code: ${error.code}`);
      console.log(`   📊 Error Type: ${error.type || 'Unknown'}`);
      console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
    });
  });
};

module.exports = { setupUnifiedVoiceServer };