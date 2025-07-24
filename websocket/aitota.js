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

// Add global error handlers to prevent server shutdown
process.on('unhandledRejection', (reason, promise) => {
  console.error('💥 [UNHANDLED_REJECTION] Caught unhandled rejection:', reason);
  console.error('Promise:', promise);
  // Don't exit the process, just log the error
});

process.on('uncaughtException', (error) => {
  console.error('💥 [UNCAUGHT_EXCEPTION] Caught uncaught exception:', error);
  // Don't exit the process, just log the error
});

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
        
        ta: "நீங்கள் ஐதோதா, ஒரு கண்ணியமான, உணர்வுபூர்வமாக புத்திசாலித்தனமான AI வாடிக்கையாளர் சேவை நிர்வாகி. நீங்கள் தமிழில் சரளமாக பேசுகிறீர்கள். அன்பு மற்றும் அனுதாபத்துடன் இயற்கையான, உரையாடல் மொழியைப் பயன்படுத்துங்கள்.",
        mr: "तुम्ही एआय तोता आहात, एक नम्र आणि भावनिकदृष्ट्या बुद्धिमान AI ग्राहक सेवा कार्यकारी. तुम्ही मराठीतून प्रवाहीपणे बोलता. नैसर्गिक, संवादात्मक भाषा वापरा जी उबदारपणा आणि सहानुभूतीने भरलेली आहे. उत्तरे लहान ठेवा—फक्त 1-2 ओळी. ग्राहकांना ऐकले, समर्थित आणि मौल्यवान वाटले पाहिजे, हे तुमचे ध्येय आहे."
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

// Add this helper function near the top
function resolveSarvamSpeaker(agentVoice, language, modelVersion = 'bulbul:v2') {
  // Bulbul V2 speakers
  const v2Speakers = ['anushka', 'abhilash', 'manisha', 'vidya', 'arya', 'karun', 'hitesh'];
  // Bulbul V1 speakers
  const v1Speakers = ['meera', 'pavithra', 'maitreyi', 'amol', 'amartya', 'arvind', 'maya', 'arjun', 'diya', 'neel', 'misha', 'vian'];

  if (modelVersion === 'bulbul:v2') {
    if (v2Speakers.includes(agentVoice)) return agentVoice;
    // Fallback mapping for v2
    if (language === 'hi') return 'manisha';
    if (language === 'en') return 'manisha';
    // Pick any v2 speaker as fallback
    return 'manisha';
  } else {
    if (v1Speakers.includes(agentVoice)) return agentVoice;
    // Fallback mapping for v1
    if (language === 'hi') return 'meera';
    if (language === 'mr') return 'maitreyi';
    if (language === 'en') return 'maya';
    return 'meera';
  }
}

// Enhanced WebSocket-based SarvamTTSProcessor with better interruption handling
// Enhanced WebSocket-based SarvamTTSProcessor with robust error handling
class SarvamWebSocketTTSProcessor {
  constructor(language, ws, streamSid, voice, modelVersion = 'bulbul:v2') {
    this.language = language;
    this.ws = ws;
    this.streamSid = streamSid;
    this.voice = voice;
    this.modelVersion = modelVersion;
    this.sarvamWs = null;
    this.isInterrupted = false;
    this.audioChunkCount = 0;
    this.pingInterval = null;
    this.currentAudioStreaming = false;
    this.audioQueue = [];
    this.connectionTimeout = null;
    this.isConfigSent = false;
    this.isTextSent = false;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 3;
  }

  async synthesizeAndStream(text) {
    if (!text || this.isInterrupted) return;
    
    const sarvamUrl = `wss://api.sarvam.ai/text-to-speech/ws?model=${this.modelVersion}`;
    const ttsStart = Date.now();
    console.log(`[TTS] Starting Sarvam TTS for text: "${text.substring(0, 60)}..."`);
    
    return new Promise((resolve) => {
      this.createSarvamConnection(sarvamUrl, text, ttsStart, resolve);
    });
  }

  createSarvamConnection(sarvamUrl, text, ttsStart, resolve) {
    try {
      // Clear any existing connection
      this.cleanup();

      this.sarvamWs = new WebSocket(sarvamUrl, {
        headers: {
          'API-Subscription-Key': process.env.SARVAM_API_KEY,
        },
        // Add connection timeout
        timeout: 10000,
      });
      
      let resolved = false;
      const safeResolve = () => {
        if (!resolved) {
          resolved = true;
          resolve();
        }
      };

      // Set connection timeout
      this.connectionTimeout = setTimeout(() => {
        console.error('[SARVAM-WS] Connection timeout');
        this.cleanup();
        safeResolve();
      }, 15000);
      
      // Handle WebSocket errors
      this.sarvamWs.on('error', (err) => {
        console.error('[SARVAM-WS] WebSocket error:', err.message);
        this.cleanup();
        
        // Retry connection if not interrupted and within retry limits
        if (!this.isInterrupted && this.reconnectAttempts < this.maxReconnectAttempts) {
          this.reconnectAttempts++;
          console.log(`[SARVAM-WS] Retrying connection (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
          setTimeout(() => {
            this.createSarvamConnection(sarvamUrl, text, ttsStart, resolve);
          }, 1000 * this.reconnectAttempts);
        } else {
          safeResolve();
        }
      });
      
      this.sarvamWs.on('open', () => {
        if (this.isInterrupted) {
          this.cleanup();
          safeResolve();
          return;
        }

        console.log('[SARVAM-WS] Connection opened successfully');
        
        // Clear connection timeout since we're connected
        if (this.connectionTimeout) {
          clearTimeout(this.connectionTimeout);
          this.connectionTimeout = null;
        }
        
        // Start ping interval to keep connection alive
        this.startPingInterval();
        
        // Send configuration
        this.sendConfiguration(text, safeResolve);
      });
      
      this.sarvamWs.on('message', (data) => {
        try {
          if (!data) {
            console.warn('[SARVAM-WS] Received null/undefined message data');
            return;
          }

          const msg = JSON.parse(data.toString());
          this.handleMessage(msg, ttsStart, safeResolve);
          
        } catch (err) {
          console.error('[SARVAM-WS] Message parse error:', err.message);
        }
      });
      
      this.sarvamWs.on('close', (code, reason) => {
        console.log(`[SARVAM-WS] Connection closed: ${code} - ${reason || 'No reason'}`);
        this.cleanup();
        safeResolve();
      });
      
    } catch (err) {
      console.error('[SARVAM-WS] Connection creation error:', err.message);
      this.cleanup();
      resolve();
    }
  }

  sendConfiguration(text, safeResolve) {
    try {
      // Wait a bit for connection to stabilize
      setTimeout(() => {
        if (this.isInterrupted || !this.sarvamWs || this.sarvamWs.readyState !== WebSocket.OPEN) {
          safeResolve();
          return;
        }

        const configMsg = {
          type: 'config',
          data: {
            target_language_code: getSarvamLanguage(this.language),
            speaker: resolveSarvamSpeaker(this.voice, this.language, this.modelVersion),
            pitch: 0,
            pace: 1.0,
            loudness: 1.0,
            speech_sample_rate: 8000,
            enable_preprocessing: false,
            output_audio_codec: 'linear16',
          },
        };
        
        console.log('[SARVAM-WS] Sending config:', JSON.stringify(configMsg));
        this.sarvamWs.send(JSON.stringify(configMsg));
        this.isConfigSent = true;
        
        // Send text after a short delay
        setTimeout(() => {
          if (this.isInterrupted || !this.sarvamWs || this.sarvamWs.readyState !== WebSocket.OPEN) {
            safeResolve();
            return;
          }

          const textMsg = {
            type: 'text',
            data: { text: String(text) },
          };
          
          console.log('[SARVAM-WS] Sending text:', JSON.stringify(textMsg));
          this.sarvamWs.send(JSON.stringify(textMsg));
          this.isTextSent = true;
          
        }, 100); // Small delay between config and text
        
      }, 50); // Small delay after connection opens
      
    } catch (err) {
      console.error('[SARVAM-WS] Send configuration error:', err.message);
      this.cleanup();
      safeResolve();
    }
  }

  handleMessage(msg, ttsStart, safeResolve) {
    if (!msg || !msg.type) {
      console.warn('[SARVAM-WS] Invalid message format:', msg);
      return;
    }

    switch (msg.type) {
      case 'audio':
        if (msg.data?.audio && !this.isInterrupted) {
          this.audioChunkCount++;
          const audioBuffer = Buffer.from(msg.data.audio, 'base64');
          this.streamAudioChunks(audioBuffer);
        }
        break;
        
      case 'end':
        const ttsEnd = Date.now();
        console.log(`[TTS] Sarvam TTS streaming complete. Chunks: ${this.audioChunkCount}, Time: ${ttsEnd - ttsStart}ms`);
        this.cleanup();
        safeResolve();
        break;
        
      case 'error':
        console.error('[SARVAM-WS] Server error:', msg.data?.message || 'Unknown error');
        this.cleanup();
        safeResolve();
        break;
        
      case 'pong':
        console.log('[SARVAM-WS] Received pong');
        break;
        
      default:
        console.log('[SARVAM-WS] Unknown message type:', msg.type);
    }
  }

  startPingInterval() {
    // Clear any existing interval
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
    }

    // Start new ping interval (every 10 seconds to prevent timeout)
    this.pingInterval = setInterval(() => {
      if (this.sarvamWs && this.sarvamWs.readyState === WebSocket.OPEN && !this.isInterrupted) {
        try {
          this.sarvamWs.send(JSON.stringify({ type: 'ping' }));
          console.log('[SARVAM-WS] Sent ping');
        } catch (err) {
          console.error('[SARVAM-WS] Ping error:', err.message);
          this.cleanup();
        }
      } else {
        // Clear interval if connection is not open
        if (this.pingInterval) {
          clearInterval(this.pingInterval);
          this.pingInterval = null;
        }
      }
    }, 10000); // 10 seconds
  }

  streamAudioChunks(audioBuffer) {
    if (!audioBuffer || this.isInterrupted) return;

    const CHUNK_SIZE = 640; // 40ms of 8000Hz 16-bit mono PCM
    let position = 0;
    
    const sendNextChunk = () => {
      // Check for interruption before sending each chunk
      if (this.isInterrupted || position >= audioBuffer.length) {
        if (this.isInterrupted) {
          console.log('[TTS] Audio streaming interrupted mid-chunk, stopping immediately');
        }
        return;
      }
      
      const chunk = audioBuffer.slice(position, position + CHUNK_SIZE);
      const mediaMessage = {
        event: 'media',
        streamSid: this.streamSid,
        media: {
          payload: chunk.toString('base64'),
        },
      };
      
      if (this.ws && this.ws.readyState === WebSocket.OPEN && !this.isInterrupted) {
        try {
          this.ws.send(JSON.stringify(mediaMessage));
          position += CHUNK_SIZE;
          
          // Schedule next chunk with small delay to prevent overwhelming
          if (position < audioBuffer.length && !this.isInterrupted) {
            setTimeout(sendNextChunk, 5); // 5ms delay between chunks
          }
        } catch (err) {
          console.error('[TTS] Error sending audio chunk:', err.message);
          return;
        }
      }
    };
    
    // Start streaming chunks
    sendNextChunk();
  }

  cleanup() {
    // Clear ping interval
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
    
    // Clear connection timeout
    if (this.connectionTimeout) {
      clearTimeout(this.connectionTimeout);
      this.connectionTimeout = null;
    }
    
    // Close WebSocket connection safely
    if (this.sarvamWs) {
      try {
        // Remove all listeners to prevent further events
        this.sarvamWs.removeAllListeners();
        
        if (this.sarvamWs.readyState === WebSocket.OPEN || this.sarvamWs.readyState === WebSocket.CONNECTING) {
          this.sarvamWs.close(1000, 'Normal closure');
        }
      } catch (err) {
        console.error('[SARVAM-WS] Error during cleanup:', err.message);
      }
      this.sarvamWs = null;
    }
    
    // Clear audio queue and reset flags
    this.audioQueue = [];
    this.currentAudioStreaming = false;
    this.isConfigSent = false;
    this.isTextSent = false;
  }

  interrupt() {
    console.log('[TTS] TTS interrupted, cleaning up connection and stopping audio');
    this.isInterrupted = true;
    this.cleanup();
  }

  // Method to check if TTS is currently active
  isActive() {
    return !this.isInterrupted && 
           this.sarvamWs && 
           this.sarvamWs.readyState === WebSocket.OPEN;
  }
}

// Updated processUserUtterance function with better error handling
const processUserUtterance = async (text) => {
  if (!text.trim() || text === lastProcessedText) return;

  // Interrupt any ongoing processing
  if (optimizedTTS) {
    optimizedTTS.interrupt();
    optimizedTTS = null; // Clear reference
  }
  
  isProcessing = true;
  lastProcessedText = text;
  const currentRequestId = ++processingRequestId;
  const timer = createTimer("UTTERANCE_PROCESSING");

  try {
    console.log(`🎤 [USER] Processing: "${text}"`);

    // Step 1: Detect language using OpenAI
    const detectedLanguage = await detectLanguageWithOpenAI(text);
    
    // Step 2: Update current language
    if (detectedLanguage !== currentLanguage) {
      console.log(`🌍 [LANGUAGE] Changed: ${currentLanguage} → ${detectedLanguage}`);
      currentLanguage = detectedLanguage;
    }

    // Step 3: Check for interruption function
    const checkInterruption = () => {
      return processingRequestId !== currentRequestId;
    };

    // Step 4: Process with OpenAI streaming
    const response = await processWithOpenAIStreaming(
      text,
      conversationHistory,
      detectedLanguage,
      async (phrase, lang) => {
        // Handle phrase chunks - only if not interrupted
        if (processingRequestId === currentRequestId && !checkInterruption()) {
          console.log(`📤 [PHRASE] "${phrase}" (${lang})`);
          
          // Create a new TTS processor for each phrase to avoid connection issues
          const phraseTTS = new SarvamWebSocketTTSProcessor(
            lang, 
            ws, 
            streamSid, 
            ws.sessionAgentConfig?.voiceSelection, 
            'bulbul:v2'
          );
          
          try {
            await phraseTTS.synthesizeAndStream(phrase);
          } catch (error) {
            console.error(`[TTS] Error processing phrase: ${error.message}`);
          }
        }
      },
      (fullResponse) => {
        // Handle completion - only if not interrupted
        if (processingRequestId === currentRequestId && !checkInterruption()) {
          console.log(`✅ [COMPLETE] "${fullResponse}"`);
          
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

// Main WebSocket server setup
const setupUnifiedVoiceServer = (wss) => {
  console.log("🚀 [ENHANCED] Voice Server started with language detection and interruption handling");

  wss.on("connection", (ws, req) => {
    console.log("🔗 [CONNECTION] New enhanced WebSocket connection");

    // Session state
    let streamSid = null;
    let conversationHistory = [];
    let isProcessing = false;
    let userUtteranceBuffer = "";
    let lastProcessedText = "";
    let optimizedTTS = null;
    let currentLanguage = undefined;
    let processingRequestId = 0; // To track processing requests

    // Deepgram WebSocket connection
    let deepgramWs = null;
    let deepgramReady = false;
    let deepgramAudioQueue = [];

    // Add at the top of the connection handler:
    let sessionTranscript = '';
    let callStartTime = null;
    let callEndTime = null;
    let callDuration = null;
    let sessionMobile = null;

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
          
          deepgramAudioQueue.forEach(buffer => {
            try {
              deepgramWs.send(buffer);
            } catch (err) {
              console.error('[DEEPGRAM] Error sending queued audio:', err.message);
            }
          });
          deepgramAudioQueue = [];
        };

        deepgramWs.onmessage = async (event) => {
          try {
            const data = JSON.parse(event.data);
            await handleDeepgramResponse(data);
          } catch (err) {
            console.error('[DEEPGRAM] Message parse error:', err.message);
          }
        };

        deepgramWs.onerror = (error) => {
          console.error("❌ [DEEPGRAM] Error:", error.message);
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
      try {
        if (data.type === "Results") {
          const transcript = data.channel?.alternatives?.[0]?.transcript;
          const is_final = data.is_final;
          
          if (transcript?.trim()) {
            // Add to session transcript
            sessionTranscript += transcript + ' ';
            
            // CRITICAL: Interrupt current TTS immediately when ANY speech is detected
            if (optimizedTTS && optimizedTTS.isActive()) {
              console.log(`🛑 [INTERRUPT] New speech detected: "${transcript.trim()}", interrupting current TTS`);
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
      } catch (err) {
        console.error('[DEEPGRAM] Response handling error:', err.message);
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
        optimizedTTS = new SarvamWebSocketTTSProcessor(detectedLanguage, ws, streamSid, ws.sessionAgentConfig?.voiceSelection, 'bulbul:v2');
        console.log('[TTS] New TTS session started for language:', detectedLanguage);

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
              optimizedTTS.synthesizeAndStream(phrase); // Stream phrase directly
            }
          },
          (fullResponse) => {
            // Handle completion - only if not interrupted
            if (processingRequestId === currentRequestId && !checkInterruption()) {
              console.log(`✅ [COMPLETE] "${fullResponse}"`);
              optimizedTTS.synthesizeAndStream(fullResponse); // Stream full response
              
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

    // WebSocket message handling
    ws.on("message", async (message) => {
      try {
        const data = JSON.parse(message.toString());

        switch (data.event) {
          case "connected":
            console.log(`🔗 [ENHANCED] Connected - Protocol: ${data.protocol}`);
            break;

          case "start": {
            streamSid = data.streamSid || data.start?.streamSid;
            const accountSid = data.start?.accountSid;
            console.log(`🎯 [ENHANCED] Stream started - StreamSid: ${streamSid}, AccountSid: ${accountSid}`);

            // Fetch agent config from DB using accountSid (MANDATORY)
            let agentConfig = null;
            if (accountSid) {
              try {
                agentConfig = await Agent.findOne({ accountSid }).lean();
                if (!agentConfig) {
                  ws.send(JSON.stringify({ event: 'error', message: `No agent found for accountSid: ${accountSid}` }));
                  ws.close();
                  return;
                }
                else {
                  console.log("Account sid matched")
                }
                
              } catch (err) {
                ws.send(JSON.stringify({ event: 'error', message: `DB error for accountSid: ${accountSid}` }));
                ws.close();
                return;
              }
            } else {
              ws.send(JSON.stringify({ event: 'error', message: 'Missing accountSid in start event' }));
              ws.close();
              return;
            }
            ws.sessionAgentConfig = agentConfig;
            currentLanguage = agentConfig.language;

            await connectToDeepgram();
            // Use agent's firstMessage for greeting
            const greeting = agentConfig.firstMessage;
            console.log(greeting)
            const tts = new SarvamWebSocketTTSProcessor(currentLanguage, ws, streamSid, agentConfig.voiceSelection, 'bulbul:v2');
            await tts.synthesizeAndStream(greeting);
            callStartTime = new Date();
            // If mobile is available in event, set sessionMobile = ...
            sessionMobile = data.start?.mobile;
            break;
          }

          case "media":
            if (data.media?.payload) {
              const audioBuffer = Buffer.from(data.media.payload, "base64");
              
              if (deepgramWs && deepgramReady && deepgramWs.readyState === WebSocket.OPEN) {
                try {
                  deepgramWs.send(audioBuffer);
                } catch (err) {
                  console.error('[DEEPGRAM] Error sending audio:', err.message);
                  deepgramAudioQueue.push(audioBuffer); // Queue if send fails
                }
              } else {
                deepgramAudioQueue.push(audioBuffer);
              }
            }
            break;

          case "stop": {
            callEndTime = new Date();
            callDuration = callStartTime ? Math.round((callEndTime - callStartTime) / 1000) : null;
            // Simple lead status detection (keyword-based)
            let leadStatus = 'medium';
            if (/very interested|definitely|sure|yes|want|buy|purchase|order|confirm/i.test(sessionTranscript)) {
              leadStatus = 'very_interested';
            } else if (/not interested|no|never|stop|don't want|don't call/i.test(sessionTranscript)) {
              leadStatus = 'not_interested';
            } else if (/not connected|disconnected|no answer|busy|unreachable/i.test(sessionTranscript)) {
              leadStatus = 'not_connected';
            }
            // Prepare CallLog object
            const callLogObj = {
              clientId: ws.sessionAgentConfig?.clientId,
              mobile: sessionMobile,
              time: callEndTime,
              transcript: sessionTranscript,
              audioUrl: null, // If you have audio URL, set here
              duration: callDuration,
              leadStatus,
            };
            console.log('[CALLLOG] Attempting to save:', callLogObj);
            if (!callLogObj.clientId) {
              console.error('[CALLLOG] Missing clientId, not saving log.');
              break;
            }
            try {
              await CallLog.create(callLogObj);
              console.log(`[CALLLOG] Saved for accountSid ${ws.sessionAgentConfig.accountSid}`);
            } catch (err) {
              console.error(`[CALLLOG] Error saving log:`, err);
            }
            break;
          }

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
      
      // Close Deepgram connection if open
      if (deepgramWs?.readyState === WebSocket.OPEN) {
        try {
          deepgramWs.close();
        } catch (err) {
          console.error('[DEEPGRAM] Error closing connection:', err.message);
        }
      }

      // Interrupt any ongoing TTS
      if (optimizedTTS) {
        optimizedTTS.interrupt();
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
      sessionTranscript = '';
      callStartTime = null;
      callEndTime = null;
      callDuration = null;
      sessionMobile = null;
    });

    ws.on("error", (error) => {
      console.error(`❌ [ENHANCED] WebSocket error: ${error.message}`);
    });
  });
};

module.exports = { setupUnifiedVoiceServer };