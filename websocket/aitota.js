const WebSocket = require("ws");

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

// Basic configuration
const DEFAULT_CONFIG = {
  agentName: "हिंदी सहायक",
  language: "hi",
  voiceSelection: "pavithra",
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
      ...conversationHistory.slice(-6), // Keep more context for better responses
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
              
              // Phrase-based chunking: send when we have meaningful phrases
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
  // Send phrase if we have:
  // 1. Complete sentence (ends with punctuation)
  // 2. Meaningful phrase (8+ chars with space)
  // 3. Natural break points
  
  const trimmed = buffer.trim();
  
  // Complete sentences
  if (/[.!?।]$/.test(trimmed)) return true;
  
  // Meaningful phrases with natural breaks
  if (trimmed.length >= 8 && /[,;।]\s*$/.test(trimmed)) return true;
  
  // Longer phrases (prevent too much buffering)
  if (trimmed.length >= 25 && /\s/.test(trimmed)) return true;
  
  return false;
};

// Enhanced TTS processor with sentence-based optimization and SIP streaming
class OptimizedSarvamTTSProcessor {
  constructor(language, ws, streamSid) {
    this.language = language;
    this.ws = ws;
    this.streamSid = streamSid;
    this.queue = [];
    this.isProcessing = false;
    this.sarvamLanguage = getSarvamLanguage(language);
    this.voice = getValidSarvamVoice(DEFAULT_CONFIG.voiceSelection);
    
    // Sentence-based processing settings
    this.sentenceBuffer = "";
    this.processingTimeout = 100; // Faster processing for real-time
    this.sentenceTimer = null;
    
    // Audio streaming stats
    this.totalChunks = 0;
    this.totalAudioBytes = 0;
  }

  addPhrase(phrase) {
    if (!phrase.trim()) return;
    
    this.sentenceBuffer += (this.sentenceBuffer ? " " : "") + phrase.trim();
    
    // Process immediately if we have complete sentences
    if (this.hasCompleteSentence(this.sentenceBuffer)) {
      this.processCompleteSentences();
    } else {
      // Schedule processing for incomplete sentences
      this.scheduleProcessing();
    }
  }

  hasCompleteSentence(text) {
    // Check for sentence endings in Hindi and English
    return /[.!?।॥]/.test(text);
  }

  extractCompleteSentences(text) {
    // Split by sentence endings, keeping the punctuation
    const sentences = text.split(/([.!?।॥])/).filter(s => s.trim());
    
    let completeSentences = "";
    let remainingText = "";
    
    for (let i = 0; i < sentences.length; i += 2) {
      const sentence = sentences[i];
      const punctuation = sentences[i + 1];
      
      if (punctuation) {
        // Complete sentence
        completeSentences += sentence + punctuation + " ";
      } else {
        // Incomplete sentence
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
      
      // Process next item in queue
      if (this.queue.length > 0) {
        setTimeout(() => this.processQueue(), 10);
      }
    }
  }

  async synthesizeAndStream(text) {
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
          pace: 1.0, // Optimal pace for SIP
          loudness: 1.0,
          speech_sample_rate: 8000, // Match SIP requirements
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
      
      // Stream audio with optimized SIP chunking
      await this.streamAudioOptimizedForSIP(audioBase64);
      
      // Update stats
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
    
    // SIP audio chunk specifications
    const SAMPLE_RATE = 8000; // 8kHz
    const BYTES_PER_SAMPLE = 2; // 16-bit audio = 2 bytes per sample
    const BYTES_PER_MS = (SAMPLE_RATE * BYTES_PER_SAMPLE) / 1000; // 16 bytes per ms
    
    // Chunk size constraints for SIP (20ms - 100ms)
    const MIN_CHUNK_SIZE = Math.floor(20 * BYTES_PER_MS);   // 320 bytes (20ms)
    const MAX_CHUNK_SIZE = Math.floor(100 * BYTES_PER_MS);  // 1600 bytes (100ms)
    const OPTIMAL_CHUNK_SIZE = Math.floor(20 * BYTES_PER_MS); // 640 bytes (40ms)
    
    // Ensure chunk sizes are aligned to sample boundaries (even numbers)
    const alignToSample = (size) => Math.floor(size / 2) * 2;
    
    const minChunk = alignToSample(MIN_CHUNK_SIZE);
    const maxChunk = alignToSample(MAX_CHUNK_SIZE);
    const optimalChunk = alignToSample(OPTIMAL_CHUNK_SIZE);
    
    console.log(`📦 [SARVAM-SIP] Streaming ${audioBuffer.length} bytes`);
    console.log(`📦 [SARVAM-SIP] Chunk config: ${minChunk}-${maxChunk} bytes (${minChunk/16}-${maxChunk/16}ms)`);
    
    let position = 0;
    let chunkIndex = 0;
    
    while (position < audioBuffer.length) {
      // Calculate chunk size for this iteration
      const remaining = audioBuffer.length - position;
      let chunkSize;
      
      if (remaining <= maxChunk) {
        // Last chunk - use all remaining data if >= minimum
        chunkSize = remaining >= minChunk ? remaining : minChunk;
      } else {
        // Use optimal chunk size
        chunkSize = optimalChunk;
      }
      
      // Ensure we don't exceed buffer length
      chunkSize = Math.min(chunkSize, remaining);
      
      // Extract chunk
      const chunk = audioBuffer.slice(position, position + chunkSize);
      
      // Only send if chunk meets minimum size requirement
      if (chunk.length >= minChunk) {
        const durationMs = (chunk.length / BYTES_PER_MS).toFixed(1);
        
        console.log(`📤 [SARVAM-SIP] Chunk ${chunkIndex + 1}: ${chunk.length} bytes (${durationMs}ms)`);
        
        // Send to SIP
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
        
        // Calculate delay based on actual chunk duration
        const chunkDurationMs = Math.floor(chunk.length / BYTES_PER_MS);
        
        // Add small buffer time for network transmission (2-3ms)
        const networkBufferMs = 2;
        const delayMs = Math.max(chunkDurationMs - networkBufferMs, 10);
        
        // Wait before sending next chunk (except for last chunk)
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
    // Process any remaining buffered text
    if (this.sentenceBuffer.trim()) {
      this.queue.push(this.sentenceBuffer.trim());
      this.sentenceBuffer = "";
    }
    
    // Force process remaining queue
    if (this.queue.length > 0) {
      this.processQueue();
    }
    
    // Log final stats
    console.log(`📊 [SARVAM-STATS] Total: ${this.totalChunks} sentences, ${this.totalAudioBytes} bytes`);
  }

  // Method to get streaming statistics
  getStats() {
    return {
      totalChunks: this.totalChunks,
      totalAudioBytes: this.totalAudioBytes,
      avgBytesPerChunk: this.totalChunks > 0 ? Math.round(this.totalAudioBytes / this.totalChunks) : 0
    };
  }
}

// Main WebSocket server setup
const setupUnifiedVoiceServer = (wss) => {
  console.log("🚀 [OPTIMIZED] Voice Server started");

  wss.on("connection", (ws, req) => {
    console.log("🔗 [CONNECTION] New optimized WebSocket connection");

    // Session state
    let streamSid = null;
    let conversationHistory = [];
    let isProcessing = false;
    let userUtteranceBuffer = "";
    let lastProcessedText = "";
    let optimizedTTS = null;

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
        deepgramUrl.searchParams.append("endpointing", "300"); // Faster endpointing

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

    // Optimized utterance processing with enhanced TTS
    const processUserUtterance = async (text) => {
      if (!text.trim() || isProcessing || text === lastProcessedText) return;

      isProcessing = true;
      lastProcessedText = text;
      const timer = createTimer("UTTERANCE_PROCESSING");

      try {
        console.log(`🎤 [USER] Processing: "${text}"`);

        // Use the enhanced TTS processor
        optimizedTTS = new OptimizedSarvamTTSProcessor(DEFAULT_CONFIG.language, ws, streamSid);

        // Process with OpenAI streaming
        const response = await processWithOpenAIStreaming(
          text,
          conversationHistory,
          (phrase) => {
            // Handle phrase chunks with sentence-based optimization
            console.log(`📤 [PHRASE] "${phrase}"`);
            optimizedTTS.addPhrase(phrase);
          },
          (fullResponse) => {
            // Handle completion
            console.log(`✅ [COMPLETE] "${fullResponse}"`);
            optimizedTTS.complete();
            
            // Log TTS stats
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
        );

        console.log(`⚡ [TOTAL] Processing time: ${timer.end()}ms`);

      } catch (error) {
        console.error(`❌ [PROCESSING] Error: ${error.message}`);
      } finally {
        isProcessing = false;
      }
    };

    // Optimized initial greeting
    const sendInitialGreeting = async () => {
      console.log("👋 [GREETING] Sending initial greeting");
      const tts = new OptimizedSarvamTTSProcessor(DEFAULT_CONFIG.language, ws, streamSid);
      await tts.synthesizeAndStream(DEFAULT_CONFIG.firstMessage);
    };

    // WebSocket message handling
    ws.on("message", async (message) => {
      try {
        const data = JSON.parse(message.toString());

        switch (data.event) {
          case "connected":
            console.log(`🔗 [OPTIMIZED] Connected - Protocol: ${data.protocol}`);
            break;

          case "start":
            streamSid = data.streamSid || data.start?.streamSid;
            console.log(`🎯 [OPTIMIZED] Stream started - StreamSid: ${streamSid}`);
            
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
            console.log(`📞 [OPTIMIZED] Stream stopped`);
            if (deepgramWs?.readyState === WebSocket.OPEN) {
              deepgramWs.close();
            }
            break;

          default:
            console.log(`❓ [OPTIMIZED] Unknown event: ${data.event}`);
        }
      } catch (error) {
        console.error(`❌ [OPTIMIZED] Message error: ${error.message}`);
      }
    });

    // Connection cleanup
    ws.on("close", () => {
      console.log("🔗 [OPTIMIZED] Connection closed");
      
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
    });

    ws.on("error", (error) => {
      console.error(`❌ [OPTIMIZED] WebSocket error: ${error.message}`);
    });
  });
};

module.exports = { setupUnifiedVoiceServer };