const WebSocket = require('ws');
require('dotenv').config();

// API Keys from environment
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const SARVAM_API_KEY = process.env.SARVAM_API_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Validate API keys
if (!DEEPGRAM_API_KEY || !SARVAM_API_KEY || !OPENAI_API_KEY) {
  console.error('Missing required API keys in environment variables');
  process.exit(1);
}

const fetch = globalThis.fetch || require('node-fetch');

// Enhanced quick responses with more coverage
const QUICK_RESPONSES = {
  'hello': 'Hello! How can I help you today?',
  'hi': 'Hi there! What can I do for you?',
  'how are you': 'I\'m doing great! How about you?',
  'thank you': 'You\'re welcome! Is there anything else I can help with?',
  'thanks': 'My pleasure! What else can I assist you with?',
  'yes': 'Great! What would you like to know more about?',
  'no': 'No problem! Is there something else I can help you with?',
  'okay': 'Perfect! What\'s next?',
  'bye': 'Goodbye! Have a great day!',
  'goodbye': 'Take care! Feel free to call back anytime.',
  'help': 'I\'m here to help! What do you need assistance with?'
};

/**
 * Enhanced C-Zentrix voice server with improved audio timing and connection management
 */
const setupUnifiedVoiceServer = (ws) => {
  console.log('Setting up enhanced C-Zentrix voice server connection');

  // Enhanced session state
  let streamSid = null;
  let callSid = null;
  let accountSid = null;
  let conversationHistory = [];
  let deepgramWs = null;
  let isProcessing = false;
  let userUtteranceBuffer = '';
  let silenceTimer = null;
  let connectionActive = true;
  let lastActivityTime = Date.now();
  let keepAliveInterval = null;
  let sttFailed = false;
  
  // Audio streaming state
  let currentStreamId = 0;
  let audioMetrics = {
    totalChunksSent: 0,
    totalAudioDuration: 0,
    averageLatency: 0
  };

  /**
   * Keep-alive mechanism to prevent timeout
   */
  const startKeepAlive = () => {
    keepAliveInterval = setInterval(() => {
      if (connectionActive && ws.readyState === WebSocket.OPEN) {
        lastActivityTime = Date.now();
        // Send a minimal heartbeat if no recent activity
        const timeSinceActivity = Date.now() - lastActivityTime;
        if (timeSinceActivity > 25000) { // 25 seconds
          console.log('[KEEPALIVE] Sending heartbeat');
        }
      }
    }, 10000); // Check every 10 seconds
  };

  /**
   * Enhanced audio streaming with real-time pacing
   */
  const streamAudioToCall = async (audioBase64, options = {}) => {
    const {
      realTimePacing = true,
      chunkSize = 160, // 10ms chunks for 8kHz 16-bit
      maxConcurrentChunks = 5
    } = options;

    const streamId = ++currentStreamId;
    const audioBuffer = Buffer.from(audioBase64, 'base64');
    const totalChunks = Math.ceil(audioBuffer.length / chunkSize);
    const chunkDuration = 10; // 10ms per chunk
    
    console.log(`[STREAM-${streamId}] Starting enhanced audio stream: ${audioBuffer.length} bytes in ${totalChunks} chunks`);
    console.log(`[STREAM-${streamId}] Real-time pacing: ${realTimePacing}, StreamSID: ${streamSid}`);
    
    if (!streamSid || ws.readyState !== WebSocket.OPEN) {
      console.error(`[STREAM-${streamId}] Cannot stream - invalid connection state`);
      return false;
    }

    let position = 0;
    let chunksSuccessfullySent = 0;
    const streamStart = Date.now();
    let nextChunkTime = streamStart;
    
    // Stream with real-time pacing to prevent buffer overflow
    while (position < audioBuffer.length && ws.readyState === WebSocket.OPEN && connectionActive) {
      const chunk = audioBuffer.slice(position, position + chunkSize);
      
      // Pad chunk if necessary
      const paddedChunk = chunk.length < chunkSize ? 
        Buffer.concat([chunk, Buffer.alloc(chunkSize - chunk.length, 0)]) : chunk;
      
      const mediaMessage = {
        event: 'media',
        streamSid: streamSid,
        media: {
          payload: paddedChunk.toString('base64')
        }
      };

      try {
        ws.send(JSON.stringify(mediaMessage));
        chunksSuccessfullySent++;
        position += chunkSize;
        lastActivityTime = Date.now();

        // Real-time pacing: wait for the appropriate time
        if (realTimePacing && position < audioBuffer.length) {
          nextChunkTime += chunkDuration;
          const currentTime = Date.now();
          const waitTime = nextChunkTime - currentTime;
          
          if (waitTime > 0) {
            await new Promise(resolve => setTimeout(resolve, waitTime));
          }
        }
        
      } catch (error) {
        console.error(`[STREAM-${streamId}] Failed to send chunk ${chunksSuccessfullySent}:`, error.message);
        break;
      }
    }
    
    // Add small silence buffer at the end
    try {
      const silenceChunk = Buffer.alloc(chunkSize, 0);
      const silenceMessage = {
        event: 'media',
        streamSid: streamSid,
        media: {
          payload: silenceChunk.toString('base64')
        }
      };
      ws.send(JSON.stringify(silenceMessage));
    } catch (error) {
      console.error(`[STREAM-${streamId}] Failed to send end silence:`, error.message);
    }
    
    const streamDuration = Date.now() - streamStart;
    const expectedDuration = totalChunks * chunkDuration;
    const efficiency = ((expectedDuration / streamDuration) * 100).toFixed(1);
    
    console.log(`[STREAM-${streamId}] Completed: ${chunksSuccessfullySent}/${totalChunks} chunks in ${streamDuration}ms`);
    console.log(`[STREAM-${streamId}] Expected: ${expectedDuration}ms, Efficiency: ${efficiency}%`);
    
    // Update metrics
    audioMetrics.totalChunksSent += chunksSuccessfullySent;
    audioMetrics.totalAudioDuration += expectedDuration;
    
    return chunksSuccessfullySent === totalChunks;
  };

  /**
   * Enhanced TTS with better error handling and audio processing
   */
  const synthesizeAndStreamAudio = async (text, language = 'en-IN', isTestTone = false) => {
    if (!connectionActive) return;
    
    try {
      let audioBase64;
      
      if (isTestTone) {
        console.log(`[TTS] Generating test tone`);
        audioBase64 = generateTestTone(800, 1.0, 0.3);
      } else {
        console.log(`[TTS] Synthesizing: "${text.substring(0, 50)}${text.length > 50 ? '...' : ''}"`);
        const startTime = Date.now();
        
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 6000); // Increased timeout

        const response = await fetch('https://api.sarvam.ai/text-to-speech', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'API-Subscription-Key': SARVAM_API_KEY,
            'Connection': 'keep-alive'
          },
          body: JSON.stringify({
            inputs: [text],
            target_language_code: language,
            speaker: 'meera',
            pitch: 0,
            pace: 1.0, // Slightly slower for better clarity
            loudness: 1.0,
            speech_sample_rate: 8000,
            enable_preprocessing: true,
            model: 'bulbul:v1'
          }),
          signal: controller.signal
        });

        clearTimeout(timeoutId);

        if (!response.ok) {
          const errorText = await response.text();
          throw new Error(`Sarvam API error: ${response.status} - ${errorText}`);
        }

        const data = await response.json();
        audioBase64 = data.audios?.[0];

        if (!audioBase64) {
          throw new Error('No audio data received from Sarvam');
        }

        const ttsTime = Date.now() - startTime;
        console.log(`[TTS] Audio generated in ${ttsTime}ms`);
      }
      
      // Enhanced audio processing with normalization
      const processedAudio = processAudioForCZentrix(audioBase64);
      
      // Stream with real-time pacing
      const success = await streamAudioToCall(processedAudio, {
        realTimePacing: true,
        chunkSize: 160
      });
      
      if (success) {
        console.log('[TTS] Audio streaming completed successfully');
      } else {
        console.warn('[TTS] Audio streaming may have been interrupted');
      }
      
    } catch (error) {
      console.error('[TTS] Error:', error.message);
      
      // Fallback: send a short beep tone
      if (connectionActive) {
        console.log('[TTS] Sending fallback tone');
        const fallbackAudio = generateTestTone(600, 0.5, 0.3);
        await streamAudioToCall(fallbackAudio, { realTimePacing: true });
      }
    }
  };

  /**
   * Enhanced audio processing for C-Zentrix compatibility
   */
  const processAudioForCZentrix = (audioBase64) => {
    const audioBuffer = Buffer.from(audioBase64, 'base64');
    console.log(`[AUDIO] Processing ${audioBuffer.length} bytes`);
    
    // Ensure proper 16-bit PCM format with normalization
    const processed = Buffer.alloc(audioBuffer.length);
    let maxAmplitude = 0;
    
    // First pass: find max amplitude for normalization
    for (let i = 0; i < audioBuffer.length; i += 2) {
      const sample = Math.abs(audioBuffer.readInt16LE(i));
      if (sample > maxAmplitude) maxAmplitude = sample;
    }
    
    // Second pass: normalize and copy
    const normalizeRatio = maxAmplitude > 0 ? Math.min(1.0, 20000 / maxAmplitude) : 1.0;
    for (let i = 0; i < audioBuffer.length; i += 2) {
      let sample = audioBuffer.readInt16LE(i);
      sample = Math.floor(sample * normalizeRatio);
      sample = Math.max(-32768, Math.min(32767, sample));
      processed.writeInt16LE(sample, i);
    }
    
    return processed.toString('base64');
  };

  /**
   * Generate test tone with proper 8kHz sampling
   */
  const generateTestTone = (frequency = 800, duration = 1.0, volume = 0.3) => {
    const sampleRate = 8000;
    const samples = Math.floor(sampleRate * duration);
    const buffer = Buffer.alloc(samples * 2); // 16-bit samples
    
    for (let i = 0; i < samples; i++) {
      const sample = Math.sin(2 * Math.PI * frequency * i / sampleRate) * volume;
      const intSample = Math.floor(sample * 32767);
      buffer.writeInt16LE(intSample, i * 2);
    }
    
    return buffer.toString('base64');
  };

  /**
   * Enhanced Deepgram connection with better retry logic
   */
  const connectToDeepgram = (attemptCount = 0) => {
    console.log(`[STT] Connecting to Deepgram (attempt ${attemptCount + 1})`);

    const params = {
      'encoding': 'linear16',
      'sample_rate': '8000',
      'channels': '1',
      'model': attemptCount === 0 ? 'nova-2' : 'base',
      'language': 'en',
      'interim_results': 'true',
      'smart_format': 'true',
      'endpointing': '500', // Increased for better phrase detection
      'punctuate': 'true',
      'diarize': 'false'
    };

    const deepgramUrl = `wss://api.deepgram.com/v1/listen?${new URLSearchParams(params).toString()}`;

    deepgramWs = new WebSocket(deepgramUrl, {
      headers: {
        'Authorization': `Token ${DEEPGRAM_API_KEY}`
      }
    });

    deepgramWs.on('open', () => {
      console.log('[STT] Connected to Deepgram successfully');
      sttFailed = false;
    });

    deepgramWs.on('message', async (data) => {
      const response = JSON.parse(data);
      lastActivityTime = Date.now(); // Update activity time
      
      if (response.type === 'Results') {
        const transcript = response.channel?.alternatives?.[0]?.transcript;
        const isFinal = response.is_final;
        const confidence = response.channel?.alternatives?.[0]?.confidence || 0;

        if (transcript?.trim() && confidence > 0.6) { // Slightly lower threshold
          if (isFinal) {
            console.log(`[STT] Final transcript: "${transcript}" (confidence: ${confidence.toFixed(3)})`);
            
            // Clear existing timer
            if (silenceTimer) {
              clearTimeout(silenceTimer);
              silenceTimer = null;
            }
            
            // Process the complete utterance
            await processUserInput(transcript.trim());
          }
        }
      } else if (response.type === 'UtteranceEnd') {
        console.log(`[STT] Utterance ended`);
      }
    });

    deepgramWs.on('error', (error) => {
      console.error(`[STT] Deepgram error (attempt ${attemptCount + 1}):`, error.message);
      
      if (attemptCount < 2) {
        console.log(`[STT] Retrying connection...`);
        setTimeout(() => {
          connectToDeepgram(attemptCount + 1);
        }, 2000 * (attemptCount + 1));
      } else {
        console.error('[STT] All Deepgram connection attempts failed');
        sttFailed = true;
        
        // Provide user feedback about STT issue
        if (connectionActive) {
          const fallbackMessage = "I'm having trouble with speech recognition. Please speak clearly or use the keypad for options.";
          synthesizeAndStreamAudio(fallbackMessage).catch(console.error);
        }
      }
    });

    deepgramWs.on('close', (code, reason) => {
      console.log(`[STT] Deepgram connection closed: ${code} - ${reason || 'No reason'}`);
    });
  };

  /**
   * Enhanced AI response with better context management
   */
  const getAIResponse = async (userMessage) => {
    try {
      console.log(`[LLM] Processing: "${userMessage}"`);
      const startTime = Date.now();

      // Quick response check
      const quickResponse = QUICK_RESPONSES[userMessage.toLowerCase().trim()];
      if (quickResponse) {
        console.log(`[LLM] Quick response selected (0ms)`);
        return quickResponse;
      }

      // Build context-aware messages
      const systemPrompt = `You are a helpful AI assistant on a phone call. Keep responses:
- Very concise (1-2 sentences maximum)
- Natural and conversational
- Clear and easy to understand over the phone
- Helpful and professional`;

      const messages = [
        { role: 'system', content: systemPrompt },
        ...conversationHistory.slice(-6), // Keep more context
        { role: 'user', content: userMessage }
      ];

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000);

      const response = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${OPENAI_API_KEY}`
        },
        body: JSON.stringify({
          model: 'gpt-4o-mini',
          messages: messages,
          max_tokens: 100,
          temperature: 0.7,
          stream: false
        }),
        signal: controller.signal
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new Error(`OpenAI API error: ${response.status}`);
      }

      const data = await response.json();
      const aiResponse = data.choices[0]?.message?.content?.trim();
      
      const llmTime = Date.now() - startTime;
      console.log(`[LLM] Response generated in ${llmTime}ms`);
      return aiResponse || "I'm sorry, could you repeat that?";
      
    } catch (error) {
      console.error('[LLM] Error:', error.message);
      return "I apologize, I'm having trouble processing that. Could you try again?";
    }
  };

  /**
   * Enhanced user input processing with better flow control
   */
  const processUserInput = async (transcript) => {
    if (isProcessing || !transcript?.trim() || !connectionActive) {
      console.log('[PROCESS] Skipping - already processing or invalid input');
      return;
    }
    
    isProcessing = true;
    const totalStart = Date.now();
    
    try {
      console.log(`[PROCESS] Starting processing for: "${transcript}"`);
      
      // Get AI response
      const aiResponse = await getAIResponse(transcript);
      
      if (aiResponse && connectionActive) {
        // Update conversation history
        conversationHistory.push(
          { role: 'user', content: transcript },
          { role: 'assistant', content: aiResponse }
        );

        // Trim history to prevent context bloat
        if (conversationHistory.length > 8) {
          conversationHistory = conversationHistory.slice(-8);
        }

        // Synthesize and stream response
        await synthesizeAndStreamAudio(aiResponse);
        
        const totalTime = Date.now() - totalStart;
        console.log(`[PROCESS] Total processing time: ${totalTime}ms`);
      }
      
    } catch (error) {
      console.error('[PROCESS] Error processing user input:', error.message);
      
      if (connectionActive) {
        await synthesizeAndStreamAudio("I'm sorry, I didn't catch that. Could you repeat?");
      }
    } finally {
      isProcessing = false;
    }
  };

  // Enhanced message handling
  ws.on('message', async (message) => {
    try {
      const messageStr = message.toString();
      lastActivityTime = Date.now();
      
      if (!messageStr.startsWith('{')) {
        return;
      }

      const data = JSON.parse(messageStr);

      switch (data.event) {
        case 'connected':
          console.log('[CZ] Connected - Protocol:', data.protocol, 'Version:', data.version);
          break;

        case 'start':
          console.log('[CZ] Call started');
          
          streamSid = data.streamSid || data.start?.streamSid;
          callSid = data.start?.callSid;
          accountSid = data.start?.accountSid;
          
          console.log('[CZ] Call Details:');
          console.log(`  StreamSID: ${streamSid}`);
          console.log(`  CallSID: ${callSid}`);
          console.log(`  AccountSID: ${accountSid}`);
          console.log(`  Tracks: ${JSON.stringify(data.start?.tracks)}`);
          console.log(`  Format: ${JSON.stringify(data.start?.mediaFormat)}`);
          
          // Custom parameters
          if (data.start?.customParameters) {
            console.log(`  Custom Params:`, data.start.customParameters);
          }

          // Start enhanced services
          connectToDeepgram(0);
          startKeepAlive();
          
          // Send initial test and greeting
          console.log('[CZ] Sending initial audio sequence');
          
          // Test tone first
          await synthesizeAndStreamAudio("test", 'en-IN', true);
          
          // Wait a moment, then send greeting
          setTimeout(async () => {
            if (connectionActive) {
              const greeting = data.start?.customParameters?.FirstName 
                ? `Hello ${data.start.customParameters.FirstName}! How can I help you today?`
                : 'Hello! How can I help you today?';
              
              console.log('[CZ] Sending greeting:', greeting);
              await synthesizeAndStreamAudio(greeting);
            }
          }, 2500);
          break;

        case 'media':
          // Forward to Deepgram if available
          if (data.media?.payload && deepgramWs && deepgramWs.readyState === WebSocket.OPEN && !sttFailed) {
            const audioBuffer = Buffer.from(data.media.payload, 'base64');
            deepgramWs.send(audioBuffer);
          }
          break;

        case 'stop':
          console.log('[CZ] Call ended - cleaning up');
          connectionActive = false;
          
          // Cleanup all resources
          if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
            deepgramWs.close();
          }
          
          if (silenceTimer) {
            clearTimeout(silenceTimer);
          }
          
          if (keepAliveInterval) {
            clearInterval(keepAliveInterval);
          }
          
          // Log session metrics
          console.log('[CZ] Session metrics:');
          console.log(`  Total chunks sent: ${audioMetrics.totalChunksSent}`);
          console.log(`  Total audio duration: ${(audioMetrics.totalAudioDuration / 1000).toFixed(1)}s`);
          console.log(`  Conversation turns: ${Math.floor(conversationHistory.length / 2)}`);
          break;

        case 'dtmf':
          console.log('[CZ] DTMF received:', data.dtmf?.digit);
          
          // Handle DTMF input as fallback when STT fails
          if (sttFailed && data.dtmf?.digit) {
            const dtmfResponses = {
              '1': 'You pressed 1. How can I help with your account?',
              '2': 'You pressed 2. Let me transfer you to support.',
              '0': 'You pressed 0. Connecting you to an operator.',
              '*': 'Let me repeat the options for you.',
              '#': 'Thank you for using our service.'
            };
            
            const response = dtmfResponses[data.dtmf.digit] || `You pressed ${data.dtmf.digit}. Please hold on.`;
            await synthesizeAndStreamAudio(response);
          }
          break;

        case 'vad':
          // Voice Activity Detection - could be used for improved timing
          if (data.vad?.value) {
            console.log(`[CZ] VAD: ${data.vad.value}`);
          }
          break;

        default:
          console.log(`[CZ] Unknown event: ${data.event}`);
      }
      
    } catch (error) {
      console.error('[CZ] Error processing message:', error.message);
    }
  });

  // Enhanced connection management
  ws.on('close', () => {
    console.log('[CZ] WebSocket connection closed');
    connectionActive = false;
    
    // Cleanup all resources
    if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
      deepgramWs.close();
    }
    
    if (silenceTimer) {
      clearTimeout(silenceTimer);
    }
    
    if (keepAliveInterval) {
      clearInterval(keepAliveInterval);
    }
    
    // Reset session state
    streamSid = null;
    callSid = null;
    accountSid = null;
    conversationHistory = [];
    isProcessing = false;
    userUtteranceBuffer = '';
    sttFailed = false;
  });

  ws.on('error', (error) => {
    console.error('[CZ] WebSocket error:', error.message);
    connectionActive = false;
  });
};

module.exports = {
  setupUnifiedVoiceServer
};