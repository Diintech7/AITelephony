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

// Test Deepgram API key on startup
const testDeepgramConnection = async () => {
  try {
    const response = await fetch('https://api.deepgram.com/v1/projects', {
      headers: {
        'Authorization': `Token ${DEEPGRAM_API_KEY}`
      }
    });
    
    if (response.ok) {
      console.log('✅ Deepgram API key is valid');
    } else {
      console.error(`❌ Deepgram API key test failed: ${response.status}`);
      const errorText = await response.text();
      console.error('Error details:', errorText);
    }
  } catch (error) {
    console.error('❌ Deepgram API key test error:', error.message);
  }
};

// Run test on startup
testDeepgramConnection();

const fetch = globalThis.fetch || require('node-fetch');

// Precompiled responses for common queries (instant responses)
const QUICK_RESPONSES = {
  'hello': 'Hello! How can I help you?',
  'hi': 'Hi there! What can I do for you?',
  'how are you': 'I\'m doing great! How about you?',
  'thank you': 'You\'re welcome! Is there anything else I can help with?',
  'thanks': 'My pleasure! What else can I assist you with?',
  'yes': 'Great! What would you like to know more about?',
  'no': 'No problem! Is there something else I can help you with?',
  'okay': 'Perfect! What\'s next?'
};

/**
 * Setup unified voice server for C-Zentrix integration
 * @param {WebSocket} ws - The WebSocket connection from C-Zentrix
 */
const setupUnifiedVoiceServer = (ws) => {
  console.log('Setting up C-Zentrix voice server connection');

  // Session state for this connection
  let streamSid = null;
  let callSid = null;
  let accountSid = null;
  let conversationHistory = [];
  let deepgramWs = null;
  let isProcessing = false;
  let userUtteranceBuffer = '';
  let silenceTimer = null;
  let audioQueue = [];
  let isStreaming = false;
  let sttFailed = false; // Track STT failure state

  /**
   * Alternative streaming method - try different formats
   */
  const streamAudioAlternative = async (audioBase64) => {
    console.log('[STREAM-ALT] Trying alternative streaming method');
    
    const audioBuffer = Buffer.from(audioBase64, 'base64');
    
    // Try method 1: Larger chunks (20ms)
    const CHUNK_SIZE = 320; // 20ms chunks
    let position = 0;
    
    while (position < audioBuffer.length && ws.readyState === WebSocket.OPEN) {
      const chunk = audioBuffer.slice(position, position + CHUNK_SIZE);
      
      const mediaMessage = {
        event: 'media',
        streamSid: streamSid,
        media: {
          payload: chunk.toString('base64')
        }
      };

      try {
        ws.send(JSON.stringify(mediaMessage));
        position += CHUNK_SIZE;
        await new Promise(resolve => setTimeout(resolve, 20));
      } catch (error) {
        console.error('[STREAM-ALT] Error:', error.message);
        break;
      }
    }
    
    console.log('[STREAM-ALT] Alternative streaming completed');
  };

  /**
   * Convert linear16 to µ-law encoding (alternative format)
   */
  const convertToMuLaw = (linearBuffer) => {
    const muLawBuffer = Buffer.alloc(linearBuffer.length / 2);
    
    for (let i = 0; i < linearBuffer.length; i += 2) {
      const sample = linearBuffer.readInt16LE(i);
      // Convert 16-bit linear to µ-law
      const muLawValue = linearToMuLaw(sample);
      muLawBuffer[i / 2] = muLawValue;
    }
    
    return muLawBuffer;
  };

  /**
   * Linear to µ-law conversion
   */
  const linearToMuLaw = (sample) => {
    const BIAS = 0x84;
    const CLIP = 32635;
    const MULAW_MAX = 0x7F;
    
    if (sample < 0) {
      sample = -sample;
      let mulaw = 0x7F;
      
      if (sample < CLIP) {
        sample += BIAS;
        let exp = 7;
        
        for (let expMask = 0x4000; sample < expMask && exp > 0; exp--, expMask >>= 1) {}
        
        mulaw = (exp << 4) | ((sample >> (exp + 3)) & 0x0F);
      }
      
      return mulaw;
    } else {
      let mulaw = 0xFF;
      
      if (sample < CLIP) {
        sample += BIAS;
        let exp = 7;
        
        for (let expMask = 0x4000; sample < expMask && exp > 0; exp--, expMask >>= 1) {}
        
        mulaw = ~((exp << 4) | ((sample >> (exp + 3)) & 0x0F));
      }
      
      return mulaw & 0xFF;
    }
  };

  /**
   * Generate a simple test tone to verify audio pipeline
   */
  const generateTestTone = (frequency = 800, duration = 2.0, volume = 0.3) => {
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
   * Convert audio format - try different processing
   */
  const processAudioData = (audioBase64) => {
    const audioBuffer = Buffer.from(audioBase64, 'base64');
    
    // Check if audio needs format conversion
    console.log(`[AUDIO] Processing ${audioBuffer.length} bytes of audio data`);
    
    // Ensure proper 16-bit PCM format
    const processed = Buffer.alloc(audioBuffer.length);
    audioBuffer.copy(processed);
    
    // Apply volume normalization if needed
    for (let i = 0; i < processed.length; i += 2) {
      let sample = processed.readInt16LE(i);
      // Boost volume slightly
      sample = Math.min(32767, Math.max(-32768, Math.floor(sample * 1.2)));
      processed.writeInt16LE(sample, i);
    }
    
    return processed.toString('base64');
  };

  /**
   * Generate a simple tone as fallback audio
   */
  const generateSimpleTone = (frequency = 440, duration = 0.5) => {
    const sampleRate = 8000;
    const samples = Math.floor(sampleRate * duration);
    const buffer = Buffer.alloc(samples * 2); // 16-bit samples
    
    for (let i = 0; i < samples; i++) {
      const sample = Math.sin(2 * Math.PI * frequency * i / sampleRate) * 0.3; // 30% volume
      const intSample = Math.floor(sample * 32767);
      buffer.writeInt16LE(intSample, i * 2);
    }
    
    return buffer.toString('base64');
  };

  /**
   * Check for quick responses first (0ms latency)
   */
  const getQuickResponse = (text) => {
    const normalized = text.toLowerCase().trim();
    return QUICK_RESPONSES[normalized] || null;
  };

  /**
   * Optimized text-to-speech with audio format processing
   */
  const synthesizeAndStreamAudio = async (text, language = 'en-IN', testTone = false) => {
    try {
      let audioBase64;
      
      if (testTone) {
        console.log(`[TTS] Generating test tone instead of TTS`);
        audioBase64 = generateTestTone(800, 2.0, 0.3);
      } else {
        console.log(`[TTS] Synthesizing: "${text}"`);
        const startTime = Date.now();
        
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);

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
            pace: 1.1,
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
        console.log(`[TTS] Audio generated in ${ttsTime}ms, size: ${audioBase64.length} chars`);
      }
      
      // Process audio data
      const processedAudio = processAudioData(audioBase64);
      
      // Add a small delay before streaming
      await new Promise(resolve => setTimeout(resolve, 100));
      
      // Try streaming with linear16 format first
      console.log('[TTS] Attempting linear16 format streaming');
      const success = await streamAudioToCall(processedAudio, false);
      
      // If linear16 didn't work, try µ-law format after delay
      if (success) {
        console.log('[TTS] Linear16 streaming completed successfully');
      } else {
        console.log('[TTS] Trying µ-law format as alternative');
        setTimeout(async () => {
          await streamAudioToCall(processedAudio, true);
        }, 1000);
      }
      
    } catch (error) {
      console.error('[TTS] Error:', error.message);
      
      // Send a test tone as fallback
      console.log('[TTS] Sending test tone as fallback');
      const fallbackAudio = generateTestTone(600, 1.0, 0.4);
      const processedFallback = processAudioData(fallbackAudio);
      await streamAudioToCall(processedFallback);
    }
  };

  /**
   * Optimized audio streaming with format options for C-Zentrix
   */
  const streamAudioToCall = async (audioBase64, tryMuLaw = false) => {
    let audioBuffer = Buffer.from(audioBase64, 'base64');
    
    // Try µ-law format if specified
    if (tryMuLaw) {
      console.log('[STREAM] Converting to µ-law format');
      audioBuffer = convertToMuLaw(audioBuffer);
    }
    
    const CHUNK_SIZE = tryMuLaw ? 80 : 160; // µ-law uses 80 bytes, linear16 uses 160
    const BATCH_SIZE = 10;
    const BATCH_DELAY = 50;
    
    let position = 0;
    const streamStart = Date.now();
    const totalChunks = Math.ceil(audioBuffer.length / CHUNK_SIZE);
    
    console.log(`[STREAM] Starting ${tryMuLaw ? 'µ-law' : 'linear16'} audio stream: ${audioBuffer.length} bytes in ${totalChunks} chunks`);
    console.log(`[STREAM] StreamSID: ${streamSid}, WS State: ${ws.readyState}`);
    
    let chunksSuccessfullySent = 0;
    let batchCount = 0;
    
    while (position < audioBuffer.length && ws.readyState === WebSocket.OPEN) {
      // Send a batch of chunks quickly
      for (let i = 0; i < BATCH_SIZE && position < audioBuffer.length; i++) {
        const chunk = audioBuffer.slice(position, position + CHUNK_SIZE);
        
        // Pad smaller chunks with silence if needed
        const paddedChunk = chunk.length < CHUNK_SIZE ? 
          Buffer.concat([chunk, Buffer.alloc(CHUNK_SIZE - chunk.length)]) : chunk;
        
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
          position += CHUNK_SIZE;
        } catch (error) {
          console.error(`[STREAM] Failed to send chunk ${chunksSuccessfullySent}:`, error.message);
          return;
        }
      }
      
      batchCount++;
      
      // Log progress every 20 batches
      if (batchCount % 20 === 0) {
        console.log(`[STREAM] Sent ${chunksSuccessfullySent}/${totalChunks} chunks (${batchCount} batches)`);
      }
      
      // Short delay between batches
      if (position < audioBuffer.length) {
        await new Promise(resolve => setTimeout(resolve, BATCH_DELAY));
      }
    }
    
    // Add end silence
    try {
      const silenceChunk = Buffer.alloc(CHUNK_SIZE);
      const silenceMessage = {
        event: 'media',
        streamSid: streamSid,
        media: {
          payload: silenceChunk.toString('base64')
        }
      };
      ws.send(JSON.stringify(silenceMessage));
    } catch (error) {
      console.error('[STREAM] Failed to send end silence:', error.message);
    }
    
    const streamDuration = Date.now() - streamStart;
    const expectedAudioDuration = (totalChunks * (tryMuLaw ? 10 : 10)); // 10ms per chunk
    console.log(`[STREAM] Completed in ${streamDuration}ms, sent ${chunksSuccessfullySent} chunks`);
    console.log(`[STREAM] Expected audio duration: ${expectedAudioDuration}ms, Stream efficiency: ${(expectedAudioDuration/streamDuration*100).toFixed(1)}%`);
    
    return chunksSuccessfullySent === totalChunks; // Return success status
  };

  /**
   * Connect to Deepgram with optimized settings and fallback
   */
  const connectToDeepgram = (attemptCount = 0) => {
    console.log(`[STT] Connecting to Deepgram (attempt ${attemptCount + 1})`);

    // Start with minimal parameters that always work
    let params = {
      'encoding': 'linear16',
      'sample_rate': '8000',
      'channels': '1'
    };

    // Add additional parameters based on attempt
    if (attemptCount === 0) {
      // First attempt: Full feature set
      params = {
        ...params,
        'model': 'nova-2',
        'language': 'en',
        'interim_results': 'true',
        'smart_format': 'true',
        'endpointing': '300',
        'punctuate': 'true'
      };
    } else if (attemptCount === 1) {
      // Second attempt: Basic features
      params = {
        ...params,
        'model': 'base',
        'language': 'en',
        'interim_results': 'true'
      };
    }
    // Third attempt uses minimal params only

    const deepgramUrl = `wss://api.deepgram.com/v1/listen?${new URLSearchParams(params).toString()}`;
    
    console.log(`[STT] Connecting to URL: ${deepgramUrl}`);
    console.log(`[STT] Using API key: ${DEEPGRAM_API_KEY ? `${DEEPGRAM_API_KEY.substring(0, 8)}...` : 'MISSING'}`);

    deepgramWs = new WebSocket(deepgramUrl, {
      headers: {
        'Authorization': `Token ${DEEPGRAM_API_KEY}`
      }
    });

    deepgramWs.on('open', () => {
      console.log('[STT] Connected to Deepgram successfully');
    });

    deepgramWs.on('message', async (data) => {
      const response = JSON.parse(data);
      
      if (response.type === 'Results') {
        const transcript = response.channel?.alternatives?.[0]?.transcript;
        const isFinal = response.is_final;
        const confidence = response.channel?.alternatives?.[0]?.confidence;

        if (transcript?.trim() && confidence > 0.7) { // Only process high-confidence results
          if (isFinal) {
            console.log(`[STT] Final transcript: "${transcript}" (${confidence})`);
            userUtteranceBuffer += (userUtteranceBuffer ? ' ' : '') + transcript.trim();
            
            // Clear any existing silence timer
            if (silenceTimer) {
              clearTimeout(silenceTimer);
              silenceTimer = null;
            }
            
            // Process immediately for short utterances
            if (userUtteranceBuffer.length < 50) {
              await processUserInput(userUtteranceBuffer);
              userUtteranceBuffer = '';
            } else {
              // Set shorter timer for longer utterances
              silenceTimer = setTimeout(async () => {
                if (userUtteranceBuffer.trim()) {
                  await processUserInput(userUtteranceBuffer);
                  userUtteranceBuffer = '';
                }
              }, 100); // 100ms vs longer delays
            }
          }
        }
      } else if (response.type === 'UtteranceEnd') {
        if (userUtteranceBuffer.trim()) {
          console.log(`[STT] Utterance end: "${userUtteranceBuffer}"`);
          await processUserInput(userUtteranceBuffer);
          userUtteranceBuffer = '';
        }
      }
    });

    deepgramWs.on('error', (error) => {
      console.error(`[STT] Deepgram error (attempt ${attemptCount + 1}):`, error.message);
      
      // Try different connection approaches
      if (attemptCount < 3) {
        console.log(`[STT] Retrying with different parameters...`);
        setTimeout(() => {
          connectToDeepgram(attemptCount + 1);
        }, 1000 * (attemptCount + 1)); // Increasing delay
      } else {
        console.error('[STT] All Deepgram connection attempts failed. Check API key and permissions.');
        sttFailed = true;
        
        // Provide feedback to user about STT failure (without await - fire and forget)
        const fallbackMessage = "I'm having trouble with speech recognition right now, but I can still help you. Please use the keypad to navigate options.";
        synthesizeAndStreamAudio(fallbackMessage).catch(err => 
          console.error('[STT] Fallback message error:', err.message)
        );
      }
    });

    deepgramWs.on('close', (code, reason) => {
      console.log(`[STT] Deepgram connection closed: ${code} - ${reason}`);
    });
  };

  /**
   * Optimized AI response with parallel processing
   */
  const getAIResponse = async (userMessage) => {
    try {
      console.log(`[LLM] Processing: "${userMessage}"`);
      const startTime = Date.now();

      // Check for quick responses first
      const quickResponse = getQuickResponse(userMessage);
      if (quickResponse) {
        console.log(`[LLM] Quick response: "${quickResponse}" (0ms)`);
        return quickResponse;
      }

      const messages = [
        {
          role: 'system',
          content: 'You are a helpful AI assistant. Give very concise responses (1-2 sentences max). Be direct and helpful.'
        },
        ...conversationHistory.slice(-4), // Reduced context for faster processing
        {
          role: 'user',
          content: userMessage
        }
      ];

      // Optimized API call with timeout
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 4000); // 4s timeout

      const response = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${OPENAI_API_KEY}`
        },
        body: JSON.stringify({
          model: 'gpt-4o-mini',
          messages: messages,
          max_tokens: 80, // Reduced from 150 for faster generation
          temperature: 0.5, // Reduced for more focused responses
          stream: false // Disable streaming for this use case
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
      console.log(`[LLM] Response: "${aiResponse}" (${llmTime}ms)`);
      return aiResponse;
      
    } catch (error) {
      console.error('[LLM] Error:', error.message);
      return 'I apologize, but I encountered an issue. Could you please try again?';
    }
  };

  /**
   * Process user speech input with optimized flow
   */
  const processUserInput = async (transcript) => {
    if (isProcessing || !transcript.trim()) return;
    
    isProcessing = true;
    const totalStart = Date.now();
    
    try {
      // Start AI response and TTS in parallel for quick responses
      const quickResponse = getQuickResponse(transcript);
      
      if (quickResponse) {
        // Immediate response for common phrases
        conversationHistory.push(
          { role: 'user', content: transcript },
          { role: 'assistant', content: quickResponse }
        );
        
        await synthesizeAndStreamAudio(quickResponse);
      } else {
        // Parallel processing: Start AI response generation
        const aiResponsePromise = getAIResponse(transcript);
        
        // Get AI response
        const aiResponse = await aiResponsePromise;
        
        if (aiResponse) {
          // Add to conversation history
          conversationHistory.push(
            { role: 'user', content: transcript },
            { role: 'assistant', content: aiResponse }
          );

          // Keep history lean for performance
          if (conversationHistory.length > 6) {
            conversationHistory = conversationHistory.slice(-6);
          }

          // Convert to speech and stream back
          await synthesizeAndStreamAudio(aiResponse);
        }
      }
      
      const totalTime = Date.now() - totalStart;
      console.log(`[TOTAL] Processing completed in ${totalTime}ms`);
      
    } catch (error) {
      console.error('[PROCESS] Error processing user input:', error.message);
    } finally {
      isProcessing = false;
    }
  };

  // Handle incoming messages from C-Zentrix
  ws.on('message', async (message) => {
    try {
      const messageStr = message.toString();
      
      // Skip non-JSON messages
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
          
          // Extract call information
          streamSid = data.streamSid || data.start?.streamSid;
          callSid = data.start?.callSid;
          accountSid = data.start?.accountSid;
          
          console.log('[CZ] StreamSID:', streamSid);
          console.log('[CZ] CallSID:', callSid);
          console.log('[CZ] AccountSID:', accountSid);
          console.log('[CZ] Tracks:', JSON.stringify(data.start?.tracks));
          console.log('[CZ] Media Format:', JSON.stringify(data.start?.mediaFormat));
          
          // Log custom parameters if present
          if (data.start?.customParameters) {
            console.log('[CZ] Custom Parameters:', data.start.customParameters);
          }

          // Connect to Deepgram for speech recognition
          connectToDeepgram(0);
          
          // Send test tone first to verify audio pipeline works
          console.log('[CZ] Sending test tone first');
          await synthesizeAndStreamAudio("Testing", 'en-IN', true); // Send test tone
          
          // Then send greeting after a delay
          setTimeout(async () => {
            const greeting = 'Hi! How can I help you?';
            console.log('[CZ] Sending greeting:', greeting);
            await synthesizeAndStreamAudio(greeting);
          }, 3000); // 3 second delay between test tone and greeting
          break;

        case 'media':
          // Forward audio data to Deepgram for transcription
          if (data.media?.payload && deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
            const audioBuffer = Buffer.from(data.media.payload, 'base64');
            deepgramWs.send(audioBuffer);
          } else if (sttFailed) {
            // If STT failed, we can still respond to DTMF or provide menu-driven responses
            console.log('[STT] Audio received but STT unavailable - consider implementing DTMF fallback');
          }
          break;

        case 'stop':
          console.log('[CZ] Call ended');
          
          if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
            deepgramWs.close();
          }
          
          if (silenceTimer) {
            clearTimeout(silenceTimer);
          }
          break;

        case 'dtmf':
          console.log('[CZ] DTMF received:', data.dtmf?.digit);
          break;

        default:
          // Reduced logging for performance
          break;
      }
      
    } catch (error) {
      console.error('[CZ] Error processing message:', error.message);
    }
  });

  // Handle connection close
  ws.on('close', () => {
    console.log('[CZ] WebSocket connection closed');
    
    // Cleanup
    if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
      deepgramWs.close();
    }
    
    if (silenceTimer) {
      clearTimeout(silenceTimer);
    }
    
    // Reset session state
    streamSid = null;
    callSid = null;
    accountSid = null;
    conversationHistory = [];
    isProcessing = false;
    userUtteranceBuffer = '';
    audioQueue = [];
    isStreaming = false;
  });

  // Handle errors
  ws.on('error', (error) => {
    console.error('[CZ] WebSocket error:', error.message);
  });
};

module.exports = {
  setupUnifiedVoiceServer
};