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

  /**
   * Check for quick responses first (0ms latency)
   */
  const getQuickResponse = (text) => {
    const normalized = text.toLowerCase().trim();
    return QUICK_RESPONSES[normalized] || null;
  };

  /**
   * Optimized text-to-speech with parallel processing and streaming
   */
  const synthesizeAndStreamAudio = async (text, language = 'en-IN') => {
    try {
      console.log(`[TTS] Synthesizing: "${text}"`);
      const startTime = Date.now();
      
      // Use fetch with timeout and optimized parameters
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 3000); // 3s timeout

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
          pace: 1.2, // Slightly faster pace
          loudness: 1.0,
          speech_sample_rate: 8000,
          enable_preprocessing: false, // Disable to save processing time
          model: 'bulbul:v1'
        }),
        signal: controller.signal
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new Error(`Sarvam API error: ${response.status}`);
      }

      const data = await response.json();
      const audioBase64 = data.audios?.[0];

      if (!audioBase64) {
        throw new Error('No audio data received from Sarvam');
      }

      const ttsTime = Date.now() - startTime;
      console.log(`[TTS] Audio generated in ${ttsTime}ms, streaming to call`);
      
      await streamAudioToCall(audioBase64);
      
    } catch (error) {
      console.error('[TTS] Error:', error.message);
      // Fallback: send a quick beep or skip
    }
  };

  /**
   * Optimized audio streaming with reduced chunks for lower latency
   */
  const streamAudioToCall = async (audioBase64) => {
    const audioBuffer = Buffer.from(audioBase64, 'base64');
    const CHUNK_SIZE = 160; // 10ms chunks for lower latency (was 320)
    
    let position = 0;
    const streamStart = Date.now();
    
    while (position < audioBuffer.length && ws.readyState === WebSocket.OPEN) {
      const chunk = audioBuffer.slice(position, position + CHUNK_SIZE);
      
      const mediaMessage = {
        event: 'media',
        streamSid: streamSid,
        media: {
          payload: chunk.toString('base64')
        }
      };

      ws.send(JSON.stringify(mediaMessage));
      position += CHUNK_SIZE;
      
      // Reduced delay for faster streaming
      if (position < audioBuffer.length) {
        await new Promise(resolve => setTimeout(resolve, 8)); // 8ms vs 20ms
      }
    }
    
    console.log(`[STREAM] Completed in ${Date.now() - streamStart}ms`);
  };

  /**
   * Connect to Deepgram with optimized settings and fallback
   */
  const connectToDeepgram = () => {
    console.log('[STT] Connecting to Deepgram');

    // Try primary connection first
    const primaryParams = {
      'sample_rate': '8000',
      'channels': '1', 
      'encoding': 'linear16',
      'model': 'nova-2',
      'language': 'en',
      'interim_results': 'true',
      'smart_format': 'true',
      'endpointing': '200',
      'vad_turnoff': '250',
      'punctuate': 'true'
    };

    const deepgramUrl = `wss://api.deepgram.com/v1/listen?${new URLSearchParams(primaryParams).toString()}`;
    
    console.log('[STT] Connecting to URL:', deepgramUrl);

    deepgramWs = new WebSocket(deepgramUrl, {
      headers: {
        'Authorization': `Token ${DEEPGRAM_API_KEY}`,
        'User-Agent': 'CZentrix-VoiceBot/1.0'
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
      console.error('[STT] Deepgram error:', error.message);
      console.error('[STT] Full error details:', error);
      
      // Attempt to reconnect after a brief delay
      setTimeout(() => {
        if (!deepgramWs || deepgramWs.readyState === WebSocket.CLOSED) {
          console.log('[STT] Attempting to reconnect to Deepgram...');
          connectToDeepgram();
        }
      }, 2000);
    });

    deepgramWs.on('close', () => {
      console.log('[STT] Deepgram connection closed');
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
          
          // Connect to Deepgram immediately
          connectToDeepgram();
          
          // Send optimized greeting
          const greeting = 'Hi! How can I help you?';
          console.log('[CZ] Sending greeting:', greeting);
          await synthesizeAndStreamAudio(greeting);
          break;

        case 'media':
          // Forward audio data to Deepgram for transcription
          if (data.media?.payload && deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
            const audioBuffer = Buffer.from(data.media.payload, 'base64');
            deepgramWs.send(audioBuffer);
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