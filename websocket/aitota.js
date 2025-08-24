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

  /**
   * Synthesize text to speech using Sarvam API and stream to call
   */
  const synthesizeAndStreamAudio = async (text, language = 'en-IN') => {
    try {
      console.log(`[TTS] Synthesizing: "${text}"`);
      
      const response = await fetch('https://api.sarvam.ai/text-to-speech', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'API-Subscription-Key': SARVAM_API_KEY
        },
        body: JSON.stringify({
          inputs: [text],
          target_language_code: language,
          speaker: 'meera',
          pitch: 0,
          pace: 1.0,
          loudness: 1.0,
          speech_sample_rate: 8000,
          enable_preprocessing: true,
          model: 'bulbul:v1'
        })
      });

      if (!response.ok) {
        throw new Error(`Sarvam API error: ${response.status}`);
      }

      const data = await response.json();
      const audioBase64 = data.audios?.[0];

      if (!audioBase64) {
        throw new Error('No audio data received from Sarvam');
      }

      console.log('[TTS] Audio generated, streaming to call');
      await streamAudioToCall(audioBase64);
      
    } catch (error) {
      console.error('[TTS] Error:', error.message);
    }
  };

  /**
   * Stream audio back to C-Zentrix in optimal chunks
   */
  const streamAudioToCall = async (audioBase64) => {
    const audioBuffer = Buffer.from(audioBase64, 'base64');
    const CHUNK_SIZE = 320; // 20ms at 8kHz, 16-bit = 320 bytes
    
    let position = 0;
    while (position < audioBuffer.length) {
      const chunk = audioBuffer.slice(position, position + CHUNK_SIZE);
      
      const mediaMessage = {
        event: 'media',
        streamSid: streamSid,
        media: {
          payload: chunk.toString('base64')
        }
      };

      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(mediaMessage));
      } else {
        break;
      }

      position += CHUNK_SIZE;
      
      // Maintain proper audio timing
      if (position < audioBuffer.length) {
        await new Promise(resolve => setTimeout(resolve, 20));
      }
    }
  };

  /**
   * Connect to Deepgram for speech-to-text
   */
  const connectToDeepgram = () => {
    console.log('[STT] Connecting to Deepgram');

    const deepgramUrl = 'wss://api.deepgram.com/v1/listen?' + 
      'sample_rate=8000&' +
      'channels=1&' +
      'encoding=linear16&' +
      'model=nova-2&' +
      'language=en&' +
      'interim_results=true&' +
      'smart_format=true&' +
      'endpointing=300';

    deepgramWs = new WebSocket(deepgramUrl, {
      headers: {
        'Authorization': `Token ${DEEPGRAM_API_KEY}`
      }
    });

    deepgramWs.on('open', () => {
      console.log('[STT] Connected to Deepgram');
    });

    deepgramWs.on('message', async (data) => {
      const response = JSON.parse(data);
      
      if (response.type === 'Results') {
        const transcript = response.channel?.alternatives?.[0]?.transcript;
        const isFinal = response.is_final;

        if (transcript?.trim()) {
          if (isFinal) {
            console.log(`[STT] Final transcript: "${transcript}"`);
            userUtteranceBuffer += (userUtteranceBuffer ? ' ' : '') + transcript.trim();
            await processUserInput(userUtteranceBuffer);
            userUtteranceBuffer = '';
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
    });

    deepgramWs.on('close', () => {
      console.log('[STT] Deepgram connection closed');
    });
  };

  /**
   * Get AI response from OpenAI
   */
  const getAIResponse = async (userMessage) => {
    try {
      console.log(`[LLM] Processing: "${userMessage}"`);

      const messages = [
        {
          role: 'system',
          content: 'You are a helpful AI assistant. Keep responses concise and conversational. Always end with a relevant follow-up question to keep the conversation flowing.'
        },
        ...conversationHistory.slice(-6),
        {
          role: 'user',
          content: userMessage
        }
      ];

      const response = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${OPENAI_API_KEY}`
        },
        body: JSON.stringify({
          model: 'gpt-4o-mini',
          messages: messages,
          max_tokens: 150,
          temperature: 0.7
        })
      });

      if (!response.ok) {
        throw new Error(`OpenAI API error: ${response.status}`);
      }

      const data = await response.json();
      const aiResponse = data.choices[0]?.message?.content?.trim();
      
      console.log(`[LLM] Response: "${aiResponse}"`);
      return aiResponse;
      
    } catch (error) {
      console.error('[LLM] Error:', error.message);
      return 'I apologize, but I encountered an issue. Could you please try again?';
    }
  };

  /**
   * Process user speech input
   */
  const processUserInput = async (transcript) => {
    if (isProcessing || !transcript.trim()) return;
    
    isProcessing = true;
    
    try {
      // Get AI response
      const aiResponse = await getAIResponse(transcript);
      
      if (aiResponse) {
        // Add to conversation history
        conversationHistory.push(
          { role: 'user', content: transcript },
          { role: 'assistant', content: aiResponse }
        );

        // Keep history manageable
        if (conversationHistory.length > 10) {
          conversationHistory = conversationHistory.slice(-10);
        }

        // Convert to speech and stream back
        await synthesizeAndStreamAudio(aiResponse);
      }
      
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
          console.log('[CZ] Tracks:', data.start?.tracks);
          
          // Log custom parameters if present
          if (data.start?.customParameters) {
            console.log('[CZ] Custom Parameters:', data.start.customParameters);
          }

          // Connect to Deepgram for speech recognition
          connectToDeepgram();
          
          // Send static greeting message
          const greeting = 'Hello! I am your AI assistant. How can I help you today?';
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
          console.log('[CZ] Stop details:', data.stop);
          
          if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
            deepgramWs.close();
          }
          break;

        case 'dtmf':
          console.log('[CZ] DTMF received:', data.dtmf?.digit);
          break;

        case 'vad':
          console.log('[CZ] Voice activity:', data.vad?.value);
          break;

        default:
          console.log('[CZ] Unknown event:', data.event);
      }
      
    } catch (error) {
      console.error('[CZ] Error processing message:', error.message);
    }
  });

  // Handle connection close
  ws.on('close', () => {
    console.log('[CZ] WebSocket connection closed');
    
    // Cleanup Deepgram connection
    if (deepgramWs && deepgramWs.readyState === WebSocket.OPEN) {
      deepgramWs.close();
    }
    
    // Reset session state
    streamSid = null;
    callSid = null;
    accountSid = null;
    conversationHistory = [];
    isProcessing = false;
    userUtteranceBuffer = '';
  });

  // Handle errors
  ws.on('error', (error) => {
    console.error('[CZ] WebSocket error:', error.message);
  });
};

module.exports = {
  setupUnifiedVoiceServer
};