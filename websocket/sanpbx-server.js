// websocket/sanpbx-server-integrated.js
// Complete SanIPPBX WebSocket Server with AI Integration and Performance Optimization

const WebSocket = require('ws');
const { createClient } = require('@deepgram/sdk');
const OpenAI = require('openai');
const { AudioProcessor, AUDIO_FORMATS } = require('./audio-utils');
const { PerformanceMonitor, LatencyOptimizer } = require('./performance-monitor');
const EventEmitter = require('events');

// Initialize performance monitoring
const performanceMonitor = new PerformanceMonitor({
  enableRealTimeMonitoring: true,
  latencyThresholds: {
    excellent: 50,
    good: 100,
    acceptable: 150,
    poor: 300
  },
  alertThreshold: 200
});

const latencyOptimizer = new LatencyOptimizer(performanceMonitor, {
  autoOptimize: true,
  targetLatency: 100,
  aggressiveness: 'high'
});

// Session storage with performance tracking
const activeSessions = new Map();
const sessionStats = new Map();

class OptimizedSanIPPBXSession extends EventEmitter {
  constructor(ws, sessionData) {
    super();
    
    // Basic session info
    this.ws = ws;
    this.callId = sessionData.callId;
    this.streamId = sessionData.streamId;
    this.channelId = sessionData.channelId;
    this.callerId = sessionData.callerId;
    this.callDirection = sessionData.callDirection;
    this.did = sessionData.did;
    
    // Performance tracking
    this.sessionStartTime = Date.now();
    this.lastActivityTime = Date.now();
    this.audioProcessor = new AudioProcessor({
      enablePreprocessing: true,
      enableVAD: true,
      bufferOptimization: true,
      silenceThreshold: 300
    });
    
    // AI services
    this.deepgram = null;
    this.openai = null;
    this.sarvam = null;
    
    // Conversation management
    this.conversationHistory = [];
    this.isProcessingAudio = false;
    this.isSpeaking = false;
    this.currentTranscription = '';
    
    // Performance metrics
    this.metrics = {
      packetsReceived: 0,
      packetsProcessed: 0,
      audioLatency: [],
      aiLatency: [],
      errors: 0,
      conversationTurns: 0
    };
    
    // Initialize all services
    this.initializeServices();
  }

  async initializeServices() {
    try {
      const startTime = performance.now();
      
      await Promise.all([
        this.initializeDeepgram(),
        this.initializeOpenAI(),
        this.initializeSarvam()
      ]);
      
      const initTime = performance.now() - startTime;
      performanceMonitor.recordAudioLatency(startTime, performance.now());
      
      console.log(`🚀 [SANPBX-SESSION] All services initialized in ${initTime.toFixed(2)}ms for: ${this.callId}`);
      
      // Send welcome message after short delay
      setTimeout(() => {
        this.sendWelcomeMessage();
      }, 500);
      
    } catch (error) {
      console.error(`❌ [SANPBX-SESSION] Service initialization failed:`, error.message);
      this.metrics.errors++;
      this.emit('error', error);
    }
  }

  async initializeDeepgram() {
    const deepgram = createClient(process.env.DEEPGRAM_API_KEY);
    
    // Optimized connection settings for minimal latency
    const connection = deepgram.listen.live({
      model: 'nova-2',
      language: 'en',
      smart_format: true,
      interim_results: false, // Disable for lower latency
      endpointing: 200, // Faster endpointing
      vad_events: true,
      punctuate: true,
      profanity_filter: false,
      diarize: false,
      multichannel: false,
      alternatives: 1,
      numerals: true,
      filler_words: false,
      utterance_end_ms: 1000, // Changed from 800 to 1000 for Deepgram API compliance
      encoding: 'linear16',
      sample_rate: 8000,
      channels: 1
    });

    connection.on('open', () => {
      console.log(`🎤 [DEEPGRAM] Connected for session: ${this.callId}`);
    });

    connection.on('Results', async (data) => {
      const transcript = data.channel?.alternatives?.[0]?.transcript;
      if (transcript && transcript.trim() && data.is_final) {
        const transcriptionEndTime = performance.now();
        
        // Record transcription latency
        performanceMonitor.recordAILatency('transcription', this.lastActivityTime, transcriptionEndTime, true);
        
        console.log(`🗣️ [TRANSCRIPT] "${transcript}" (${this.callId})`);
        this.currentTranscription = transcript;
        
        // Process conversation
        await this.processConversation(transcript);
        
        this.metrics.conversationTurns++;
      }
    });

    connection.on('error', (error) => {
      console.error(`❌ [DEEPGRAM] Error:`, error);
      this.metrics.errors++;
      performanceMonitor.recordAILatency('transcription', this.lastActivityTime, performance.now(), false);
    });

    this.deepgram = connection;
  }

  initializeOpenAI() {
    this.openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY,
      timeout: 10000 // 10 second timeout for real-time performance
    });
    
    // Optimized system prompt for telephony
    this.conversationHistory = [{
      role: 'system',
      content: `You are a professional phone assistant. Rules:
- Keep responses under 25 words
- Be natural and conversational  
- Ask clarifying questions when needed
- End calls politely if requested
- Current caller: ${this.callerId}, DID: ${this.did}`
    }];
    
    console.log(`🧠 [OPENAI] Initialized for session: ${this.callId}`);
  }

  async initializeSarvam() {
    // Sarvam AI for Indian language support (optional)
    this.sarvam = {
      apiKey: process.env.SARVAM_API_KEY,
      baseUrl: 'https://api.sarvam.ai',
      available: !!process.env.SARVAM_API_KEY
    };
    
    if (this.sarvam.available) {
      console.log(`🇮🇳 [SARVAM] Initialized for session: ${this.callId}`);
    }
  }

  async sendWelcomeMessage() {
    const welcomeMessages = [
      "Hello! How can I assist you today?",
      "Hi there! What can I help you with?",
      "Welcome! How may I help you?"
    ];
    
    const message = welcomeMessages[Math.floor(Math.random() * welcomeMessages.length)];
    await this.generateAndSendResponse(message);
  }

  processIncomingAudio(base64Audio) {
    const audioStartTime = performance.now();
    this.metrics.packetsReceived++;
    this.lastActivityTime = Date.now();
    
    try {
      // Process audio with optimized pipeline
      const processedAudio = this.audioProcessor.processIncomingAudio(base64Audio);
      
      if (processedAudio && this.deepgram && !this.isSpeaking) {
        // Send to Deepgram only if we have valid audio and aren't speaking
        this.deepgram.send(processedAudio);
        this.metrics.packetsProcessed++;
        
        // Record audio processing latency
        const audioLatency = performance.now() - audioStartTime;
        performanceMonitor.recordAudioLatency(audioStartTime, performance.now());
        this.metrics.audioLatency.push(audioLatency);
        
        // Keep latency history manageable
        if (this.metrics.audioLatency.length > 100) {
          this.metrics.audioLatency.shift();
        }
      }
      
    } catch (error) {
      console.error(`❌ [AUDIO-PROCESSING] Error:`, error.message);
      this.metrics.errors++;
    }
  }

  async processConversation(transcript) {
    const conversationStartTime = performance.now();
    
    try {
      // Add user message to history
      this.conversationHistory.push({
        role: 'user',
        content: transcript
      });
      
      // Keep conversation history manageable (last 6 exchanges)
      if (this.conversationHistory.length > 13) { // 1 system + 12 messages (6 exchanges)
        this.conversationHistory = [
          this.conversationHistory[0],
          ...this.conversationHistory.slice(-12)
        ];
      }
      
      // Generate AI response with optimized settings
      const completion = await this.openai.chat.completions.create({
        model: 'gpt-3.5-turbo', // Fastest available model
        messages: this.conversationHistory,
        max_tokens: 50, // Limit response length for speed
        temperature: 0.7,
        presence_penalty: 0.6,
        frequency_penalty: 0.3,
        stream: false // Disable streaming for simplicity
      });
      
      const aiResponse = completion.choices[0]?.message?.content?.trim();
      
      if (aiResponse) {
        // Record conversation latency
        const conversationLatency = performance.now() - conversationStartTime;
        performanceMonitor.recordAILatency('conversation', conversationStartTime, performance.now(), true);
        this.metrics.aiLatency.push(conversationLatency);
        
        // Add assistant response to history
        this.conversationHistory.push({
          role: 'assistant',
          content: aiResponse
        });
        
        console.log(`🤖 [AI-RESPONSE] "${aiResponse}" (${conversationLatency.toFixed(2)}ms)`);
        
        // Convert to speech and send
        await this.generateAndSendResponse(aiResponse);
      }
      
    } catch (error) {
      console.error(`❌ [CONVERSATION] Error:`, error.message);
      this.metrics.errors++;
      performanceMonitor.recordAILatency('conversation', conversationStartTime, performance.now(), false);
      
      // Send fallback response
      await this.generateAndSendResponse("I'm sorry, could you please repeat that?");
    }
  }

  async generateAndSendResponse(text) {
    const ttsStartTime = performance.now();
    this.isSpeaking = true;
    
    try {
      // Generate speech with OpenAI TTS
      const mp3 = await this.openai.audio.speech.create({
        model: 'tts-1', // Fastest TTS model
        voice: 'alloy',
        input: text,
        response_format: 'mp3',
        speed: 1.0 // Normal speed for clarity
      });
      
      const mp3Buffer = Buffer.from(await mp3.arrayBuffer());
      
      // Convert MP3 to PCM for telephony
      const pcmAudio = await this.audioProcessor.convertMp3ToPcm(mp3Buffer);
      
      // Convert to base64 and send to SanIPPBX
      const base64Audio = this.audioProcessor.convertPcmToBase64(pcmAudio);
      this.sendAudioToSanIPPBX(base64Audio);
      
      // Record TTS latency
      const ttsLatency = performance.now() - ttsStartTime;
      performanceMonitor.recordAILatency('tts', ttsStartTime, performance.now(), true);
      
      console.log(`🔊 [TTS] Audio generated and sent in ${ttsLatency.toFixed(2)}ms`);
      
      // Allow audio processing to resume after speech
      setTimeout(() => {
        this.isSpeaking = false;
      }, Math.max(1000, text.length * 50)); // Estimate speech duration
      
    } catch (error) {
      console.error(`❌ [TTS] Error:`, error.message);
      this.metrics.errors++;
      performanceMonitor.recordAILatency('tts', ttsStartTime, performance.now(), false);
      this.isSpeaking = false;
    }
  }

  sendAudioToSanIPPBX(base64Audio) {
    try {
      if (this.ws.readyState !== WebSocket.OPEN) {
        console.warn(`⚠️ [SANPBX-AUDIO] WebSocket not open for ${this.callId}`);
        return;
      }
      
      // Send audio in chunks to avoid overwhelming the WebSocket
      const chunks = this.audioProcessor.createAudioChunks(Buffer.from(base64Audio, 'base64'));
      
      chunks.forEach((chunk, index) => {
        setTimeout(() => {
          const audioEvent = {
            event: 'media',
            streamId: this.streamId,
            callId: this.callId,
            channelId: this.channelId,
            media: {
              payload: chunk.toString('base64')
            },
            timestamp: new Date().toISOString()
          };
          
          this.ws.send(JSON.stringify(audioEvent));
        }, index * 20); // 20ms between chunks
      });
      
      console.log(`📤 [SANPBX-AUDIO] Sent ${chunks.length} audio chunks`);
      
    } catch (error) {
      console.error(`❌ [SANPBX-AUDIO] Send error:`, error.message);
      this.metrics.errors++;
    }
  }

  handleDTMF(digit, duration) {
    console.log(`📞 [DTMF] ${digit} pressed (${duration}ms) - ${this.callId}`);
    
    const dtmfResponses = {
      '0': "You pressed zero. Connecting you to an operator.",
      '1': "You pressed one. Please hold while I process your request.",
      '2': "You pressed two. Let me check that information for you.",
      '3': "You pressed three. I'll help you with that.",
      '4': "You pressed four. One moment please.",
      '5': "You pressed five. How can I assist you further?",
      '6': "You pressed six. I'm here to help.",
      '7': "You pressed seven. Please continue.",
      '8': "You pressed eight. What would you like to know?",
      '9': "You pressed nine. I'm listening.",
      '*': "Thank you for using our service. Is there anything else I can help you with?",
      '#': "Thank you for calling. Have a great day! Goodbye."
    };
    
    const response = dtmfResponses[digit] || `You pressed ${digit}. How can I help you?`;
    
    // Send immediate response
    this.generateAndSendResponse(response);
    
    // Handle special actions
    if (digit === '#') {
      setTimeout(() => {
        this.hangup();
      }, 3000); // Hang up after 3 seconds
    }
    
    if (digit === '0') {
      // Could implement transfer logic here
      this.emit('transfer-requested', { destination: 'operator' });
    }
  }

  handleTransferCall(transferTo) {
    console.log(`🔄 [TRANSFER] Call ${this.callId} transferring to: ${transferTo}`);
    
    // Send transfer confirmation
    this.generateAndSendResponse("Transferring your call now. Please hold.");
    
    const transferEvent = {
      event: 'transfer-call-response',
      status: true,
      message: 'Transfer initiated successfully',
      data: { transferTo },
      status_code: 200,
      channelId: this.channelId,
      callId: this.callId,
      streamId: this.streamId,
      timestamp: new Date().toISOString()
    };
    
    if (this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(transferEvent));
    }
    
    // Clean up session after transfer
    setTimeout(() => {
      this.cleanup();
    }, 2000);
  }

  hangup() {
    console.log(`📞 [HANGUP] Terminating call: ${this.callId}`);
    
    const hangupEvent = {
      event: 'hangup-call-response',
      status: true,
      message: 'Call terminated successfully',
      data: {
        duration: Date.now() - this.sessionStartTime,
        conversationTurns: this.metrics.conversationTurns,
        packetsProcessed: this.metrics.packetsProcessed
      },
      status_code: 200,
      channelId: this.channelId,
      callId: this.callId,
      streamId: this.streamId,
      timestamp: new Date().toISOString()
    };
    
    if (this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(hangupEvent));
    }
    
    this.cleanup();
  }

  getSessionStats() {
    const sessionDuration = Date.now() - this.sessionStartTime;
    const avgAudioLatency = this.metrics.audioLatency.length > 0 ? 
      this.metrics.audioLatency.reduce((a, b) => a + b, 0) / this.metrics.audioLatency.length : 0;
    const avgAILatency = this.metrics.aiLatency.length > 0 ?
      this.metrics.aiLatency.reduce((a, b) => a + b, 0) / this.metrics.aiLatency.length : 0;
    
    return {
      callId: this.callId,
      duration: sessionDuration,
      packetsReceived: this.metrics.packetsReceived,
      packetsProcessed: this.metrics.packetsProcessed,
      conversationTurns: this.metrics.conversationTurns,
      errors: this.metrics.errors,
      avgAudioLatency: Math.round(avgAudioLatency),
      avgAILatency: Math.round(avgAILatency),
      processingRate: this.metrics.packetsProcessed / (sessionDuration / 1000),
      errorRate: this.metrics.errors / Math.max(1, this.metrics.packetsReceived) * 100,
      quality: this.calculateCallQuality()
    };
  }

  calculateCallQuality() {
    const stats = this.getSessionStats();
    let score = 100;
    
    // Penalize high latency
    if (stats.avgAudioLatency > 100) score -= 20;
    if (stats.avgAILatency > 500) score -= 15;
    
    // Penalize errors
    if (stats.errorRate > 5) score -= 30;
    if (stats.errorRate > 10) score -= 50;
    
    // Penalize low processing rate
    if (stats.processingRate < 10) score -= 25;
    
    return Math.max(0, Math.min(100, score));
  }

  cleanup() {
    try {
      console.log(`🧹 [CLEANUP] Session cleanup for: ${this.callId}`);
      
      // Log final statistics
      const finalStats = this.getSessionStats();
      console.log(`📊 [SESSION-STATS] Final stats for ${this.callId}:`, finalStats);
      
      // Close Deepgram connection
      if (this.deepgram) {
        try {
          this.deepgram.finish();
        } catch (error) {
          console.warn(`⚠️ [CLEANUP] Deepgram cleanup error:`, error.message);
        }
      }
      
      // Clean up audio processor
      if (this.audioProcessor) {
        this.audioProcessor.cleanup();
      }
      
      // Store session stats for analytics
      sessionStats.set(this.callId, finalStats);
      
      // Remove from active sessions
      activeSessions.delete(this.callId);
      activeSessions.delete(this.streamId);
      
      // Clean up event listeners
      this.removeAllListeners();
      
      console.log(`✅ [CLEANUP] Session ${this.callId} cleaned up successfully`);
      
    } catch (error) {
      console.error(`❌ [CLEANUP] Error during cleanup:`, error.message);
    }
  }
}

/**
 * Enhanced WebSocket Server Setup with Performance Optimization
 */
function setupEnhancedSanPbxWebSocketServer(wss) {
  console.log('🚀 [SANPBX-WS] Setting up enhanced SanIPPBX WebSocket server with AI integration...');
  
  // Performance monitoring setup
  performanceMonitor.on('alert', (alert) => {
    console.warn(`🚨 [PERFORMANCE-ALERT] ${alert.type}: ${alert.message} (${alert.latency.toFixed(2)}ms)`);
  });
  
  latencyOptimizer.on('optimization-applied', (optimization) => {
    console.log(`🔧 [OPTIMIZER] Applied optimizations for ${optimization.category}:`, optimization.optimizations);
  });
  
  wss.on('connection', (ws, req) => {
    const clientIP = req.socket.remoteAddress;
    console.log(`🔗 [SANPBX-WS] New enhanced connection from ${clientIP}`);
    
    let currentSession = null;
    let connectionStartTime = Date.now();
    
    // Send enhanced welcome message
    const welcomeMessage = {
      event: 'connection-ready',
      message: 'Enhanced SanIPPBX WebSocket server ready with AI integration',
      features: [
        'Real-time speech recognition',
        'AI-powered conversations', 
        'Text-to-speech synthesis',
        'Performance optimization',
        'Latency monitoring'
      ],
      performance: {
        targetLatency: '< 200ms',
        audioProcessing: 'optimized',
        aiIntegration: 'enabled'
      },
      timestamp: new Date().toISOString()
    };
    
    ws.send(JSON.stringify(welcomeMessage));
    console.log('📝 [SANPBX-WS] Sent enhanced welcome message');
    
    ws.on('message', async (data) => {
      const messageStartTime = performance.now();
      
      try {
        const message = JSON.parse(data.toString());
        console.log(`📨 [SANPBX-WS] Event: ${message.event} (${message.callId || 'unknown'})`);
        
        // Record network latency
        performanceMonitor.recordNetworkMetrics('websocket_latency', performance.now() - messageStartTime);
        
        switch (message.event) {
          case 'connected':
            console.log(`🔗 [CONNECTED] Call: ${message.callId} | Stream: ${message.streamId}`);
            console.log(`📞 [CONNECTED] Caller: ${message.callerId} → DID: ${message.did} (${message.callDirection})`);
            
            currentSession = new OptimizedSanIPPBXSession(ws, {
              callId: message.callId,
              streamId: message.streamId,
              channelId: message.channelId,
              callerId: message.callerId,
              callDirection: message.callDirection,
              did: message.did
            });
            
            // Store session with both keys for fast lookup
            activeSessions.set(message.callId, currentSession);
            activeSessions.set(message.streamId, currentSession);
            
            // Set up session event handlers
            currentSession.on('error', (error) => {
              console.error(`❌ [SESSION-ERROR] ${message.callId}:`, error.message);
            });
            
            currentSession.on('transfer-requested', (data) => {
              console.log(`🔄 [TRANSFER-REQUEST] ${message.callId} → ${data.destination}`);
            });
            
            break;
            
          case 'start':
            if (currentSession) {
              console.log(`🚀 [START] Session starting: ${message.callId}`);
              console.log(`🎵 [START] Audio format:`, message.mediaFormat);
              
              // Validate audio format compatibility
              const format = message.mediaFormat;
              if (format && format.sampleRate !== 8000) {
                console.warn(`⚠️ [AUDIO-FORMAT] Non-optimal sample rate: ${format.sampleRate}Hz (recommended: 8000Hz)`);
              }
            }
            break;
            
          case 'answer':
            if (currentSession) {
              console.log(`📞 [ANSWER] Call answered: ${message.callId}`);
            }
            break;
            
          case 'media':
            if (currentSession && message.media?.payload) {
              // High-frequency event - minimal logging
              currentSession.processIncomingAudio(message.media.payload);
            }
            break;
            
          case 'dtmf':
            if (currentSession) {
              currentSession.handleDTMF(message.digit, parseInt(message.dtmfDurationMs) || 0);
            }
            break;
            
          case 'transfer-call':
            if (currentSession) {
              currentSession.handleTransferCall(message.transferTo);
            }
            break;
            
          case 'hangup-call':
            if (currentSession) {
              currentSession.hangup();
            }
            break;
            
          case 'stop':
            console.log(`🛑 [STOP] Call ended: ${message.callId} | Reason: ${message.disconnectedBy}`);
            if (currentSession) {
              const stats = currentSession.getSessionStats();
              console.log(`📊 [STOP] Final call stats:`, stats);
              currentSession.cleanup();
              currentSession = null;
            }
            break;
            
          case 'ping':
            // Handle ping for connection keep-alive
            ws.send(JSON.stringify({
              event: 'pong',
              timestamp: new Date().toISOString()
            }));
            break;
            
          default:
            console.log(`❓ [SANPBX-WS] Unknown event: ${message.event}`);
        }
        
      } catch (error) {
        console.error('❌ [SANPBX-WS] Message processing error:', error.message);
        console.error('📝 [SANPBX-WS] Raw message:', data.toString().substring(0, 500));
        
        // Send error response
        try {
          ws.send(JSON.stringify({
            event: 'error',
            message: 'Message processing failed',
            timestamp: new Date().toISOString()
          }));
        } catch (sendError) {
          console.error('❌ [SANPBX-WS] Failed to send error response:', sendError.message);
        }
      }
    });
    
    ws.on('close', (code, reason) => {
      const connectionDuration = Date.now() - connectionStartTime;
      console.log(`🔗 [SANPBX-WS] Connection closed: ${code} ${reason} (duration: ${connectionDuration}ms)`);
      
      if (currentSession) {
        currentSession.cleanup();
        currentSession = null;
      }
    });
    
    ws.on('error', (error) => {
      console.error('❌ [SANPBX-WS] WebSocket error:', error.message);
      
      if (currentSession) {
        currentSession.cleanup();
        currentSession = null;
      }
    });
    
    // Connection health monitoring
    const healthCheckInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping();
      } else {
        clearInterval(healthCheckInterval);
      }
    }, 30000); // Ping every 30 seconds
    
    ws.on('pong', () => {
      // Connection is healthy
    });
  });
  
  // Periodic cleanup of stale sessions and statistics
  setInterval(() => {
    const now = Date.now();
    const staleThreshold = 5 * 60 * 1000; // 5 minutes
    let cleanedCount = 0;
    
    for (const [key, session] of activeSessions.entries()) {
      if (now - session.lastActivityTime > staleThreshold) {
        console.log(`🧹 [CLEANUP] Removing stale session: ${key}`);
        session.cleanup();
        cleanedCount++;
      }
    }
    
    if (cleanedCount > 0) {
      console.log(`🧹 [CLEANUP] Cleaned up ${cleanedCount} stale sessions`);
    }
    
    // Log server health
    if (activeSessions.size > 0) {
      console.log(`📊 [SERVER-HEALTH] Active sessions: ${activeSessions.size}, Total processed: ${sessionStats.size}`);
    }
    
  }, 60000); // Check every minute
  
  // Graceful shutdown handler
  const gracefulShutdown = () => {
    console.log('🛑 [SANPBX-WS] Shutting down gracefully...');
    
    // Close all active sessions
    for (const [key, session] of activeSessions.entries()) {
      try {
        session.cleanup();
      } catch (error) {
        console.error(`❌ [SHUTDOWN] Session cleanup error for ${key}:`, error.message);
      }
    }
    
    // Stop performance monitoring
    performanceMonitor.stop();
    latencyOptimizer.stop();
    
    console.log('✅ [SANPBX-WS] Graceful shutdown complete');
  };
  
  process.on('SIGINT', gracefulShutdown);
  process.on('SIGTERM', gracefulShutdown);
  
  console.log('✅ [SANPBX-WS] Enhanced SanIPPBX WebSocket server setup complete');
  console.log('📊 [SANPBX-WS] Performance monitoring and optimization enabled');
  console.log('🤖 [SANPBX-WS] AI services: Deepgram + OpenAI + Sarvam ready');
}

/**
 * Get comprehensive server statistics
 */
function getServerStatistics() {
  const stats = {
    server: {
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      timestamp: new Date().toISOString()
    },
    sessions: {
      active: activeSessions.size,
      total: sessionStats.size,
      activeSessions: Array.from(activeSessions.entries()).map(([key, session]) => ({
        id: key,
        callId: session.callId,
        duration: Date.now() - session.sessionStartTime,
        metrics: session.getSessionStats()
      }))
    },
    performance: performanceMonitor.getPerformanceReport(),
    optimizations: latencyOptimizer.getOptimizationHistory(5)
  };
  
  return stats;
}

module.exports = {
  setupEnhancedSanPbxWebSocketServer: setupEnhancedSanPbxWebSocketServer,
  OptimizedSanIPPBXSession,
  activeSessions,
  sessionStats,
  getServerStatistics,
  performanceMonitor,
  latencyOptimizer
};