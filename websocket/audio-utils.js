const ffmpeg = require('fluent-ffmpeg');
const { Transform } = require('stream');
const EventEmitter = require('events');

// Audio format constants optimized for telephony
const AUDIO_FORMATS = {
  SANIPBX: {
    sampleRate: 8000,
    channels: 1,
    bitDepth: 16,
    encoding: 'pcm_s16le',
    frameSize: 160, // 20ms frames
    bytesPerFrame: 320 // 160 samples * 2 bytes
  },
  DEEPGRAM: {
    sampleRate: 8000,
    channels: 1,
    encoding: 'linear16'
  },
  OPENAI_TTS: {
    format: 'mp3',
    quality: 'standard',
    speed: 1.1
  }
};

class AudioProcessor extends EventEmitter {
  constructor(options = {}) {
    super();
    this.options = {
      enablePreprocessing: options.enablePreprocessing || true,
      enableVAD: options.enableVAD || true,
      enableNoiseSupression: options.enableNoiseSupression || false,
      bufferOptimization: options.bufferOptimization || true,
      ...options
    };
    
    this.audioBuffer = Buffer.alloc(0);
    this.silenceThreshold = options.silenceThreshold || 500;
    this.lastAudioTime = Date.now();
    this.isProcessing = false;
    
    // Performance tracking
    this.stats = {
      processed: 0,
      converted: 0,
      avgProcessingTime: 0,
      avgConversionTime: 0,
      bufferSize: 0
    };
  }

  /**
   * Process incoming base64 audio from SanIPPBX
   */
  processIncomingAudio(base64Audio) {
    const startTime = Date.now();
    
    try {
      // Validate base64 input
      if (!this.isValidBase64(base64Audio)) {
        throw new Error('Invalid base64 audio data');
      }
      
      // Decode base64 to buffer
      const audioBuffer = Buffer.from(base64Audio, 'base64');
      
      if (audioBuffer.length === 0) {
        return null;
      }
      
      // Add to processing buffer if enabled
      if (this.options.bufferOptimization) {
        this.audioBuffer = Buffer.concat([this.audioBuffer, audioBuffer]);
        
        // Process in chunks for optimal performance
        if (this.audioBuffer.length >= AUDIO_FORMATS.SANIPBX.bytesPerFrame * 5) {
          const processBuffer = this.audioBuffer.slice(0, AUDIO_FORMATS.SANIPBX.bytesPerFrame * 5);
          this.audioBuffer = this.audioBuffer.slice(AUDIO_FORMATS.SANIPBX.bytesPerFrame * 5);
          
          const processedAudio = this.preprocessAudio(processBuffer);
          this.updateStats('processed', Date.now() - startTime);
          return processedAudio;
        }
        return null;
      } else {
        const processedAudio = this.preprocessAudio(audioBuffer);
        this.updateStats('processed', Date.now() - startTime);
        return processedAudio;
      }
      
    } catch (error) {
      console.error('❌ [AUDIO-PROCESSOR] Error processing audio:', error.message);
      return null;
    }
  }

  /**
   * Preprocess audio for better quality and reduced latency
   */
  preprocessAudio(audioBuffer) {
    if (!this.options.enablePreprocessing) {
      return audioBuffer;
    }
    
    try {
      // Convert buffer to samples for processing
      const samples = new Int16Array(audioBuffer.buffer, audioBuffer.byteOffset, audioBuffer.length / 2);
      const processedSamples = new Int16Array(samples.length);
      
      // Apply basic noise gate and normalization
      let maxAmplitude = 0;
      const noiseGate = 100; // Adjust based on environment
      
      // First pass: find max amplitude and apply noise gate
      for (let i = 0; i < samples.length; i++) {
        const sample = samples[i];
        const amplitude = Math.abs(sample);
        
        if (amplitude > noiseGate) {
          processedSamples[i] = sample;
          if (amplitude > maxAmplitude) {
            maxAmplitude = amplitude;
          }
        } else {
          processedSamples[i] = 0; // Apply noise gate
        }
      }
      
      // Second pass: normalize if needed
      if (maxAmplitude > 0 && maxAmplitude < 16384) { // If audio is too quiet
        const gain = Math.min(2.0, 16384 / maxAmplitude);
        for (let i = 0; i < processedSamples.length; i++) {
          processedSamples[i] = Math.max(-32768, Math.min(32767, processedSamples[i] * gain));
        }
      }
      
      // Voice Activity Detection (VAD)
      if (this.options.enableVAD) {
        const hasVoice = this.detectVoiceActivity(processedSamples);
        if (!hasVoice) {
          this.lastAudioTime = Date.now();
          return null; // Skip silent frames
        }
      }
      
      // Convert back to buffer
      return Buffer.from(processedSamples.buffer);
      
    } catch (error) {
      console.error('❌ [AUDIO-PREPROCESSOR] Error:', error.message);
      return audioBuffer; // Return original on error
    }
  }

  /**
   * Simple Voice Activity Detection
   */
  detectVoiceActivity(samples) {
    const frameSize = 160; // 20ms at 8kHz
    let energySum = 0;
    let zeroCrossings = 0;
    
    // Calculate energy and zero crossings
    for (let i = 0; i < samples.length; i++) {
      energySum += samples[i] * samples[i];
      if (i > 0 && ((samples[i] >= 0) !== (samples[i - 1] >= 0))) {
        zeroCrossings++;
      }
    }
    
    const avgEnergy = energySum / samples.length;
    const zcr = zeroCrossings / samples.length;
    
    // Simple threshold-based VAD
    const energyThreshold = 1000000; // Adjust based on environment
    const zcrThreshold = 0.1;
    
    return avgEnergy > energyThreshold || zcr > zcrThreshold;
  }

  /**
   * Convert MP3 to PCM for SanIPPBX with optimized streaming
   */
  async convertMp3ToPcm(mp3Buffer) {
    const startTime = Date.now();
    
    return new Promise((resolve, reject) => {
      const { PassThrough } = require('stream');
      const inputStream = new PassThrough();
      const chunks = [];
      
      // Setup FFmpeg conversion
      const command = ffmpeg(inputStream)
        .audioFrequency(AUDIO_FORMATS.SANIPBX.sampleRate)
        .audioChannels(AUDIO_FORMATS.SANIPBX.channels)
        .audioCodec('pcm_s16le')
        .format('s16le')
        .audioFilters([
          'highpass=f=300', // Remove low frequency noise
          'lowpass=f=3400',  // Telephony band limit
          'volume=1.5'       // Slight amplification
        ])
        .on('start', (commandLine) => {
          console.log('🔄 [AUDIO-CONVERT] Started FFmpeg:', commandLine.substring(0, 100));
        })
        .on('error', (error) => {
          console.error('❌ [AUDIO-CONVERT] FFmpeg error:', error.message);
          reject(error);
        })
        .on('end', () => {
          const pcmBuffer = Buffer.concat(chunks);
          const conversionTime = Date.now() - startTime;
          this.updateStats('converted', conversionTime);
          console.log(`✅ [AUDIO-CONVERT] Converted ${mp3Buffer.length}b MP3 to ${pcmBuffer.length}b PCM in ${conversionTime}ms`);
          resolve(pcmBuffer);
        });
      
      // Pipe output to chunks collector
      const outputStream = new PassThrough();
      outputStream.on('data', chunk => chunks.push(chunk));
      command.pipe(outputStream);
      
      // Send MP3 data to FFmpeg
      inputStream.end(mp3Buffer);
    });
  }

  /**
   * Convert PCM to base64 for SanIPPBX transmission
   */
  convertPcmToBase64(pcmBuffer) {
    try {
      const startTime = Date.now();
      
      // Apply final processing if needed
      let processedBuffer = pcmBuffer;
      
      if (this.options.enablePreprocessing) {
        // Apply telephony-specific processing
        processedBuffer = this.applyTelephonyProcessing(pcmBuffer);
      }
      
      // Convert to base64
      const base64Audio = processedBuffer.toString('base64');
      
      const conversionTime = Date.now() - startTime;
      console.log(`📤 [AUDIO-BASE64] Converted ${pcmBuffer.length}b PCM to ${base64Audio.length}c base64 in ${conversionTime}ms`);
      
      return base64Audio;
      
    } catch (error) {
      console.error('❌ [AUDIO-BASE64] Conversion error:', error.message);
      return pcmBuffer.toString('base64'); // Fallback
    }
  }

  /**
   * Apply telephony-specific audio processing
   */
  applyTelephonyProcessing(pcmBuffer) {
    try {
      const samples = new Int16Array(pcmBuffer.buffer, pcmBuffer.byteOffset, pcmBuffer.length / 2);
      const processed = new Int16Array(samples.length);
      
      // Apply μ-law companding simulation for better telephony quality
      for (let i = 0; i < samples.length; i++) {
        let sample = samples[i];
        
        // Apply gentle compression
        const sign = sample < 0 ? -1 : 1;
        sample = Math.abs(sample);
        
        // Logarithmic compression (simplified μ-law)
        const compressed = Math.log(1 + 255 * (sample / 32768)) / Math.log(1 + 255) * 32768;
        processed[i] = sign * compressed;
      }
      
      return Buffer.from(processed.buffer);
      
    } catch (error) {
      console.error('❌ [TELEPHONY-PROCESSING] Error:', error.message);
      return pcmBuffer;
    }
  }

  /**
   * Validate base64 audio data
   */
  isValidBase64(str) {
    if (!str || typeof str !== 'string') return false;
    
    // Check if string contains only valid base64 characters
    const base64Regex = /^[A-Za-z0-9+/]*={0,2}$/;
    if (!base64Regex.test(str)) return false;
    
    // Check if length is valid (multiple of 4)
    if (str.length % 4 !== 0) return false;
    
    // Additional validation: try to decode small sample
    try {
      const sample = str.substring(0, Math.min(100, str.length));
      Buffer.from(sample, 'base64');
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Update performance statistics
   */
  updateStats(type, processingTime) {
    this.stats[type]++;
    const avgKey = `avg${type.charAt(0).toUpperCase() + type.slice(1)}Time`;
    this.stats[avgKey] = (this.stats[avgKey] + processingTime) / 2;
    this.stats.bufferSize = this.audioBuffer.length;
  }

  /**
   * Get current performance statistics
   */
  getStats() {
    return {
      ...this.stats,
      bufferHealth: this.audioBuffer.length < AUDIO_FORMATS.SANIPBX.bytesPerFrame * 10 ? 'good' : 'high',
      timestamp: new Date().toISOString()
    };
  }

  /**
   * Reset statistics
   */
  resetStats() {
    this.stats = {
      processed: 0,
      converted: 0,
      avgProcessingTime: 0,
      avgConversionTime: 0,
      bufferSize: 0
    };
  }

  /**
   * Create optimized audio chunks for streaming
   */
  createAudioChunks(audioBuffer, chunkSize = AUDIO_FORMATS.SANIPBX.bytesPerFrame) {
    const chunks = [];
    
    for (let i = 0; i < audioBuffer.length; i += chunkSize) {
      const chunk = audioBuffer.slice(i, i + chunkSize);
      if (chunk.length === chunkSize) { // Only use complete frames
        chunks.push(chunk);
      }
    }
    
    return chunks;
  }

  /**
   * Cleanup resources
   */
  cleanup() {
    this.audioBuffer = Buffer.alloc(0);
    this.resetStats();
    this.removeAllListeners();
  }
}

/**
 * Optimized real-time audio stream processor
 */
class RealTimeAudioStream extends Transform {
  constructor(options = {}) {
    super({ objectMode: true });
    
    this.processor = new AudioProcessor(options);
    this.frameSize = AUDIO_FORMATS.SANIPBX.bytesPerFrame;
    this.buffer = Buffer.alloc(0);
  }

  _transform(chunk, encoding, callback) {
    try {
      // Accumulate audio data
      this.buffer = Buffer.concat([this.buffer, chunk]);
      
      // Process complete frames
      while (this.buffer.length >= this.frameSize) {
        const frame = this.buffer.slice(0, this.frameSize);
        this.buffer = this.buffer.slice(this.frameSize);
        
        // Convert to base64 and push
        const base64Frame = frame.toString('base64');
        this.push(base64Frame);
      }
      
      callback();
    } catch (error) {
      callback(error);
    }
  }

  _flush(callback) {
    // Process any remaining buffer
    if (this.buffer.length > 0) {
      const base64Frame = this.buffer.toString('base64');
      this.push(base64Frame);
    }
    callback();
  }
}

/**
 * Utility functions for audio format detection and validation
 */
const AudioUtils = {
  /**
   * Detect audio format from buffer
   */
  detectAudioFormat(buffer) {
    if (buffer.length < 4) return 'unknown';
    
    // Check for common audio format signatures
    const header = buffer.slice(0, 4);
    
    if (header[0] === 0xFF && (header[1] & 0xE0) === 0xE0) {
      return 'mp3';
    }
    if (header.toString() === 'RIFF') {
      return 'wav';
    }
    if (header.toString() === 'OggS') {
      return 'ogg';
    }
    
    // Assume PCM if no signature found
    return 'pcm';
  },

  /**
   * Calculate audio duration from PCM buffer
   */
  calculateDuration(buffer, sampleRate = 8000, channels = 1, bitDepth = 16) {
    const bytesPerSample = bitDepth / 8;
    const totalSamples = buffer.length / (bytesPerSample * channels);
    return totalSamples / sampleRate;
  },

  /**
   * Generate silence buffer
   */
  generateSilence(durationMs, sampleRate = 8000, channels = 1, bitDepth = 16) {
    const bytesPerSample = bitDepth / 8;
    const totalSamples = Math.floor((durationMs / 1000) * sampleRate);
    const bufferSize = totalSamples * channels * bytesPerSample;
    return Buffer.alloc(bufferSize);
  },

  /**
   * Mix two audio buffers
   */
  mixAudioBuffers(buffer1, buffer2, ratio = 0.5) {
    const minLength = Math.min(buffer1.length, buffer2.length);
    const mixed = Buffer.alloc(minLength);
    
    const samples1 = new Int16Array(buffer1.buffer, buffer1.byteOffset, minLength / 2);
    const samples2 = new Int16Array(buffer2.buffer, buffer2.byteOffset, minLength / 2);
    const mixedSamples = new Int16Array(mixed.buffer);
    
    for (let i = 0; i < samples1.length; i++) {
      const sample1 = samples1[i] * ratio;
      const sample2 = samples2[i] * (1 - ratio);
      mixedSamples[i] = Math.max(-32768, Math.min(32767, sample1 + sample2));
    }
    
    return mixed;
  }
};

module.exports = {
  AudioProcessor,
  RealTimeAudioStream,
  AudioUtils,
  AUDIO_FORMATS
};