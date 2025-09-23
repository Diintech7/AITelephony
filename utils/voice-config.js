/**
 * Unified Voice Configuration System
 * Handles voice selection and TTS service provider configuration based on Agent model
 */

// Valid Sarvam voice options
const VALID_SARVAM_VOICES = new Set([
  "abhilash", "anushka", "meera", "pavithra", "maitreyi", "arvind", "amol", 
  "amartya", "diya", "neel", "misha", "vian", "arjun", "maya", "manisha", 
  "vidya", "arya", "karun", "hitesh", "kumaran", "monika", "aahir", "kanika"
])

// Default ElevenLabs voice ID (Rachel)
const DEFAULT_ELEVENLABS_VOICE_ID = "21m00Tcm4TlvDq8ikWAM"

// Known ElevenLabs stock voices (name -> voiceId)
const ELEVENLABS_VOICES = {
  adam: "pNInz6obpgDQGcFmaJgB",
  alice: "Xb7hH8MSUJpSbSDYk0k2",
  antoni: "ErXwobaYiN019PkySvjV",
  aria: "9BWtsMINqrJLrRacOk9x",
  arnold: "VR6AewLTigWG4xSOukaG",
  bill: "pqHfZKP75CvOlQylNhV4",
  brian: "nPczCjzI2devNBz1zQrb",
  callum: "N2lVS1w4EtoT3dr4eOWO",
  charlie: "IKne3meq5aSn9XLyUdCD",
  charlotte: "XB0fDUnXU5powFXDhCwa",
  kumaran: "rgltZvTfiMmgWweZhh7n",
  monika: "NaKPQmdr7mMxXuXrNeFC",
  aahir: "RKshBIkZ7DwU6YNPq5Jd",
  kanika: "xccfcojYYGnqTTxwZEDU",
}

/**
 * Get valid Sarvam voice from agent configuration
 * @param {Object} agentConfig - Agent configuration object
 * @returns {string} Valid Sarvam voice name
 */
const getValidSarvamVoice = (agentConfig) => {
  const voiceSelection = agentConfig?.voiceSelection || "pavithra"
  const normalized = (voiceSelection || "").toString().trim().toLowerCase()
  
  // Direct match with valid Sarvam voices
  if (VALID_SARVAM_VOICES.has(normalized)) {
    return normalized
  }

  // Map generic labels to Sarvam voices
  const voiceMapping = {
    "male-professional": "arvind",
    "female-professional": "pavithra", 
    "male-friendly": "amol",
    "female-friendly": "maya",
    "neutral": "pavithra",
    "default": "pavithra",
    "male": "arvind",
    "female": "pavithra",
  }

  return voiceMapping[normalized] || "pavithra"
}

/**
 * Get valid ElevenLabs voice ID from agent configuration
 * @param {Object} agentConfig - Agent configuration object
 * @returns {string} Valid ElevenLabs voice ID
 */
const getValidElevenLabsVoiceId = (agentConfig) => {
  try {
    const voiceId = agentConfig?.voiceId
    const voiceSelection = agentConfig?.voiceSelection || ""
    
    // If voiceId is provided and looks like a valid ElevenLabs voice ID, use it
    if (voiceId && /^[A-Za-z0-9]{15,}$/.test(voiceId)) {
      console.log(`✅ [VOICE-CONFIG] Using ElevenLabs voice ID from database: ${voiceId}`)
      return voiceId
    }
    
    // Map voice selection to ElevenLabs voice IDs
    const raw = (voiceSelection || "").toString().trim()
    const key = raw.toLowerCase()
    // First check known ElevenLabs stock voices
    if (ELEVENLABS_VOICES[key]) {
      return ELEVENLABS_VOICES[key]
    }
    // Fallback generic mapping to a sensible default
    const generic = {
      rachel: DEFAULT_ELEVENLABS_VOICE_ID,
      female: DEFAULT_ELEVENLABS_VOICE_ID,
      female_professional: DEFAULT_ELEVENLABS_VOICE_ID,
      "female-professional": DEFAULT_ELEVENLABS_VOICE_ID,
      neutral: DEFAULT_ELEVENLABS_VOICE_ID,
      default: DEFAULT_ELEVENLABS_VOICE_ID,
      pavithra: DEFAULT_ELEVENLABS_VOICE_ID,
      arvind: DEFAULT_ELEVENLABS_VOICE_ID,
      male: DEFAULT_ELEVENLABS_VOICE_ID,
      "male-professional": DEFAULT_ELEVENLABS_VOICE_ID,
    }
    return generic[key] || DEFAULT_ELEVENLABS_VOICE_ID
  } catch (error) {
    console.log(`❌ [VOICE-CONFIG] Error getting ElevenLabs voice ID: ${error.message}`)
    return DEFAULT_ELEVENLABS_VOICE_ID
  }
}

/**
 * Get TTS service provider from agent configuration
 * @param {Object} agentConfig - Agent configuration object
 * @returns {string} TTS service provider ('sarvam' or 'elevenlabs')
 */
const getTtsServiceProvider = (agentConfig) => {
  // Check voiceServiceProvider first (new field)
  if (agentConfig?.voiceServiceProvider) {
    return agentConfig.voiceServiceProvider
  }
  
  // Fallback to ttsSelection field
  if (agentConfig?.ttsSelection) {
    const ttsSelection = agentConfig.ttsSelection.toLowerCase()
    if (ttsSelection === 'sarvam') return 'sarvam'
    if (ttsSelection === 'elevenlabs') return 'elevenlabs'
  }
  
  // Default to Sarvam
  return 'sarvam'
}

/**
 * Get unified voice configuration for TTS processing
 * @param {Object} agentConfig - Agent configuration object
 * @returns {Object} Voice configuration object
 */
const getVoiceConfig = (agentConfig) => {
  const serviceProvider = getTtsServiceProvider(agentConfig)
  
  const config = {
    serviceProvider,
    language: agentConfig?.language || 'en',
    voiceSelection: agentConfig?.voiceSelection || 'pavithra',
    voiceId: agentConfig?.voiceId || null,
  }
  
  // Add service-specific configuration
  if (serviceProvider === 'sarvam') {
    config.sarvamVoice = getValidSarvamVoice(agentConfig)
  } else if (serviceProvider === 'elevenlabs') {
    config.elevenLabsVoiceId = getValidElevenLabsVoiceId(agentConfig)
  }
  
  console.log(`🎤 [VOICE-CONFIG] Service: ${config.serviceProvider}, Language: ${config.language}, Voice: ${config.voiceSelection}, VoiceID: ${config.voiceId}`)
  if (config.sarvamVoice) {
    console.log(`🎤 [VOICE-CONFIG] Sarvam Voice: ${config.sarvamVoice}`)
  }
  if (config.elevenLabsVoiceId) {
    console.log(`🎤 [VOICE-CONFIG] ElevenLabs Voice ID: ${config.elevenLabsVoiceId}`)
  }
  
  return config
}

/**
 * Check if agent configuration supports a specific TTS service
 * @param {Object} agentConfig - Agent configuration object
 * @param {string} service - TTS service name ('sarvam' or 'elevenlabs')
 * @returns {boolean} True if service is supported
 */
const supportsTtsService = (agentConfig, service) => {
  const configuredService = getTtsServiceProvider(agentConfig)
  return configuredService === service
}

/**
 * Get language code for Sarvam API
 * @param {string} language - Language code
 * @returns {string} Sarvam-compatible language code
 */
const getSarvamLanguage = (language = "hi") => {
  const LANGUAGE_MAPPING = { 
    hi: "hi-IN", en: "en-IN", bn: "bn-IN", te: "te-IN", ta: "ta-IN", 
    mr: "mr-IN", gu: "gu-IN", kn: "kn-IN", ml: "ml-IN", pa: "pa-IN", 
    or: "or-IN", as: "as-IN", ur: "ur-IN" 
  }
  return LANGUAGE_MAPPING[(language || "hi").toLowerCase()] || "hi-IN"
}

/**
 * Get language code for Deepgram API
 * @param {string} language - Language code
 * @returns {string} Deepgram-compatible language code
 */
const getDeepgramLanguage = (language = "hi") => {
  const lang = (language || "hi").toLowerCase()
  if (lang === "hi") return "hi"
  if (lang === "en") return "en-IN"
  if (lang === "mr") return "mr"
  return lang
}

module.exports = {
  VALID_SARVAM_VOICES,
  DEFAULT_ELEVENLABS_VOICE_ID,
  ELEVENLABS_VOICES,
  getValidSarvamVoice,
  getValidElevenLabsVoiceId,
  getTtsServiceProvider,
  getVoiceConfig,
  supportsTtsService,
  getSarvamLanguage,
  getDeepgramLanguage
}
