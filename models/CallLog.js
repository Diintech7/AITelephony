const mongoose = require("mongoose")

const agentSchema = new mongoose.Schema({
  // Client Information
  clientId: { type: String, required: true, index: true },

  // Personal Information
  agentName: { type: String, required: true },
  description: { type: String, required: true },
  category: { type: String },
  personality: {
    type: String,
    enum: ["formal", "informal", "friendly", "flirty", "disciplined"],
    default: "formal",
  },
  language: { type: String, default: "en" },

  // System Information
  firstMessage: { type: String, required: true },
  systemPrompt: { type: String, required: true },
  sttSelection: {
    type: String,
    enum: ["deepgram", "whisper", "google", "azure", "aws"],
    default: "deepgram",
  },
  ttsSelection: {
    type: String,
    enum: ["sarvam", "elevenlabs", "openai", "google", "azure", "aws"],
    default: "sarvam",
  },
  llmSelection: {
    type: String,
    enum: ["openai", "anthropic", "google", "azure"],
    default: "openai",
  },
  voiceSelection: {
    type: String,
    enum: [
      "default",
      "male-professional",
      "female-professional",
      "male-friendly",
      "female-friendly",
      "neutral",
      "abhilash",
      "anushka",
      "meera",
      "pavithra",
      "maitreyi",
      "arvind",
      "amol",
      "amartya",
      "diya",
      "neel",
      "misha",
      "vian",
      "arjun",
      "maya",
      "manisha",
      "vidya",
      "arya",
      "karun",
      "hitesh",
    ],
    default: "default",
  },
  contextMemory: { type: String },
  brandInfo: { type: String },

  // Multiple starting messages
  startingMessages: [
    {
      text: { type: String, required: true },
      audioBase64: { type: String },
    },
  ],

  // Telephony
  accountSid: { type: String },
  callerId: { type: String, index: true }, // For outbound call matching
  serviceProvider: {
    type: String,
    enum: ["twilio", "vonage", "plivo", "bandwidth", "other"],
  },

  // Audio storage - Store as base64 string instead of Buffer
  audioFile: { type: String }, // File path (legacy support)
  audioBytes: {
    type: String, // Store as base64 string
    validate: {
      validator: (v) => !v || typeof v === "string",
      message: "audioBytes must be a string",
    },
  },
  audioMetadata: {
    format: { type: String, default: "mp3" },
    sampleRate: { type: Number, default: 22050 },
    channels: { type: Number, default: 1 },
    size: { type: Number },
    generatedAt: { type: Date },
    language: { type: String, default: "en" },
    speaker: { type: String },
    provider: { type: String, default: "sarvam" },
  },

  // Timestamps
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
})

// Compound index for client + agent name uniqueness
agentSchema.index({ clientId: 1, agentName: 1 }, { unique: true })

// Additional index for callerId lookup (outbound calls)
agentSchema.index({ callerId: 1 })

// Update the updatedAt field before saving
agentSchema.pre("save", function (next) {
  this.updatedAt = Date.now()

  // Validate and convert audioBytes if present
  if (this.audioBytes) {
    if (typeof this.audioBytes === "string") {
      // Already a string, ensure metadata is updated
      if (!this.audioMetadata) {
        this.audioMetadata = {}
      }
      // Calculate actual byte size from base64 string
      const byteSize = Math.ceil((this.audioBytes.length * 3) / 4)
      this.audioMetadata.size = byteSize
      console.log(`[AGENT_MODEL] Audio stored as base64 string: ${this.audioBytes.length} chars (${byteSize} bytes)`)
    } else {
      return next(new Error("audioBytes must be a string"))
    }
  }

  next()
})

// Method to get audio as base64
agentSchema.methods.getAudioBase64 = function () {
  if (this.audioBytes && typeof this.audioBytes === "string") {
    return this.audioBytes
  }
  return null
}

// Method to set audio from base64
agentSchema.methods.setAudioFromBase64 = function (base64String) {
  if (base64String && typeof base64String === "string") {
    this.audioBytes = base64String
    if (!this.audioMetadata) {
      this.audioMetadata = {}
    }
    // Calculate actual byte size from base64 string
    const byteSize = Math.ceil((base64String.length * 3) / 4)
    this.audioMetadata.size = byteSize
  }
}

module.exports = mongoose.model("Agent", agentSchema)