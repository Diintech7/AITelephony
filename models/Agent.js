const mongoose = require("mongoose")

const agentSchema = new mongoose.Schema({
  // Client Information
  clientId: { type: String }, // Optional - can be set by admin or client
  
  // Creation tracking
  createdBy: { type: String }, // ID of the user who created the agent
  createdByType: { 
    type: String, 
    enum: ["client", "admin"], 
    default: "admin" 
  }, // Type of user who created the agent
  
  agentId: {type: String},
  // Active Status
  isActive: { type: Boolean, default: true, index: true },

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
      "male-professional",
      "female-professional",
      "male-friendly",
      "female-friendly",
      "neutral",
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
    ],
    default: "anushka",
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
  callingNumber: { type: String }, // Add missing callingNumber field
  callerId: { type: String, index: true }, // For outbound call matching
  serviceProvider: {
    type: String,
    enum: ["twilio", "vonage", "plivo", "bandwidth", "other", "c-zentrix", "tata"],
  },
  X_API_KEY: { type: String }, // Add missing X_API_KEY field

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

// // Compound index for client + agent name uniqueness
// agentSchema.index({ clientId: 1, agentName: 1 }, { unique: true })

// // Additional index for callerId lookup (outbound calls) with isActive filter
// agentSchema.index({ callerId: 1, isActive: 1 })

// // Additional index for accountSid lookup with isActive filter
// agentSchema.index({ accountSid: 1, isActive: 1 })

// // Ensure only one active agent per (clientId, accountSid)
// // Partial unique index applies only when isActive is true and accountSid exists
// try {
//   agentSchema.index(
//     { clientId: 1, accountSid: 1 },
//     {
//       unique: true,
//       partialFilterExpression: {
//         isActive: true,
//         accountSid: { $exists: true, $type: 'string' },
//       },
//     }
//   )
// } catch (e) {
//   // Index creation failures will be logged by Mongoose at startup; continue
// }

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