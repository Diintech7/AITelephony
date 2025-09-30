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
  agentKey: { type: String, unique: true, index: true }, // Encrypted unique key for agent
  // Active Status
  isActive: { type: Boolean, default: true, index: true },
  isApproved: { type: Boolean, default: false, index: true },
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
  voiceServiceProvider: {
    type: String,
    enum: ["sarvam", "elevenlabs"],
    default: "sarvam",
  },
  voiceId: { type: String }, // Store the actual voice ID for the selected service
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
      "kumaran",
      "monika",
      "aahir",
      "kanika",
    ],
    default: "meera",
  },
  contextMemory: { type: String },
  brandInfo: { type: String },
  // Dynamic configuration details (separate from brandInfo)
  details: { type: String },

  // Q&A items for agent (managed via dashboard System Configuration)
  qa: [
    {
      question: { type: String, required: true },
      answer: { type: String, required: true },
      createdAt: { type: Date, default: Date.now },
      updatedAt: { type: Date },
    }
  ],


  // Multiple starting messages
  startingMessages: [
    {
      text: { type: String, required: true },
      audioBase64: { type: String },
    },
  ],

  // Multiple dynamic info items
  dynamicInfoList: [
    {
      text: { type: String, required: true },
      isDefault: { type: Boolean, default: false },
    },
  ],
  defaultDynamicInfoIndex: { type: Number, default: 0 },

  // Multiple system prompts
  systemPromptList: [
    {
      text: { type: String, required: true },
      isDefault: { type: Boolean, default: false },
    },
  ],
  defaultSystemPromptIndex: { type: Number, default: 0 },

  // Telephony
  accountSid: { type: String },
  callingNumber: { type: String }, // Add missing callingNumber field
  callerId: { type: String, index: true }, // For outbound call matching
  serviceProvider: {
    type: String,
    enum: ["twilio", "vonage", "plivo", "bandwidth", "other", "c-zentrix", "tata", "sanpbx", "snapbx"],
  },
  X_API_KEY: { type: String }, // Add missing X_API_KEY field

  // SnapBX provider fields
  didNumber: { type: String },
  accessToken: { type: String },
  accessKey: { type: String },
  appId: { type: String },

  // UI customization
  uiImage: { type: String }, // base64 image for agent avatar/logo
  backgroundImage: { type: String }, // base64 image for background
  backgroundColor: { type: String }, // hex color string

  // Knowledge Base: reference to separate KnowledgeBase collection
  // This field is kept for backward compatibility but will be populated from KnowledgeBase model
  knowledgeBase: [
    {
      // Old format fields (for backward compatibility)
      key: { type: String }, // Made optional to support new format
      name: { type: String },
      uploadedAt: { type: Date, default: Date.now },
      // New format fields (from KnowledgeBase model)
      _id: { type: mongoose.Schema.Types.ObjectId },
      agentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Agent' },
      clientId: { type: String },
      type: { 
        type: String, 
        enum: ['pdf', 'text', 'image', 'youtube', 'link', 'website'] 
      },
      title: { type: String },
      description: { type: String },
      content: {
        s3Key: { type: String },
        text: { type: String },
        youtubeId: { type: String },
        youtubeUrl: { type: String },
        url: { type: String },
        linkText: { type: String },
      },
      fileMetadata: {
        originalName: { type: String },
        fileSize: { type: Number },
        mimeType: { type: String },
        uploadedAt: { type: Date },
      },
      tags: [{ type: String }],
      isActive: { type: Boolean, default: true },
      createdAt: { type: Date },
      updatedAt: { type: Date },
    },
  ],

  // Depositions with sub-depositions
  depositions: [
    {
      title: { type: String, required: true },
      sub: [{ type: String }],
    },
  ],
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

  //socials
  // Social media enable flags
  whatsappEnabled: { type: Boolean, default: false },
  telegramEnabled: { type: Boolean, default: false },
  emailEnabled: { type: Boolean, default: false },
  smsEnabled: { type: Boolean, default: false },

  // Convenience single WhatsApp link for quick access
  whatsapplink: { type: String },

  whatsapp: [
    {
      link: { type: String, required: true },
    },
  ],
  telegram: [
    {
      link: { type: String, required: true },
    },
  ],
  email: [
    {
      link: { type: String, required: true },
    },
  ],
  sms: [
    {
      link: { type: String, required: true },
    },
  ],

  // Assigned templates (admin -> agent)
  templates: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Template' }],
  
  // WhatsApp template data for approved templates
  whatsappTemplates: [
    {
      templateId: { type: String },
      templateName: { type: String },
      templateUrl: { type: String },
      description: { type: String },
      language: { type: String },
      status: { type: String },
      category: { type: String },
      assignedAt: { type: Date, default: Date.now }
    }
  ],

  // Default template for each platform
  defaultTemplate: {
    templateId: { type: String },
    templateName: { type: String },
    templateUrl: { type: String },
    platform: { type: String }
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

  // Handle dynamic info list migration and default selection
  if (this.dynamicInfoList && this.dynamicInfoList.length > 0) {
    // Ensure default index is valid
    if (this.defaultDynamicInfoIndex >= this.dynamicInfoList.length) {
      this.defaultDynamicInfoIndex = 0;
    }
    // Set primary details field from default selection
    const defaultDynamicInfo = this.dynamicInfoList[this.defaultDynamicInfoIndex];
    if (defaultDynamicInfo && defaultDynamicInfo.text) {
      this.details = defaultDynamicInfo.text;
    }
  } else if (this.details && !this.dynamicInfoList) {
    // Migrate single details field to list format
    this.dynamicInfoList = [{ text: this.details, isDefault: true }];
    this.defaultDynamicInfoIndex = 0;
  }

  // Handle system prompt list migration and default selection
  if (this.systemPromptList && this.systemPromptList.length > 0) {
    // Ensure default index is valid
    if (this.defaultSystemPromptIndex >= this.systemPromptList.length) {
      this.defaultSystemPromptIndex = 0;
    }
    // Set primary systemPrompt field from default selection
    const defaultSystemPrompt = this.systemPromptList[this.defaultSystemPromptIndex];
    if (defaultSystemPrompt && defaultSystemPrompt.text) {
      this.systemPrompt = defaultSystemPrompt.text;
    }
  } else if (this.systemPrompt && !this.systemPromptList) {
    // Migrate single systemPrompt field to list format
    this.systemPromptList = [{ text: this.systemPrompt, isDefault: true }];
    this.defaultSystemPromptIndex = 0;
  }

  // Clean up disabled social media fields
  if (!this.whatsappEnabled) {
    this.whatsapp = undefined;
    this.whatsapplink = undefined;
  }
  if (!this.telegramEnabled) {
    this.telegram = undefined;
  }
  if (!this.emailEnabled) {
    this.email = undefined;
  }
  if (!this.smsEnabled) {
    this.sms = undefined;
  }

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

// Method to get only enabled social media fields
agentSchema.methods.getEnabledSocials = function() {
  const enabledSocials = {};
  
  if (this.whatsappEnabled && this.whatsapp && this.whatsapp.length > 0) {
    enabledSocials.whatsapp = this.whatsapp;
  }
  
  if (this.telegramEnabled && this.telegram && this.telegram.length > 0) {
    enabledSocials.telegram = this.telegram;
  }
  
  if (this.emailEnabled && this.email && this.email.length > 0) {
    enabledSocials.email = this.email;
  }
  
  if (this.smsEnabled && this.sms && this.sms.length > 0) {
    enabledSocials.sms = this.sms;
  }
  
  return enabledSocials;
}

// Method to check if a social media platform is enabled
agentSchema.methods.isSocialEnabled = function(platform) {
  const enabledPlatforms = {
    whatsapp: this.whatsappEnabled,
    telegram: this.telegramEnabled,
    email: this.emailEnabled,
    sms: this.smsEnabled
  };
  
  return enabledPlatforms[platform] || false;
}

// Method to enable/disable a social media platform
agentSchema.methods.toggleSocial = function(platform, enabled) {
  const platformField = `${platform}Enabled`;
  if (this.schema.paths[platformField]) {
    this[platformField] = enabled;
    
    // If disabling, clear the social media data
    if (!enabled) {
      this[platform] = undefined;
    }
    
    return true;
  }
  return false;
}

// Method to get the default dynamic info
agentSchema.methods.getDefaultDynamicInfo = function() {
  if (this.dynamicInfoList && this.dynamicInfoList.length > 0) {
    const defaultIndex = this.defaultDynamicInfoIndex || 0;
    return this.dynamicInfoList[defaultIndex] || this.dynamicInfoList[0];
  }
  return null;
}

// Method to get the default system prompt
agentSchema.methods.getDefaultSystemPrompt = function() {
  if (this.systemPromptList && this.systemPromptList.length > 0) {
    const defaultIndex = this.defaultSystemPromptIndex || 0;
    return this.systemPromptList[defaultIndex] || this.systemPromptList[0];
  }
  return null;
}

// Method to add a new dynamic info item
agentSchema.methods.addDynamicInfo = function(text, isDefault = false) {
  if (!this.dynamicInfoList) {
    this.dynamicInfoList = [];
  }
  
  const newItem = { text, isDefault };
  this.dynamicInfoList.push(newItem);
  
  if (isDefault) {
    this.defaultDynamicInfoIndex = this.dynamicInfoList.length - 1;
  }
  
  return this.dynamicInfoList.length - 1;
}

// Method to add a new system prompt item
agentSchema.methods.addSystemPrompt = function(text, isDefault = false) {
  if (!this.systemPromptList) {
    this.systemPromptList = [];
  }
  
  const newItem = { text, isDefault };
  this.systemPromptList.push(newItem);
  
  if (isDefault) {
    this.defaultSystemPromptIndex = this.systemPromptList.length - 1;
  }
  
  return this.systemPromptList.length - 1;
}

// Method to set default dynamic info by index
agentSchema.methods.setDefaultDynamicInfo = function(index) {
  if (this.dynamicInfoList && index >= 0 && index < this.dynamicInfoList.length) {
    this.defaultDynamicInfoIndex = index;
    return true;
  }
  return false;
}

// Method to set default system prompt by index
agentSchema.methods.setDefaultSystemPrompt = function(index) {
  if (this.systemPromptList && index >= 0 && index < this.systemPromptList.length) {
    this.defaultSystemPromptIndex = index;
    return true;
  }
  return false;
}

// Encryption/Decryption methods for agentKey
const crypto = require("crypto");

agentSchema.statics.encryptAgentKey = (key) => {
  const algorithm = "aes-256-cbc";
  const secretKey = crypto
    .createHash("sha256")
    .update(process.env.ENCRYPTION_SECRET || "default-secret-key-change-in-production")
    .digest();
  const iv = crypto.randomBytes(16);

  const cipher = crypto.createCipheriv(algorithm, secretKey, iv);
  let encrypted = cipher.update(key, "utf8", "hex");
  encrypted += cipher.final("hex");

  return iv.toString("hex") + ":" + encrypted;
};

agentSchema.statics.decryptAgentKey = (encryptedKey) => {
  const algorithm = "aes-256-cbc";
  const secretKey = crypto
    .createHash("sha256")
    .update(process.env.ENCRYPTION_SECRET || "default-secret-key-change-in-production")
    .digest();

  const parts = encryptedKey.split(":");
  const iv = Buffer.from(parts[0], "hex");
  const encrypted = parts[1];

  const decipher = crypto.createDecipheriv(algorithm, secretKey, iv);
  let decrypted = decipher.update(encrypted, "hex", "utf8");
  decrypted += decipher.final("utf8");

  return decrypted;
};

agentSchema.methods.getDecryptedAgentKey = function () {
  return this.constructor.decryptAgentKey(this.agentKey);
};

// Generate a unique agent key (8 characters max) - no encryption needed since it's already a hash
agentSchema.statics.generateAgentKey = (agentId) => {
  if (!agentId) {
    // Fallback for cases without agentId
    return crypto.randomBytes(4).toString('hex');
  }
  
  // Create a simple hash of the ObjectId and take first 8 characters
  const hash = crypto.createHash('md5').update(agentId.toString()).digest('hex');
  return hash.substring(0, 8);
};

// Find agent by agentKey and return the document ID
agentSchema.statics.findByAgentKey = async function(agentKey) {
  try {
    const agent = await this.findOne({ agentKey: agentKey });
    if (agent) {
      return {
        success: true,
        agentId: agent._id,
        agent: agent
      };
    } else {
      return {
        success: false,
        message: 'Agent not found with this agentKey'
      };
    }
  } catch (error) {
    return {
      success: false,
      message: 'Error finding agent: ' + error.message
    };
  }
};

module.exports = mongoose.model("Agent", agentSchema)