const mongoose = require('mongoose');

const SystemPromptSchema = new mongoose.Schema(
  {
    title: { type: String, required: true, trim: true },
    promptText: { type: String, required: true },
    isDefault: { type: Boolean, default: false, index: true },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'Admin' },
    tags: [{ type: String, trim: true }],
  },
  { timestamps: true }
);

module.exports = mongoose.model('SystemPrompt', SystemPromptSchema);


