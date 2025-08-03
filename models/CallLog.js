const mongoose = require('mongoose');

const CallLogSchema = new mongoose.Schema({
  clientId: { type: String, required: true, index: true },
  campaignId: { type: mongoose.Schema.Types.ObjectId, ref: 'Campaign', index: true },
  agentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Agent', index: true },
  mobile: String,
  time: Date,
  transcript: String,
  audioUrl: String,
  duration: Number,
  leadStatus: { 
    type: String, 
    enum: [
      // Connected - Interested
      'vvi',                    // very very interested
      'maybe',                  // maybe
      'enrolled',               // enrolled
      
      // Connected - Not Interested
      'junk_lead',              // junk lead
      'not_required',           // not required
      'enrolled_other',         // enrolled other
      'decline',                // decline
      'not_eligible',           // not eligible
      'wrong_number',           // wrong number
      
      // Connected - Followup
      'hot_followup',           // hot followup
      'cold_followup',          // cold followup
      'schedule',               // schedule
      
      // Not Connected
      'not_connected'           // not connected
    ], 
    default: 'maybe' 
  }
}, {
  timestamps: true
});

// Index for better query performance
CallLogSchema.index({ clientId: 1, campaignId: 1, agentId: 1, time: -1 });
CallLogSchema.index({ clientId: 1, leadStatus: 1 });

module.exports = mongoose.model('CallLog', CallLogSchema); 