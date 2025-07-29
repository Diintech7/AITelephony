const mongoose = require('mongoose');

const CallLogSchema = new mongoose.Schema({
  clientId: { type: String, required: true, index: true },
  mobile: String,
  time: Date,
  transcript: String,
  audioUrl: String,
  duration: Number,
  leadStatus: { 
    type: String, 
    enum: [
      // Very Interested
      'vvi', 
      'maybe', 
      'enrolled',
      // Medium
      'medium',
      // Not Interested  
      'junk_leads',
      'not_required',
      'enroll_other', 
      'declined',
      'not_eligible',
      'wrong_number',
      // Not Connected
      'not_connected'
    ], 
    default: 'medium' 
  },
  // Additional metadata for better tracking
  metadata: {
    userTranscriptCount: Number,
    aiResponseCount: Number,
    languages: [String],
    callEndTime: Date,
    leadCategory: {
      type: String,
      enum: ['very_interested', 'medium', 'not_interested', 'not_connected'],
      default: 'medium'
    }
  }
}, {
  timestamps: true // Adds createdAt and updatedAt automatically
});

// Index for better query performance
CallLogSchema.index({ clientId: 1, time: -1 });
CallLogSchema.index({ leadStatus: 1 });
CallLogSchema.index({ 'metadata.leadCategory': 1 });

// Virtual field to get lead category based on status
CallLogSchema.virtual('leadCategory').get(function() {
  const veryInterestedStatuses = ['vvi', 'maybe', 'enrolled'];
  const notInterestedStatuses = ['junk_leads', 'not_required', 'enroll_other', 'declined', 'not_eligible', 'wrong_number'];
  
  if (veryInterestedStatuses.includes(this.leadStatus)) {
    return 'very_interested';
  } else if (notInterestedStatuses.includes(this.leadStatus)) {
    return 'not_interested';
  } else if (this.leadStatus === 'not_connected') {
    return 'not_connected';
  } else {
    return 'medium';
  }
});

// Pre-save middleware to set leadCategory in metadata
CallLogSchema.pre('save', function(next) {
  if (!this.metadata) {
    this.metadata = {};
  }
  
  const veryInterestedStatuses = ['vvi', 'maybe', 'enrolled'];
  const notInterestedStatuses = ['junk_leads', 'not_required', 'enroll_other', 'declined', 'not_eligible', 'wrong_number'];
  
  if (veryInterestedStatuses.includes(this.leadStatus)) {
    this.metadata.leadCategory = 'very_interested';
  } else if (notInterestedStatuses.includes(this.leadStatus)) {
    this.metadata.leadCategory = 'not_interested';
  } else if (this.leadStatus === 'not_connected') {
    this.metadata.leadCategory = 'not_connected';
  } else {
    this.metadata.leadCategory = 'medium';
  }
  
  next();
});

module.exports = mongoose.model('CallLog', CallLogSchema);