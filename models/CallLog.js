const mongoose = require('mongoose');
const CallLogSchema = new mongoose.Schema({
  clientId: { type: String, required: true, index: true },
  mobile: String,
  time: Date,
  transcript: String,
  audioUrl: String,
  duration: Number,
  leadStatus: { type: String, enum: ['very_interested', 'medium', 'not_interested', 'not_connected'], default: 'medium' }
});
module.exports = mongoose.model('CallLog', CallLogSchema); 