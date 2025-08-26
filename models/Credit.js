const mongoose = require("mongoose");

const CreditSchema = new mongoose.Schema({
  // Client Reference
  clientId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Client',
    required: true,
    index: true
  },
  
  // Current Balance
  currentBalance: {
    type: Number,
    default: 0,
    min: 0
  },
  
  // Total Credits Ever Purchased
  totalPurchased: {
    type: Number,
    default: 0,
    min: 0
  },
  
  // Total Credits Ever Used
  totalUsed: {
    type: Number,
    default: 0,
    min: 0
  },
  
  // Credit History
  history: [{
    type: {
      type: String,
      enum: ['purchase', 'usage', 'refund', 'bonus', 'expiry', 'adjustment'],
      required: true
    },
    amount: {
      type: Number,
      required: true
    },
    description: {
      type: String,
      required: true
    },
    planId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'Plan'
    },
    transactionId: {
      type: String
    },
    usageType: {
      type: String,
      enum: ['call', 'whatsapp', 'telegram', 'email', 'sms', 'other']
    },
    duration: {
      type: Number // For calls in minutes
    },
    messageCount: {
      type: Number // For messages
    },
    metadata: {
      type: Map,
      of: String
    },
    timestamp: {
      type: Date,
      default: Date.now
    }
  }],
  
  // Current Plan Information
  currentPlan: {
    planId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'Plan'
    },
    startDate: {
      type: Date
    },
    endDate: {
      type: Date
    },
    billingCycle: {
      type: String,
      enum: ['monthly', 'quarterly', 'yearly']
    },
    autoRenew: {
      type: Boolean,
      default: false
    }
  },
  
  // Usage Statistics
  usageStats: {
    calls: {
      total: { type: Number, default: 0 },
      minutes: { type: Number, default: 0 },
      creditsUsed: { type: Number, default: 0 }
    },
    whatsapp: {
      messages: { type: Number, default: 0 },
      creditsUsed: { type: Number, default: 0 }
    },
    telegram: {
      messages: { type: Number, default: 0 },
      creditsUsed: { type: Number, default: 0 }
    },
    email: {
      messages: { type: Number, default: 0 },
      creditsUsed: { type: Number, default: 0 }
    },
    sms: {
      messages: { type: Number, default: 0 },
      creditsUsed: { type: Number, default: 0 }
    }
  },
  
  // Monthly Usage Tracking
  monthlyUsage: [{
    month: {
      type: String, // Format: "YYYY-MM"
      required: true
    },
    calls: {
      count: { type: Number, default: 0 },
      minutes: { type: Number, default: 0 },
      credits: { type: Number, default: 0 }
    },
    whatsapp: {
      count: { type: Number, default: 0 },
      credits: { type: Number, default: 0 }
    },
    telegram: {
      count: { type: Number, default: 0 },
      credits: { type: Number, default: 0 }
    },
    email: {
      count: { type: Number, default: 0 },
      credits: { type: Number, default: 0 }
    },
    sms: {
      count: { type: Number, default: 0 },
      credits: { type: Number, default: 0 }
    },
    totalCredits: { type: Number, default: 0 }
  }],
  
  // Settings
  settings: {
    lowBalanceAlert: {
      enabled: { type: Boolean, default: true },
      threshold: { type: Number, default: 100 }
    },
    autoPurchase: {
      enabled: { type: Boolean, default: false },
      planId: { type: mongoose.Schema.Types.ObjectId, ref: 'Plan' },
      threshold: { type: Number, default: 50 }
    }
  },
  
  // Timestamps
  createdAt: {
    type: Date,
    default: Date.now
  },
  updatedAt: {
    type: Date,
    default: Date.now
  }
});

// Update timestamp on save
CreditSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  next();
});

// Index for efficient queries
CreditSchema.index({ clientId: 1, 'history.timestamp': -1 });

// Method to add credits (purchase, bonus, refund)
CreditSchema.methods.addCredits = function(amount, type, description, planId = null, transactionId = null) {
  this.currentBalance += amount;
  this.totalPurchased += amount;
  
  this.history.push({
    type: type,
    amount: amount,
    description: description,
    planId: planId,
    transactionId: transactionId,
    timestamp: new Date()
  });
  
  return this.save();
};

// Method to use credits
CreditSchema.methods.useCredits = function(amount, usageType, description, metadata = {}) {
  // Normalize usage type keys (schema uses 'calls' in usageStats but history enum has 'call')
  const normalizedUsageType = usageType === 'call' ? 'calls' : usageType;
  if (this.currentBalance < amount) {
    throw new Error('Insufficient credits');
  }
  
  this.currentBalance -= amount;
  this.totalUsed += amount;
  
  // Update usage statistics
  if (normalizedUsageType && this.usageStats[normalizedUsageType]) {
    this.usageStats[normalizedUsageType].creditsUsed += amount;
    if (normalizedUsageType === 'calls') {
      this.usageStats.calls.total += 1;
      this.usageStats.calls.minutes += (metadata.duration || 0);
    } else {
      this.usageStats[normalizedUsageType].messages += (metadata.messageCount || 1);
    }
  }
  
  // Update monthly usage
  this.updateMonthlyUsage(amount, normalizedUsageType, metadata);
  
  this.history.push({
    type: 'usage',
    amount: -amount,
    description: description,
    usageType: usageType, // preserve original label in history
    duration: metadata.duration,
    messageCount: metadata.messageCount,
    metadata: metadata,
    timestamp: new Date()
  });
  
  return this.save();
};

// Method to update monthly usage
CreditSchema.methods.updateMonthlyUsage = function(amount, usageType, metadata = {}) {
  const now = new Date();
  const monthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  
  let monthlyRecord = this.monthlyUsage.find(record => record.month === monthKey);
  
  if (!monthlyRecord) {
    monthlyRecord = {
      month: monthKey,
      calls: { count: 0, minutes: 0, credits: 0 },
      whatsapp: { count: 0, credits: 0 },
      telegram: { count: 0, credits: 0 },
      email: { count: 0, credits: 0 },
      sms: { count: 0, credits: 0 },
      totalCredits: 0
    };
    this.monthlyUsage.push(monthlyRecord);
  }
  
  monthlyRecord.totalCredits += amount;
  
  if (usageType && monthlyRecord[usageType]) {
    monthlyRecord[usageType].credits += amount;
    if (usageType === 'calls') {
      monthlyRecord.calls.count += 1;
      monthlyRecord.calls.minutes += (metadata.duration || 0);
    } else {
      monthlyRecord[usageType].count += (metadata.messageCount || 1);
    }
  }
};

// Method to get current month usage
CreditSchema.methods.getCurrentMonthUsage = function() {
  const now = new Date();
  const monthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  
  return this.monthlyUsage.find(record => record.month === monthKey) || {
    month: monthKey,
    calls: { count: 0, minutes: 0, credits: 0 },
    whatsapp: { count: 0, credits: 0 },
    telegram: { count: 0, credits: 0 },
    email: { count: 0, credits: 0 },
    sms: { count: 0, credits: 0 },
    totalCredits: 0
  };
};

// Method to check if low balance alert should be triggered
CreditSchema.methods.shouldTriggerLowBalanceAlert = function() {
  return this.settings.lowBalanceAlert.enabled && 
         this.currentBalance <= this.settings.lowBalanceAlert.threshold;
};

// Static method to get credit balance for a client
CreditSchema.statics.getClientBalance = function(clientId) {
  return this.findOne({ clientId }).populate('currentPlan.planId');
};

// Static method to create or get credit record for a client
CreditSchema.statics.getOrCreateCreditRecord = async function(clientId) {
  let creditRecord = await this.findOne({ clientId });
  
  if (!creditRecord) {
    creditRecord = new this({
      clientId: clientId,
      currentBalance: 0,
      totalPurchased: 0,
      totalUsed: 0,
      history: [],
      usageStats: {
        calls: { total: 0, minutes: 0, creditsUsed: 0 },
        whatsapp: { messages: 0, creditsUsed: 0 },
        telegram: { messages: 0, creditsUsed: 0 },
        email: { messages: 0, creditsUsed: 0 },
        sms: { messages: 0, creditsUsed: 0 }
      },
      monthlyUsage: [],
      settings: {
        lowBalanceAlert: { enabled: true, threshold: 100 },
        autoPurchase: { enabled: false, threshold: 50 }
      }
    });
    await creditRecord.save();
  }
  
  return creditRecord;
};

module.exports = mongoose.model("Credit", CreditSchema);
