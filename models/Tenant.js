const mongoose = require("mongoose")

const tenantSchema = new mongoose.Schema({
  tenantId: {
    type: String,
    required: true,
    unique: true,
    index: true,
  },
  tenantName: {
    type: String,
    required: true,
  },
  email: {
    type: String,
    required: true,
    unique: true,
  },
  status: {
    type: String,
    enum: ["active", "inactive", "suspended"],
    default: "active",
  },
  subscription: {
    plan: {
      type: String,
      enum: ["free", "basic", "premium", "enterprise"],
      default: "free",
    },
    startDate: { type: Date, default: Date.now },
    endDate: { type: Date },
    features: [
      {
        name: String,
        enabled: { type: Boolean, default: true },
        limit: Number,
      },
    ],
  },
  settings: {
    defaultLanguage: { type: String, default: "en" },
    timezone: { type: String, default: "UTC" },
    preferences: { type: Map, of: String },
  },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
})

tenantSchema.pre("save", function (next) {
  this.updatedAt = Date.now()
  next()
})

module.exports = mongoose.model("Tenant", tenantSchema)
