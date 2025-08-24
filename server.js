const express = require("express")
const http = require("http")
const WebSocket = require("ws")
const cors = require("cors")
const path = require("path")
const url = require("url")
require("dotenv").config()

// Import only the simplified voice server
const { setupUnifiedVoiceServer } = require("./websocket/aitota")

// Environment configuration
const PORT = process.env.PORT || 3000
const NODE_ENV = process.env.NODE_ENV || "development"

// Express app setup
const app = express()
const server = http.createServer(app)

app.use(cors())
app.use(express.json({ limit: "10mb" }))
app.use(express.urlencoded({ extended: true }))
app.use(express.static(path.join(__dirname, "public")))

// Connection tracking - simplified
let activeConnections = 0
let totalConnections = 0

// Create single WebSocket server for voice AI
const wss = new WebSocket.Server({
  noServer: true,
  perMessageDeflate: false,
  clientTracking: true,
})

// Manual WebSocket upgrade handling - only for /ws path
server.on("upgrade", (request, socket, head) => {
  const pathname = url.parse(request.url).pathname

  console.log(`🔄 [SERVER] WebSocket upgrade request for path: ${pathname}`)

  if (pathname === "/ws") {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit("connection", ws, request)
    })
  } else {
    console.log(`❌ [SERVER] Unknown WebSocket path: ${pathname}`)
    socket.destroy()
  }
})

// Voice AI WebSocket connection handling
wss.on("connection", (ws, req) => {
  activeConnections++
  totalConnections++

  const clientIP = req.socket.remoteAddress
  console.log(`🔗 [VOICE-AI] New connection from ${clientIP}`)
  console.log(`📊 [VOICE-AI] Active: ${activeConnections}, Total: ${totalConnections}`)

  // Add connection metadata
  ws.connectionId = `voice_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
  ws.connectedAt = new Date()
  ws.clientIP = clientIP

  // Setup the unified voice server for this connection
  setupUnifiedVoiceServer(ws)

  ws.on("close", (code, reason) => {
    activeConnections--
    const duration = Date.now() - ws.connectedAt.getTime()
    console.log(`🔗 [VOICE-AI] Connection closed: ${ws.connectionId}`)
    console.log(`📊 [VOICE-AI] Duration: ${Math.round(duration / 1000)}s, Active: ${activeConnections}`)
  })

  ws.on("error", (error) => {
    console.error(`❌ [VOICE-AI] WebSocket error for ${ws.connectionId}:`, error.message)
  })
})

// Add error handling for WebSocket server
wss.on("error", (error) => {
  console.error("❌ [VOICE-AI] WebSocket server error:", error.message)
})

// Health check endpoint - simplified
app.get("/health", (req, res) => {
  const health = {
    status: "ok",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    server: {
      port: PORT,
      environment: NODE_ENV,
      memory: process.memoryUsage(),
    },
    websocket: {
      clients: wss.clients.size,
      active: activeConnections,
      total: totalConnections,
    },
    services: {
      deepgram: !!process.env.DEEPGRAM_API_KEY,
      sarvam: !!process.env.SARVAM_API_KEY,
      openai: !!process.env.OPENAI_API_KEY,
    },
  }

  res.json(health)
})

// Server info endpoint - simplified
app.get("/api/info", (req, res) => {
  res.json({
    server: {
      name: "Simplified Voice AI Server",
      version: "2.0.0",
      environment: NODE_ENV,
      port: PORT,
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      nodeVersion: process.version,
    },
    services: ["Deepgram STT", "OpenAI LLM", "Sarvam TTS"],
    endpoints: {
      websocket: `/ws`,
      health: `/health`,
      info: `/api/info`,
      stats: `/api/stats`,
    },
  })
})

// Server statistics endpoint - simplified
app.get("/api/stats", (req, res) => {
  const stats = {
    server: {
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      activeConnections: activeConnections,
      totalConnections: totalConnections,
      timestamp: new Date().toISOString(),
    },
    websocket: {
      clients: wss.clients.size,
      active: activeConnections,
      total: totalConnections,
      connections: Array.from(wss.clients).map((ws) => ({
        id: ws.connectionId,
        connectedAt: ws.connectedAt,
        readyState: ws.readyState,
        clientIP: ws.clientIP?.replace(/^.*:/, ""), // Hide full IP for privacy
      })),
    },
    services: {
      deepgram: {
        configured: !!process.env.DEEPGRAM_API_KEY,
        status: "ready",
      },
      sarvam: {
        configured: !!process.env.SARVAM_API_KEY,
        status: "ready",
      },
      openai: {
        configured: !!process.env.OPENAI_API_KEY,
        status: "ready",
      },
    },
  }

  res.json(stats)
})

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(`❌ [SERVER] Express error:`, err.stack)
  res.status(500).json({
    error: "Internal Server Error",
    message: NODE_ENV === "development" ? err.message : "Something went wrong!",
  })
})

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: "Not Found",
    message: "The requested resource was not found",
    path: req.path,
  })
})

// Graceful shutdown handling
const gracefulShutdown = (signal) => {
  console.log(`\n🛑 [SERVER] Received ${signal}, shutting down gracefully...`)

  server.close(() => {
    console.log("📞 [SERVER] HTTP server closed")

    // Close all WebSocket connections
    wss.clients.forEach((ws) => {
      ws.terminate()
    })

    wss.close(() => {
      console.log("🔌 [SERVER] WebSocket server closed")
      console.log("✅ [SERVER] Graceful shutdown complete")
      process.exit(0)
    })
  })
}

// Handle shutdown signals
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"))
process.on("SIGINT", () => gracefulShutdown("SIGINT"))

// Handle uncaught exceptions
process.on("uncaughtException", (error) => {
  console.error("💥 [SERVER] Uncaught Exception:", error)
  gracefulShutdown("UNCAUGHT_EXCEPTION")
})

process.on("unhandledRejection", (reason, promise) => {
  console.error("💥 [SERVER] Unhandled Rejection at:", promise, "reason:", reason)
  gracefulShutdown("UNHANDLED_REJECTION")
})

// Start the server
const startServer = async () => {
  try {
    console.log("\n🚀 ====== SIMPLIFIED VOICE AI SERVER STARTING ======")
    console.log(`🌍 Environment: ${NODE_ENV}`)
    console.log(`📍 Port: ${PORT}`)

    // Validate API keys
    const requiredKeys = ["DEEPGRAM_API_KEY", "SARVAM_API_KEY", "OPENAI_API_KEY"]
    const missingKeys = requiredKeys.filter((key) => !process.env[key])

    if (missingKeys.length > 0) {
      console.error(`❌ [SERVER] Missing required environment variables: ${missingKeys.join(", ")}`)
      process.exit(1)
    }

    // Start HTTP server
    server.listen(PORT, () => {
      console.log("\n✅ ====== SERVER STARTED SUCCESSFULLY ======")
      console.log(`📍 Server running on port ${PORT}`)
      console.log(`🌍 Environment: ${NODE_ENV}`)
      console.log(`🔗 Voice AI WebSocket: ws://localhost:${PORT}/ws`)
      console.log(`🩺 Health check: http://localhost:${PORT}/health`)
      console.log(`📊 Server stats: http://localhost:${PORT}/api/stats`)
      console.log(`📋 Server info: http://localhost:${PORT}/api/info`)
      console.log("\n🎤 Services configured:")
      console.log("🎤 Deepgram STT - Speech to Text")
      console.log("🤖 OpenAI GPT - Language Processing")
      console.log("🔊 Sarvam TTS - Text to Speech")
      console.log("==============================================\n")
    })
  } catch (error) {
    console.error("❌ [SERVER] Failed to start:", error.message)
    process.exit(1)
  }
}

startServer()

module.exports = { app, server, wss }
