const express = require("express")
const http = require("http")
const WebSocket = require("ws")
const cors = require("cors")
const path = require("path")
const url = require("url")
require("dotenv").config()

// Import database connection
const { connectDatabase, checkDatabaseHealth, getDatabaseStats, getConnectionState } = require("./config/database")

// Import the unified voice server from aitota.js
const { setupUnifiedVoiceServer, terminateCallByStreamSid } = require("./websocket/aitota")
const { setupSipWebSocketServer } = require("./websocket/sip-server")

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

// Initialize database connection
const initializeDatabase = async () => {
  try {
    await connectDatabase()
    console.log("🎯 [SERVER] Database initialization complete")
    return true
  } catch (error) {
    console.error("❌ [SERVER] Database initialization failed:", error.message)
    return false
  }
}

// Connection tracking
let activeConnections = 0
let totalConnections = 0
let sipActiveConnections = 0
let sipTotalConnections = 0

// Create WebSocket servers WITHOUT path specification initially
const wss = new WebSocket.Server({
  noServer: true, // This is key - we'll handle upgrades manually
  perMessageDeflate: false,
  clientTracking: true,
})

const sipWss = new WebSocket.Server({
  noServer: true, // This is key - we'll handle upgrades manually
  perMessageDeflate: false,
  clientTracking: true
})

// Manual WebSocket upgrade handling based on path
server.on('upgrade', (request, socket, head) => {
  const pathname = url.parse(request.url).pathname;
  
  console.log(`🔄 [SERVER] WebSocket upgrade request for path: ${pathname}`);
  
  if (pathname === '/ws') {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  } else if (pathname === '/sip-ws') {
    sipWss.handleUpgrade(request, socket, head, (ws) => {
      sipWss.emit('connection', ws, request);
    });
  } else {
    console.log(`❌ [SERVER] Unknown WebSocket path: ${pathname}`);
    socket.destroy();
  }
});

// AITOTA WebSocket connection handling
wss.on("connection", (ws, req) => {
  activeConnections++
  totalConnections++

  const clientIP = req.socket.remoteAddress
  const userAgent = req.headers["user-agent"]

  console.log(`🔗 [AITOTA-WS] New connection from ${clientIP}`)
  console.log(`📊 [AITOTA-WS] Active: ${activeConnections}, Total: ${totalConnections}`)

  // Add connection metadata
  ws.connectionId = `aitota_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
  ws.connectedAt = new Date()
  ws.clientIP = clientIP
  ws.userAgent = userAgent

  ws.on("close", (code, reason) => {
    activeConnections--
    const duration = Date.now() - ws.connectedAt.getTime()
    console.log(`🔗 [AITOTA-WS] Connection closed: ${ws.connectionId}`)
    console.log(`📊 [AITOTA-WS] Duration: ${Math.round(duration / 1000)}s, Active: ${activeConnections}`)
  })

  ws.on("error", (error) => {
    console.error(`❌ [AITOTA-WS] WebSocket error for ${ws.connectionId}:`, error.message)
  })
})

// SIP WebSocket connection handling
sipWss.on('connection', (ws, req) => {
  sipActiveConnections++
  sipTotalConnections++
  
  const clientIP = req.socket.remoteAddress;
  console.log(`🔗 [SIP-WS] New connection from ${clientIP}`);
  console.log(`📊 [SIP-WS] Active: ${sipActiveConnections}, Total: ${sipTotalConnections}`);
  
  // Add connection metadata
  ws.connectionId = `sip_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
  ws.connectedAt = new Date()
  ws.clientIP = clientIP
  
  ws.on('close', (code, reason) => {
    sipActiveConnections--
    const duration = Date.now() - ws.connectedAt.getTime()
    console.log(`🔗 [SIP-WS] Connection closed: ${ws.connectionId}`)
    console.log(`📊 [SIP-WS] Duration: ${Math.round(duration / 1000)}s, Active: ${sipActiveConnections}`)
  });
  
  ws.on('error', (error) => {
    console.error(`❌ [SIP-WS] Connection error for ${ws.connectionId}:`, error.message);
  });
});

// Add error handling for both WebSocket servers
wss.on('error', (error) => {
  console.error('❌ [AITOTA-WS] WebSocket server error:', error.message);
});

sipWss.on('error', (error) => {
  console.error('❌ [SIP-WS] WebSocket server error:', error.message);
});

// Initialize the unified voice server with the WebSocket server
setupUnifiedVoiceServer(wss)

// Setup SIP WebSocket server
setupSipWebSocketServer(sipWss)
console.log('✅ [SERVER] SIP WebSocket server setup enabled');

// Your existing API endpoints...
app.get("/api/logs", async (req, res) => {
  try {
    const { clientId, limit = 50, page = 1, leadStatus, isActive, sortBy = "createdAt", sortOrder = "desc" } = req.query

    const filters = {}
    if (clientId) filters.clientId = clientId
    if (leadStatus) filters.leadStatus = leadStatus
    if (isActive !== undefined) filters["metadata.isActive"] = isActive === "true"

    const sort = {}
    sort[sortBy] = sortOrder === "desc" ? -1 : 1
    const skip = (Number.parseInt(page) - 1) * Number.parseInt(limit)

    const CallLog = require("./models/CallLog")

    const [logs, totalCount, activeCount] = await Promise.all([
      CallLog.find(filters).sort(sort).limit(Number.parseInt(limit)).skip(skip).lean().exec(),
      CallLog.countDocuments(filters),
      CallLog.countDocuments({
        ...filters,
        "metadata.isActive": true,
      }),
    ])

    const clientIds = await CallLog.distinct("clientId", {})

    const response = {
      logs,
      pagination: {
        total: totalCount,
        page: Number.parseInt(page),
        limit: Number.parseInt(limit),
        pages: Math.ceil(totalCount / Number.parseInt(limit)),
      },
      stats: {
        total: totalCount,
        active: activeCount,
        clients: clientIds.length,
        timestamp: new Date().toISOString(),
      },
      filters: {
        clientId,
        leadStatus,
        isActive,
        availableClients: clientIds.sort(),
      },
    }

    res.json(response)
  } catch (error) {
    console.error("❌ [LOGS-API] Error fetching logs:", error.message)
    res.status(500).json({
      error: "Failed to fetch logs",
      message: error.message,
      timestamp: new Date().toISOString(),
    })
  }
})

app.get("/health", async (req, res) => {
  try {
    const dbHealth = await checkDatabaseHealth()
    const connectionState = getConnectionState()

    const health = {
      status: "ok",
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      database: {
        ...dbHealth,
        connection: connectionState,
      },
      server: {
        port: PORT,
        environment: NODE_ENV,
        memory: process.memoryUsage(),
      },
      websockets: {
        aitota: {
          clients: wss.clients.size,
          active: activeConnections,
        },
        sip: {
          clients: sipWss.clients.size,
          active: sipActiveConnections,
        },
      },
    }

    if (dbHealth.status !== "healthy") {
      return res.status(503).json({
        ...health,
        status: "degraded",
        message: "Database connection issues",
      })
    }

    res.json(health)
  } catch (error) {
    res.status(500).json({
      status: "error",
      timestamp: new Date().toISOString(),
      error: error.message,
    })
  }
})

app.get("/api/info", async (req, res) => {
  try {
    const dbStats = await getDatabaseStats()
    const connectionState = getConnectionState()

    res.json({
      server: {
        name: "AITOTA Voice AI Server with SIP Support",
        version: "1.1.0",
        environment: NODE_ENV,
        port: PORT,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        nodeVersion: process.version,
      },
      database: {
        connection: connectionState,
        statistics: dbStats,
      },
      endpoints: {
        websocket: `/ws`,
        sipWebsocket: `/sip-ws`,
        health: `/health`,
        stats: `/api/stats`,
        info: `/api/info`,
      },
    })
  } catch (error) {
    res.status(500).json({
      error: "Failed to get server info",
      message: error.message,
    })
  }
})

app.get("/api/stats", async (req, res) => {
  try {
    const dbHealth = await checkDatabaseHealth()
    const dbStats = await getDatabaseStats()
    const connectionState = getConnectionState()

    const stats = {
      server: {
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        activeConnections: activeConnections + sipActiveConnections,
        totalConnections: totalConnections + sipTotalConnections,
        timestamp: new Date().toISOString(),
      },
      websocket: {
        aitota: {
          clients: wss.clients.size,
          active: activeConnections,
          total: totalConnections,
          connections: Array.from(wss.clients).map((ws) => ({
            id: ws.connectionId,
            connectedAt: ws.connectedAt,
            readyState: ws.readyState,
            clientIP: ws.clientIP?.replace(/^.*:/, ""),
          })),
        },
        sip: {
          clients: sipWss.clients.size,
          active: sipActiveConnections,
          total: sipTotalConnections,
          connections: Array.from(sipWss.clients).map((ws) => ({
            id: ws.connectionId,
            connectedAt: ws.connectedAt,
            readyState: ws.readyState,
            clientIP: ws.clientIP?.replace(/^.*:/, ""),
          })),
        },
      },
      database: {
        health: dbHealth,
        connection: connectionState,
        statistics: dbStats,
      },
    }

    res.json(stats)
  } catch (error) {
    res.status(500).json({
      error: "Failed to get server statistics",
      message: error.message,
    })
  }
})

// Graceful shutdown handling
const gracefulShutdown = (signal) => {
  console.log(`\n🛑 [SERVER] Received ${signal}, shutting down gracefully...`)

  server.close(() => {
    console.log("📞 [SERVER] HTTP server closed")

    wss.clients.forEach((ws) => {
      ws.terminate()
    })

    sipWss.clients.forEach((ws) => {
      ws.terminate()
    })

    wss.close(() => {
      console.log("🔌 [SERVER] AITOTA WebSocket server closed")

      sipWss.close(() => {
        console.log("🔌 [SERVER] SIP WebSocket server closed")
        console.log("✅ [SERVER] Graceful shutdown complete")
        process.exit(0)
      })
    })
  })
}

// Handle shutdown signals
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'))
process.on('SIGINT', () => gracefulShutdown('SIGINT'))

// Start the server after database initialization
const startServer = async () => {
  try {
    console.log("\n🚀 ====== AITOTA VOICE AI SERVER WITH SIP SUPPORT STARTING ======")
    console.log(`🌍 Environment: ${NODE_ENV}`)
    console.log(`📍 Port: ${PORT}`)

    const dbInitialized = await initializeDatabase()
    if (!dbInitialized) {
      console.error("❌ [SERVER] Failed to initialize database, exiting...")
      process.exit(1)
    }

    server.listen(PORT, () => {
      console.log("\n✅ ====== SERVER STARTED SUCCESSFULLY ======")
      console.log(`📍 Server running on port ${PORT}`)
      console.log(`🌍 Environment: ${NODE_ENV}`)
      console.log(`🔗 AITOTA WebSocket endpoint: ws://localhost:${PORT}/ws`)
      console.log(`🔗 SIP WebSocket endpoint: ws://localhost:${PORT}/sip-ws`)
      console.log(`🩺 Health check: http://localhost:${PORT}/health`)
      console.log(`📊 Server stats: http://localhost:${PORT}/api/stats`)
      console.log(`📋 Server info: http://localhost:${PORT}/api/info`)
      console.log("==============================================\n")
    })
  } catch (error) {
    console.error("❌ [SERVER] Failed to start:", error.message)
    process.exit(1)
  }
}

startServer()

module.exports = { app, server, wss, sipWss }