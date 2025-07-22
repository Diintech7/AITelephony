const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const cors = require("cors");
const path = require("path");
require("dotenv").config();

// Import database connection
const { 
  connectDatabase, 
  checkDatabaseHealth, 
  getDatabaseStats, 
  getConnectionState 
} = require("./config/database");

// Import the unified voice server from aitota.js
const { setupUnifiedVoiceServer } = require("./websocket/aitota");

// Environment configuration
const PORT = process.env.PORT || 3000;
const NODE_ENV = process.env.NODE_ENV || "development";

// Initialize database connection
const initializeDatabase = async () => {
  try {
    await connectDatabase();
    console.log("🎯 [SERVER] Database initialization complete");
    return true;
  } catch (error) {
    console.error("❌ [SERVER] Database initialization failed:", error.message);
    return false;
  }
};

// Express app setup
const app = express();
const server = http.createServer(app);

app.use(cors());

app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true }));

// Static files (if you have any frontend files)
app.use(express.static(path.join(__dirname, "public")));

// Health check endpoint
app.get("/health", async (req, res) => {
  try {
    const dbHealth = await checkDatabaseHealth();
    const connectionState = getConnectionState();
    
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
    };

    // If database is unhealthy, return 503
    if (dbHealth.status !== "healthy") {
      return res.status(503).json({
        ...health,
        status: "degraded",
        message: "Database connection issues",
      });
    }

    res.json(health);
  } catch (error) {
    res.status(500).json({
      status: "error",
      timestamp: new Date().toISOString(),
      error: error.message,
    });
  }
});

// Server info endpoint
app.get("/api/info", async (req, res) => {
  try {
    const dbStats = await getDatabaseStats();
    const connectionState = getConnectionState();
    
    res.json({
      server: {
        name: "AITOTA Voice AI Server",
        version: "1.0.0",
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
        health: `/health`,
        stats: `/api/stats`,
        info: `/api/info`,
      },
    });
  } catch (error) {
    res.status(500).json({
      error: "Failed to get server info",
      message: error.message,
    });
  }
});

// WebSocket server setup
const wss = new WebSocket.Server({
  server,
  path: "/ws",
  perMessageDeflate: false,
  clientTracking: true
});

// Connection tracking
let activeConnections = 0;
let totalConnections = 0;

wss.on("connection", (ws, req) => {
  activeConnections++;
  totalConnections++;
  
  const clientIP = req.socket.remoteAddress;
  const userAgent = req.headers["user-agent"];
  
  console.log(`🔗 [SERVER] New connection from ${clientIP}`);
  console.log(`📊 [SERVER] Active: ${activeConnections}, Total: ${totalConnections}`);
  
  // Add connection metadata
  ws.connectionId = `conn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  ws.connectedAt = new Date();
  ws.clientIP = clientIP;
  ws.userAgent = userAgent;

  ws.on("close", (code, reason) => {
    activeConnections--;
    const duration = Date.now() - ws.connectedAt.getTime();
    console.log(`🔗 [SERVER] Connection closed: ${ws.connectionId}`);
    console.log(`📊 [SERVER] Duration: ${Math.round(duration/1000)}s, Active: ${activeConnections}`);
  });

  ws.on("error", (error) => {
    console.error(`❌ [SERVER] WebSocket error for ${ws.connectionId}:`, error.message);
  });
});

// Initialize the unified voice server with the WebSocket server
setupUnifiedVoiceServer(wss);

// Server statistics endpoint
app.get("/api/stats", async (req, res) => {
  try {
    const dbHealth = await checkDatabaseHealth();
    const dbStats = await getDatabaseStats();
    const connectionState = getConnectionState();
    
    const stats = {
      server: {
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        activeConnections,
        totalConnections,
        timestamp: new Date().toISOString(),
      },
      websocket: {
        clients: wss.clients.size,
        connections: Array.from(wss.clients).map(ws => ({
          id: ws.connectionId,
          connectedAt: ws.connectedAt,
          readyState: ws.readyState,
          clientIP: ws.clientIP?.replace(/^.*:/, '') // Hide full IP for privacy
        }))
      },
      database: {
        health: dbHealth,
        connection: connectionState,
        statistics: dbStats,
      },
    };
    
    res.json(stats);
  } catch (error) {
    res.status(500).json({
      error: "Failed to get server statistics",
      message: error.message,
    });
  }
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(`❌ [SERVER] Express error:`, err.stack);
  res.status(500).json({
    error: "Internal Server Error",
    message: NODE_ENV === "development" ? err.message : "Something went wrong!"
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: "Not Found",
    message: "The requested resource was not found",
    path: req.path,
  });
});

// Graceful shutdown handling
const gracefulShutdown = (signal) => {
  console.log(`\n🛑 [SERVER] Received ${signal}, shutting down gracefully...`);
  
  server.close(() => {
    console.log("📞 [SERVER] HTTP server closed");
    
    // Close all WebSocket connections
    wss.clients.forEach((ws) => {
      ws.terminate();
    });
    
    wss.close(() => {
      console.log("🔌 [SERVER] WebSocket server closed");
      console.log("✅ [SERVER] Graceful shutdown complete");
      process.exit(0);
    });
  });
  
  // Force close after 10 seconds
  setTimeout(() => {
    console.error("⚠️ [SERVER] Forcing shutdown after timeout");
    process.exit(1);
  }, 10000);
};

// Handle shutdown signals
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

// Handle uncaught exceptions
process.on("uncaughtException", (error) => {
  console.error("💥 [SERVER] Uncaught Exception:", error);
  gracefulShutdown("UNCAUGHT_EXCEPTION");
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("💥 [SERVER] Unhandled Rejection at:", promise, "reason:", reason);
  gracefulShutdown("UNHANDLED_REJECTION");
});

// Start the server after database initialization
const startServer = async () => {
  try {
    console.log("\n🚀 ====== AITOTA VOICE AI SERVER STARTING ======");
    console.log(`🌐 Environment: ${NODE_ENV}`);
    console.log(`📍 Port: ${PORT}`);
    
    // Initialize database first
    const dbInitialized = await initializeDatabase();
    if (!dbInitialized) {
      console.error("❌ [SERVER] Failed to initialize database, exiting...");
      process.exit(1);
    }
    
    // Start HTTP server
    server.listen(PORT, () => {
      console.log("\n✅ ====== SERVER STARTED SUCCESSFULLY ======");
      console.log(`📍 Server running on port ${PORT}`);
      console.log(`🌐 Environment: ${NODE_ENV}`);
      console.log(`🔗 WebSocket endpoint: ws://localhost:${PORT}/ws`);
      console.log(`🩺 Health check: http://localhost:${PORT}/health`);
      console.log(`📊 Server stats: http://localhost:${PORT}/api/stats`);
      console.log(`📋 Server info: http://localhost:${PORT}/api/info`);
      console.log("==============================================\n");
    });
    
  } catch (error) {
    console.error("❌ [SERVER] Failed to start:", error.message);
    process.exit(1);
  }
};

// Start the server
startServer();

// Export server for testing purposes
module.exports = { app, server, wss };