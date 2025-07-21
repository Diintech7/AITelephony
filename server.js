const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const cors = require("cors");
const path = require("path");
require("dotenv").config();

// Import the unified voice server from aitota.js
const { setupUnifiedVoiceServer } = require("./websocket/aitota");

// Environment configuration

const PORT = process.env.PORT;
const NODE_ENV = process.env.NODE_ENV;

// Express app setup
const app = express();
const server = http.createServer(app);

app.use(cors());

app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true }));

// Static files (if you have any frontend files)
app.use(express.static(path.join(__dirname, "public")));

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
app.get("/api/stats", (req, res) => {
  const stats = {
    server: {
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      activeConnections,
      totalConnections,
      timestamp: new Date().toISOString()
    },
    websocket: {
      clients: wss.clients.size,
      connections: Array.from(wss.clients).map(ws => ({
        id: ws.connectionId,
        connectedAt: ws.connectedAt,
        readyState: ws.readyState,
        clientIP: ws.clientIP?.replace(/^.*:/, '') // Hide full IP for privacy
      }))
    }
  };
  
  res.json(stats);
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(`❌ [SERVER] Express error:`, err.stack);
  res.status(500).json({
    error: "Internal Server Error",
    message: NODE_ENV === "development" ? err.message : "Something went wrong!"
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
  
};

// Handle shutdown signals
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));


// Start the server
server.listen(PORT, () => {
  console.log("\n🚀 ====== AITOTA VOICE AI SERVER STARTED ======");
  console.log(`📍 Server running on port ${PORT}`);
  console.log(`🌐 Environment: ${NODE_ENV}`);
  console.log(`🔗 WebSocket endpoint: ws://localhost:${PORT}/ws`);
  console.log(`🩺 Health check: http://localhost:${PORT}/health`);
  console.log(`📊 Server stats: http://localhost:${PORT}/api/stats`);
  console.log(`📋 Server info: http://localhost:${PORT}/api/info`);
  console.log("================================================\n");
  
});

// Export server for testing purposes
module.exports = { app, server, wss };