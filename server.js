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

// Add this route to your server.js file (before the error handling middleware)

// Live logs endpoint
app.get("/api/logs", async (req, res) => {
  try {
    const {
      clientId,
      limit = 50,
      page = 1,
      leadStatus,
      isActive,
      sortBy = 'createdAt',
      sortOrder = 'desc'
    } = req.query;

    // Build query filters
    const filters = {};
    
    if (clientId) {
      filters.clientId = clientId;
    }
    
    if (leadStatus) {
      filters.leadStatus = leadStatus;
    }
    
    if (isActive !== undefined) {
      filters['metadata.isActive'] = isActive === 'true';
    }

    // Build sort object
    const sort = {};
    sort[sortBy] = sortOrder === 'desc' ? -1 : 1;

    // Calculate skip for pagination
    const skip = (parseInt(page) - 1) * parseInt(limit);

    console.log(`📊 [LOGS-API] Query filters:`, filters);
    console.log(`📊 [LOGS-API] Sort:`, sort);
    console.log(`📊 [LOGS-API] Limit: ${limit}, Skip: ${skip}`);

    // Import CallLog model
    const CallLog = require("./models/CallLog");

    // Execute query with pagination
    const [logs, totalCount, activeCount] = await Promise.all([
      CallLog.find(filters)
        .sort(sort)
        .limit(parseInt(limit))
        .skip(skip)
        .lean()
        .exec(),
      
      CallLog.countDocuments(filters),
      
      CallLog.countDocuments({ 
        ...filters, 
        'metadata.isActive': true 
      })
    ]);

    // Get unique clients for filter options
    const clientIds = await CallLog.distinct('clientId', {});

    // Response with logs and metadata
    const response = {
      logs,
      pagination: {
        total: totalCount,
        page: parseInt(page),
        limit: parseInt(limit),
        pages: Math.ceil(totalCount / parseInt(limit)),
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
      }
    };

    console.log(`📊 [LOGS-API] Returning ${logs.length} logs (${totalCount} total, ${activeCount} active)`);

    res.json(response);

  } catch (error) {
    console.error("❌ [LOGS-API] Error fetching logs:", error.message);
    res.status(500).json({
      error: "Failed to fetch logs",
      message: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// Get specific call log by ID
app.get("/api/logs/:id", async (req, res) => {
  try {
    const { id } = req.params;
    
    const CallLog = require("./models/CallLog");
    const log = await CallLog.findById(id).lean();
    
    if (!log) {
      return res.status(404).json({
        error: "Call log not found",
        id: id,
        timestamp: new Date().toISOString(),
      });
    }

    console.log(`📊 [LOGS-API] Retrieved log: ${id}`);
    
    res.json({
      log,
      timestamp: new Date().toISOString(),
    });

  } catch (error) {
    console.error("❌ [LOGS-API] Error fetching log:", error.message);
    res.status(500).json({
      error: "Failed to fetch log",
      message: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// Get live statistics
app.get("/api/logs/stats", async (req, res) => {
  try {
    const CallLog = require("./models/CallLog");
    
    const [
      totalCalls,
      activeCalls,
      todaysCalls,
      statusBreakdown,
      clientBreakdown
    ] = await Promise.all([
      CallLog.countDocuments(),
      CallLog.countDocuments({ 'metadata.isActive': true }),
      CallLog.countDocuments({
        createdAt: {
          $gte: new Date(new Date().setHours(0, 0, 0, 0))
        }
      }),
      CallLog.aggregate([
        {
          $group: {
            _id: "$leadStatus",
            count: { $sum: 1 }
          }
        }
      ]),
      CallLog.aggregate([
        {
          $group: {
            _id: "$clientId",
            count: { $sum: 1 },
            activeCalls: {
              $sum: {
                $cond: ["$metadata.isActive", 1, 0]
              }
            }
          }
        },
        { $sort: { count: -1 } },
        { $limit: 10 }
      ])
    ]);

    const stats = {
      overview: {
        total: totalCalls,
        active: activeCalls,
        today: todaysCalls,
        timestamp: new Date().toISOString(),
      },
      statusBreakdown: statusBreakdown.reduce((acc, item) => {
        acc[item._id] = item.count;
        return acc;
      }, {}),
      topClients: clientBreakdown,
      server: {
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        activeConnections,
        totalConnections,
      }
    };

    console.log(`📊 [LOGS-STATS] Generated stats - Total: ${totalCalls}, Active: ${activeCalls}`);
    
    res.json(stats);

  } catch (error) {
    console.error("❌ [LOGS-STATS] Error generating stats:", error.message);
    res.status(500).json({
      error: "Failed to generate statistics",
      message: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// Clean up stale active calls (utility endpoint)
app.post("/api/logs/cleanup", async (req, res) => {
  try {
    const CallLog = require("./models/CallLog");
    const result = await CallLog.cleanupStaleActiveCalls();
    
    console.log(`🧹 [LOGS-CLEANUP] Cleaned up ${result.modifiedCount} stale active calls`);
    
    res.json({
      message: "Cleanup completed",
      modifiedCount: result.modifiedCount,
      timestamp: new Date().toISOString(),
    });

  } catch (error) {
    console.error("❌ [LOGS-CLEANUP] Error during cleanup:", error.message);
    res.status(500).json({
      error: "Failed to cleanup stale calls",
      message: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

console.log("📊 [SERVER] Live logs API routes registered:");
console.log("📊 [SERVER] GET /api/logs - Get call logs with filtering");
console.log("📊 [SERVER] GET /api/logs/:id - Get specific call log");
console.log("📊 [SERVER] GET /api/logs/stats - Get live statistics");
console.log("📊 [SERVER] POST /api/logs/cleanup - Cleanup stale active calls");


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

startServer();

// Export server for testing purposes
module.exports = { app, server, wss };