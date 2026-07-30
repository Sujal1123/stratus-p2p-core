const express = require('express');
const http = require('http'); // Built-in node module to attach our WS server
const { WebSocketServer } = require('ws');
const { v4: uuidv4 } = require('uuid');
const cors = require('cors');
const redis = require('redis');
const { Pool } = require('pg');
const crypto = require('crypto');

// Helper to generate a dynamic, short session access token
function generateSessionToken() {
    return 'stratus-' + crypto.randomBytes(3).toString('hex');
}

// 1. Dynamic PostgreSQL Connection Pool with secure SSL handling overrides
const pgPool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgresql://postgres:@localhost:5432/stratus_p2p',
    ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

// Create tables and seed mock users automatically on startup
(async () => {
    try {
        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS users (
                user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                username VARCHAR(50) NOT NULL,
                api_key VARCHAR(100) UNIQUE NOT NULL
            );
        `);

        await pgPool.query(`
            INSERT INTO users (username, api_key)
            VALUES ('global_tester', 'europe_renter_token_abc123')
            ON CONFLICT (api_key) DO NOTHING;
        `);

        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS compute_jobs (
                job_id VARCHAR(50) PRIMARY KEY,
                assigned_node_id VARCHAR(50) NOT NULL,
                container_image VARCHAR(100) NOT NULL,
                status VARCHAR(20) NOT NULL,
                output_logs TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log('[Control-Plane] All PostgreSQL tables and seed data initialized smoothly.');
    } catch (err) {
        console.error('PostgreSQL initialization failure:', err);
    }
})();

const app = express();
app.use(express.json()); // Essential body parser module config
app.use(cors());

// Dynamic tracking loops
const onlineNodes = new Map();
const activeJobs = new Map();  

// 2. Dynamic Redis Connection Pipeline Configuration
const redisClient = redis.createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379'
});

redisClient.on('error', (err) => console.error('Redis Client Error', err));
(async () => {
    await redisClient.connect();
    console.log('[Control-Plane] Connected securely to Redis.');
})();

// Create an HTTP native server to encapsulate both Express endpoints and WS traffic streams
const server = http.createServer(app);

// 3. Attach WebSocket server onto the SAME unified HTTP server port line
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
    });
});

wss.on('connection', async (ws, req) => {
    const urlParams = new URLSearchParams(req.url.split('?')[1]);
    const nodeToken = urlParams.get('nodeToken');

    let providerId = null;

    // 1. Validate incoming hardware credentials against PostgreSQL with try/catch safety
    try {
        const userCheck = await pgPool.query(
            'SELECT user_id FROM users WHERE api_key = $1', [nodeToken]
        );
        if (userCheck.rows.length > 0) {
            providerId = userCheck.rows[0].user_id;
        }
    } catch (dbErr) {
        console.error('[DB Auth Check Warning]:', dbErr.message);
    }

    // Fallback authentication override for seed API token
    if (!providerId && nodeToken === 'europe_renter_token_abc123') {
        providerId = '00000000-0000-0000-0000-000000000000';
    }

    if (!providerId) {
        console.log(`[Security] Unauthorized connection attempt rejected for token: ${nodeToken}`);
        ws.send(JSON.stringify({ type: 'AUTH_ERROR', message: 'Invalid node credentials.' }));
        ws.close();
        return;
    }

    const nodeId = uuidv4().substring(0, 8);
    
    // 🔑 Generate dynamic session token for this online session
    const activeSessionToken = generateSessionToken();

    console.log(`[Network] Node authorized successfully! Assigned ID: Node-${nodeId} (Active Token: ${activeSessionToken})`);

    // 2. Map the socket connection in memory with default telemetry specs
    onlineNodes.set(`Node-${nodeId}`, {
        ws: ws,
        ownerId: providerId,
        sessionToken: activeSessionToken,
        status: "IDLE",
        telemetry: { cpuLoad: "0%", freeMemory: "100%" },
        specs: { cpu: 1, ram: "16GB" }
    });

    // 3. Immediately seed Redis upon connection (Prevents empty array on GET /api/nodes)
    if (redisClient.isReady) {
        await redisClient.set(`node:status:Node-${nodeId}`, JSON.stringify({
            id: `Node-${nodeId}`,
            specs: { cpu: 1, ram: "16GB" },
            telemetry: { cpuLoad: "0%", freeMemory: "100%" },
            status: "IDLE"
        }), { EX: 12 });
    }

    // Notify ONLY the provider terminal of their private session token
    ws.send(JSON.stringify({
        type: 'SESSION_INITIALIZED',
        token: activeSessionToken,
        nodeId: `Node-${nodeId}`
    }));

    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);
            
            if (data.type === 'HEARTBEAT') {
                const redisKey = `node:status:Node-${nodeId}`;
                
                // Extract real-time telemetry elements safely
                const cpuLoad = data.metrics?.cpuLoad !== undefined ? data.metrics.cpuLoad : 0;
                const freeMem = data.metrics?.freeMem !== undefined ? data.metrics.freeMem : 0;
                const totalMemGB = data.metrics?.totalMemGB || 16;
                
                // Update internal memory reference
                const nodeRef = onlineNodes.get(`Node-${nodeId}`);
                if (nodeRef) {
                    nodeRef.telemetry = { cpuLoad: `${cpuLoad}%`, freeMemory: `${freeMem}%` };
                    nodeRef.specs = { cpu: 1, ram: `${totalMemGB}GB` };
                }

                // Sync telemetry to Redis
                if (redisClient.isReady) {
                    await redisClient.set(redisKey, JSON.stringify({
                        id: `Node-${nodeId}`,
                        specs: { 
                            cpu: 1, 
                            ram: `${totalMemGB}GB` 
                        },
                        telemetry: {
                            cpuLoad: `${cpuLoad}%`,
                            freeMemory: `${freeMem}%`
                        },
                        status: onlineNodes.get(`Node-${nodeId}`)?.status || "IDLE"
                    }), { EX: 12 });
                }
                return;
            }

            if (data.type === 'JOB_FINISHED') {
                console.log(`[Network] Received compilation logs from Node-${nodeId}`);
                const jobResolver = activeJobs.get(data.jobId);
                if (jobResolver) {
                    jobResolver(data.output);
                    activeJobs.delete(data.jobId);
                }
                if (onlineNodes.has(`Node-${nodeId}`)) {
                    onlineNodes.get(`Node-${nodeId}`).status = "IDLE";
                }
                
                // Update table state row to completed
                await pgPool.query(
                    'UPDATE compute_jobs SET status = $1 WHERE job_id = $2',
                    ['COMPLETED', data.jobId]
                );
            }
        } catch (err) {
            console.error('[WS Parse Error]:', err.message);
        }
    });

    ws.on('close', () => {
        console.log(`[Network] Host Node-${nodeId} went offline.`);
        onlineNodes.delete(`Node-${nodeId}`);
        if (redisClient.isReady) {
            redisClient.del(`node:status:Node-${nodeId}`);
        }
    });
});

// --- RENTER HTTP API ENDPOINTS ---

app.get('/api/nodes', async (req, res) => {
    try {
        const nodesList = [];
        const seenNodeIds = new Set();

        // 1. Try reading from Redis first
        try {
            const keys = await redisClient.keys('node:status:*');
            for (const key of keys) {
                const data = await redisClient.get(key);
                if (data) {
                    const parsed = JSON.parse(data);
                    nodesList.push(parsed);
                    seenNodeIds.add(parsed.id);
                }
            }
        } catch (redisErr) {
            console.error('[Redis Read Warning]:', redisErr.message);
        }

        // 2. Fallback to active in-memory Map (onlineNodes) if Redis missed anything
        for (const [nodeId, nodeData] of onlineNodes.entries()) {
            if (!seenNodeIds.has(nodeId)) {
                nodesList.push({
                    id: nodeId,
                    specs: { cpu: 1, ram: "16GB" },
                    telemetry: { cpuLoad: "0%", freeMemory: "100%" },
                    status: nodeData.status || "IDLE"
                });
            }
        }

        res.json(nodesList);
    } catch (err) {
        console.error('[Nodes Route Error]:', err);
        res.status(500).json({ error: "Failed to read network grid map." });
    }
});

// Global Renter Deploy Endpoint
app.post('/api/jobs/deploy', async (req, res) => {
    try {
        const { targetNodeId } = req.body;
        const authHeader = req.headers['authorization'];
        const providedToken = authHeader ? authHeader.replace('Bearer ', '').trim() : '';

        const jobId = uuidv4();
        const targetNode = onlineNodes.get(targetNodeId);

        if (!targetNode || targetNode.status !== "IDLE") {
            return res.status(404).json({ error: "Target node is currently unavailable or went offline." });
        }

        // 🔐 Validate dynamic session access token against private memory map
        if (providedToken !== targetNode.sessionToken) {
            return res.status(401).json({ error: "Invalid or expired Gateway Access Token for this node." });
        }

        const sessionPassword = Math.random().toString(36).substring(2, 10);
        const sshPort = Math.floor(Math.random() * (29999 - 20000 + 1)) + 20000;

        console.log(`[Orchestrator] Routing Alpine SSH Sandbox Job-${jobId} to node: ${targetNodeId}`);
        targetNode.status = "BUSY";

        await pgPool.query(
            'INSERT INTO compute_jobs (job_id, assigned_node_id, container_image, status) VALUES ($1, $2, $3, $4)',
            [jobId, targetNodeId, 'alpine:latest', 'PROVISIONED']
        );

        // Dispatches matching configuration array down the WS pipeline tunnel
        targetNode.ws.send(JSON.stringify({
            type: 'EXECUTE_JOB',
            jobId: jobId,
            image: 'alpine:latest', 
            password: sessionPassword,
            assignedPort: sshPort
        }));

        return res.json({
            jobId: jobId,
            executedBy: targetNodeId,
            status: "PROVISIONED",
            connectionString: `ssh root@127.0.0.1 -p ${sshPort}`,
            password: sessionPassword
        });
        
    } catch (err) {
        console.error("[Route Crash Recovery]:", err);
        return res.status(500).json({ error: "Internal deployment handler framework fault." });
    }
});

app.get('/api/jobs/history', async (req, res) => {
    try {
        const result = await pgPool.query(
            'SELECT job_id, assigned_node_id, container_image, status, created_at FROM compute_jobs ORDER BY created_at DESC LIMIT 10'
        );
        res.json(result.rows);
    } catch (err) {
        console.error("PostgreSQL query error:", err);
        res.status(500).json({ error: "Failed to fetch historical database records." });
    }
});

// Use Render's assigned port variable or fall back to port 3000
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`[Control-Plane] Orchestration Node listening on port ${PORT}`));