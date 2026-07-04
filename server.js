const express = require('express');
const http = require('http'); // Built-in node module to attach our WS server
const { WebSocketServer } = require('ws');
const { v4: uuidv4 } = require('uuid');
const cors = require('cors');
const redis = require('redis');
const { Pool } = require('pg');

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

    // Validate the incoming hardware node credentials against PostgreSQL
    const userCheck = await pgPool.query(
        'SELECT user_id FROM users WHERE api_key = $1', [nodeToken]
    );

    if (userCheck.rows.length === 0) {
        console.log(`[Security] Unauthorized connection attempt rejected.`);
        ws.send(JSON.stringify({ type: 'AUTH_ERROR', message: 'Invalid node credentials.' }));
        ws.close();
        return;
    }

    const providerId = userCheck.rows[0].user_id;
    const nodeId = uuidv4().substring(0, 8);
    console.log(`[Network] Node authorized successfully! Assigned ID: Node-${nodeId} (Tied to User: ${providerId})`);

    // Map the socket connection with permission scope
    onlineNodes.set(`Node-${nodeId}`, {
        ws: ws,
        ownerId: providerId,
        status: "IDLE"
    });

    ws.on('message', async (message) => {
        const data = JSON.parse(message);
        
        if (data.type === 'HEARTBEAT') {
            const redisKey = `node:status:Node-${nodeId}`;
            await redisClient.set(redisKey, JSON.stringify({
                id: `Node-${nodeId}`,
                specs: { cpu: 1, ram: "2GB" }, // Updated RAM status payload profile
                status: onlineNodes.get(`Node-${nodeId}`)?.status || "IDLE"
            }), { EX: 25 });
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
    });

    ws.on('close', () => {
        console.log(`[Network] Host Node-${nodeId} went offline.`);
        onlineNodes.delete(`Node-${nodeId}`);
    });
});

// --- RENTER HTTP API ENDPOINTS ---

app.get('/api/nodes', async (req, res) => {
    try {
        const keys = await redisClient.keys('node:status:*');
        const nodesList = [];
        
        for (const key of keys) {
            const data = await redisClient.get(key);
            if (data) nodesList.push(JSON.parse(data));
        }
        
        res.json(nodesList);
    } catch (err) {
        res.status(500).json({ error: "Failed to read network grid map." });
    }
});

// Global Renter Deploy Endpoint with parameter signature overrides fixed (req, res)
app.post('/api/jobs/deploy', async (req, res) => {
    try {
        // 1. Extract parameters safely out of req.body now that parsing handles it cleanly
        const { targetNodeId } = req.body;

        // 2. Generate clean session deployment tracking IDs
        const jobId = uuidv4();

        // 3. Look up active target element from onlineNodes tracking registry map
        const targetNode = onlineNodes.get(targetNodeId); 
        if (!targetNode || targetNode.status !== "IDLE") {
            return res.status(404).json({ error: "Target node is currently unavailable or went offline" });
        }

        // 4. Generate random secure access variables
        const sessionPassword = Math.random().toString(36).substring(2, 10); 
        const sshPort = Math.floor(Math.random() * (29999 - 20000 + 1)) + 20000; 

        console.log(`[Orchestrator] Routing SSH Sandbox Job-${jobId} to node: ${targetNodeId}`);

        // Update tracking states
        targetNode.status = "BUSY";

        // Log transaction record securely inside your PostgreSQL Ledger database rows
        await pgPool.query(
            'INSERT INTO compute_jobs (job_id, assigned_node_id, container_image, status) VALUES ($1, $2, $3, $4)',
            [jobId, targetNodeId, 'linuxserver/openssh-server:latest', 'PROVISIONED']
        );

        // 5. Dispatch raw instruction over the WebSocket bridge down to client provider binary
        targetNode.ws.send(JSON.stringify({
            type: 'EXECUTE_JOB',
            jobId: jobId,
            image: 'linuxserver/openssh-server:latest', 
            password: sessionPassword,
            assignedPort: sshPort
        }));

        // 6. Return connection keys cleanly back to frontend client state
        return res.json({
            jobId: jobId,
            executedBy: targetNodeId,
            status: "PROVISIONED",
            connectionString: `ssh linuxserver.io@localhost -p ${sshPort}`, 
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