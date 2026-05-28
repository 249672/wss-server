const { WebSocketServer } = require('ws');
const http = require('http');

// Render sets the execution port dynamically via environment variables
const port = process.env.PORT || 8080; 

// 1. HTTP Server to handle Render Health Monitoring Checks
const server = http.createServer((req, res) => {
    if (req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Genesys AudioHook Protocol Server: ONLINE & SECURE');
    } else {
        res.writeHead(404);
        res.end();
    }
});

// 2. Attach WebSocket Listener Engine
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
    console.log('[Handshake] Genesys socket connection channel established.');

    ws.on('message', (message, isBinary) => {
        // Handle raw audio transport bytes safely
        if (isBinary) {
            // Raw PCMU binary packets stream here during an active call session
            return; 
        }

        try {
            const request = JSON.parse(message.toString());
            console.log(`[Protocol Incoming] Event Type Received: ${request.type}`);

            // STEP A: COMPLIANT OPEN PROBE HANDSHAKE
            if (request.type === 'open') {
                const response = {
                    version: request.version,
                    type: 'opened',
                    seq: 1,
                    clientSeq: request.seq,    // Exact attribute name matching standard spec rules
                    clientseq: request.seq,    // Fallback redundancy matching legacy forum targets
                    id: request.id,
                    status: 200,               // Standard execution status check code
                    parameters: {
                        startPaused: false,
                        media: [
                            {
                                type: 'audio',
                                format: 'PCMU',
                                channels: ['external'],
                                rate: 8000
                            }
                        ]
                    }
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake OK] Sent "opened" frame payload verification for ID: ${request.id}`);
            } 
            
            // STEP B: COMPLIANT PROBE CLOSE ACKNOWLEDGMENT
            else if (request.type === 'close') {
                const response = {
                    version: request.version,
                    type: 'closed',
                    seq: request.seq + 1,
                    clientSeq: request.seq,
                    clientseq: request.seq,
                    id: request.id
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake Ended] Sent "closed" frame response context for ID: ${request.id}`);
                ws.close(1000); // Trigger clean infrastructure websocket disconnect state
            }

            // STEP C: KEEPALIVE LIFELINE PROBE
            else if (request.type === 'ping') {
                const response = {
                    version: request.version,
                    type: 'pong',
                    seq: request.seq,
                    clientSeq: request.seq,
                    clientseq: request.seq,
                    id: request.id
                };
                ws.send(JSON.stringify(response));
            }

        } catch (err) {
            console.error('[Structural Error] Malformed parsing dropped:', err.message);
        }
    });

    ws.on('error', (error) => {
        console.error('[Connection Error Details]:', error.message);
    });

    ws.on('close', (code, reason) => {
        console.log(`[Disconnected] Connection state closed. Code: ${code}`);
    });
});

// 3. Launch App Event Loop Listener
server.listen(port, () => {
    console.log(`Application successfully listening for incoming traffic on port ${port}`);
});
