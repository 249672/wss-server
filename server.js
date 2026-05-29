const { WebSocketServer } = require('ws');
const http = require('http');

// Render sets the environment port dynamically 
const port = process.env.PORT || 8080; 

// 1. HTTP Infrastructure Layer to serve Render Router Health Check Probes
const server = http.createServer((req, res) => {
    if (req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Genesys Cloud AudioHook Gateway Active');
    } else {
        res.writeHead(404);
        res.end();
    }
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
    console.log('[Handshake] Genesys socket connection channel established.');

    ws.on('message', (message, isBinary) => {
        // Handle streaming media blocks (Audio bytes stream here during active call sessions)
        if (isBinary) {
            return; 
        }

        try {
            const request = JSON.parse(message.toString());
            console.log(`[Protocol Incoming] Event Type Received: ${request.type}`);

            // STEP A: OFFICIAL COMPLIANT OPEN HANDSHAKE MATRIX
            if (request.type === 'open') {
                const response = {
                    version: request.version,
                    type: 'opened',
                    seq: 1,                    // SPEC REQ: Server initialization tracker begins at 1
                    clientSeq: request.seq,    // SPEC REQ: Directly references the request seq value
                    id: request.id,
                    status: 200,               // SPEC REQ: Enforces status handshake execution boundaries
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
            
            // STEP B: OFFICIAL COMPLIANT PROBE CLOSE HANDSHAKE
            else if (request.type === 'close') {
                const response = {
                    version: request.version,
                    type: 'closed',
                    seq: 2,                    // SPEC REQ: Server close transaction incremented sequence tracking
                    clientSeq: request.seq,    // References the incoming close seq property
                    id: request.id
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake Ended] Sent "closed" frame response context for ID: ${request.id}`);
                ws.close(1000); 
            }

            // STEP C: KEEPALIVE LIFELINE ALIGNMENT
            else if (request.type === 'ping') {
                const response = {
                    version: request.version,
                    type: 'pong',
                    seq: request.seq,
                    clientSeq: request.seq,
                    id: request.id
                };
                ws.send(JSON.stringify(response));
            }

        } catch (err) {
            console.error('[Structural Error] Malformed JSON structure dropped:', err.message);
        }
    });

    ws.on('error', (error) => {
        console.error('[Connection Error Details]:', error.message);
    });

    ws.on('close', (code, reason) => {
        console.log(`[Disconnected] Connection state closed. Code: ${code}`);
    });
});

server.listen(port, () => {
    console.log(`Application successfully listening for incoming traffic on port ${port}`);
});
