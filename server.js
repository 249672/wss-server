const { WebSocketServer } = require('ws');
const http = require('http');

// Render sets the environment port dynamically 
const port = process.env.PORT || 8080; 

// Maintain the Render Router Health Check Probes
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
        if (isBinary) {
            return; // Ignore raw streaming audio bytes during the connection probe
        }

        try {
            const request = JSON.parse(message.toString());
            console.log(`[Protocol Incoming] Event Type Received: ${request.type}`);

            // 1. STRICT COMPLIANT OPEN HANDSHAKE
            if (request.type === 'open') {
                const response = {
                    version: request.version,
                    type: 'opened',
                    seq: 1,                    // Spec: Server's first message tracking starts at 1
                    clientSeq: request.seq,    // Spec: Links directly back to incoming request seq
                    id: request.id,
                    status: 200,
                    // FIX: startPaused must live at the message ROOT level, not inside parameters!
                    startPaused: false,        
                    parameters: {
                        media: [
                            {
                                type: 'audio',
                                format: 'PCMU',
                                channels: ['external'],
                                rate: 8000,
                                channelCount: 1 // FIX: Required by the AudioHook specification matrix
                            }
                        ]
                    }
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake OK] Sent "opened" frame payload for ID: ${request.id}`);
            } 
            
            // 2. STRICT COMPLIANT CLOSE ACKNOWLEDGMENT
            else if (request.type === 'close') {
                const response = {
                    version: request.version,
                    type: 'closed',
                    seq: 2,                    // Spec: Sequential message tracker incremented
                    clientSeq: request.seq,
                    id: request.id
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake Ended] Sent "closed" frame response context for ID: ${request.id}`);
                ws.close(1000); 
            }

            // 3. LIFELINE KEEPALIVE PING/PONG
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

server.listen(port, () => {
    console.log(`Application successfully listening for incoming traffic on port ${port}`);
});
