const { WebSocketServer } = require('ws');
const http = require('http');

// Render sets the web environment port dynamically
const port = process.env.PORT || 8080; 

// 1. Maintain the Render Web Routing Infrastructure Health Probe
const server = http.createServer((req, res) => {
    if (req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Genesys AudioHook Production Gateway Online');
    } else {
        res.writeHead(404);
        res.end();
    }
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
    console.log('[Handshake] Genesys socket connection channel established.');

    ws.on('message', (message, isBinary) => {
        // Bypass streaming media blocks (Audio bytes loop here during live call)
        if (isBinary) {
            return; 
        }

        try {
            const request = JSON.parse(message.toString());
            console.log(`[Protocol Incoming] Event Type Received: ${request.type}`);

            // STEP A: SPEC-COMPLIANT OPEN HANDSHAKE
            if (request.type === 'open') {
                const response = {
                    version: request.version,
                    type: 'opened',
                    seq: request.seq + 1,      // SPEC REQ: Must increment incoming seq 
                    clientSeq: request.seq,    // Matches standard platform key casing
                    id: request.id,
                    status: 200,
                    parameters: {
                        startPaused: false,    // SPEC REQ: Tells Genesys to start recording stream instantly
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
            
            // STEP B: SPEC-COMPLIANT CLOSE ACKNOWLEDGMENT
            else if (request.type === 'close') {
                const response = {
                    version: request.version,
                    type: 'closed',
                    seq: request.seq + 1,      // SPEC REQ: Must increment incoming close seq
                    clientSeq: request.seq,
                    id: request.id
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake Ended] Sent "closed" frame response context for ID: ${request.id}`);
                ws.close(1000); 
            }

            // STEP C: KEEPALIVE LIFELINE RESPONSE
            else if (request.type === 'ping') {
                const response = {
                    version: request.version,
                    type: 'pong',
                    seq: request.seq + 1,
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
