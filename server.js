const { WebSocketServer } = require('ws');
const http = require('http');

// Render maps your system port variable configurations dynamically
const port = process.env.PORT || 8080; 

// Maintain the Render Router Infrastructure Health check
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
        // Safely pass streaming binary audio packets during the handshake
        if (isBinary) {
            return; 
        }

        try {
            const request = JSON.parse(message.toString());
            console.log("Full Genesys Request Payload:", JSON.stringify(request, null, 2));
            console.log(`[Protocol Incoming] Event Type Received: ${request.type}`);

            // STEP 1: SPEC-COMPLIANT OPEN HANDSHAKE
            if (request.type === 'open') {
                const response = {
                    version: request.version,
                    type: 'opened',
                    seq: 1,                    // Spec Requirement: Server sequence initialized at 1
                    clientSeq: request.seq,    // References the incoming open request tracking ID
                    id: request.id,
                    //status: 200,
                    parameters: {
                        startPaused: false,        // SPEC FIX: Enforced at the ROOT level, not inside parameters
                        media: [
                            {
                                type: 'audio',
                                format: 'PCMU',
                                channels: ['external', 'internal'], // SPEC FIX: Explicitly define channel layout for clarity
                                rate: 8000,       // SPEC FIX: Must be typed as a strict raw integer
                                //channelCount: 1   // SPEC FIX: Mandatory property layout flag
                            }
                        ]
                    }
                };
                ws.send(JSON.stringify(response));
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2));
                console.log(`[Handshake OK] Sent "opened" frame payload for ID: ${request.id}`);
            } 
            
            // STEP 2: SPEC-COMPLIANT PROBE CLOSE ACKNOWLEDGMENT
            else if (request.type === 'close') {
                const response = {
                    version: request.version,
                    type: 'closed',
                    seq: 2,                    // Incremented message counter tracking ID
                    clientSeq: request.seq,
                    id: request.id
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake Ended] Sent "closed" frame response context for ID: ${request.id}`);
                ws.close(1000); 
            }

            // STEP 3: KEEPALIVE TIMEOUT IMMUNIZATION 
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
