const { WebSocketServer } = require('ws');
const http = require('http');

const port = process.env.PORT || 8080; 

// Maintain the Render Infrastructure Router Health Checks
const server = http.createServer((req, res) => {
    if (req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Genesys Cloud AudioHook Production Gateway Online');
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
            return; // Raw binary audio bytes pass here during the streaming session
        }

        try {
            const request = JSON.parse(message.toString());
            console.log("Full Genesys Request Payload:", JSON.stringify(request, null, 2));

            // STEP 1: COMPLIANT OPEN PROBE RESPONSE
            if (request.type === 'open') {
                const response = {
                    version: request.version,
                    type: 'opened',
                    id: request.id,
                    seq: 1,                    // Spec Requirement: Server index starts at 1
                    clientSeq: request.seq,    // References incoming request seq tracker
                    serverseq: 0,              // CRITICAL PROBE FIX: Must be explicitly passed at root
                    parameters: {
                        media: [
                            {
                                type: 'audio',
                                format: 'PCMU',
                                channels: ['external', 'internal'], // CRITICAL PROBE FIX: Must match incoming spec array
                                rate: 8000
                            }
                        ]
                    }
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake OK] Sent matching response frame layout for ID: ${request.id}`);
            } 
            
            // STEP 2: COMPLIANT CLOSE RESPONSE
            else if (request.type === 'close') {
                const response = {
                    version: request.version,
                    type: 'closed',
                    id: request.id,
                    seq: 2,                    // Increments server response sequence counter
                    clientSeq: request.seq,    
                    serverseq: 0               // Enforces protocol sequence state limits
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake Ended] Sent closed confirmation payload frame for ID: ${request.id}`);
                ws.close(1000); 
            }

            // STEP 3: KEEPALIVE TIMEOUT RESPONSES
            else if (request.type === 'ping') {
                const response = {
                    version: request.version,
                    type: 'pong',
                    id: request.id,
                    seq: request.seq,
                    clientSeq: request.seq,
                    serverseq: 0
                };
                ws.send(JSON.stringify(response));
            }

        } catch (err) {
            console.error('[Structural Error] Parsing exception caught:', err.message);
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
