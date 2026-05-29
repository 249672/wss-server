const { WebSocketServer } = require('ws');
const http = require('http');

// Render sets the web environment port dynamically via process.env.PORT
const port = process.env.PORT || 8080; 

// 1. Maintain the Render Infrastructure Web Router Health Check
const server = http.createServer((req, res) => {
    if (req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Genesys Cloud AudioHook Service Online');
    } else {
        res.writeHead(404);
        res.end();
    }
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
    console.log('[Handshake] Genesys socket connection channel established.');

    ws.on('message', (message, isBinary) => {
        // Skip processing streaming binary voice data chunks
        if (isBinary) {
            return; 
        }

        try {
            const request = JSON.parse(message.toString());
            console.log("Full Genesys Request Payload:", JSON.stringify(request, null, 2));

            // STEP A: MATCH SCRIPT EXACTLY TO THE CHOSEN "OPEN" 
            if (request.type === 'open') {
                const response = {
                    version: request.version,
                    type: 'opened',
                    seq: 1,
                    clientseq: request.seq, 
                    id: request.id,
                    parameters: {
                        startPaused: false, 
                        media: [
                            {
                                type: 'audio',
                                format: 'PCMU',
                                channels: ['external', 'internal'], 
                                rate: 8000
                            }
                        ]
                    }
                };
                
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2));
                ws.send(JSON.stringify(response));
                console.log(`[Handshake OK]  ID: ${request.id}`);
            } 
            
            // STEP B: SPECS CLOSE SESSION CLEANUP HANDSHAKE
            else if (request.type === 'close') {
                const response = {
                    version: request.version,
                    type: 'closed',
                    seq: 2,
                    clientseq: request.seq,
                    id: request.id
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake Ended] Sent close acknowledgement for ID: ${request.id}`);
                ws.close(1000); 
            }

            // STEP C: KEEPALIVE INFRASTRUCTURE LIFELINE
            else if (request.type === 'ping') {
                const response = {
                    version: request.version,
                    type: 'pong',
                    seq: request.seq,
                    clientseq: request.seq,
                    id: request.id
                };
                ws.send(JSON.stringify(response));
            }

        } catch (err) {
            console.error('[Structural Error] schema violation caught:', err.message);
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
