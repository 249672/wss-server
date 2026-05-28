const { WebSocketServer } = require('ws');
const http = require('http');

const port = process.env.PORT || 8080; 

// 1. Maintain Render Web Server Health Checks
const server = http.createServer((req, res) => {
    if (req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('AudioHook Server Online');
    } else {
        res.writeHead(404);
        res.end();
    }
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
    console.log('Genesys AudioHook connection initiated.');

    ws.on('message', (message, isBinary) => {
        // 2. Handle Binary Streaming Audio Data from Genesys
        if (isBinary) {
            // This is the raw voice packet (PCMU/L16 format)
            // You can stream this to an AI, transcription engine, or recorder here
            return; 
        }

        // 3. Handle Structured JSON Control Messages
        try {
            const request = JSON.parse(message.toString());
            console.log(`Received Genesys Event: ${request.type}`);

            // Genesys Handshake Phase 1: OPEN Request
            if (request.type === 'open') {
                const response = {
                    version: request.version,
                    type: 'opened', // Required acknowledgment string
                    seq: 1,
                    clientSeq: request.seq,
                    id: request.id,
                    parameters: {
                        media: [
                            {
                                type: 'audio',
                                format: 'PCMU', // 8kHz mu-law audio format
                                channels: ['external'],
                                rate: 8000
                            }
                        ]
                    }
                };
                ws.send(JSON.stringify(response));
                console.log('Handshake successful: Sent "opened" frame.');
            } 
            
            // Genesys Handshake Phase 2: CLOSE Request
            else if (request.type === 'close') {
                const response = {
                    version: request.version,
                    type: 'closed', // Required close acknowledgment string
                    seq: request.seq + 1,
                    clientSeq: request.seq,
                    id: request.id
                };
                ws.send(JSON.stringify(response));
                console.log('Session ended: Sent "closed" frame.');
                ws.close();
            }

            // Genesys Handshake Phase 3: Heartbeat PING
            else if (request.type === 'ping') {
                const response = {
                    version: request.version,
                    type: 'pong', // Prevents 'Close transaction timed out'
                    seq: request.seq + 1,
                    clientSeq: request.seq,
                    id: request.id
                };
                ws.send(JSON.stringify(response));
            }

        } catch (err) {
            console.error('Failed to parse Genesys frame:', err.message);
        }
    });
});

server.listen(port, () => {
    console.log(`AudioHook protocol listener live on port ${port}`);
});
