const { WebSocketServer } = require('ws');
const http = require('http');

const port = process.env.PORT || 8080;

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
    
    // Dynamically track the unique outbound sequence counter per connection
    let serverSeqCounter = 0;

    ws.on('message', (message, isBinary) => {
        if (isBinary) {
            // 1. DE-INTERLEAVE THE AUDIO CHANNELS
            const customerBuffer = Buffer.alloc(message.length / 2);
            const agentBuffer = Buffer.alloc(message.length / 2);
            let c = 0, a = 0;

            for (let i = 0; i < message.length; i += 2) {
                customerBuffer[c++] = message[i];     // Customer Audio
                agentBuffer[a++] = message[i + 1];    // Agent Audio
            }

            // 2. PROCESS CHUNKS HERE 
            // Send customerBuffer and agentBuffer to your downstream transcription/AI API
            return;
        }

        try {
            const request = JSON.parse(message.toString());
            console.log("Full Genesys Request Payload:", JSON.stringify(request, null, 2));

            if (request.type === 'open') {
                serverSeqCounter = 1;
                const response = {
                    version: request.version,
                    type: 'opened',
                    seq: serverSeqCounter,
                    clientseq: request.seq,
                    id: request.id,
                    parameters: {
                        startPaused: false,
                        media: [{ type: 'audio', format: 'PCMU', channels: ['external', 'internal'], rate: 8000 }]
                    }
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake OK] ID: ${request.id}`);
            } 
            else if (request.type === 'paused') {
                serverSeqCounter++;
                const response = {
                    version: request.version,
                    type: 'paused',
                    seq: serverSeqCounter,
                    clientseq: request.seq,
                    id: request.id
                };
                ws.send(JSON.stringify(response));
                console.log(`⏸️ [Session Paused] ID: ${request.id}`);
            } 
            else if (request.type === 'resumed') {
                serverSeqCounter++;
                const response = {
                    version: request.version,
                    type: 'resumed',
                    seq: serverSeqCounter,
                    clientseq: request.seq,
                    id: request.id
                };
                ws.send(JSON.stringify(response));
                console.log(`▶️ [Session Resumed] ID: ${request.id}`);
            } 
            else if (request.type === 'close') {
                serverSeqCounter++;
                const response = {
                    version: request.version,
                    type: 'closed',
                    seq: serverSeqCounter,
                    clientseq: request.seq,
                    id: request.id,
                    parameters: {}
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake Ended] Sent close acknowledgement for ID: ${request.id}`);
                setImmediate(() => ws.close(1000));
            } 
            else if (request.type === 'ping') {
                // Pings do not advance the server sequence counter in standard protocol responses
                const response = {
                    version: request.version,
                    type: 'pong',
                    seq: serverSeqCounter, 
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

    ws.on('close', (code) => {
        console.log(`[Disconnected] Connection state closed. Code: ${code}`);
    });
});

server.listen(port, () => {
    console.log(`Application successfully listening for incoming traffic on port ${port}`);
});
