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
    
    let serverSeqCounter = 0;

    ws.on('message', (message, isBinary) => {
        // STRICT ENFORCEMENT: If it is binary data OR a raw Buffer instance, block it from the JSON parser
        if (isBinary || Buffer.isBuffer(message) || typeof message !== 'string') {
            console.log(`🎙️ [Streaming Media] Receiving raw audio chunk: ${message.length} bytes`);
            // Raw PCMU μ-law bytes land here safely every 20ms. Do NOT call ws.send() inside this block.
            return;
        }

        // Only process text payloads below this line
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
                        media: [
                            { type: 'audio', format: 'PCMU', channels: ['external', 'internal'], rate: 8000 }
                        ]
                    }
                };
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2));
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
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2));
                ws.send(JSON.stringify(response));
                console.log(`\u23F8\uFE0F [Session Paused] ID: ${request.id}`);
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
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2));
                ws.send(JSON.stringify(response));
                console.log(`\u25B6\uFE0F [Session Resumed] ID: ${request.id}`);
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
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2));
                ws.send(JSON.stringify(response));
                console.log(`[Handshake Ended] Sent close acknowledgement for ID: ${request.id}`);
                
                setImmediate(() => {
                    ws.close(1000);
                });
            } 
            else if (request.type === 'ping') {
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
            // NEVER send error strings back to Genesys inside the catch block. Just log it locally.
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
