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
        // FIX: STRICT STREAM PROTECTION
        // Force fully intercepts any raw binary Buffers or objects even if the isBinary flag is missing
        if (isBinary || Buffer.isBuffer(message) || typeof message !== 'string') { 
            console.log(`🎙️ [Streaming Media] Receiving raw audio chunk: ${message.length} bytes`); 
            // Raw PCMU μ-law bytes land here every 20ms and are ready for transcription or recording 
            return; 
        } 

        try { 
            // Clean up whitespace to ensure precise parsing
            const cleanText = message.toString().trim();
            if (!cleanText.startsWith('{')) return; // Ignore any fragmented text blocks

            const request = JSON.parse(cleanText); 
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
                            { type: 'audio', format: 'PCMU', channels: ['external', 'internal'], rate: 8000 } 
                        ] 
                    } 
                }; 
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2)); 
                ws.send(JSON.stringify(response)); 
                console.log(`[Handshake OK] ID: ${request.id}`); 
            } 
            // STEP A-2: SPECS PAUSED HANDSHAKE (Fixed: Correct sequence index tracking) 
            else if (request.type === 'paused') { 
                const response = { 
                    version: request.version, 
                    type: 'paused', 
                    seq: (request.serverseq || 0) + 1, 
                    clientseq: request.seq, 
                    id: request.id 
                }; 
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2)); 
                ws.send(JSON.stringify(response)); 
                console.log(`⏸️ [Session Paused] Call state changed to paused for ID: ${request.id}`); 
            } 
            // STEP A-3: SPECS RESUMED HANDSHAKE (Fixed: Correct sequence index tracking) 
            else if (request.type === 'resumed') { 
                const response = { 
                    version: request.version, 
                    type: 'resumed', 
                    seq: (request.serverseq || 0) + 1, 
                    clientseq: request.seq, 
                    id: request.id 
                }; 
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2)); 
                ws.send(JSON.stringify(response)); 
                console.log(`▶️ [Session Resumed] Call state changed to streaming for ID: ${request.id}`); 
            } 
            // STEP B: SPECS CLOSE SESSION CLEANUP HANDSHAKE 
            else if (request.type === 'close') { 
                const response = { 
                    version: request.version, 
                    type: 'closed', 
                    seq: (request.serverseq || 0) + 1, 
                    clientseq: request.seq, 
                    id: request.id, 
                    parameters: {} 
                }; 
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2)); 
                ws.send(JSON.stringify(response)); 
                console.log(`[Handshake Ended] Sent close acknowledgement for ID: ${request.id}`); 
                
                // Safely allow the message queue to flush before severing the socket 
                setImmediate(() => { 
                    ws.close(1000); 
                }); 
            } 
            // STEP C: KEEPALIVE INFRASTRUCTURE LIFELINE 
            else if (request.type === 'ping') { 
                const response = { 
                    version: request.version, 
                    type: 'pong', 
                    seq: (request.serverseq || 0) + 1, 
                    clientseq: request.seq, 
                    id: request.id 
                }; 
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2));
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
