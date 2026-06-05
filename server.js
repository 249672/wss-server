const { WebSocketServer } = require('ws'); 
const http = require('http'); 
// 1. ADDED: Import AWS Transcribe Streaming Client 
const { TranscribeStreamingClient, StartStreamTranscriptionCommand } = require('@aws-sdk/client-transcribe-streaming'); 

// Render sets the web environment port dynamically via process.env.PORT 
const port = process.env.PORT || 8080; 

// 2. ADDED: Initialize AWS Client (uses Render Environment Variables) 
const transcribeClient = new TranscribeStreamingClient({ region: process.env.AWS_REGION || 'us-east-1' }); 

// --- Real-time Mu-Law (PCMU) to Linear 16 PCM conversion table lookup ---
const MU_LAW_TO_PCM = new Int16Array(256);
for (let i = 0; i < 256; i++) {
    let sign = (i & 0x80) ? -1 : 1;
    let mask = ~i;
    let exponent = (mask >> 4) & 0x07;
    let leadingDigit = (mask & 0x0F) + 33;
    let value = (leadingDigit << (exponent + 1)) - 33;
    MU_LAW_TO_PCM[i] = sign * value << 2; 
}

function decodeMuLawToPCM(muLawBuffer) {
    const pcmBuffer = Buffer.alloc(muLawBuffer.length * 2);
    for (let i = 0; i < muLawBuffer.length; i++) {
        const sample = MU_LAW_TO_PCM[muLawBuffer[i]];
        pcmBuffer.writeInt16LE(sample, i * 2);
    }
    return pcmBuffer;
}
// ------------------------------------------------------------------------

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
    
    // 3. ADDED: Manage audio pipeline streams per socket connection 
    let audioQueue = []; 
    let isTranscribing = false; 

    // Async generator function to pipe chunks into AWS SDK safely 
    async function* audioStreamGenerator() { 
        while (ws.readyState === ws.OPEN || audioQueue.length > 0) { 
            if (audioQueue.length > 0) { 
                const chunk = audioQueue.shift(); 
                yield { AudioEvent: { AudioChunk: chunk } }; 
            } else { 
                // Wait 20ms before checking for new audio packets to prevent loop blocking 
                await new Promise(resolve => setTimeout(resolve, 20)); 
            } 
        } 
    } 

    // Start AWS transcription session 
    async function startAwsTranscription() { 
        if (isTranscribing) return; 
        isTranscribing = true; 
        
        try { 
            console.log("=== Initializing AWS Transcribe Stream... ==="); 
            const command = new StartStreamTranscriptionCommand({ 
                LanguageCode: 'en-US', 
                MediaSampleRateHertz: 8000, 
                MediaEncoding: 'pcm', // For raw audio pipelines 
                AudioStream: audioStreamGenerator() 
            }); 
            
            const response = await transcribeClient.send(command); 
            console.log("=== AWS Transcribe Session Active ==="); 
            
            for await (const event of response.TranscriptResultStream) { 
                if (event.TranscriptEvent?.Transcript?.Results) { 
                    event.TranscriptEvent.Transcript.Results.forEach(result => { 
                        if (!result.IsPartial) { 
                            const alternatives = result.Alternatives;
                            if (alternatives && alternatives.length > 0) {
                                console.log(`📝 [Transcription]: ${alternatives.Transcript}`); 
                            }
                        } 
                    }); 
                } 
            } 
        } catch (err) { 
            console.error('❌ AWS Transcribe Error Loop Caught:', err); 
            isTranscribing = false; 
        } 
    } 

    ws.on('message', (message, isBinary) => { 
        // FIX: STRICT STREAM PROTECTION 
        if (isBinary || Buffer.isBuffer(message) || typeof message !== 'string') { 
            console.log(`🎙️ [Streaming Media] Receiving raw audio chunk: ${message.length} bytes`); 
            
            const rawMuLaw = Buffer.from(message);
            const linearPCM = decodeMuLawToPCM(rawMuLaw);
            audioQueue.push(linearPCM); 
            return; 
        } 
        
        try { 
            const cleanText = message.toString().trim(); 
            
            // FIX: Use an explicit string lookup to avoid missing the JSON frame due to hidden leading characters
            if (!cleanText.includes('"type"') && !cleanText.includes('{')) {
                return; 
            }
            
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
                
                // Trigger the transcription pipeline right when connection opens 
                startAwsTranscription(); 
            } 
            // STEP A-2: SPECS PAUSED HANDSHAKE 
            else if (request.type === 'paused') { 
                const response = { 
                    version: request.version, 
                    type: 'paused', 
                    seq: (request.serverseq || 0) + 1, 
                    clientseq: request.seq, 
                    id: request.id 
                }; 
                ws.send(JSON.stringify(response)); 
            } 
            // STEP A-3: SPECS RESUMED HANDSHAKE 
            else if (request.type === 'resumed') { 
                const response = { 
                    version: request.version, 
                    type: 'resumed', 
                    seq: (request.serverseq || 0) + 1, 
                    clientseq: request.seq, 
                    id: request.id 
                }; 
                ws.send(JSON.stringify(response)); 
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
                ws.send(JSON.stringify(response)); 
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
        audioQueue = []; 
        isTranscribing = false; 
    }); 
}); 

server.listen(port, () => { 
    console.log(`Application successfully listening for incoming traffic on port ${port}`); 
});
