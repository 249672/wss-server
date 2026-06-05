const { WebSocketServer } = require('ws'); 
const http = require('http'); 
const { TranscribeStreamingClient, StartStreamTranscriptionCommand } = require('@aws-sdk/client-transcribe-streaming'); 

const port = process.env.PORT || 8080; 

// Initialize AWS Client - Make sure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are in Render Envs!
const transcribeClient = new TranscribeStreamingClient({ 
    region: process.env.AWS_REGION || 'us-east-1' 
}); 

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
    
    let audioQueue = []; 
    let isTranscribing = false; 

    // Optimized Generator: Immediate resolution to prevent AWS stream starvation
    async function* audioStreamGenerator() { 
        while (ws.readyState === ws.OPEN || audioQueue.length > 0) { 
            if (audioQueue.length > 0) { 
                const chunk = audioQueue.shift(); 
                yield { AudioEvent: { AudioChunk: chunk } }; 
            } else { 
                // Reduced timeout to keep the pipeline hot and responsive
                await new Promise(resolve => setImmediate(resolve)); 
            } 
        } 
    } 

    async function startAwsTranscription() { 
        if (isTranscribing) return; 
        isTranscribing = true; 
        
        try { 
            console.log("=== Initializing AWS Transcribe Stream... ==="); 
            
            const command = new StartStreamTranscriptionCommand({ 
                LanguageCode: 'en-US', 
                MediaSampleRateHertz: 8000, 
                MediaEncoding: 'g711-mu', // Correct encoding for Genesys PCMU
                AudioStream: audioStreamGenerator() 
            }); 
            
            const response = await transcribeClient.send(command); 
            console.log("=== AWS Transcribe Session Active ==="); 
            
            for await (const event of response.TranscriptResultStream) { 
                if (event.TranscriptEvent?.Transcript?.Results) { 
                    event.TranscriptEvent.Transcript.Results.forEach(result => { 
                        if (!result.IsPartial) { 
                            // Access the array safely
                            const alternatives = result.Alternatives;
                            if (alternatives && alternatives.length > 0) {
                                console.log(`📝 [Transcription]: ${alternatives[0].Transcript}`); 
                            }
                        } 
                    }); 
                } 
            } 
        } catch (err) { 
            // This will print the exact AWS validation, permission, or region error to Render logs
            console.error('❌ AWS Transcribe Error Loop Caught:', err); 
            isTranscribing = false; 
        } 
    } 

    ws.on('message', (message, isBinary) => { 
        if (isBinary || Buffer.isBuffer(message) || typeof message !== 'string') { 
            // Push incoming chunks instantly
            audioQueue.push(Buffer.from(message)); 
            return; 
        } 
        
        try { 
            const cleanText = message.toString().trim(); 
            if (!cleanText.startsWith('{')) return; 
            
            const request = JSON.parse(cleanText); 
            
            if (request.type === 'open') { 
                console.log("Full Genesys Request Payload:", JSON.stringify(request, null, 2)); 
                
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
                
                ws.send(JSON.stringify(response)); 
                console.log(`[Handshake OK] ID: ${request.id}`); 
                
                // Start AWS pipeline
                startAwsTranscription(); 
            } 
            else if (request.type === 'paused') { 
                const response = { version: request.version, type: 'paused', seq: (request.serverseq || 0) + 1, clientseq: request.seq, id: request.id }; 
                ws.send(JSON.stringify(response)); 
            } 
            else if (request.type === 'resumed') { 
                const response = { version: request.version, type: 'resumed', seq: (request.serverseq || 0) + 1, clientseq: request.seq, id: request.id }; 
                ws.send(JSON.stringify(response)); 
            } 
            else if (request.type === 'close') { 
                const response = { version: request.version, type: 'closed', seq: (request.serverseq || 0) + 1, clientseq: request.seq, id: request.id, parameters: {} }; 
                ws.send(JSON.stringify(response)); 
                setImmediate(() => ws.close(1000)); 
            } 
            else if (request.type === 'ping') { 
                const response = { version: request.version, type: 'pong', seq: (request.serverseq || 0) + 1, clientseq: request.seq, id: request.id }; 
                ws.send(JSON.stringify(response)); 
            } 
        } catch (err) { 
            console.error('[Structural Error]:', err.message); 
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
