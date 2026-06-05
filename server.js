const { WebSocketServer } = require('ws'); 
const http = require('http'); 
const { TranscribeStreamingClient, StartStreamTranscriptionCommand } = require('@aws-sdk/client-transcribe-streaming'); 

const port = process.env.PORT || 8080; 

// Initialize AWS Client
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
    let serverSeq = 1;

    // Continuous pipe generator for AWS Transcribe
    async function* audioStreamGenerator() { 
        while (ws.readyState === ws.OPEN || audioQueue.length > 0) { 
            if (audioQueue.length > 0) { 
                const chunk = audioQueue.shift(); 
                yield { AudioEvent: { AudioChunk: chunk } }; 
            } else { 
                // Keep the pipe hot and open
                await new Promise(resolve => setImmediate(resolve)); 
            } 
        } 
    } 

    async function startAwsTranscription() { 
        if (isTranscribing) return; 
        isTranscribing = true; 
        
        try { 
            console.log("=== Initializing AWS Transcribe Stream Instantly ==="); 
            
            const command = new StartStreamTranscriptionCommand({ 
                LanguageCode: 'en-US', 
                MediaSampleRateHertz: 8000, 
                MediaEncoding: 'g711-ulaw', // FIX: Changed 'g711-mu' to 'g711-ulaw' to pass AWS validation
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
                                // Safely print out the final text
                                console.log(`📝 [Transcription]: ${alternatives[0].Transcript}`); 
                            } 
                        } 
                    }); 
                } 
            } 
        } catch (err) { 
            console.error('❌ AWS Transcribe Error:', err.message); 
            isTranscribing = false; 
        } 
    } 

    // FORCE INSTANT START: Do not wait for the text frame to execute AWS
    startAwsTranscription();

    ws.on('message', (message, isBinary) => { 
        // 1. Process Binary Audio Frames Immediately
        if (isBinary || Buffer.isBuffer(message)) { 
            audioQueue.push(Buffer.from(message)); 
            return; 
        } 
        
        // 2. Process Text Frame Metadata
        try { 
            const cleanText = message.toString().trim(); 
            const request = JSON.parse(cleanText); 
            console.log(`📨 Received Text Frame: ${request.type}`);
            
            if (request.type === 'open') { 
                const response = { 
                    version: request.version || "2", 
                    type: 'opened', 
                    seq: serverSeq++, 
                    clientseq: request.seq, 
                    id: request.id, 
                    parameters: { 
                        startPaused: false, 
                        media: [ 
                            { type: 'audio', format: 'PCMU', channels: ['external'], rate: 8000 } 
                        ] 
                    } 
                }; 
                ws.send(JSON.stringify(response)); 
                console.log(`[Handshake OK] Sent 'opened' acknowledgement response to Genesys.`); 
            } 
            else if (request.type === 'paused') { 
                ws.send(JSON.stringify({ version: request.version, type: 'paused', seq: serverSeq++, clientseq: request.seq, id: request.id })); 
            } 
            else if (request.type === 'resumed') { 
                ws.send(JSON.stringify({ version: request.version, type: 'resumed', seq: serverSeq++, clientseq: request.seq, id: request.id })); 
            } 
            else if (request.type === 'close') { 
                ws.send(JSON.stringify({ version: request.version, type: 'closed', seq: serverSeq++, clientseq: request.seq, id: request.id, parameters: {} })); 
                setImmediate(() => ws.close(1000)); 
            } 
            else if (request.type === 'ping') { 
                ws.send(JSON.stringify({ version: request.version, type: 'pong', seq: serverSeq++, clientseq: request.seq, id: request.id })); 
            } 
        } catch (err) { 
            // Silently absorb unparsed fragments so they don't crash the server connection
            console.error('⚠️ Non-JSON or malformed text payload skipped:', err.message); 
        } 
    }); 

    ws.on('error', (error) => { 
        console.error('[Connection Error]:', error.message); 
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
