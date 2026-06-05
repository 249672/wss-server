const { WebSocketServer } = require('ws'); 
const http = require('http'); 
// 1. ADDED: Import AWS Transcribe Streaming Client 
const { TranscribeStreamingClient, StartStreamTranscriptionCommand } = require('@aws-sdk/client-transcribe-streaming'); 

// Render sets the web environment port dynamically via process.env.PORT 
const port = process.env.PORT || 8080; 

// 2. ADDED: Initialize AWS Client (uses Render Environment Variables) 
const transcribeClient = new TranscribeStreamingClient({ region: process.env.AWS_REGION || 'us-east-1' }); 

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
    let fullTranscript = ""; // Variable to store the full text for future operations

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
            const command = new StartStreamTranscriptionCommand({ 
                IdentifyMultipleLanguages: true,
                LanguageOptions: "en-US,es-US",
                MediaSampleRateHertz: 8000, 
                MediaEncoding: 'pcm', // For raw audio pipelines 
                AudioStream: audioStreamGenerator(),
                EnablePartialResultsStabilization: true, 
                PartialResultsStability: "high"
            }); 
            
            console.log("=== Initializing AWS Transcribe Stream... ==="); 
            const response = await transcribeClient.send(command); 
            console.log("=== AWS Transcribe Session Active ==="); 
            
            for await (const event of response.TranscriptResultStream) { 
                if (event.TranscriptEvent?.Transcript?.Results) { 
                    event.TranscriptEvent.Transcript.Results.forEach(result => { 
                        if (!result.IsPartial) { 
                            const transcript = result.Alternatives[0].Transcript;
                            fullTranscript += transcript + " "; // Add to downstream string
                            // Only log finalized text with its identified language
                            console.log(`📝 [Transcription - ${result.LanguageCode}]: ${transcript}`); 
                        } 
                    }); 
                } 
            } 
        } catch (err) { 
            console.error('❌ AWS Transcribe Error:', err.message); 
            isTranscribing = false; 
        } 
    } 

    ws.on('message', (message, isBinary) => { 
        // FIX: STRICT STREAM PROTECTION 
        // Force fully intercepts any raw binary Buffers or objects even if the isBinary flag is missing 
        if (isBinary || Buffer.isBuffer(message) || typeof message !== 'string') { 
            // 4. ADDED: Directly capture your accepted packet and push it to AWS queue 
            audioQueue.push(Buffer.from(message)); 
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
                
                // 5. ADDED: Trigger the translation pipeline right when connection opens 
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
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2)); 
                ws.send(JSON.stringify(response)); 
                console.log(`\u23F8\uFE0F [Session Paused] Call state changed to paused for ID: ${request.id}`); 
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
                console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2)); 
                ws.send(JSON.stringify(response)); 
                console.log(`\u25B6\uFE0F [Session Resumed] Call state changed to streaming for ID: ${request.id}`); 
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
        // 6. ADDED: Reset states on disconnection 
        audioQueue = []; 
        isTranscribing = false; 
        fullTranscript = "";
    }); 
}); 

server.listen(port, () => { 
    console.log(`Application successfully listening for incoming traffic on port ${port}`); 
});
