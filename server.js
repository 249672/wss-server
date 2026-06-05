const { WebSocketServer } = require('ws'); 
const http = require('http'); 
// 1. IMPORT: AWS Transcribe Streaming Client 
const { TranscribeStreamingClient, StartStreamTranscriptionCommand } = require('@aws-sdk/client-transcribe-streaming'); 

// Render sets the web environment port dynamically via process.env.PORT 
const port = process.env.PORT || 8080; 

// 2. INITIALIZE: AWS Client 
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

function decodeAndMixDualChannelToMonoPCM(muLawBuffer) {
    const pcmBuffer = Buffer.alloc(muLawBuffer.length);
    let outputSampleIndex = 0;

    for (let i = 0; i < muLawBuffer.length - 1; i += 2) {
        const sample1 = MU_LAW_TO_PCM[muLawBuffer[i]];
        const sample2 = MU_LAW_TO_PCM[muLawBuffer[i + 1]];
        
        const s1 = isNaN(sample1) ? 0 : sample1;
        const s2 = isNaN(sample2) ? 0 : sample2;

        const mixedMonoSample = Math.floor((s1 + s2) / 2);
        
        pcmBuffer.writeInt16LE(mixedMonoSample, outputSampleIndex * 2);
        outputSampleIndex++;
    }
    return pcmBuffer.subarray(0, outputSampleIndex * 2);
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
    
    // 3. MANAGE: Audio pipeline streams per socket connection 
    let audioQueue = []; 
    let isTranscribing = false; 
    let serverSeq = 0; 

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
                MediaEncoding: 'pcm', 
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
                                console.log(`📝 [Transcription]: ${alternatives[0].Transcript}`); 
                            } 
                        } 
                    }); 
                } 
            } 
        } catch (err) { 
            console.error('❌ AWS Transcribe Error Loop Caught:', err.message); 
            isTranscribing = false; 
        } 
    } 

    ws.on('message', (message, isBinary) => { 
        // FIX: Strict type-guard filtering
        // If the websocket layer flags it as binary or it's a raw Buffer block, route straight to audio router
        if (isBinary || Buffer.isBuffer(message)) {
            const rawMuLaw = Buffer.from(message); 
            const monoPCM = decodeAndMixDualChannelToMonoPCM(rawMuLaw); 
            audioQueue.push(monoPCM); 
            return;
        }

        const textCheck = message.toString().trim(); 
        
        // Ensure the string payload is valid JSON before attempting to parse it
        if (textCheck.startsWith('{') && textCheck.endsWith('}')) { 
            try { 
                const request = JSON.parse(textCheck); 
                console.log(`📨 Received Valid Handshake Frame Type: "${request.type}"`); 
                
                // STEP A: MATCH SCRIPT EXACTLY TO THE CHOSEN "OPEN" 
                if (request.type === 'open') { 
                    const response = { 
                        version: request.version, 
                        type: 'opened', 
                        seq: serverSeq++, 
                        clientseq: request.seq, 
                        id: request.id, 
                        parameters: request.parameters || {} 
                    }; 
                    
                    console.log("Full Genesys Response Payload:", JSON.stringify(response, null, 2)); 
                    ws.send(JSON.stringify(response)); 
                    console.log(`[Handshake OK] ID: ${request.id}`); 
                    
                    // Trigger the translation pipeline right when connection opens 
                    startAwsTranscription(); 
                } 
                else if (request.type === 'paused') { 
                    const response = { version: request.version, type: 'paused', seq: serverSeq++, clientseq: request.seq, id: request.id }; 
                    ws.send(JSON.stringify(response)); 
                } 
                else if (request.type === 'resumed') { 
                    const response = { version: request.version, type: 'resumed', seq: serverSeq++, clientseq: request.seq, id: request.id }; 
                    ws.send(JSON.stringify(response)); 
                } 
                else if (request.type === 'close') { 
                    const response = { version: request.version, type: 'closed', seq: serverSeq++, clientseq: request.seq, id: request.id, parameters: {} }; 
                    ws.send(JSON.stringify(response)); 
                    setImmediate(() => { 
                        ws.close(1000); 
                    }); 
                } 
                else if (request.type === 'ping') { 
                    const response = { version: request.version, type: 'pong', seq: serverSeq++, clientseq: request.seq, id: request.id }; 
                    ws.send(JSON.stringify(response)); 
                } 
                return; // JSON text frame handled completely
            } catch (err) { 
                // Fallback: If JSON parsing fails due to trailing audio data inside text strings, mix it down as audio data instead
                console.log("⚠️ Text frame format boundary mismatch. Routing packet to conversion engine.");
            } 
        } 
        
        // 4. AUDIO CHUNKS ROUTER FALLBACK
        console.log(`🎙️ [Streaming Media] Receiving fallback raw audio chunk: ${message.length} bytes`); 
        const rawMuLaw = Buffer.from(message); 
        const monoPCM = decodeAndMixDualChannelToMonoPCM(rawMuLaw); 
        audioQueue.push(monoPCM); 
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
