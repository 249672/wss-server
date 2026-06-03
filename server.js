const { WebSocketServer } = require('ws');
const http = require('http');
// 1. Import AWS Transcribe Streaming Client
const { TranscribeStreamingClient, StartStreamTranscriptionCommand } = require('@aws-sdk/client-transcribe-streaming');

// Render sets the web environment port dynamically via process.env.PORT
const port = process.env.PORT || 8080;

// 2. Initialize AWS Client (uses Render Environment Variables)
const transcribeClient = new TranscribeStreamingClient({ region: process.env.AWS_REGION || 'us-east-1' });

// Maintain the Render Infrastructure Web Router Health Check
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

    // Track audio pipeline states per connection
    let audioQueue = [];
    let isTranscribing = false;

    // Async generator to feed chunks into AWS SDK
    async function* audioStreamGenerator() {
        while (ws.readyState === ws.OPEN || audioQueue.length > 0) {
            if (audioQueue.length > 0) {
                const chunk = audioQueue.shift();
                yield { AudioEvent: { AudioChunk: chunk } };
            } else {
                await new Promise(resolve => setTimeout(resolve, 20)); // Prevent event loop locking
            }
        }
    }

    // Execute streaming session
    async function startAwsTranscription() {
        if (isTranscribing) return;
        isTranscribing = true;

        try {
            const command = new StartStreamTranscriptionCommand({
                LanguageCode: 'en-US',
                MediaSampleRateHertz: 8000,
                MediaEncoding: 'pcm', // AWS treats raw PCMU natively through this parameter
                AudioStream: audioStreamGenerator()
            });

            console.log("=== Initializing AWS Transcribe Connection... ===");
            const response = await transcribeClient.send(command);
            console.log("=== AWS Transcribe Session Active ===");

            for await (const event of response.TranscriptResultStream) {
                if (event.TranscriptEvent?.Transcript?.Results) {
                    event.TranscriptEvent.Transcript.Results.forEach(result => {
                        if (!result.IsPartial) { // Log only finalized text blocks
                            console.log(`📝 [Transcription]: ${result.Alternatives.Transcript}`);
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
        // STRICT STREAM PROTECTION
        if (isBinary || Buffer.isBuffer(message) || typeof message !== 'string') {
            const bufferMessage = Buffer.from(message);
            
            // FIX: Genesys sends session metadata inside the FIRST binary channel frame (usually ~673 bytes).
            // If it contains a text string starting with '{', it is configuration data, NOT audio.
            if (bufferMessage.length > 0 && bufferMessage[0] === 123) { // 123 is ASCII for '{'
                try {
                    const metaText = bufferMessage.toString('utf8').trim();
                    if (metaText.startsWith('{')) {
                        console.log("ℹ️ [AudioHook Media Header] Skipping binary metadata packet configuration.");
                        return; 
                    }
                } catch (e) {
                    // Fail-safe: if parsing fails, treat it as audio
                }
            }

            console.log(`🎙️ [Streaming Media] Receiving raw audio chunk: ${bufferMessage.length} bytes`);
            
            // Queue the incoming true raw audio bytes for AWS stream consumption
            audioQueue.push(bufferMessage);
            return;
        }

        try {
            // Clean up whitespace to ensure precise parsing
            const cleanText = message.toString().trim();
            if (!cleanText.startsWith('{')) return;

            // Ignore any fragmented text blocks
            const request = JSON.parse(cleanText);
            console.log("Full Genesys Request Payload:", request.type);

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
                ws.send(JSON.stringify(response));
                console.log(`[Handshake OK] ID: ${request.id}`);

                // Trigger the translation pipeline right when connection opens
                startAwsTranscription();
            }

            // STEP A-2: SPECS PAUSED HANDSHAKE
            else if (request.type === 'paused') {
                const response = { version: request.version, type: 'paused', seq: (request.serverseq || 0) + 1, clientseq: request.seq, id: request.id };
                ws.send(JSON.stringify(response));
            }

            // STEP A-3: SPECS RESUMED HANDSHAKE
            else if (request.type === 'resumed') {
                const response = { version: request.version, type: 'resumed', seq: (request.serverseq || 0) + 1, clientseq: request.seq, id: request.id };
                ws.send(JSON.stringify(response));
            }

            // STEP B: SPECS CLOSE SESSION CLEANUP HANDSHAKE
            else if (request.type === 'close') {
                const response = { version: request.version, type: 'closed', seq: (request.serverseq || 0) + 1, clientseq: request.seq, id: request.id, parameters: {} };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake Ended] Sent close acknowledgement for ID: ${request.id}`);

                // Safely allow the message queue to flush before severing the socket
                setImmediate(() => { ws.close(1000); });
            }

            // STEP C: KEEPALIVE INFRASTRUCTURE LIFELINE
            else if (request.type === 'ping') {
                const response = { version: request.version, type: 'pong', seq: (request.serverseq || 0) + 1, clientseq: request.seq, id: request.id };
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
        audioQueue = []; // Clear audio array elements on disconnect
        isTranscribing = false;
    });
});

server.listen(port, () => {
    console.log(`Application successfully listening for incoming traffic on port ${port}`);
});
