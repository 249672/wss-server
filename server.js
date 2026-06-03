const { WebSocketServer } = require('ws');
const http = require('http');
// 1. Import AWS Transcribe Streaming Client
const { TranscribeStreamingClient, StartStreamTranscriptionCommand } = require('@aws-sdk/client-transcribe-streaming');

const port = process.env.PORT || 8080;

// Initialize AWS Client (Ensure AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY env variables are set)
const transcribeClient = new TranscribeStreamingClient({ region: process.env.AWS_REGION || 'us-east-1' });

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

    // 2. Manage audio stream pipeline per connection
    let audioQueue = [];
    let isTranscribing = false;

    // Async generator function to pipe chunks into AWS SDK
    async function* audioStreamGenerator() {
        while (ws.readyState === ws.OPEN || audioQueue.length > 0) {
            if (audioQueue.length > 0) {
                const chunk = audioQueue.shift();
                yield { AudioEvent: { AudioChunk: chunk } };
            } else {
                // Wait 20ms before checking for new audio packets to prevent event loop locking
                await new Promise(resolve => setTimeout(resolve, 20));
            }
        }
    }

    // Start transcription session
    async function startAwsTranscription() {
        if (isTranscribing) return;
        isTranscribing = true;

        try {
            const command = new StartStreamTranscriptionCommand({
                LanguageCode: 'en-US',
                MediaSampleRateHertz: 8000,
                MediaEncoding: 'pcm', // AWS treats raw mu-law/pcm linearly if configured correctly or handled via media type
                AudioStream: audioStreamGenerator()
            });

            const response = await transcribeClient.send(command);
            console.log("=== AWS Transcribe Session Started ===");

            for await (const event of response.TranscriptResultStream) {
                if (event.TranscriptEvent && event.TranscriptEvent.Transcript) {
                    const results = event.TranscriptEvent.Transcript.Results;
                    results.forEach(result => {
                        if (!result.IsPartial) { // Only log finalized text
                            const transcript = result.Alternatives[0].Transcript;
                            console.log(`📝 [Transcription]: ${transcript}`);
                        }
                    });
                }
            }
        } catch (err) {
            console.error('❌ AWS Transcribe Error:', err.message);
        }
    }

    ws.on('message', (message, isBinary) => {
        // Handle incoming raw binary PCMU audio packets
        if (isBinary || Buffer.isBuffer(message) || typeof message !== 'string') {
            // Push raw data chunk to transcription queue
            audioQueue.push(Buffer.from(message));
            return;
        }

        try {
            const cleanText = message.toString().trim();
            if (!cleanText.startsWith('{')) return;
            const request = JSON.parse(cleanText);

            if (request.type === 'open') {
                const response = {
                    version: request.version,
                    type: 'opened',
                    seq: 1,
                    clientseq: request.seq,
                    id: request.id,
                    parameters: {
                        startPaused: false,
                        media: [{ type: 'audio', format: 'PCMU', channels: ['external'], rate: 8000 }] // Simplified to single channel for standard POC transcription
                    }
                };
                ws.send(JSON.stringify(response));
                console.log(`[Handshake OK] ID: ${request.id}`);
                
                // 3. Start AWS Session immediately when Genesys confirms stream opening
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
                setImmediate(() => { ws.close(1000); });
            } 
            else if (request.type === 'ping') {
                const response = { version: request.version, type: 'pong', seq: (request.serverseq || 0) + 1, clientseq: request.seq, id: request.id };
                ws.send(JSON.stringify(response));
            }
        } catch (err) {
            console.error('[Structural Error] schema violation caught:', err.message);
        }
    });

    ws.on('error', (error) => { console.error('[Connection Error Details]:', error.message); });
    ws.on('close', () => { 
        console.log(`[Disconnected] Connection state closed.`);
        audioQueue = []; // Clear resources
    });
});

server.listen(port, () => {
    console.log(`Application successfully listening for incoming traffic on port ${port}`);
});
