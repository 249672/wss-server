import 'dotenv/config';
import { WebSocketServer } from 'ws';
import { 
    TranscribeStreamingClient, 
    StartStreamTranscriptionCommand 
} from "@aws-sdk/client-transcribe-streaming";
import { BedrockRuntimeClient, InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";

const port = process.env.SERVER_PORT || 3000;
const wss = new WebSocketServer({ port });

const transcribeClient = new TranscribeStreamingClient({ 
    region: process.env.AWS_REGION 
});

const bedrockClient = new BedrockRuntimeClient({ 
    region: process.env.AWS_REGION 
});

wss.on('connection', (ws) => {
    console.log('--- New Genesys Cloud Connection ---');

    let audioQueue = [];
    let isActive = true; 
    let fullTranscript = ""; // Variable to store the full text for summarization

    async function* getAudioStream() {
        while (isActive) {
            if (audioQueue.length > 0) {
                const chunk = audioQueue.shift();
                yield { AudioEvent: { AudioChunk: chunk } };
            } else {
                await new Promise(resolve => setTimeout(resolve, 10)); 
            }
        }
    }

    ws.on('message', async (data, isBinary) => {
        const buffer = Buffer.from(data);

        if (isBinary || buffer.length === 480) {
            if (isActive) audioQueue.push(buffer);
            return;
        }

        try {
            const message = JSON.parse(buffer.toString());
            
            if (message.type === 'open') {
                console.log(`Starting Transcribe for: ${message.parameters.conversationId}`);

                ws.send(JSON.stringify({
                    version: "2",
                    type: "opened",
                    seq: message.seq,
                    clientseq: message.seq,
                    parameters: {
                        media: [{ type: "audio", format: "PCMU", rate: 8000, channels: ["external"] }]
                    }
                }));

                const command = new StartStreamTranscriptionCommand({
                    IdentifyMultipleLanguages: true, 
                    LanguageOptions: "en-US,es-US",
                    MediaEncoding: "pcm",
                    MediaSampleRateHertz: 8000,
                    AudioStream: getAudioStream(),
                    EnablePartialResultsStabilization: true,
                    PartialResultsStability: "high"
                });

                try {
                    const response = await transcribeClient.send(command);
                    console.log("✅ AWS Transcribe stream established.");

                    (async () => {
                        try {
                            for await (const event of response.TranscriptResultStream) {
                                if (event.TranscriptEvent?.Transcript?.Results) {
                                    const results = event.TranscriptEvent.Transcript.Results;
                                    results.forEach(result => {
                                        if (!result.IsPartial) {
                                            const transcript = result.Alternatives[0].Transcript;
                                            fullTranscript += transcript + " "; // Add to summary string
                                            console.log(`[${result.LanguageCode}] Final: ${transcript}`);
                                        }
                                    });
                                }
                            }
                        } catch (err) {
                            console.error("⚠️ Transcript Stream Error:", err.message);
                        }
                    })();

                } catch (err) {
                    console.error("❌ Failed to start AWS Command:", err.message);
                }
            } 
            
            if (message.type === 'close') {
                console.log("Genesys sent close request.");
                isActive = false;
                // Optional: Trigger summary immediately upon receiving protocol close
                if (fullTranscript.length > 0) {
                    await summarizeTranscript(fullTranscript);
                    fullTranscript = ""; // Clear so 'close' event doesn't repeat it
                }
            }

        } catch (e) {
            console.error("Protocol Error:", e.message);
        }
    });

    ws.on('close', async () => {
        console.log('Session Ended');
        isActive = false;
        
        // Final check for summary if not already triggered by 'close' message
        if (fullTranscript.trim().length > 0) {
            await summarizeTranscript(fullTranscript);
        }
        
        audioQueue = []; 
        fullTranscript = "";
    });
});

async function summarizeTranscript(fullTranscript) {
    console.log("--- Generating AI Summary via Amazon Bedrock ---");
    const prompt = `Summarize the following call transcript. 
    Provide the Customer's Issue, the Agent's Actions, and any follow-up Action Items.
    
    Transcript:
    ${fullTranscript}`;

    const input = {
        // Updated to Claude 3.5 Sonnet for 2026 stability/performance
        modelId: "anthropic.claude-3-5-sonnet-20240620-v1:0", 
        contentType: "application/json",
        accept: "application/json",
        body: JSON.stringify({
            anthropic_version: "bedrock-2023-05-31",
            max_tokens: 1000,
            messages: [{ role: "user", content: prompt }]
        }),
    };

    try {
        const response = await bedrockClient.send(new InvokeModelCommand(input));
        const resBody = JSON.parse(new TextDecoder().decode(response.body));
        console.log("--- Call Summary ---");
        // Accessing content[0].text is the standard for Claude on Bedrock
        console.log(resBody.content[0].text);
    } catch (err) {
        console.error("Summarization Error:", err);
    }
}

console.log(`AudioHook Bridge listening on port ${port}`);
