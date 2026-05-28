const { WebSocketServer } = require('ws');
const http = require('http');

// Render provides the port automatically via process.env.PORT
const port = process.env.PORT || 8080; 
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('WebSocket Server is Running');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
    console.log('Client connected to cloud server');
    ws.on('message', (message) => {
        ws.send(`Echo back: ${message}`);
    });
});

server.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
