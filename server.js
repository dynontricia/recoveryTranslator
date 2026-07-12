require('dotenv').config();
const http = require('http');
const fs = require('fs');
const WebSocket = require('ws');

const PORT = 3000;

// These will hold the connected phone clients, separated by language
const sessions = {};

function createSession(apiKey, sourceLanguage) {
    const code = Math.random().toString(36).substring(2, 8).toUpperCase();

    sessions[code] = {
        apiKey,
        sourceLanguage,
        primarySocket: null,
        secondarySocket: null,
        clients: {
            english: [],
            spanish: []
        },
        speakQueue: [],
        leaderSocket: null,
        createdAt: Date.now()
    };

    return code;
}

function broadcast(sessionCode, language, text) {
    const session = sessions[sessionCode];
    if (!session) return;

    const targetClients = session.clients[language];
    const message = `data: ${JSON.stringify({ text })}\n\n`;

    targetClients.forEach(client => {
        try {
            client.write(message);
        } catch (err) {
            console.log(`Error writing to ${language} client, removing.`);
            session.clients[language] = session.clients[language].filter(c => c !== client);
        }
    });
}

const server = http.createServer((req, res) => {

    // Serve the attendee web page
    if (req.url === '/' || req.url === '/index.html') {
        fs.readFile('./index.html', (err, data) => {
            if (err) {
                res.writeHead(500);
                res.end('Error loading page');
                return;
            }
            res.writeHead(200, { 'Content-Type': 'text/html; charset=UTF-8' });
            res.end(data);
        });
    }

    else if (req.method === 'POST' && req.url === '/session/client-secret') {
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });
        req.on('end', async () => {
            const { sessionCode } = JSON.parse(body);
            const session = sessions[sessionCode];

            if (!session) {
                res.writeHead(404, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Session not found' }));
                return;
            }

            const response = await fetch(
                'https://api.openai.com/v1/realtime/translations/client_secrets',
                {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${session.apiKey}`,
                        'Content-Type': 'application/json',
                        'OpenAI-Safety-Identifier': 'recovery-translator'
                    },
                    body: JSON.stringify({
                        session: {
                            model: 'gpt-realtime-translate',
                            audio: {
                                input: { transcription: { model: 'gpt-realtime-whisper' } },
                                output: { language: session.sourceLanguage === 'en' ? 'es' : 'en' }
                            }
                        }
                    })
                }
            );

            const data = await response.json();
            res.writeHead(response.status, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(data));
        });
    }

    else if (req.method === 'POST' && req.url === '/session/create') {
        let body = '';

        req.on('data', chunk => {
            body += chunk.toString();
        });

        req.on('end', () => {
            let creds = JSON.parse(body);

            if (creds.accessCode !== process.env.ACCESS_CODE) {
                res.writeHead(401, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Invalid access code' }));
                return;
            }

            if(creds.apiKey === 'default-api-key') {
                creds.apiKey = process.env.OPENAI_API_KEY;
            }

            const code = createSession(creds.apiKey, creds.sourceLanguage);

            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ sessionCode: code }));
        });
    }

    else if (req.method === 'GET' && req.url.startsWith('/stream/')) {
        const parts = req.url.split('/');
        const sessionCode = parts[2];
        const language = parts[3];

        if (!sessions[sessionCode]) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Session not found' }));
            return;
        }

        if (!['english', 'spanish'].includes(language)) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Invalid language' }));
            return;
        }

        res.writeHead(200, {
            'Content-Type': 'text/event-stream; charset=UTF-8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
        });

        sessions[sessionCode].clients[language].push(res);
        console.log(`[${sessionCode}] ${language} client connected. Total: ${sessions[sessionCode].clients[language].length}`);

        req.on('close', () => {
            if (!sessions[sessionCode]) return;
            sessions[sessionCode].clients[language] = sessions[sessionCode].clients[language].filter(c => c !== res);
            console.log(`[${sessionCode}] ${language} client disconnected. Total: ${sessions[sessionCode].clients[language].length}`);
        });
    }

    else if (req.url === '/logo.png') {
        fs.readFile('./recoveryTrans.png', (err, data) => {
            if (err) { res.writeHead(404); res.end(); return; }
            res.writeHead(200, { 'Content-Type': 'image/png' });
            res.end(data);
        });
    }

    else if (req.method === 'POST' && req.url === '/session/listener-secret') {
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });
        req.on('end', async () => {
            const { sessionCode } = JSON.parse(body);
            const session = sessions[sessionCode];

            if (!session) {
                res.writeHead(404, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Session not found' }));
                return;
            }

            // Flip the language direction vs the primary session
            const inputLang = session.sourceLanguage === 'en' ? 'es' : 'en';
            const outputLang = session.sourceLanguage === 'en' ? 'en' : 'es';

            const response = await fetch(
                'https://api.openai.com/v1/realtime/translations/client_secrets',
                {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${session.apiKey}`,
                        'Content-Type': 'application/json',
                        'OpenAI-Safety-Identifier': 'recovery-translator'
                    },
                    body: JSON.stringify({
                        session: {
                            model: 'gpt-realtime-translate',
                            audio: {
                                input: {
                                    language: inputLang,
                                    transcription: { model: 'gpt-realtime-whisper' }
                                },
                                output: { language: outputLang }
                            }
                        }
                    })
                }
            );

            const data = await response.json();
            res.writeHead(response.status, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(data));
        });
    }

    else {
        res.writeHead(404);
        res.end('Not found');
    }
});

server.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}`);
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
    const params = new URL(req.url, `http://localhost:${PORT}`).searchParams;
    const sessionCode = params.get('session');
    const role = params.get('role');

    if (!sessions[sessionCode]) {
        ws.close();
        return;
    }

    if (role === 'leader') {
        sessions[sessionCode].leaderSocket = ws;
        console.log(`[${sessionCode}] Leader signaling connected`);

        ws.on('message', (data) => {
            const msg = JSON.parse(data);
            if (msg.type === 'transcript') {
                const session = sessions[sessionCode];
                const flipped = session.flipped;

                if (msg.direction === 'input') {
                    const lang = flipped ? 'spanish' : (session.sourceLanguage === 'en' ? 'english' : 'spanish');
                    broadcast(sessionCode, lang, msg.delta);
                }
                if (msg.direction === 'output') {
                    const lang = flipped ? 'english' : (session.sourceLanguage === 'en' ? 'spanish' : 'english');
                    broadcast(sessionCode, lang, msg.delta);
                }
            }

            if (msg.type === 'flip_confirmed') {
                const session = sessions[sessionCode];
                if (!session.pendingApproval) return;
                broadcastAll(sessionCode, '\n\n');
                const pendingListener = session.pendingApproval;
                pendingListener.ws.send(JSON.stringify({ type: 'approved' }));
                session.pendingApproval = null;
            }

            if (msg.type === 'approve') {
                const request = sessions[sessionCode].speakQueue.find(r => r.id === msg.id);
                if (request) {
                    sessions[sessionCode].speakQueue = sessions[sessionCode].speakQueue.filter(r => r.id !== msg.id);
                    sessions[sessionCode].flipped = true;
                    sessions[sessionCode].pendingApproval = request;

                    const leaderSocket = sessions[sessionCode].leaderSocket;
                    if (leaderSocket && leaderSocket.readyState === WebSocket.OPEN) {
                        leaderSocket.send(JSON.stringify({
                            type: 'flip_language',
                            language: sessions[sessionCode].sourceLanguage === 'en' ? 'en' : 'es'
                        }));
                    }
                }
            }

            if (msg.type === 'resume_primary') {
                console.log(`[${sessionCode}] Resuming primary, flipping back to ${sessions[sessionCode].sourceLanguage === 'en' ? 'es' : 'en'}`);
                sessions[sessionCode].flipped = false;
                broadcastAll(sessionCode, '\n\n');
                resumePrimary(sessionCode);
                const leaderSocket = sessions[sessionCode].leaderSocket;
                if (leaderSocket && leaderSocket.readyState === WebSocket.OPEN) {
                    leaderSocket.send(JSON.stringify({
                        type: 'flip_language',
                        language: sessions[sessionCode].sourceLanguage === 'en' ? 'es' : 'en'
                    }));
                }
            }

            if (msg.type === 'deny') {
                const request = sessions[sessionCode].speakQueue.find(r => r.id === msg.id);
                if (request) {
                    request.ws.send(JSON.stringify({ type: 'denied' }));
                    sessions[sessionCode].speakQueue = sessions[sessionCode].speakQueue.filter(r => r.id !== msg.id);
                }
            }

            if (msg.type === 'end_session') {
                endSession(sessionCode);
            }
        });

        ws.on('close', () => {
            console.log(`[${sessionCode}] Leader disconnected`);
        });

    } else if (role === 'listener') {
        const id = Math.random().toString(36).substring(2, 8);
        ws.listenerId = id;

        ws.on('message', (data) => {
            const msg = JSON.parse(data);

            if (msg.type === 'listener_transcript') {
                const session = sessions[sessionCode];
                if (msg.direction === 'input') {
                    const inputLang = session.sourceLanguage === 'en' ? 'spanish' : 'english';
                    broadcast(sessionCode, inputLang, msg.delta);
                }
                if (msg.direction === 'output') {
                    const outputLang = session.sourceLanguage === 'en' ? 'english' : 'spanish';
                    broadcast(sessionCode, outputLang, msg.delta);
                }
            }

            if (msg.type === 'request_speak') {
                const request = { id, ws, language: msg.language };
                sessions[sessionCode].speakQueue.push(request);
                console.log(`[${sessionCode}] Speak request from ${id}`);

                const leaderSocket = sessions[sessionCode].leaderSocket;
                if (leaderSocket && leaderSocket.readyState === WebSocket.OPEN) {
                    leaderSocket.send(JSON.stringify({
                        type: 'speak_request',
                        id,
                        language: msg.language
                    }));
                }
            }

            if (msg.type === 'done_speaking') {
                //resumePrimary(sessionCode);
            }
        });
    }
});

function pausePrimary(sessionCode) {
    sessions[sessionCode].isPaused = true;
    console.log(`[${sessionCode}] Primary stream paused`);
}

function resumePrimary(sessionCode) {
    //sessions[sessionCode].isPaused = false;
    sessions[sessionCode].flipped = false;
    console.log(`[${sessionCode}] Primary stream resumed`);
}

function endSession(sessionCode) {
    const session = sessions[sessionCode];
    if (!session) return;

    const endMessage = `data: ${JSON.stringify({ type: 'session_ended' })}\n\n`;
    [...session.clients.english, ...session.clients.spanish].forEach(client => {
        try { client.write(endMessage); } catch(e) {}
    });

    // Give clients 1 second to receive the message before closing
    setTimeout(() => {
        if (!sessions[sessionCode]) return;
        [...session.clients.english, ...session.clients.spanish].forEach(client => {
            try { client.end(); } catch(e) {}
        });
        if (session.primarySocket) session.primarySocket.close();
        if (session.secondarySocket) session.secondarySocket.close();
        if (session.leaderSocket) session.leaderSocket.close();
        delete sessions[sessionCode];
        console.log(`[${sessionCode}] Session ended`);
    }, 1000);
}

function broadcastAll(sessionCode, text) {
    broadcast(sessionCode, 'english', text);
    broadcast(sessionCode, 'spanish', text);
}

function connectToOpenAI(sessionCode) {
    const session = sessions[sessionCode];

    const ws = new WebSocket(
        'wss://api.openai.com/v1/realtime/translations?model=gpt-realtime-translate',
        {
            headers: {
                'Authorization': `Bearer ${session.apiKey}`,
                'OpenAI-Safety-Identifier': sessionCode
            }
        }
    );

    ws.on('open', () => {
        console.log(`[${sessionCode}] Connected to OpenAI`);

        ws.send(JSON.stringify({
            type: 'session.update',
            session: {
                audio: {
                    input: {
                        transcription: {
                            model: 'gpt-realtime-whisper'
                        }
                    },
                    output: {
                        language: session.sourceLanguage === 'en' ? 'es' : 'en'
                    }
                }
            }
        }));
    });

    ws.on('message', (data) => {
        const event = JSON.parse(data);
        console.log(`[${sessionCode}] Event: ${event.type}`);

        if (event.type === 'session.input_transcript.delta') {
            const inputLang = session.sourceLanguage === 'en' ? 'english' : 'spanish';
            broadcast(sessionCode, inputLang, event.delta);
        }

        if (event.type === 'session.output_transcript.delta') {
            const outputLang = session.sourceLanguage === 'en' ? 'spanish' : 'english';
            broadcast(sessionCode, outputLang, event.delta);
        }
    });

    ws.on('error', (err) => {
        console.error(`[${sessionCode}] OpenAI error:`, err.message);
    });

    ws.on('close', () => {
        console.log(`[${sessionCode}] OpenAI connection closed`);
    });

    return ws;
}