require('dotenv').config();
const http = require('http');
const fs = require('fs');
const crypto = require('crypto');
const WebSocket = require('ws');

// Safety net: an uncaught error anywhere -- especially from the native
// @zoom/rtms package, which we don't fully control -- would otherwise crash
// this entire process by Node's default behavior, wiping every in-memory
// session (including completely unrelated browser sessions that have
// nothing to do with Zoom). Log and keep running instead.
process.on('uncaughtException', (err) => {
    console.error('UNCAUGHT EXCEPTION (server kept running):', err && err.stack || err);
});
process.on('unhandledRejection', (reason) => {
    console.error('UNHANDLED REJECTION (server kept running):', reason);
});

const PORT = 3000;

// sessions[code] = {
//   apiKey,
//   clients: { english: [SSE res...], spanish: [SSE res...] },
//   leaderSocket: ws | null,
//   listenerSockets: { [listenerId]: { ws, language } },
//   speakQueue: [{ id, language }],
//   spanishTurn: bool,
//   createdAt
// }
const sessions = {};

// Active RTMS clients, keyed by rtms_stream_id -- lets meeting.rtms_stopped
// find and cleanly close the right client, per Zoom's own SDK example.
const rtmsClients = new Map();

// Per-stream caption pipeline state: { sessionCode, openaiWs, pauseTimer,
// mode, awaitingSegmentSpace }. Keyed by the same rtms_stream_id as
// rtmsClients, so a stream's whole pipeline (Zoom audio in, OpenAI
// transcription, caption broadcast) can be torn down together on stop.
const rtmsCaptionPipelines = new Map();

// Lets zoom-app.html discover its own session code after calling
// startRTMS() (the SDK response doesn't include one), and lets the
// Spanish-turn toggle endpoint find the right pipeline by session code
// instead of the raw Zoom stream ID.
let lastRtmsSessionCode = null;

// RTMS's default audio is 16kHz mono PCM16, but OpenAI's realtime API
// requires 24kHz. 16000->24000 is a clean 2:3 ratio, so a simple linear
// interpolation resampler is enough -- no need for a heavier audio library
// or another native dependency (we've had enough native-package risk
// tonight with @zoom/rtms already).
function resamplePCM16(inputBuffer, inputRate, outputRate) {
    const inSamples = inputBuffer.length / 2;
    const ratio = outputRate / inputRate;
    const outSamples = Math.floor(inSamples * ratio);
    const output = Buffer.alloc(outSamples * 2);

    for (let i = 0; i < outSamples; i++) {
        const srcPos = i / ratio;
        const srcIndexLow = Math.floor(srcPos);
        const srcIndexHigh = Math.min(srcIndexLow + 1, inSamples - 1);
        const frac = srcPos - srcIndexLow;

        const sampleLow = inputBuffer.readInt16LE(srcIndexLow * 2);
        const sampleHigh = inputBuffer.readInt16LE(srcIndexHigh * 2);
        const interpolated = Math.round(sampleLow + (sampleHigh - sampleLow) * frac);

        output.writeInt16LE(Math.max(-32768, Math.min(32767, interpolated)), i * 2);
    }
    return output;
}

// Opens a server-side WebSocket to OpenAI's realtime API and configures it
// as an English transcription session -- the server-side equivalent of the
// browser's WebRTC transcription session, same model/settings (gpt-realtime-
// whisper, delay: high, no VAD -- manual commit only), same pause-triggered
// commit design. Logs each stage clearly since this connection pattern
// (standard realtime endpoint + session.update to type:"transcription")
// hasn't been verified against a live response yet -- these logs are how
// we'll know definitively whether it's right.
// Opens the English transcription WebSocket (same model/settings as the
// browser pipeline: gpt-realtime-whisper, delay: high, no VAD, pause-
// triggered commit). Audio is only actually sent to this connection when
// NOT in a Spanish turn -- see the onAudioData routing below.
function connectTranscriptionWs(pipeline) {
    const apiKey = sessions[pipeline.sessionCode].apiKey;
    // Confirmed by a live error from OpenAI: a session's type must be set at
    // connection time via ?intent=transcription, not switched afterward with
    // session.update on a connection opened as a general realtime session
    // ("Passing a transcription session update to a realtime session is not
    // allowed"). This matches the older, precedented connection pattern
    // rather than the "connect generic, then convert" approach that failed.
    const ws = new WebSocket('wss://api.openai.com/v1/realtime?intent=transcription', {
        headers: { 'Authorization': `Bearer ${apiKey}`, 'OpenAI-Safety-Identifier': 'recovery-translator' }
    });
    pipeline.transcriptionWs = ws;
    pipeline.transcriptionReady = false;

    ws.on('open', () => {
        console.log(`RTMS/OpenAI [${pipeline.sessionCode}] transcription: connected, sending session.update`);
        ws.send(JSON.stringify({
            type: 'session.update',
            session: {
                type: 'transcription',
                audio: {
                    input: {
                        format: { type: 'audio/pcm', rate: 24000 },
                        transcription: { model: 'gpt-realtime-whisper', language: 'en', delay: 'high' },
                        turn_detection: null
                    }
                }
            }
        }));
    });

    ws.on('message', (raw) => {
        let ev; try { ev = JSON.parse(raw.toString()); } catch (e) { return; }

        if (ev.type === 'session.updated') {
            console.log(`RTMS/OpenAI [${pipeline.sessionCode}] transcription: session.updated confirmed -- live`);
            pipeline.transcriptionReady = true;
        }
        if (ev.type === 'error') {
            console.error(`RTMS/OpenAI [${pipeline.sessionCode}] transcription: ERROR:`, JSON.stringify(ev.error || ev));
        }
        if (ev.type === 'conversation.item.input_audio_transcription.delta') {
            let delta = ev.delta;
            if (pipeline.awaitingSegmentSpace) { delta = ' ' + delta; pipeline.awaitingSegmentSpace = false; }
            broadcast(pipeline.sessionCode, 'english', delta);
        }
    });

    ws.on('error', (err) => console.error(`RTMS/OpenAI [${pipeline.sessionCode}] transcription: WS error:`, err.message));
    ws.on('close', (code, reason) => console.log(`RTMS/OpenAI [${pipeline.sessionCode}] transcription: closed. code=${code}`));
}

// Opens a translate WebSocket (gpt-realtime-translate). Used for both the
// always-on baseline (output: es) and the on-demand turn session
// (output: en, spun up only while a Spanish speaker has the floor).
function connectTranslateWs(pipeline, targetLanguage, wsKey, readyKey, broadcastLanguage) {
    const apiKey = sessions[pipeline.sessionCode].apiKey;
    const ws = new WebSocket('wss://api.openai.com/v1/realtime/translations?model=gpt-realtime-translate', {
        headers: { 'Authorization': `Bearer ${apiKey}`, 'OpenAI-Safety-Identifier': 'recovery-translator' }
    });
    pipeline[wsKey] = ws;
    pipeline[readyKey] = false;

    ws.on('open', () => {
        console.log(`RTMS/OpenAI [${pipeline.sessionCode}] translate(${targetLanguage}): connected, sending session.update`);
        ws.send(JSON.stringify({ type: 'session.update', session: { audio: { output: { language: targetLanguage } } } }));
    });

    ws.on('message', (raw) => {
        let ev; try { ev = JSON.parse(raw.toString()); } catch (e) { return; }

        if (ev.type === 'session.updated') {
            console.log(`RTMS/OpenAI [${pipeline.sessionCode}] translate(${targetLanguage}): session.updated confirmed -- live`);
            pipeline[readyKey] = true;
        }
        if (ev.type === 'error') {
            console.error(`RTMS/OpenAI [${pipeline.sessionCode}] translate(${targetLanguage}): ERROR:`, JSON.stringify(ev.error || ev));
        }
        if (ev.type === 'session.output_transcript.delta') {
            // Baseline (Spanish) audio keeps flowing continuously even during
            // a turn -- OpenAI's guidance is to keep appending audio without
            // gaps -- but its captions are suppressed at broadcast time while
            // a Spanish speaker has the floor, matching the browser pipeline
            // and avoiding same-language "cleanup" text leaking through.
            if (broadcastLanguage === 'spanish' && pipeline.spanishTurn) return;
            broadcast(pipeline.sessionCode, broadcastLanguage, ev.delta);
        }
    });

    ws.on('error', (err) => console.error(`RTMS/OpenAI [${pipeline.sessionCode}] translate(${targetLanguage}): WS error:`, err.message));
    ws.on('close', (code, reason) => console.log(`RTMS/OpenAI [${pipeline.sessionCode}] translate(${targetLanguage}): closed. code=${code}`));
}

// Both the toggle and the queue (if ever added server-side) funnel through
// these. Unlike the browser version, we don't need a "discard garbage"
// flush at turn end -- we simply never send audio to the transcription
// session while spanishTurn is true, so its buffer is genuinely empty when
// the turn ends, and no cleanup commit is needed.
function beginSpanishTurn(pipeline) {
    if (pipeline.spanishTurn) return;
    pipeline.spanishTurn = true;
    console.log(`RTMS [${pipeline.sessionCode}]: Spanish turn started`);
    broadcastControl(pipeline.sessionCode, { type: 'spanish_turn', active: true });

    // Flush the transcription session's in-flight English tail before we
    // stop feeding it audio, so nothing is cut off mid-word.
    if (pipeline.transcriptionWs && pipeline.transcriptionWs.readyState === WebSocket.OPEN) {
        pipeline.transcriptionWs.send(JSON.stringify({ type: 'input_audio_buffer.commit' }));
    }
    if (pipeline.pauseTimer) { clearTimeout(pipeline.pauseTimer); pipeline.pauseTimer = null; }
    broadcast(pipeline.sessionCode, 'english', '\n\n');

    connectTranslateWs(pipeline, 'en', 'turnWs', 'turnReady', 'english');
}

function endSpanishTurn(pipeline) {
    if (!pipeline.spanishTurn) return;
    pipeline.spanishTurn = false;
    console.log(`RTMS [${pipeline.sessionCode}]: Spanish turn ended`);
    broadcastControl(pipeline.sessionCode, { type: 'spanish_turn', active: false });

    if (pipeline.turnWs) { pipeline.turnWs.close(); pipeline.turnWs = null; pipeline.turnReady = false; }
    broadcast(pipeline.sessionCode, 'english', '\n\n');
    // No discard-commit needed here -- see comment above beginSpanishTurn.
}

function createSession(apiKey, micDistance, mode) {
    const code = Math.random().toString(36).substring(2, 8).toUpperCase();
    sessions[code] = {
        apiKey,
        micDistance: micDistance === 'near_field' ? 'near_field' : 'far_field',
        mode: mode === 'transcript_only' ? 'transcript_only' : 'bilingual',
        clients: { english: [], spanish: [] },
        leaderSocket: null,
        listenerSockets: {},
        speakQueue: [],
        spanishTurn: false,
        createdAt: Date.now()
    };
    return code;
}

function broadcast(sessionCode, language, text) {
    const session = sessions[sessionCode];
    if (!session) return;
    const message = `data: ${JSON.stringify({ text })}\n\n`;
    session.clients[language].forEach(client => {
        try {
            client.write(message);
        } catch (err) {
            session.clients[language] = session.clients[language].filter(c => c !== client);
        }
    });
}

// Sends a non-caption control event (e.g. spanish_turn) to every SSE client.
function broadcastControl(sessionCode, obj) {
    const session = sessions[sessionCode];
    if (!session) return;
    const message = `data: ${JSON.stringify(obj)}\n\n`;
    [...session.clients.english, ...session.clients.spanish].forEach(client => {
        try { client.write(message); } catch (e) {}
    });
}

function serveFile(res, path, contentType, extraHeaders) {
    fs.readFile(path, (err, data) => {
        if (err) { res.writeHead(err.code === 'ENOENT' ? 404 : 500); res.end(); return; }
        res.writeHead(200, { 'Content-Type': contentType, ...(extraHeaders || {}) });
        res.end(data);
    });
}

// Zoom's app review runs an automated OWASP header check specifically on the
// Home URL. Scoped to that one page rather than applied globally, since the
// main app (index.html) has WebRTC/OpenAI connections and third-party
// scripts already working and tested -- a broad CSP change risks breaking
// that for a fix that's really only about this one page.
const ZOOM_APP_SECURITY_HEADERS = {
    'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
    'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
    'Content-Security-Policy': [
        "default-src 'self'",
        "script-src 'self' 'unsafe-inline' https://appssdk.zoom.us",
        "style-src 'self' 'unsafe-inline'",
        "connect-src 'self' https://appssdk.zoom.us",
        "frame-ancestors 'self' https://*.zoom.us https://*.zoomgov.com"
    ].join('; ')
};

// The native @zoom/rtms package appears to write its own internal debug
// logs to /app/logs/node_<id> and repeatedly fails when that directory
// doesn't exist in this container. Create it defensively -- harmless if
// unused, cheap to rule out as a source of noise (or worse) later.
try { fs.mkdirSync('/app/logs', { recursive: true }); } catch (e) {
    console.error('Could not create /app/logs (non-fatal):', e.message);
}

const server = http.createServer((req, res) => {
    const parsedUrl = new URL(req.url, `http://localhost:${PORT}`);
    const pathname = parsedUrl.pathname;

    if (pathname === '/' || pathname === '/index.html') {
        serveFile(res, './index.html', 'text/html; charset=UTF-8');
    }
    else if (pathname === '/display' || pathname === '/display.html') {
        serveFile(res, './display.html', 'text/html; charset=UTF-8');
    }
    else if (pathname === '/transcript' || pathname === '/transcript.html') {
        serveFile(res, './transcript.html', 'text/html; charset=UTF-8');
    }
    else if (pathname === '/zoom-app' || pathname === '/zoom-app.html') {
        serveFile(res, './zoom-app.html', 'text/html; charset=UTF-8', ZOOM_APP_SECURITY_HEADERS);
    }
    else if (pathname === '/logo.png') {
        serveFile(res, './recoveryTrans.png', 'image/png');
    }

        // One endpoint, called with targetLanguage 'es' (baseline) or 'en'
    // (spun up for a Spanish speaker's turn).
    else if (req.method === 'POST' && pathname === '/session/client-secret') {
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });
        req.on('end', async () => {
            const { sessionCode, targetLanguage } = JSON.parse(body);
            const session = sessions[sessionCode];
            if (!session) {
                res.writeHead(404, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Session not found' }));
                return;
            }
            if (!['en', 'es'].includes(targetLanguage)) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'targetLanguage must be en or es' }));
                return;
            }
            try {
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
                                        noise_reduction: { type: session.micDistance }
                                    },
                                    output: { language: targetLanguage }
                                }
                            }
                        })
                    }
                );
                const data = await response.json();
                res.writeHead(response.status, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(data));
            } catch (err) {
                res.writeHead(502, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Failed to reach OpenAI' }));
            }
        });
    }

        // Mints a client secret for the dedicated English transcription session
    // (separate from the translate sessions above -- different endpoint/shape).
    else if (req.method === 'POST' && pathname === '/session/transcription-secret') {
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
            try {
                const response = await fetch('https://api.openai.com/v1/realtime/client_secrets', {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${session.apiKey}`,
                        'Content-Type': 'application/json',
                        'OpenAI-Safety-Identifier': 'recovery-translator'
                    },
                    body: JSON.stringify({
                        session: {
                            type: 'transcription',
                            audio: {
                                input: {
                                    transcription: { model: 'gpt-realtime-whisper', language: 'en', delay: 'high' },
                                    noise_reduction: { type: session.micDistance },
                                    turn_detection: null // gpt-realtime-whisper: manual commit only
                                }
                            }
                        }
                    })
                });
                const data = await response.json();
                res.writeHead(response.status, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(data));
            } catch (err) {
                res.writeHead(502, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Failed to reach OpenAI' }));
            }
        });
    }

    else if (req.method === 'POST' && pathname === '/session/create') {
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });
        req.on('end', () => {
            let creds = JSON.parse(body);
            if (creds.accessCode !== process.env.ACCESS_CODE) {
                res.writeHead(401, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Invalid access code' }));
                return;
            }
            if (creds.apiKey === 'default-api-key') {
                creds.apiKey = process.env.OPENAI_API_KEY;
            }
            const code = createSession(creds.apiKey, creds.micDistance, creds.mode);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ sessionCode: code }));
        });
    }

    else if (req.method === 'GET' && pathname === '/session/check') {
        const code = parsedUrl.searchParams.get('code');
        const session = sessions[code];
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ exists: !!session, mode: session ? session.mode : null }));
    }

    else if (req.method === 'GET' && pathname.startsWith('/stream/')) {
        const parts = pathname.split('/');
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

        // Tell a late-joining client if a Spanish turn is already in progress.
        if (sessions[sessionCode].spanishTurn) {
            try { res.write(`data: ${JSON.stringify({ type: 'spanish_turn', active: true })}\n\n`); } catch (e) {}
        }

        req.on('close', () => {
            if (!sessions[sessionCode]) return;
            sessions[sessionCode].clients[language] =
                sessions[sessionCode].clients[language].filter(c => c !== res);
        });
    }

        // Zoom RTMS webhook. Handles two things:
        //   1. endpoint.url_validation -- Zoom's one-time challenge to prove we
        //      control this URL, sent the moment this URL is saved in the Zoom
        //      Platform Studio console. We must echo back a specific HMAC-signed
        //      response within a short window or the URL is rejected.
        //   2. meeting.rtms_started / meeting.rtms_stopped -- the real lifecycle
        //      events, telling us when to open (and later close) the RTMS media
        //      connection for a given meeting. For now this just logs them --
        //      actually connecting to the RTMS media stream and piping audio
        //      into our OpenAI sessions is the next phase, once we've confirmed
        //      the webhook itself is reachable and verified.
        // Lets zoom-app.html discover the session code for the RTMS stream it
    // just started (the startRTMS() SDK response doesn't include one).
    else if (req.method === 'GET' && pathname === '/zoom/latest-session') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ sessionCode: lastRtmsSessionCode }));
    }

        // Toggle for "a Spanish speaker has the floor," called from the Zoom
        // App panel -- same underlying begin/endSpanishTurn as the browser
        // leader flow, just driven by sessionCode instead of a button on the
    // leader screen.
    else if (req.method === 'POST' && pathname === '/zoom/spanish-turn') {
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });
        req.on('end', () => {
            let parsed;
            try { parsed = JSON.parse(body); } catch (e) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Invalid JSON' }));
                return;
            }
            const { sessionCode, active } = parsed;
            let pipeline = null;
            for (const p of rtmsCaptionPipelines.values()) {
                if (p.sessionCode === sessionCode) { pipeline = p; break; }
            }
            if (!pipeline) {
                res.writeHead(404, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'No active RTMS pipeline for that session code' }));
                return;
            }
            if (active) beginSpanishTurn(pipeline); else endSpanishTurn(pipeline);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ spanishTurn: pipeline.spanishTurn }));
        });
    }

    else if (req.method === 'POST' && pathname === '/zoom/rtms-webhook') {
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });
        req.on('end', () => {
            let payload;
            try { payload = JSON.parse(body); } catch (e) {
                res.writeHead(400); res.end('Invalid JSON'); return;
            }

            if (payload.event === 'endpoint.url_validation') {
                const plainToken = payload.payload && payload.payload.plainToken;
                const secret = process.env.ZOOM_WEBHOOK_SECRET_TOKEN;
                if (!plainToken || !secret) {
                    console.error('RTMS validation failed: missing plainToken or ZOOM_WEBHOOK_SECRET_TOKEN');
                    res.writeHead(400); res.end('Missing token'); return;
                }
                const encryptedToken = crypto
                    .createHmac('sha256', secret)
                    .update(plainToken)
                    .digest('hex');
                console.log('RTMS URL validation received, responding with signed token');
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ plainToken, encryptedToken }));
                return;
            }

            // Real lifecycle events.
            console.log('RTMS webhook event:', payload.event, JSON.stringify(payload.payload || {}));
            res.writeHead(200); res.end('ok');

            const streamId = payload.payload && payload.payload.rtms_stream_id;

            if (payload.event === 'meeting.rtms_stopped') {
                if (!streamId) {
                    console.log('meeting.rtms_stopped received without a stream ID');
                    return;
                }
                const client = rtmsClients.get(streamId);
                if (client) { client.leave(); rtmsClients.delete(streamId); }

                const pipeline = rtmsCaptionPipelines.get(streamId);
                if (pipeline) {
                    if (pipeline.pauseTimer) clearTimeout(pipeline.pauseTimer);
                    if (pipeline.transcriptionWs) pipeline.transcriptionWs.close();
                    if (pipeline.baselineWs) pipeline.baselineWs.close();
                    if (pipeline.turnWs) pipeline.turnWs.close();
                    rtmsCaptionPipelines.delete(streamId);
                }
                console.log('RTMS client and caption pipeline stopped for stream', streamId);
                return;
            }

            if (payload.event === 'meeting.rtms_started') {
                try {
                    const rtms = require('@zoom/rtms').default || require('@zoom/rtms');
                    const client = new rtms.Client();
                    if (streamId) rtmsClients.set(streamId, client);

                    // If the native client is an EventEmitter and ever emits
                    // 'error' with no listener attached, Node's default
                    // behavior is to crash the ENTIRE process -- which would
                    // wipe every unrelated in-memory session, not just this
                    // one. Attach a handler defensively even though the
                    // exact event surface of this native package isn't
                    // fully documented.
                    if (typeof client.on === 'function') {
                        client.on('error', (err) => {
                            console.error(`RTMS client error event for stream ${streamId}:`, err && err.message || err);
                        });
                    }

                    // Auto-create a bilingual caption session for this Zoom
                    // meeting, same way the leader onboarding flow does, so we
                    // get the exact same display/transcript/SSE infrastructure
                    // for free.
                    const sessionCode = createSession(process.env.OPENAI_API_KEY, 'far_field', 'bilingual');
                    lastRtmsSessionCode = sessionCode;
                    console.log(`RTMS: created bilingual caption session ${sessionCode} for stream ${streamId}`);
                    console.log(`RTMS: view captions at /display?session=${sessionCode}`);

                    const pipeline = {
                        sessionCode,
                        transcriptionWs: null, transcriptionReady: false,
                        baselineWs: null, baselineReady: false,
                        turnWs: null, turnReady: false,
                        spanishTurn: false,
                        pauseTimer: null,
                        awaitingSegmentSpace: false
                    };
                    rtmsCaptionPipelines.set(streamId, pipeline);

                    connectTranscriptionWs(pipeline);
                    connectTranslateWs(pipeline, 'es', 'baselineWs', 'baselineReady', 'spanish');

                    client.onAudioData((data, size, timestamp, metadata) => {
                        try {
                            const resampled = resamplePCM16(Buffer.from(data), 16000, 24000);
                            const b64 = resampled.toString('base64');
                            // Confirmed by a live error from OpenAI: the
                            // /realtime/translations endpoint uses different
                            // client event names than the plain /realtime
                            // endpoint -- 'session.input_audio_buffer.append',
                            // not 'input_audio_buffer.append'. Only 3 event
                            // types are valid there at all (session.update,
                            // session.input_audio_buffer.append, session.close)
                            // -- no commit, confirming translate sessions
                            // segment speech internally via built-in VAD.
                            const translateAppendMsg = JSON.stringify({ type: 'session.input_audio_buffer.append', audio: b64 });
                            const transcriptionAppendMsg = JSON.stringify({ type: 'input_audio_buffer.append', audio: b64 });

                            // Baseline (Spanish) always gets audio, same as the
                            // browser architecture -- suppressed at the
                            // BROADCAST level during a turn (see
                            // connectTranslateWs), not the audio-feed level,
                            // since OpenAI still needs continuous audio to
                            // avoid the session going stale.
                            if (pipeline.baselineWs && pipeline.baselineWs.readyState === WebSocket.OPEN) {
                                pipeline.baselineWs.send(translateAppendMsg);
                            }

                            if (pipeline.spanishTurn) {
                                if (pipeline.turnWs && pipeline.turnWs.readyState === WebSocket.OPEN) {
                                    pipeline.turnWs.send(translateAppendMsg);
                                }
                            } else if (pipeline.transcriptionWs && pipeline.transcriptionWs.readyState === WebSocket.OPEN) {
                                pipeline.transcriptionWs.send(transcriptionAppendMsg);
                                // Same pause-triggered commit design already
                                // proven out in the browser pipeline: bound the
                                // buffer by committing only after a real pause,
                                // never on a fixed timer (that caused mid-word
                                // chopping there).
                                if (pipeline.pauseTimer) clearTimeout(pipeline.pauseTimer);
                                pipeline.pauseTimer = setTimeout(() => {
                                    if (!pipeline.spanishTurn && pipeline.transcriptionWs && pipeline.transcriptionWs.readyState === WebSocket.OPEN) {
                                        pipeline.transcriptionWs.send(JSON.stringify({ type: 'input_audio_buffer.commit' }));
                                        pipeline.awaitingSegmentSpace = true;
                                    }
                                }, 2500);
                            }
                        } catch (err) {
                            console.error('RTMS audio processing error:', err.message);
                        }
                    });

                    client.join(payload.payload);
                    console.log('RTMS client.join() called -- waiting for audio frames...');
                } catch (err) {
                    console.error('RTMS client error:', err.message);
                }
            }
        });
    }

        // OAuth redirect target. Zoom sends the user here (with a one-time
        // ?code=...) after they click Allow on the app's consent screen. We
        // exchange that code for an access/refresh token, per Zoom's documented
        // authorization_code flow. This URL itself is what needs to be entered
    // as the app's development_redirect_uri in the Zoom console.
    else if (req.method === 'GET' && pathname === '/zoom/oauth/callback') {
        const code = parsedUrl.searchParams.get('code');
        if (!code) {
            res.writeHead(400, { 'Content-Type': 'text/html' });
            res.end('<p>Missing authorization code.</p>');
            return;
        }

        const clientId = process.env.ZOOM_CLIENT_ID;
        const clientSecret = process.env.ZOOM_CLIENT_SECRET;
        // Must exactly match the redirect_uri registered in the Zoom console,
        // including scheme, host, and path.
        const redirectUri = `${req.headers['x-forwarded-proto'] || 'https'}://${req.headers.host}/zoom/oauth/callback`;

        if (!clientId || !clientSecret) {
            res.writeHead(500, { 'Content-Type': 'text/html' });
            res.end('<p>Server is missing ZOOM_CLIENT_ID / ZOOM_CLIENT_SECRET.</p>');
            return;
        }

        // The outer request handler isn't declared async, so the actual
        // token-exchange work runs in this immediately-invoked async function.
        (async () => {
            try {
                const basicAuth = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
                const params = new URLSearchParams({
                    grant_type: 'authorization_code',
                    code,
                    redirect_uri: redirectUri
                });

                const tokenRes = await fetch('https://zoom.us/oauth/token', {
                    method: 'POST',
                    headers: {
                        'Authorization': `Basic ${basicAuth}`,
                        'Content-Type': 'application/x-www-form-urlencoded'
                    },
                    body: params.toString()
                });

                const tokenData = await tokenRes.json();

                if (!tokenRes.ok) {
                    console.error('Zoom OAuth token exchange failed:', tokenData);
                    res.writeHead(502, { 'Content-Type': 'text/html' });
                    res.end(`<p>Zoom rejected the token exchange: ${JSON.stringify(tokenData)}</p>`);
                    return;
                }

                // For now, just log it -- storing/using this token for further
                // API calls is a later step once the basic OAuth flow is proven
                // to work end to end.
                console.log('Zoom OAuth success. Scopes granted:', tokenData.scope);

                res.writeHead(200, { 'Content-Type': 'text/html' });
                res.end('<p>Recovery Translator is authorized. You can close this window and return to Zoom.</p>');
            } catch (err) {
                console.error('OAuth callback error:', err.message);
                res.writeHead(502, { 'Content-Type': 'text/html' });
                res.end('<p>Something went wrong contacting Zoom.</p>');
            }
        })();
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

    const session = sessions[sessionCode];
    if (!session) { ws.close(); return; }

    if (role === 'leader') {
        session.leaderSocket = ws;

        ws.on('message', (data) => {
            const msg = JSON.parse(data);

            if (msg.type === 'transcript') {
                broadcast(sessionCode, msg.language, msg.delta);
            }

            // Start/stop of a Spanish speaker's turn.
            if (msg.type === 'spanish_turn' && session.mode !== 'transcript_only') {
                session.spanishTurn = !!msg.active;
                broadcastControl(sessionCode, { type: 'spanish_turn', active: session.spanishTurn });
            }

            if (msg.type === 'approve') {
                const reqItem = session.speakQueue.find(r => r.id === msg.id);
                session.speakQueue = session.speakQueue.filter(r => r.id !== msg.id);
                if (reqItem) {
                    const entry = session.listenerSockets[reqItem.id];
                    if (entry && entry.ws.readyState === WebSocket.OPEN) {
                        entry.ws.send(JSON.stringify({ type: 'approved' }));
                    }
                }
            }

            if (msg.type === 'deny') {
                const reqItem = session.speakQueue.find(r => r.id === msg.id);
                session.speakQueue = session.speakQueue.filter(r => r.id !== msg.id);
                if (reqItem) {
                    const entry = session.listenerSockets[reqItem.id];
                    if (entry && entry.ws.readyState === WebSocket.OPEN) {
                        entry.ws.send(JSON.stringify({ type: 'denied' }));
                    }
                }
            }

            if (msg.type === 'end_session') {
                endSession(sessionCode);
            }

            if (msg.type === 'audio_offer' || msg.type === 'audio_ice') {
                const entry = session.listenerSockets[msg.listenerId];
                if (entry && entry.ws.readyState === WebSocket.OPEN) {
                    entry.ws.send(JSON.stringify(msg));
                }
            }
        });

        ws.on('close', () => {});

    } else if (role === 'listener') {
        const id = Math.random().toString(36).substring(2, 8);
        const language = params.get('language') || 'english';
        ws.listenerId = id;
        session.listenerSockets[id] = { ws, language };
        ws.send(JSON.stringify({ type: 'welcome', listenerId: id }));

        ws.on('message', (data) => {
            const msg = JSON.parse(data);

            if (msg.type === 'request_speak') {
                if (!session.speakQueue.find(r => r.id === id)) {
                    session.speakQueue.push({ id, language });
                }
                const leaderSocket = session.leaderSocket;
                if (leaderSocket && leaderSocket.readyState === WebSocket.OPEN) {
                    leaderSocket.send(JSON.stringify({ type: 'speak_request', id, language }));
                }
            }

            if (['request_audio', 'stop_audio', 'audio_answer', 'audio_ice'].includes(msg.type)) {
                const leaderSocket = session.leaderSocket;
                if (leaderSocket && leaderSocket.readyState === WebSocket.OPEN) {
                    leaderSocket.send(JSON.stringify({ ...msg, listenerId: id }));
                }
            }
        });

        ws.on('close', () => {
            delete session.listenerSockets[id];
            session.speakQueue = session.speakQueue.filter(r => r.id !== id);
        });
    }
});

function endSession(sessionCode) {
    const session = sessions[sessionCode];
    if (!session) return;
    const endMessage = `data: ${JSON.stringify({ type: 'session_ended' })}\n\n`;
    [...session.clients.english, ...session.clients.spanish].forEach(client => {
        try { client.write(endMessage); } catch (e) {}
    });
    setTimeout(() => {
        if (!sessions[sessionCode]) return;
        [...session.clients.english, ...session.clients.spanish].forEach(client => {
            try { client.end(); } catch (e) {}
        });
        if (session.leaderSocket) session.leaderSocket.close();
        Object.values(session.listenerSockets).forEach(entry => {
            try { entry.ws.close(); } catch (e) {}
        });
        delete sessions[sessionCode];
    }, 1000);
}