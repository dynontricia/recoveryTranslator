require('dotenv').config();
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');

// Safety net: an uncaught error anywhere -- especially from the native
// @zoom/rtms package, which we don't fully control -- would otherwise crash
// this entire process by Node's default behavior, wiping every in-memory
// session (including browser sessions that have nothing to do with Zoom).
// Log and keep running instead.
process.on('uncaughtException', (err) => {
    console.error('UNCAUGHT EXCEPTION (server kept running):', err && err.stack || err);
});
process.on('unhandledRejection', (reason) => {
    console.error('UNHANDLED REJECTION (server kept running):', reason);
});

const PORT = 3000;

// ---- Sessions ---------------------------------------------------------------
// sessions[code] = {
//   mode: 'audio_es' | 'audio_en' | 'audio_both',
//   audioSource: 'device' | 'zoom',
//   clients: { english: [SSE res...], spanish: [SSE res...] },
//   leaderSocket: ws | null,
//   leaderTeardownTimer,
//   listenerSockets: { [listenerId]: { ws, language, wantsAudio } },
//   zoomPaused: bool,   // host tapped "Stop captions" -- a stream stop is a pause
//   ended: bool,
//   createdAt
// }
const sessions = {};

// Every session has English and Spanish captions. The mode says which
// language(s) listeners can also hear as audio.
const SESSION_MODES = ['audio_es', 'audio_en', 'audio_both'];

function audioLanguages(session) {
    const mode = session && session.mode;
    if (mode === 'audio_en') return ['english'];
    if (mode === 'audio_both') return ['english', 'spanish'];
    return ['spanish'];
}

// Session codes are what let someone read a meeting's captions, so they come
// from a cryptographically secure source, not Math.random().
const CODE_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
function randomCode() {
    let code = '';
    for (let i = 0; i < 6; i++) code += CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
    return code;
}

function createSession(mode, audioSource) {
    let code;
    do { code = randomCode(); } while (sessions[code]);
    sessions[code] = {
        mode: SESSION_MODES.includes(mode) ? mode : 'audio_es',
        audioSource: audioSource === 'zoom' ? 'zoom' : 'device',
        clients: { english: [], spanish: [] },
        leaderSocket: null,
        listenerSockets: {},
        createdAt: Date.now()
    };
    return code;
}

function endSession(sessionCode) {
    const session = sessions[sessionCode];
    if (!session) return;
    session.ended = true;
    if (session.leaderTeardownTimer) clearTimeout(session.leaderTeardownTimer);
    teardownCaptionPipeline('leader-' + sessionCode);
    // Release any Zoom stream this session claimed (it can be claimed again).
    for (const [streamId, info] of rtmsStreamInfo) {
        if (info.sessionCode === sessionCode) { teardownCaptionPipeline(streamId); info.sessionCode = null; }
    }
    if (zoomWaitingSession && zoomWaitingSession.sessionCode === sessionCode) zoomWaitingSession = null;
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

// Caption text to every screen showing this language.
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

// Audio is PCM16 24kHz mono. Sent as a binary to every listener of this
// language who tapped the audio button.
function relayTranslatedAudio(sessionCode, language, pcmBuffer) {
    const session = sessions[sessionCode];
    if (!session) return;
    Object.entries(session.listenerSockets).forEach(([id, entry]) => {
        if (entry.language === language && entry.wantsAudio && entry.ws.readyState === WebSocket.OPEN) {
            entry.ws.send(pcmBuffer, { binary: true });
            entry.audioChunksSent = (entry.audioChunksSent || 0) + 1;
            if (entry.audioChunksSent === 1 || entry.audioChunksSent % 100 === 0) {
                console.log(`[${sessionCode}] audio: sent ${entry.audioChunksSent} chunk(s) to listener ${id}`);
            }
        }
    });
}

// ---- Zoom audio <-> session pairing ----------------------------------------
// A session started with "Zoom meeting" as its audio input waits for Zoom's
// audio stream; a Zoom stream that arrives first (e.g., RTMS auto-start)
// waits for a session. Whichever shows up second connects them. Until
// then, Zoom audio is ignored -- no Gemini connection, no cost.
// Pairs one meeting at a time: the most recent waiting session.
const rtmsClients = new Map();           // streamId -> RTMS client
let zoomWaitingSession = null;           // { sessionCode, at }
const rtmsStreamInfo = new Map();        // streamId -> { sessionCode|null, startedAt }
const ZOOM_WAIT_MAX_MS = 30 * 60 * 1000; // a waiting session expires after 30 min

function connectZoomAudio(sessionCode) {
    const session = sessions[sessionCode];
    if (!session || session.ended) return 'no-session';
    session.zoomPaused = false;
    // Already receiving Zoom audio -- nothing to do. (Re-marking it as
    // "waiting" would let the NEXT meeting's audio attach to it.)
    if ([...rtmsStreamInfo.values()].some(info => info.sessionCode === sessionCode)) return 'connected';
    // Zoom audio already flowing with no session? Claim the newest one now.
    const unclaimed = [...rtmsStreamInfo.entries()]
        .filter(([, info]) => !info.sessionCode)
        .sort((a, b) => b[1].startedAt - a[1].startedAt)[0];
    if (unclaimed) {
        attachRtmsStream(unclaimed[0], sessionCode);
        return 'connected';
    }
    zoomWaitingSession = { sessionCode, at: Date.now() };
    console.log(`[${sessionCode}] waiting for Zoom audio`);
    return 'waiting';
}

function takeWaitingZoomSession() {
    const w = zoomWaitingSession;
    if (!w) return null;
    const s = sessions[w.sessionCode];
    if (!s || s.ended || Date.now() - w.at > ZOOM_WAIT_MAX_MS) { zoomWaitingSession = null; return null; }
    return w.sessionCode;
}

function attachRtmsStream(streamId, sessionCode) {
    const info = rtmsStreamInfo.get(streamId);
    if (!info) return null;
    info.sessionCode = sessionCode;
    if (sessions[sessionCode]) sessions[sessionCode].zoomPaused = false;
    if (zoomWaitingSession && zoomWaitingSession.sessionCode === sessionCode) zoomWaitingSession = null;
    const pipeline = createCaptionPipeline(streamId, sessionCode);
    console.log(`[${sessionCode}] Zoom audio connected (stream ${streamId})`);
    return pipeline;
}

// The session a Zoom panel should show: one waiting for Zoom audio, else the
// newest session already receiving it.
function currentZoomSessionCode() {
    const waiting = takeWaitingZoomSession();
    if (waiting) return waiting;
    const newest = [...rtmsStreamInfo.values()]
        .filter(info => info.sessionCode && sessions[info.sessionCode] && !sessions[info.sessionCode].ended)
        .sort((a, b) => b.startedAt - a.startedAt)[0];
    return newest ? newest.sessionCode : null;
}

function startRtmsStream(streamId, payload) {
    const rtms = require('@zoom/rtms').default || require('@zoom/rtms');
    const client = new rtms.Client();
    if (streamId) rtmsClients.set(streamId, client);

    // If the native client ever emits 'error' with no listener attached,
    // Node crashes the whole process. Attach one defensively.
    if (typeof client.on === 'function') {
        client.on('error', (err) => {
            console.error(`RTMS client error event for stream ${streamId}:`, err && err.message || err);
        });
    }

    // Register the stream. It only gets a caption pipeline once a session
    // claims it (see connectZoomAudio).
    rtmsStreamInfo.set(streamId, { sessionCode: null, startedAt: Date.now() });
    const waitingCode = takeWaitingZoomSession();
    if (waitingCode) {
        attachRtmsStream(streamId, waitingCode);
    } else {
        console.log(`RTMS: stream ${streamId} started -- no session yet. Start one with "Zoom meeting" as the audio input.`);
    }

    // Look the pipeline up per frame, so audio flows the moment a session
    // claims this stream -- and stops if that session ends.
    client.onAudioData((data) => {
        const pipeline = rtmsCaptionPipelines.get(streamId);
        if (pipeline) feedPipelineAudio(pipeline, data);
    });

    // The SDK's default is Opus 48kHz stereo; we need raw 16kHz mono PCM16.
    client.setAudioParams({
        contentType: rtms.AudioContentType.RAW_AUDIO,
        codec: rtms.AudioCodec.L16,
        sampleRate: rtms.AudioSampleRate.SR_16K,
        channel: rtms.AudioChannel.MONO,
        dataOpt: rtms.AudioDataOption.AUDIO_MIXED_STREAM,
        duration: 20,
        frameSize: 320 // samples per 20ms frame at 16kHz mono -- 640-byte buffers
    });

    client.join(payload);
    console.log(`RTMS: joined stream ${streamId} -- waiting for audio frames`);
}

function stopRtmsStream(streamId) {
    const client = rtmsClients.get(streamId);
    if (client) { client.leave(); rtmsClients.delete(streamId); }

    const info = rtmsStreamInfo.get(streamId);
    teardownCaptionPipeline(streamId);
    rtmsStreamInfo.delete(streamId);
    console.log('RTMS: stream stopped', streamId);

    // The stream stopping ends the session for listeners -- unless the host
    // tapped "Stop captions" in the panel, which pauses.
    const session = info && info.sessionCode && sessions[info.sessionCode];
    if (!session || session.ended) return;
    if (session.zoomPaused) {
        console.log(`[${info.sessionCode}] Zoom audio paused by host -- session kept open`);
    } else {
        console.log(`[${info.sessionCode}] Zoom stream stopped -- ending session`);
        endSession(info.sessionCode);
    }
}

// ---- Gemini Live Translate --------------------------------------------------
// Each session runs two translators over the same audio: one into English and
// one into Spanish. Each gives captions (outputTranscription) and, when the
// session offers audio in that language, speech (modelTurn inlineData,
// PCM16 24kHz). echoTargetLanguage passes speech already in the target
// language through, so every speaker is captioned in both languages.
//
// Input is PCM16 16kHz -- what Zoom RTMS and the browser already send -- so
// no resampling. Google recommends ~100ms chunks; our frames are 20ms, so
// five are batched per sent package.
//
// A Gemini Live connection lasts ~10 minutes; Google sends `goAway` about a
// minute before it ends. For long meetings we do a rolling hand-off: open a
// fresh connection, switch audio to it once it's ready, and let the old one
// finish its last words before closing. Translation doesn't need memory of
// earlier speech, so no session resumption is needed. Unexpected drops
// reconnect on their own.
const GEMINI_TRANSLATE_MODEL = 'gemini-3.5-live-translate-preview';
const GEMINI_WS_URL = 'wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent';
const GEMINI_CHUNK_BYTES = 3200;               // 100ms of 16kHz mono PCM16
const GEMINI_BUFFER_MAX_BYTES = 3 * 32000;     // hold up to 3s of audio while (re)connecting
const GEMINI_ROTATE_MS = 9 * 60 * 1000;        // hand off before the ~10-minute limit
const GEMINI_HANDOFF_GRACE_MS = 5000;          // old connection's time to finish its last words
const GEMINI_RECONNECT_MAX_MS = 15000;
const GEMINI_LANGUAGE_CODES = { english: 'en', spanish: 'es' };

function createTranslator(sessionCode, language, withAudio) {
    const t = {
        sessionCode,
        language,              // 'english' | 'spanish'
        withAudio,             // generate speech, not just captions
        active: null,          // the connection audio goes to
        opening: null,         // a connection being set up (start or hand-off)
        pending: Buffer.alloc(0),
        closed: false,
        retryMs: 1000,
        retryTimer: null,
        loggedAudio: false
    };
    openGeminiConnection(t, 'start');
    return t;
}

function openGeminiConnection(t, why) {
    if (t.closed || t.opening) return;
    const key = process.env.GEMINI_API_KEY;
    if (!key) {
        console.error(`[${t.sessionCode}] gemini: GEMINI_API_KEY is not set -- no translation`);
        return;
    }
    // Key goes in the URL (Google's documented form) -- never log this URL.
    const ws = new WebSocket(`${GEMINI_WS_URL}?key=${encodeURIComponent(key)}`);
    const conn = { ws, ready: false, retiring: false, rotateTimer: null };
    t.opening = conn;
    console.log(`[${t.sessionCode}] gemini ${t.language}: connecting (${why})`);

    ws.on('open', () => {
        ws.send(JSON.stringify({
            setup: {
                model: `models/${GEMINI_TRANSLATE_MODEL}`,
                generationConfig: {
                    responseModalities: [t.withAudio ? 'AUDIO' : 'TEXT'],
                    translationConfig: {
                        targetLanguageCode: GEMINI_LANGUAGE_CODES[t.language],
                        echoTargetLanguage: true
                    }
                }
            }
        }));
    });

    ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

        if (msg.setupComplete !== undefined) {
            promoteGeminiConnection(t, conn);
            return;
        }
        if (msg.goAway) {
            console.log(`[${t.sessionCode}] gemini ${t.language}: goAway (time left ${msg.goAway.timeLeft || '?'}) -- handing off`);
            if (conn === t.active) openGeminiConnection(t, 'goAway hand-off');
            return;
        }
        if (msg.error) {
            console.error(`[${t.sessionCode}] gemini ${t.language} ERROR:`, JSON.stringify(msg.error));
            return;
        }

        const sc = msg.serverContent;
        if (!sc) return;

        if (sc.outputTranscription && sc.outputTranscription.text) {
            broadcast(t.sessionCode, t.language, sc.outputTranscription.text);
        }

        // Each translator's audio goes only to its own language's listeners,
        // so the two languages never mix in one listener's ears.
        if (t.withAudio && sc.modelTurn && sc.modelTurn.parts) {
            for (const part of sc.modelTurn.parts) {
                if (!part.inlineData || !part.inlineData.data) continue;
                if (!t.loggedAudio) {
                    t.loggedAudio = true;
                    console.log(`[${t.sessionCode}] gemini ${t.language}: producing audio (${part.inlineData.mimeType || 'pcm'})`);
                }
                relayTranslatedAudio(t.sessionCode, t.language, Buffer.from(part.inlineData.data, 'base64'));
            }
        }
    });

    ws.on('error', (err) => console.error(`[${t.sessionCode}] gemini ${t.language} WS error:`, err.message));

    ws.on('close', (code, reason) => {
        if (conn.rotateTimer) clearTimeout(conn.rotateTimer);
        if (t.closed || conn.retiring) return;
        const why = reason && reason.toString();
        console.warn(`[${t.sessionCode}] gemini ${t.language}: connection closed (code ${code}${why ? `: ${why}` : ''})`);
        if (t.opening === conn) t.opening = null;
        if (t.active === conn) t.active = null;
        // Unexpected drop: reconnect, backing off if it keeps failing.
        if (!t.active && !t.opening && !t.retryTimer) {
            t.retryTimer = setTimeout(() => {
                t.retryTimer = null;
                openGeminiConnection(t, 'reconnect');
            }, t.retryMs);
            t.retryMs = Math.min(t.retryMs * 2, GEMINI_RECONNECT_MAX_MS);
        }
    });
}

// A new connection is ready: send audio to it and retire the old one.
function promoteGeminiConnection(t, conn) {
    if (t.closed) { conn.ws.close(); return; }
    conn.ready = true;
    if (t.opening === conn) t.opening = null;
    const old = t.active;
    t.active = conn;
    t.retryMs = 1000;
    console.log(`[${t.sessionCode}] gemini ${t.language}: live${old ? ' (handed off)' : ''}`);
    if (old && old !== conn) retireGeminiConnection(old);
    // Hand off before Google's ~10-minute limit, even if goAway never comes.
    conn.rotateTimer = setTimeout(() => {
        if (t.active === conn) openGeminiConnection(t, 'scheduled hand-off');
    }, GEMINI_ROTATE_MS);
    flushGeminiAudio(t);
}

function retireGeminiConnection(conn) {
    conn.retiring = true;
    if (conn.rotateTimer) clearTimeout(conn.rotateTimer);
    // It gets no new audio; give it a moment to finish translating.
    setTimeout(() => { try { conn.ws.close(); } catch (e) {} }, GEMINI_HANDOFF_GRACE_MS);
}

function sendGeminiAudio(t, pcm16k) {
    if (t.closed) return;
    t.pending = Buffer.concat([t.pending, pcm16k]);
    flushGeminiAudio(t);
}

function flushGeminiAudio(t) {
    const ws = t.active && t.active.ready && t.active.ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        // Not connected right now: keep the most recent few seconds.
        if (t.pending.length > GEMINI_BUFFER_MAX_BYTES) {
            t.pending = t.pending.subarray(t.pending.length - GEMINI_BUFFER_MAX_BYTES);
        }
        return;
    }
    while (t.pending.length >= GEMINI_CHUNK_BYTES) {
        const chunk = t.pending.subarray(0, GEMINI_CHUNK_BYTES);
        t.pending = t.pending.subarray(GEMINI_CHUNK_BYTES);
        ws.send(JSON.stringify({
            realtimeInput: { audio: { data: chunk.toString('base64'), mimeType: 'audio/pcm;rate=16000' } }
        }));
    }
}

function closeTranslator(t) {
    t.closed = true;
    if (t.retryTimer) clearTimeout(t.retryTimer);
    for (const conn of [t.active, t.opening]) {
        if (!conn) continue;
        if (conn.rotateTimer) clearTimeout(conn.rotateTimer);
        try { conn.ws.close(); } catch (e) {}
    }
    t.active = t.opening = null;
}

// ---- Caption pipeline -------------------------------------------------------
// One per audio source: a Zoom RTMS stream, or a leader's browser mic. Both
// feed 16kHz mono PCM16 in 20ms frames, so they share all of this code.
const rtmsCaptionPipelines = new Map(); // streamKey -> pipeline

function createCaptionPipeline(streamKey, sessionCode) {
    const audio = audioLanguages(sessions[sessionCode]);
    const pipeline = {
        sessionCode,
        audioFrameCount: 0,
        translators: ['english', 'spanish'].map(lang => createTranslator(sessionCode, lang, audio.includes(lang)))
    };
    rtmsCaptionPipelines.set(streamKey, pipeline);
    return pipeline;
}

function feedPipelineAudio(pipeline, data) {
    try {
        pipeline.audioFrameCount++;
        // RTMS may hand us base64 text or binary; handle both.
        const pcm16k = typeof data === 'string' ? Buffer.from(data, 'base64') : Buffer.from(data);
        if (pipeline.audioFrameCount === 1) {
            console.log(`[${pipeline.sessionCode}] first audio frame -- byteLength=${pcm16k.length}`);
        }
        pipeline.translators.forEach(t => sendGeminiAudio(t, pcm16k));
    } catch (err) {
        console.error(`[${pipeline.sessionCode}] audio processing error:`, err.message);
    }
}

function teardownCaptionPipeline(streamKey) {
    const pipeline = rtmsCaptionPipelines.get(streamKey);
    if (!pipeline) return;
    pipeline.translators.forEach(closeTranslator);
    rtmsCaptionPipelines.delete(streamKey);
}

function findPipelineBySession(sessionCode) {
    for (const p of rtmsCaptionPipelines.values()) if (p.sessionCode === sessionCode) return p;
    return null;
}

// ---- HTTP -------------------------------------------------------------------
function serveFile(req, res, filePath, contentType, extraHeaders) {
    fs.readFile(filePath, (err, data) => {
        if (err) { res.writeHead(err.code === 'ENOENT' ? 404 : 500); res.end(); return; }
        res.writeHead(200, { 'Content-Type': contentType, ...(extraHeaders || {}) });
        res.end(req.method === 'HEAD' ? undefined : data);
    });
}

function sendJson(res, status, obj) {
    res.writeHead(status, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(obj));
}

// Reads a JSON request body; a missing or malformed body reads as {}.
// Zoom signs every webhook request (including the URL validation challenge):
//   x-zm-signature = "v0=" + HMAC-SHA256(secret, "v0:{timestamp}:{raw body}")
// Anything unsigned, wrongly signed, or older than 5 minutes is rejected, so
// nobody who finds the URL can fake "stream started/stopped" events -- or use
// the validation challenge to get our secret to sign text of their choosing.
const ZOOM_WEBHOOK_MAX_AGE_S = 300;

function verifyZoomSignature(req, rawBody) {
    const secret = process.env.ZOOM_WEBHOOK_SECRET_TOKEN;
    const timestamp = req.headers['x-zm-request-timestamp'];
    const signature = req.headers['x-zm-signature'];
    if (!secret || !timestamp || !signature) return false;
    if (Math.abs(Date.now() / 1000 - Number(timestamp)) > ZOOM_WEBHOOK_MAX_AGE_S) return false;
    const expected = 'v0=' + crypto.createHmac('sha256', secret).update(`v0:${timestamp}:${rawBody}`).digest('hex');
    const a = Buffer.from(signature), b = Buffer.from(expected);
    return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function readRawBody(req, callback) {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => callback(Buffer.concat(chunks).toString('utf8')));
}

function readJson(req, callback) {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
        let parsed = {};
        try { parsed = JSON.parse(body); } catch (e) {}
        callback(parsed || {});
    });
}

// Zoom's app review runs an automated OWASP header check, so every response
// carries these; HTML pages add a Content-Security-Policy of their own.
const SECURITY_HEADERS = {
    'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
    'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
    'Content-Security-Policy': "default-src 'none'; frame-ancestors 'none'"
};

// Browser pages: QR code library from cdnjs, the audio worklet loads from a
// blob: URL, and the QR code renders as a data: image.
const APP_PAGE_HEADERS = {
    'Content-Security-Policy': [
        "default-src 'self'",
        "script-src 'self' 'unsafe-inline' blob: https://cdnjs.cloudflare.com",
        "style-src 'self' 'unsafe-inline'",
        "img-src 'self' data:",
        "connect-src 'self'",
        "frame-ancestors 'self'"
    ].join('; ')
};

// The panel that runs inside the Zoom client.
const ZOOM_APP_HEADERS = {
    'Content-Security-Policy': [
        "default-src 'self'",
        "script-src 'self' 'unsafe-inline' https://appssdk.zoom.us",
        "style-src 'self' 'unsafe-inline'",
        "connect-src 'self' https://appssdk.zoom.us",
        "frame-ancestors 'self' https://*.zoom.us https://*.zoomgov.com"
    ].join('; ')
};

const STATIC_FILES = {
    '/': ['index.html', 'text/html; charset=UTF-8', APP_PAGE_HEADERS],
    '/index.html': ['index.html', 'text/html; charset=UTF-8', APP_PAGE_HEADERS],
    '/display': ['display.html', 'text/html; charset=UTF-8', APP_PAGE_HEADERS],
    '/display.html': ['display.html', 'text/html; charset=UTF-8', APP_PAGE_HEADERS],
    '/transcript': ['transcript.html', 'text/html; charset=UTF-8', APP_PAGE_HEADERS],
    '/transcript.html': ['transcript.html', 'text/html; charset=UTF-8', APP_PAGE_HEADERS],
    '/zoom-app': ['zoom-app.html', 'text/html; charset=UTF-8', ZOOM_APP_HEADERS],
    '/zoom-app.html': ['zoom-app.html', 'text/html; charset=UTF-8', ZOOM_APP_HEADERS],
    '/captions.js': ['captions.js', 'application/javascript; charset=UTF-8'],
    '/logo.png': ['recoveryTrans.png', 'image/png']
};

// The native @zoom/rtms package writes its own debug logs to /app/logs and
// fails repeatedly when that directory doesn't exist in the container.
try { fs.mkdirSync('/app/logs', { recursive: true }); } catch (e) {
    console.error('Could not create /app/logs (non-fatal):', e.message);
}

const server = http.createServer((req, res) => {
    const parsedUrl = new URL(req.url, `http://localhost:${PORT}`);
    const pathname = parsedUrl.pathname;
    for (const [name, value] of Object.entries(SECURITY_HEADERS)) res.setHeader(name, value);

    // HEAD too: header scanners often send HEAD rather than GET.
    if ((req.method === 'GET' || req.method === 'HEAD') && STATIC_FILES[pathname]) {
        const [file, contentType, headers] = STATIC_FILES[pathname];
        serveFile(req, res, path.join(__dirname, file), contentType, headers);
    }

    else if (req.method === 'POST' && pathname === '/session/create') {
        readJson(req, (body) => {
            if (body.accessCode !== process.env.ACCESS_CODE) {
                sendJson(res, 401, { error: 'Invalid access code' });
                return;
            }
            const code = createSession(body.mode, body.audioSource);
            const zoomAudio = body.audioSource === 'zoom' ? connectZoomAudio(code) : undefined;
            sendJson(res, 200, { sessionCode: code, zoomAudio });
        });
    }

    // Live captions for one language, as server-sent events.
    else if (req.method === 'GET' && pathname.startsWith('/stream/')) {
        const [, , sessionCode, language] = pathname.split('/');
        const session = sessions[sessionCode];
        if (!session) { sendJson(res, 404, { error: 'Session not found' }); return; }
        if (!['english', 'spanish'].includes(language)) { sendJson(res, 400, { error: 'Invalid language' }); return; }
        res.writeHead(200, {
            'Content-Type': 'text/event-stream; charset=UTF-8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
        });
        // A comment line sends the headers now, so the page shows
        // "Connected" before the first caption arrives.
        res.write(': connected\n\n');
        session.clients[language].push(res);
        req.on('close', () => {
            session.clients[language] = session.clients[language].filter(c => c !== res);
        });
    }

    // Which session the Zoom panel should show right now.
    else if (req.method === 'GET' && pathname === '/zoom/current-session') {
        const code = currentZoomSessionCode();
        sendJson(res, 200, code
            ? { sessionCode: code, mode: sessions[code].mode, audioConnected: !!findPipelineBySession(code) }
            : { sessionCode: null });
    }

    else if (req.method === 'GET' && pathname === '/zoom/session-status') {
        const code = parsedUrl.searchParams.get('code');
        const s = sessions[code];
        sendJson(res, 200, {
            exists: !!s && !s.ended,
            mode: s ? s.mode : null,
            audioConnected: !!findPipelineBySession(code)
        });
    }

        // Panel's "Start captions": claim Zoom audio for this session (now, or
    // as soon as the stream starts).
    else if (req.method === 'POST' && pathname === '/zoom/connect') {
        readJson(req, (body) => {
            const status = connectZoomAudio(body.sessionCode);
            sendJson(res, status === 'no-session' ? 404 : 200, { status });
        });
    }

        // Panel's "Stop captions": the stream stopping next is a pause, so the
    // session stays open for listeners.
    else if (req.method === 'POST' && pathname === '/zoom/pause') {
        readJson(req, (body) => {
            const s = sessions[body.sessionCode];
            if (s && !s.ended) s.zoomPaused = true;
            sendJson(res, s ? 200 : 404, { paused: !!s });
        });
    }

    // Lets the Zoom panel end a session (listeners see "session ended").
    else if (req.method === 'POST' && pathname === '/zoom/end-session') {
        readJson(req, (body) => {
            const s = sessions[body.sessionCode];
            if (!s || s.ended) { sendJson(res, 404, { error: 'No active session with that code' }); return; }
            endSession(body.sessionCode);
            sendJson(res, 200, { ended: true });
        });
    }

        // Zoom RTMS webhook:
        //   endpoint.url_validation -- Zoom's challenge when the URL is saved in
        //     the Zoom console; we echo back an HMAC-signed token.
        //   meeting.rtms_started / meeting.rtms_stopped -- join or leave the
    //     meeting's audio stream.
    else if (req.method === 'POST' && pathname === '/zoom/rtms-webhook') {
        readRawBody(req, (rawBody) => {
            if (!verifyZoomSignature(req, rawBody)) {
                console.warn('Zoom webhook rejected: missing, invalid, or expired signature');
                res.writeHead(401); res.end('Invalid signature'); return;
            }
            let payload = {};
            try { payload = JSON.parse(rawBody); } catch (e) {
                res.writeHead(400); res.end('Invalid JSON'); return;
            }
            if (payload.event === 'endpoint.url_validation') {
                const plainToken = payload.payload && payload.payload.plainToken;
                const secret = process.env.ZOOM_WEBHOOK_SECRET_TOKEN;
                if (!plainToken || !secret) {
                    console.error('RTMS validation failed: missing plainToken or ZOOM_WEBHOOK_SECRET_TOKEN');
                    res.writeHead(400); res.end('Missing token'); return;
                }
                const encryptedToken = crypto.createHmac('sha256', secret).update(plainToken).digest('hex');
                console.log('RTMS URL validation received, responding with signed token');
                sendJson(res, 200, { plainToken, encryptedToken });
                return;
            }

            // Log only the event and stream ID -- the payload also carries
            // Zoom meeting and user identifiers, which we don't keep.
            const streamId = payload.payload && payload.payload.rtms_stream_id;
            console.log('RTMS webhook event:', payload.event, streamId || '(no stream ID)');
            res.writeHead(200); res.end('ok');

            if (!streamId) return;
            if (payload.event === 'meeting.rtms_stopped') {
                stopRtmsStream(streamId);
            } else if (payload.event === 'meeting.rtms_started') {
                try {
                    startRtmsStream(streamId, payload.payload);
                } catch (err) {
                    console.error('RTMS client error:', err.message);
                }
            }
        });
    }

        // OAuth redirect target. Zoom sends the user here (with a one-time
        // ?code=...) after they click Allow on the app's consent screen. This URL
    // must match the redirect URI registered in the Zoom console.
    else if (req.method === 'GET' && pathname === '/zoom/oauth/callback') {
        const code = parsedUrl.searchParams.get('code');
        if (!code) {
            res.writeHead(400, { 'Content-Type': 'text/html' });
            res.end('<p>Missing authorization code.</p>');
            return;
        }
        const clientId = process.env.ZOOM_CLIENT_ID;
        const clientSecret = process.env.ZOOM_CLIENT_SECRET;
        if (!clientId || !clientSecret) {
            res.writeHead(500, { 'Content-Type': 'text/html' });
            res.end('<p>Server is missing ZOOM_CLIENT_ID / ZOOM_CLIENT_SECRET.</p>');
            return;
        }
        const redirectUri = `${req.headers['x-forwarded-proto'] || 'https'}://${req.headers.host}/zoom/oauth/callback`;

        (async () => {
            try {
                const tokenRes = await fetch('https://zoom.us/oauth/token', {
                    method: 'POST',
                    headers: {
                        'Authorization': `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`,
                        'Content-Type': 'application/x-www-form-urlencoded'
                    },
                    body: new URLSearchParams({ grant_type: 'authorization_code', code, redirect_uri: redirectUri }).toString()
                });
                const tokenData = await tokenRes.json();
                if (!tokenRes.ok) {
                    console.error('Zoom OAuth token exchange failed:', tokenData);
                    res.writeHead(502, { 'Content-Type': 'text/html' });
                    res.end('<p>Zoom rejected the authorization. Please try installing the app again.</p>');
                    return;
                }
                // The exchange completes Zoom's 'install flow'. The server makes
                // no Zoom API calls, so the tokens are not kept -- they're
                // discarded here and never written to disk or logs.
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

// ---- WebSockets -------------------------------------------------------------
//   role=leader   -- the leader's browser streams mic audio up (binary frames)
//                    and can end the session.
//   role=listener -- receives audio in its language (binary) after sending
//                    {type:'request_audio'}.
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
    const params = new URL(req.url, `http://localhost:${PORT}`).searchParams;
    const sessionCode = params.get('session');
    const role = params.get('role');
    const session = sessions[sessionCode];
    if (!session) { ws.close(); return; }

    if (role === 'leader') {
        session.leaderSocket = ws;
        const pipelineKey = 'leader-' + sessionCode;
        // Clicking reconnect (tab resume, network blip) cancels a pending teardown,
        // so the same pipeline keeps running.
        if (session.leaderTeardownTimer) {
            clearTimeout(session.leaderTeardownTimer);
            session.leaderTeardownTimer = null;
        }

        ws.on('message', (data, isBinary) => {
            // Binary = one 20ms frame of 16kHz mono PCM16 from the browser,
            // the same shape Zoom RTMS delivers.
            if (isBinary) {
                if (session.ended) return;
                let pipeline = rtmsCaptionPipelines.get(pipelineKey);
                if (!pipeline) {
                    pipeline = createCaptionPipeline(pipelineKey, sessionCode);
                    console.log(`[${sessionCode}] leader pipeline started`);
                }
                feedPipelineAudio(pipeline, data);
                return;
            }
            let msg;
            try { msg = JSON.parse(data); } catch (e) { return; }
            if (msg.type === 'end_session') endSession(sessionCode);
        });

        ws.on('close', () => {
            // Only the CURRENT leader socket closing matters; a stale one
            // replaced by a reconnecting call must not tear anything down.
            if (session.leaderSocket !== ws || session.ended) return;
            // Give the leader a minute to come back before stopping
            // translation, so a brief drop doesn't interrupt the meeting.
            session.leaderTeardownTimer = setTimeout(() => {
                teardownCaptionPipeline(pipelineKey);
                console.log(`[${sessionCode}] leader gone 60s -- pipeline stopped`);
            }, 60000);
        });

    } else if (role === 'listener') {
        const id = randomCode();
        const language = params.get('language') === 'spanish' ? 'spanish' : 'english';
        session.listenerSockets[id] = { ws, language, wantsAudio: false };
        // Tells the page whether to offer the audio button for this language.
        ws.send(JSON.stringify({ type: 'welcome', listenerId: id, audio: audioLanguages(session).includes(language) }));

        ws.on('message', (data) => {
            let msg;
            try { msg = JSON.parse(data); } catch (e) { return; }
            const entry = session.listenerSockets[id];
            if (!entry) return;
            if (msg.type === 'request_audio') {
                entry.wantsAudio = true;
                console.log(`[${sessionCode}] listener ${id} (${language}) turned audio on`);
            }
            if (msg.type === 'stop_audio') entry.wantsAudio = false;
        });

        ws.on('close', () => {
            delete session.listenerSockets[id];
        });
    } else {
        ws.close();
    }
});