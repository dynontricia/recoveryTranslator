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

// Zoom OAuth access token, captured at install time. Used to auto-fetch each
// meeting's closed-caption token so the host doesn't paste it every meeting.
// In-memory only, so it's lost on restart -- manual paste remains the
// fallback. Persisting (and refreshing) it is a future improvement.
let zoomAccessToken = null;

// RTMS's default audio is 16kHz mono PCM16, but OpenAI's realtime API
// requires 24kHz. 16000->24000 is a clean 2:3 ratio, so a simple linear
// interpolation resampler is enough -- no need for a heavier audio library
// or another native dependency (we've had enough native-package risk
// tonight with @zoom/rtms already).
// RTMS's own documentation describes its wire format as "base64-encoded
// binary packets" -- but it was never confirmed whether the @zoom/rtms
// package's onAudioData callback hands us that base64 text directly or
// already-decoded binary. Buffer.from(data) with no encoding treats a
// string as UTF-8 text, silently corrupting it if it's actually base64 --
// a very plausible explanation for garbled captions and an eventual
// server-side error from feeding OpenAI corrupted audio. Handle both cases
// correctly rather than assume either.
// Posts caption text into Zoom's own native caption bar via the host's
// third-party captioning URL ("Copy the API token" in the meeting's caption
// menu, or fetched via GET /meetings/{id}/token).
//
// Protocol per Zoom's docs: append &seq=N&lang=xx-XX to the URL, send the
// caption text as a RAW text/plain body (explicitly NOT form-encoded), and
// keep seq incrementing continuously across the whole meeting. Zoom
// recommends retrying with randomized exponential backoff, giving up after
// roughly 5 seconds so we move on to the next caption rather than block.
// Zoom treats every POST as its own caption line, and OpenAI's deltas arrive
// as tiny fragments (often a word or two), so posting each delta directly
// produced a line break every couple of words. Buffer deltas per language
// and flush on a natural boundary instead: either sentence-ending
// punctuation, a max length, or a short pause in new text arriving.
const CAPTION_FLUSH_PAUSE_MS = 1200;
const CAPTION_MAX_CHARS = 180;

// Recovery-specific hints for speech recognition and text translation.
// Keep these as literal terms/phrases likely to be spoken in meetings.
const RECOVERY_KEYWORDS = [
    'home group', 'group conscience', 'trusted servant', 'sponsor', 'sponsee',
    'amends', 'inventory', 'Higher Power', 'primary purpose',
    'General Service Representative', 'GSR', 'District Committee Member', 'DCM',
    'Area Delegate', 'service position', 'sobriety date', 'newcomer'
];

const RECOVERY_GLOSSARY = `
Use recovery-fellowship terminology consistently. Preferred English terms include:
- grupo base / grupo de origen -> home group
- conciencia de grupo -> group conscience
- padrino / madrina (recovery context) -> sponsor
- ahijado / ahijada (recovery context) -> sponsee
- servidor de confianza -> trusted servant
- propósito primordial -> primary purpose
- enmiendas / reparar daños (Steps context) -> amends / making amends
- inventario (Steps context) -> inventory
- Poder Superior -> Higher Power
- representante de servicios generales -> General Service Representative (GSR)
Preserve fellowship names, Step/Tradition/Concept numbers, acronyms, and proper names.
`;

function queueZoomCaption(pipeline, text, lang) {
    if (!pipeline.zoomCaptionUrl || !text) return;

    pipeline.captionBuffers = pipeline.captionBuffers || {};
    pipeline.captionFlushTimers = pipeline.captionFlushTimers || {};
    pipeline.captionBuffers[lang] = (pipeline.captionBuffers[lang] || '') + text;

    const buffered = pipeline.captionBuffers[lang];

    // Flush immediately on a sentence boundary or when the line gets long,
    // so captions stay readable rather than growing unboundedly.
    if (/[.!?¡¿]\s*$/.test(buffered) || buffered.length >= CAPTION_MAX_CHARS) {
        flushZoomCaption(pipeline, lang);
        return;
    }

    // Otherwise flush after a brief pause in new text -- that's an utterance
    // boundary in practice (speaker paused), which is where a line break
    // actually belongs.
    if (pipeline.captionFlushTimers[lang]) clearTimeout(pipeline.captionFlushTimers[lang]);
    pipeline.captionFlushTimers[lang] = setTimeout(() => {
        flushZoomCaption(pipeline, lang);
    }, CAPTION_FLUSH_PAUSE_MS);
}

function flushZoomCaption(pipeline, lang) {
    if (!pipeline.captionBuffers) return;
    const text = (pipeline.captionBuffers[lang] || '').trim();
    pipeline.captionBuffers[lang] = '';
    if (pipeline.captionFlushTimers && pipeline.captionFlushTimers[lang]) {
        clearTimeout(pipeline.captionFlushTimers[lang]);
        pipeline.captionFlushTimers[lang] = null;
    }
    if (text) postZoomCaption(pipeline, text, lang);
}

async function postZoomCaption(pipeline, text, lang) {
    if (!pipeline.zoomCaptionUrl || !text || !text.trim()) return;

    const seq = pipeline.captionSeq++;
    const sep = pipeline.zoomCaptionUrl.includes('?') ? '&' : '?';
    const url = `${pipeline.zoomCaptionUrl}${sep}seq=${seq}&lang=${lang}`;

    let delayMs = 100;
    const startedAt = Date.now();
    while (Date.now() - startedAt < 5000) {
        try {
            const res = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'text/plain; charset=utf-8' },
                body: text
            });
            if (res.ok) return;
            // Non-OK: fall through to the retry/backoff below.
            if (seq % 25 === 0) {
                console.log(`Zoom caption POST non-OK (seq ${seq}): HTTP ${res.status}`);
            }
        } catch (err) {
            if (seq % 25 === 0) {
                console.error(`Zoom caption POST error (seq ${seq}):`, err.message);
            }
        }
        await new Promise(r => setTimeout(r, Math.random() * delayMs));
        delayMs = Math.min(delayMs * 2, 1600);
    }
    console.error(`Zoom caption POST gave up after ~5s (seq ${seq})`);
}

function toAudioBuffer(data) {
    if (typeof data === 'string') {
        return Buffer.from(data, 'base64');
    }
    return Buffer.from(data);
}

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

// Opens the Spanish speech-translation WebSocket. This remains the always-on
// listening feed for Spanish attendees. English captions no longer come from
// gpt-realtime-translate; they come from the multilingual transcription path
// below so same-language English is never unnecessarily regenerated.
function connectTranslateWs(pipeline, targetLanguage, wsKey, readyKey, broadcastLanguage) {
    const apiKey = sessions[pipeline.sessionCode].apiKey;
    const ws = new WebSocket('wss://api.openai.com/v1/realtime/translations?model=gpt-realtime-translate', {
        headers: { 'Authorization': `Bearer ${apiKey}`, 'OpenAI-Safety-Identifier': 'recovery-translator' }
    });
    pipeline[wsKey] = ws;
    pipeline[readyKey] = false;

    ws.on('open', () => {
        console.log(`RTMS/OpenAI [${pipeline.sessionCode}] translate(${targetLanguage}): connected`);
        ws.send(JSON.stringify({ type: 'session.update', session: { audio: { output: { language: targetLanguage } } } }));
    });

    ws.on('message', (raw) => {
        let ev;
        try { ev = JSON.parse(raw.toString()); } catch (e) { return; }

        if (ev.type === 'session.updated') pipeline[readyKey] = true;
        if (ev.type === 'error') {
            console.error(`RTMS/OpenAI [${pipeline.sessionCode}] translate(${targetLanguage}) ERROR:`, JSON.stringify(ev.error || ev));
        }
        if (ev.type === 'session.output_transcript.delta' && ev.delta) {
            broadcast(pipeline.sessionCode, broadcastLanguage, ev.delta);
        }
    });

    ws.on('error', (err) => console.error(`RTMS/OpenAI [${pipeline.sessionCode}] translate(${targetLanguage}) WS error:`, err.message));
    ws.on('close', (code) => console.log(`RTMS/OpenAI [${pipeline.sessionCode}] translate(${targetLanguage}): closed. code=${code}`));
}

// Translate a completed non-English transcript to English. This is deliberately
// text-to-text: it gives us glossary control and avoids asking the speech
// translation model to perform English -> English passthrough.
async function translateTranscriptToEnglish(pipeline, transcript, sourceLanguage) {
    const session = sessions[pipeline.sessionCode];
    if (!session || !transcript || !transcript.trim()) return;

    try {
        const response = await fetch('https://api.openai.com/v1/responses', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${session.apiKey}`,
                'Content-Type': 'application/json',
                'OpenAI-Safety-Identifier': 'recovery-translator'
            },
            body: JSON.stringify({
                model: 'gpt-5.4-mini',
                instructions: `Translate live recovery-meeting speech into natural English captions.\n` +
                    `Do not summarize, explain, censor, or add information. Preserve first-person voice and tone.\n` +
                    `Return ONLY the English caption text.\n${RECOVERY_GLOSSARY}`,
                input: `Source language: ${sourceLanguage || 'unknown'}\nTranscript: ${transcript}`,
                max_output_tokens: 300
            })
        });

        const data = await response.json();
        if (!response.ok) {
            console.error(`RTMS/OpenAI [${pipeline.sessionCode}] text translation failed:`, JSON.stringify(data).slice(0, 1000));
            return;
        }

        const english = (data.output || [])
            .flatMap(item => item.content || [])
            .filter(part => part.type === 'output_text')
            .map(part => part.text || '')
            .join('')
            .trim();

        if (english) {
            broadcast(pipeline.sessionCode, 'english', english + ' ');
            queueZoomCaption(pipeline, english, 'en-US');
        }
    } catch (err) {
        console.error(`RTMS/OpenAI [${pipeline.sessionCode}] text translation error:`, err.message);
    }
}

// Multilingual transcription is the canonical source for English captions.
// gpt-transcribe is used rather than gpt-live-transcribe because completed
// events include detected language(s), which lets us bypass translation for
// English and invoke glossary-controlled text translation for other languages.
function connectCaptionTranscriptionWs(pipeline) {
    const apiKey = sessions[pipeline.sessionCode].apiKey;
    const ws = new WebSocket('wss://api.openai.com/v1/realtime?model=gpt-realtime', {
        headers: { 'Authorization': `Bearer ${apiKey}`, 'OpenAI-Safety-Identifier': 'recovery-translator' }
    });
    pipeline.transcribeWs = ws;
    pipeline.transcribeReady = false;

    ws.on('open', () => {
        console.log(`RTMS/OpenAI [${pipeline.sessionCode}] multilingual transcription: connected`);
        ws.send(JSON.stringify({
            type: 'session.update',
            session: {
                type: 'transcription',
                audio: {
                    input: {
                        format: { type: 'audio/pcm', rate: 24000 },
                        transcription: {
                            model: 'gpt-transcribe',
                            prompt: 'A live peer-recovery fellowship meeting. Transcribe exactly what the speaker says. Preserve recovery terminology, acronyms, names, Step/Tradition/Concept numbers, and code-switching.',
                            keywords: RECOVERY_KEYWORDS,
                            languages: ['en', 'es'],
                        },
                        // Semantic VAD is intentionally less eager here because
                        // recovery shares often contain meaningful pauses.
                        turn_detection: { type: 'semantic_vad', eagerness: 'low' }
                    }
                }
            }
        }));
    });

    ws.on('message', (raw) => {
        let ev;
        try { ev = JSON.parse(raw.toString()); } catch (e) { return; }

        if (ev.type === 'session.updated') {
            pipeline.transcribeReady = true;
            console.log(`RTMS/OpenAI [${pipeline.sessionCode}] multilingual transcription: live`);
            return;
        }
        if (ev.type === 'error') {
            console.error(`RTMS/OpenAI [${pipeline.sessionCode}] transcription ERROR:`, JSON.stringify(ev.error || ev));
            return;
        }
        if (ev.type !== 'conversation.item.input_audio_transcription.completed') return;

        const transcript = (ev.transcript || '').trim();
        if (!transcript) return;

        const detected = Array.isArray(ev.languages) && ev.languages.length
            ? ev.languages[0].code
            : null;
        console.log(`RTMS/OpenAI [${pipeline.sessionCode}] completed transcript language=${detected || 'unknown'}: ${transcript.slice(0, 160)}`);

        if (detected === 'en' || detected === 'eng') {
            broadcast(pipeline.sessionCode, 'english', transcript + ' ');
            queueZoomCaption(pipeline, transcript, 'en-US');
        } else {
            // If language detection is uncertain, translating to English is the
            // safer caption behavior: English text generally survives unchanged,
            // while non-English text becomes usable for the room.
            translateTranscriptToEnglish(pipeline, transcript, detected);
        }
    });

    ws.on('error', (err) => console.error(`RTMS/OpenAI [${pipeline.sessionCode}] transcription WS error:`, err.message));
    ws.on('close', (code) => console.log(`RTMS/OpenAI [${pipeline.sessionCode}] transcription closed. code=${code}`));
}

// Fetches the meeting's closed-caption token automatically so the host
// doesn't have to paste it every meeting. Zoom's UUIDs contain characters
// (/ + =) that MUST be double-URL-encoded -- skipping that is the documented
// cause of "3001 Meeting does not exist" errors on this endpoint.
async function fetchZoomCaptionUrl(pipeline, meetingUuid, accessToken) {
    if (!meetingUuid || !accessToken) return false;
    const encoded = encodeURIComponent(encodeURIComponent(meetingUuid));
    try {
        const res = await fetch(`https://api.zoom.us/v2/meetings/${encoded}/token?type=closed_caption`, {
            headers: { 'Authorization': `Bearer ${accessToken}` }
        });
        const data = await res.json();
        if (res.ok && data.token) {
            pipeline.zoomCaptionUrl = data.token;
            console.log(`RTMS [${pipeline.sessionCode}]: caption token fetched automatically -- no manual paste needed`);
            return true;
        }
        console.log(`RTMS [${pipeline.sessionCode}]: auto caption-token fetch failed (HTTP ${res.status}): ${JSON.stringify(data).slice(0, 200)}. Host can still paste the token manually.`);
    } catch (err) {
        console.error(`RTMS [${pipeline.sessionCode}]: auto caption-token fetch error:`, err.message);
    }
    return false;
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
    else if (pathname === '/test-dual-feed' || pathname === '/test-dual-feed.html') {
        serveFile(res, './test-dual-feed.html', 'text/html; charset=UTF-8');
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
        // App panel. Now a no-op -- dual-feed removed the need for a toggle.
        // leader flow, just driven by sessionCode instead of a button on the
        // leader screen.
        // Receives the host's Zoom closed-caption token URL ("Copy the API token"
        // from the meeting's caption menu), enabling captions to appear in Zoom's
    // own native caption bar rather than only on our display page.
    else if (req.method === 'POST' && pathname === '/zoom/caption-url') {
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });
        req.on('end', () => {
            let parsed;
            try { parsed = JSON.parse(body); } catch (e) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Invalid JSON' }));
                return;
            }
            const { sessionCode, captionUrl } = parsed;
            let pipeline = null;
            for (const p of rtmsCaptionPipelines.values()) {
                if (p.sessionCode === sessionCode) { pipeline = p; break; }
            }
            if (!pipeline) {
                const known = Array.from(rtmsCaptionPipelines.values()).map(p => p.sessionCode);
                console.log(`RTMS caption-url: no pipeline for "${sessionCode}". Active pipelines: ${known.length ? known.join(', ') : '(none)'}`);
                res.writeHead(404, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({
                    error: known.length
                        ? `No active pipeline for "${sessionCode}". Active right now: ${known.join(', ')}. The RTMS stream likely restarted -- stop and start captions again.`
                        : `No active RTMS pipeline. The stream has stopped (meeting ended or server restarted). Start captions again.`
                }));
                return;
            }
            if (!captionUrl || !/^https:\/\/[^\s]+$/i.test(captionUrl)) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'captionUrl must be a valid https URL' }));
                return;
            }
            pipeline.zoomCaptionUrl = captionUrl;
            console.log(`RTMS [${sessionCode}]: Zoom caption URL set -- captions will now also post to Zoom's native caption bar`);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: true }));
        });
    }

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
            // Dual-feed made this obsolete: both languages now run
            // continuously, so there is no "turn" to switch. Kept as a
            // harmless no-op so any older panel build doesn't error.
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ spanishTurn: false, note: 'Both languages now run continuously; no toggle needed.' }));
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
                    // Flush any buffered caption text before tearing down, so
                    // the last words spoken aren't silently dropped.
                    if (pipeline.captionBuffers) {
                        Object.keys(pipeline.captionBuffers).forEach(lang => flushZoomCaption(pipeline, lang));
                    }
                    if (pipeline.transcribeWs) pipeline.transcribeWs.close();
                    if (pipeline.esWs) pipeline.esWs.close();
                    rtmsCaptionPipelines.delete(streamId);
                    // Don't leave the panel pointing at a pipeline that no
                    // longer exists -- that produces a confusing 404 when the
                    // host later tries to set a caption URL.
                    if (lastRtmsSessionCode === pipeline.sessionCode) {
                        lastRtmsSessionCode = null;
                    }
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
                        transcribeWs: null, transcribeReady: false,
                        esWs: null, esReady: false,
                        zoomCaptionUrl: null,
                        captionSeq: 1
                    };
                    rtmsCaptionPipelines.set(streamId, pipeline);

                    // Split pipeline:
                    //   1) multilingual transcription -> canonical English captions
                    //   2) realtime speech translation -> Spanish listener feed
                    connectCaptionTranscriptionWs(pipeline);
                    connectTranslateWs(pipeline, 'es', 'esWs', 'esReady', 'spanish');

                    // Try to fetch the caption token automatically so the host
                    // doesn't paste it each meeting. Falls back silently to
                    // manual paste if this doesn't work.
                    if (zoomAccessToken && payload.payload && payload.payload.meeting_uuid) {
                        fetchZoomCaptionUrl(pipeline, payload.payload.meeting_uuid, zoomAccessToken);
                    }

                    let audioFrameCount = 0;
                    client.onAudioData((data, size, timestamp, metadata) => {
                        try {
                            audioFrameCount++;
                            if (audioFrameCount === 1) {
                                console.log(`RTMS [${pipeline.sessionCode}]: first audio frame -- byteLength=${data && data.byteLength}, size=${size}`);
                            }

                            const resampled = resamplePCM16(toAudioBuffer(data), 16000, 24000);
                            // The /realtime/translations endpoint requires the
                            // 'session.' prefix on this event name (confirmed
                            // by a live rejection from OpenAI). Translate
                            // sessions also segment speech internally via
                            // built-in VAD, so there is no commit to send.
                            const appendMsg = JSON.stringify({
                                type: 'session.input_audio_buffer.append',
                                audio: resampled.toString('base64')
                            });

                            if (audioFrameCount % 250 === 1) {
                                console.log(`RTMS [${pipeline.sessionCode}]: frame ${audioFrameCount}, transcribeWs=${pipeline.transcribeWs && pipeline.transcribeWs.readyState}, esWs=${pipeline.esWs && pipeline.esWs.readyState}`);
                            }

                            // The translation endpoint uses the session-prefixed
                            // append event. The standard Realtime transcription
                            // endpoint uses input_audio_buffer.append.
                            if (pipeline.transcribeWs && pipeline.transcribeWs.readyState === WebSocket.OPEN) {
                                pipeline.transcribeWs.send(JSON.stringify({
                                    type: 'input_audio_buffer.append',
                                    audio: resampled.toString('base64')
                                }));
                            }
                            if (pipeline.esWs && pipeline.esWs.readyState === WebSocket.OPEN) {
                                pipeline.esWs.send(appendMsg);
                            }
                        } catch (err) {
                            console.error('RTMS audio processing error:', err.message);
                        }
                    });

                    // Confirmed by a Zoom-filed bug report (zoom/rtms#92):
                    // the SDK's DEFAULT audio format is compressed Opus at
                    // 48kHz stereo -- not the simple L16/16kHz/mono raw PCM
                    // the raw WebSocket protocol defaults to. Without this,
                    // we were resampling compressed audio as if it were raw
                    // PCM samples, producing garbage. Explicitly request the
                    // format our pipeline is actually built for.
                    if (typeof client.setAudioParams === 'function') {
                        client.setAudioParams({
                            contentType: rtms.AudioContentType.RAW_AUDIO,
                            codec: rtms.AudioCodec.L16,
                            sampleRate: rtms.AudioSampleRate.SR_16K,
                            channel: rtms.AudioChannel.MONO,
                            dataOpt: rtms.AudioDataOption.AUDIO_MIXED_STREAM,
                            duration: 20,
                            frameSize: 320 // samples per 20ms frame at 16kHz mono -- yields 640-byte buffers
                        });
                        console.log(`RTMS [${sessionCode}]: requested raw L16 16kHz mono audio explicitly`);
                    } else {
                        console.error(`RTMS [${sessionCode}]: client.setAudioParams is not a function on this SDK version -- audio format may default to compressed Opus and produce garbage.`);
                    }

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
                zoomAccessToken = tokenData.access_token;
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
            console.log('message:', msg);
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
            console.log('list message:', msg);

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