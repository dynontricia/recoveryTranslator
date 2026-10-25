require('dotenv').config();
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');
const store = require('./store');
const { LANGUAGES, LANGUAGE_ORDER, normalizeLanguage } = require('./languages');

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
//   languages: ['en', 'es', ...],  // caption languages (see languages.js)
//   audio: ['es', ...],            // the ones listeners can also hear
//   audioSource: 'device' | 'zoom',
//   clients: { en: [SSE res...], es: [...], ... },  // one list per caption language
//   leaderSocket: ws | null,
//   leaderTeardownTimer,
//   listenerSockets: { [listenerId]: { ws, language, wantsAudio } },
//   zoomPaused: bool,   // host tapped "Stop captions" -- a stream stop is a pause
//   zoomWaiting: bool,  // wants Zoom audio; the next stream from its meeting attaches
//   meetingId: string | null,  // the Zoom meeting this session captions
//   leaderToken,        // secret given only to whoever started the session
//   geminiKey,          // this session's Gemini key (the group's, or the server's); memory only
//   linkCode: string | null,   // one-time code to link a browser-started session to a meeting
//   ended: bool,
//   createdAt
// }
const sessions = {};

// The leader picks the caption languages and which of them are also spoken.
// Each caption language is one more Gemini translator on the same audio, so
// cost grows with the number of languages. Speaking a language implies
// captions in it. With no choice (or an old page sending `mode`), sessions
// get English and Spanish captions with Spanish audio, as before.
const LEGACY_MODES = { audio_es: ['es'], audio_en: ['en'], audio_both: ['en', 'es'] };

function parseLanguageChoice(body) {
    const pick = list => (Array.isArray(list) ? list : []).map(normalizeLanguage).filter(Boolean);
    let languages = pick(body.languages);
    let audio = pick(body.audio);
    if (!languages.length) {
        languages = ['en', 'es'];
        if (!audio.length) audio = LEGACY_MODES[body.mode] || ['es'];
    }
    languages = LANGUAGE_ORDER.filter(l => languages.includes(l) || audio.includes(l));
    audio = languages.filter(l => audio.includes(l));
    return { languages, audio };
}

function audioLanguages(session) {
    return (session && session.audio) || [];
}

// Session codes are what let someone read a meeting's captions, so they come
// from a cryptographically secure source, not Math.random().
const CODE_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
function randomCode() {
    let code = '';
    for (let i = 0; i < 6; i++) code += CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
    return code;
}

// Compares two secrets without leaking, through timing, how much matched.
function safeEqual(a, b) {
    if (typeof a !== 'string' || typeof b !== 'string') return false;
    const x = Buffer.from(a), y = Buffer.from(b);
    return x.length === y.length && crypto.timingSafeEqual(x, y);
}

// meetingId: set when the session is started from the Zoom panel, so it only
// ever gets that meeting's audio. A Zoom session started in the browser has
// no meeting yet; it gets a one-time link code the leader types into the
// panel in their meeting.
function createSession(choice, audioSource, meetingId, geminiKey) {
    let code;
    do { code = randomCode(); } while (sessions[code]);
    const zoom = audioSource === 'zoom';
    sessions[code] = {
        languages: choice.languages,
        audio: choice.audio,
        audioSource: zoom ? 'zoom' : 'device',
        clients: Object.fromEntries(choice.languages.map(l => [l, []])),
        leaderSocket: null,
        listenerSockets: {},
        meetingId: zoom && meetingId ? meetingId : null,
        leaderToken: crypto.randomBytes(24).toString('hex'),
        geminiKey,
        linkCode: zoom && !meetingId ? randomCode() : null,
        zoomWaiting: false,
        createdAt: Date.now()
    };
    return code;
}

function endSession(sessionCode) {
    const session = sessions[sessionCode];
    if (!session) return;
    session.ended = true;
    session.geminiKey = null;
    if (session.leaderTeardownTimer) clearTimeout(session.leaderTeardownTimer);
    teardownCaptionPipeline('leader-' + sessionCode);
    // Release any Zoom stream this session claimed (it can be claimed again).
    for (const [streamId, info] of rtmsStreamInfo) {
        if (info.sessionCode === sessionCode) { teardownCaptionPipeline(streamId); info.sessionCode = null; }
    }
    session.zoomWaiting = false;
    const endMessage = `data: ${JSON.stringify({ type: 'session_ended' })}\n\n`;
    Object.values(session.clients).flat().forEach(client => {
        try { client.write(endMessage); } catch (e) {}
    });
    setTimeout(() => {
        if (!sessions[sessionCode]) return;
        Object.values(session.clients).flat().forEach(client => {
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
    if (!session || !session.clients[language]) return;
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
// Every Zoom session belongs to one meeting (session.meetingId), and every
// RTMS stream says which meeting it came from (the webhook's meeting_uuid).
// A stream only ever attaches to a session from the SAME meeting, so two
// groups captioning at the same time can never get each other's audio.
//
// A session that wants Zoom audio is marked zoomWaiting; a stream that
// arrives first (e.g., RTMS auto-start) waits unclaimed. Whichever shows up
// second connects them. Until then, Zoom audio is ignored -- no Gemini
// connection, no cost.
const rtmsClients = new Map();     // streamId -> RTMS client
const rtmsStreamInfo = new Map();  // streamId -> { sessionCode|null, meetingId, startedAt }

// Meeting IDs are Zoom identifiers we don't keep, so logs show only a short
// one-way hash -- enough to check that the panel and the stream agree.
function meetingTag(meetingId) {
    return meetingId ? '#' + crypto.createHash('sha256').update(meetingId).digest('hex').slice(0, 8) : '#none';
}

function connectZoomAudio(sessionCode) {
    const session = sessions[sessionCode];
    if (!session || session.ended) return 'no-session';
    // A browser-started session must be linked to a meeting first.
    if (!session.meetingId) return 'not-linked';
    session.zoomPaused = false;
    // Already receiving Zoom audio -- nothing to do.
    if ([...rtmsStreamInfo.values()].some(info => info.sessionCode === sessionCode)) return 'connected';
    // This meeting's audio already flowing with no session? Claim it now.
    const unclaimed = [...rtmsStreamInfo.entries()]
        .filter(([, info]) => !info.sessionCode && info.meetingId === session.meetingId)
        .sort((a, b) => b[1].startedAt - a[1].startedAt)[0];
    if (unclaimed) {
        attachRtmsStream(unclaimed[0], sessionCode);
        return 'connected';
    }
    session.zoomWaiting = true;
    console.log(`[${sessionCode}] waiting for Zoom audio (meeting ${meetingTag(session.meetingId)})`);
    return 'waiting';
}

function attachRtmsStream(streamId, sessionCode) {
    const info = rtmsStreamInfo.get(streamId);
    if (!info) return null;
    info.sessionCode = sessionCode;
    const session = sessions[sessionCode];
    if (session) { session.zoomPaused = false; session.zoomWaiting = false; }
    const pipeline = createCaptionPipeline(streamId, sessionCode);
    console.log(`[${sessionCode}] Zoom audio connected (stream ${streamId})`);
    return pipeline;
}

// The live session for one meeting -- the only one that meeting's panel sees.
function currentZoomSessionCode(meetingId) {
    if (!meetingId) return null;
    const live = Object.entries(sessions)
        .filter(([, s]) => !s.ended && s.meetingId === meetingId)
        .sort((a, b) => b[1].createdAt - a[1].createdAt)[0];
    return live ? live[0] : null;
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
    const meetingId = payload && payload.meeting_uuid ? String(payload.meeting_uuid) : null;
    rtmsStreamInfo.set(streamId, { sessionCode: null, meetingId, startedAt: Date.now() });
    const waiting = Object.entries(sessions)
        .find(([, s]) => !s.ended && s.zoomWaiting && meetingId && s.meetingId === meetingId);
    if (waiting) {
        attachRtmsStream(streamId, waiting[0]);
    } else {
        console.log(`RTMS: stream ${streamId} started (meeting ${meetingTag(meetingId)}) -- no session for this meeting yet.`);
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
// Each session runs one translator per caption language, all fed the same
// audio. Each gives captions (outputTranscription) and, when the session
// offers audio in that language, speech (modelTurn inlineData, PCM16 24kHz).
// echoTargetLanguage passes speech already in the target language through,
// so every speaker is captioned in every session language.
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

function createTranslator(sessionCode, language, withAudio) {
    const t = {
        sessionCode,
        language,              // a languages.js code: 'en', 'es', 'pl', 'fr'
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
    // Each session carries its own key: the group's, or the server's.
    const session = sessions[t.sessionCode];
    const key = session && session.geminiKey;
    if (!key) {
        console.error(`[${t.sessionCode}] gemini: no API key for this session -- no translation`);
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
                        targetLanguageCode: t.language, // languages.js keys are Gemini's codes
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
        translators: sessions[sessionCode].languages.map(lang => createTranslator(sessionCode, lang, audio.includes(lang)))
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

// ---- Zoom panel identity ----------------------------------------------------
// When Zoom loads the panel (our Home URL), it adds an x-zoom-app-context
// header: JSON encrypted with AES-256-GCM, keyed with SHA-256 of our client
// secret, so only Zoom could have written it. `uid` is the Zoom user and
// `mid` the meeting. We read it once, then hand the panel a signed token
// carrying both. The panel sends that token on every call, so the server
// knows which meeting a request comes from without trusting the browser.
//
// Header layout: [1 byte IV length][IV][2 bytes AAD length, LE][AAD]
//                [4 bytes ciphertext length, LE][ciphertext][16-byte tag]
function decryptZoomAppContext(header, clientSecret) {
    let buf = Buffer.from(header, 'base64');
    const ivLength = buf.readUInt8(0); buf = buf.subarray(1);
    const iv = buf.subarray(0, ivLength); buf = buf.subarray(ivLength);
    const aadLength = buf.readUInt16LE(0); buf = buf.subarray(2);
    const aad = buf.subarray(0, aadLength); buf = buf.subarray(aadLength);
    const cipherLength = buf.readInt32LE(0); buf = buf.subarray(4);
    const cipherText = buf.subarray(0, cipherLength);
    const tag = buf.subarray(cipherLength);
    if (tag.length !== 16) throw new Error('invalid tag length');
    const key = crypto.createHash('sha256').update(clientSecret).digest();
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAAD(aad);
    decipher.setAuthTag(tag);
    return JSON.parse(Buffer.concat([decipher.update(cipherText), decipher.final()]).toString('utf8'));
}

// TEMPORARY diagnostic, for building the attendee "viewer mode": logs which
// fields Zoom's context carries -- field NAMES only. The only values logged
// are short ones from fields named like a role or type (e.g. "host",
// "meeting"), which identify no one. Never user or meeting IDs. Logged once
// per distinct shape, so it doesn't fill the logs.
const seenContextShapes = new Set();
function logContextShape(ctx) {
    try {
        const names = Object.keys(ctx).sort();
        const roleish = names
            .filter(n => /role|typ/i.test(n) && typeof ctx[n] === 'string' && ctx[n].length <= 20)
            .map(n => `${n}=${ctx[n]}`);
        const shape = names.join(',') + ' | ' + roleish.join(',');
        if (seenContextShapes.has(shape)) return;
        seenContextShapes.add(shape);
        console.log(`Zoom context fields: ${names.join(', ')}${roleish.length ? ` | ${roleish.join(', ')}` : ''}`);
    } catch (e) { /* diagnostics must never break the panel */ }
}

// Returns { uid, mid } from a valid, unexpired context header, else null.
function readZoomAppContext(req) {
    const header = req.headers['x-zoom-app-context'];
    const secret = process.env.ZOOM_CLIENT_SECRET;
    if (!header || !secret) return null;
    try {
        const ctx = decryptZoomAppContext(header, secret);
        logContextShape(ctx);
        // Zoom's timestamps may be seconds or milliseconds; accept either.
        const exp = Number(ctx.exp);
        if (exp && (exp < 1e12 ? exp * 1000 : exp) < Date.now()) return null;
        if (!ctx.uid) return null;
        return { uid: String(ctx.uid), mid: ctx.mid ? String(ctx.mid) : null };
    } catch (err) {
        console.warn('Zoom app context rejected:', err.message);
        return null;
    }
}

// Panel tokens: base64url(JSON {uid, mid, exp}) + "." + HMAC. The signing
// key is made fresh at startup -- a restart clears all sessions anyway, and
// reopening the panel gets a new token.
const PANEL_TOKEN_KEY = crypto.randomBytes(32);
const PANEL_TOKEN_TTL_MS = 12 * 60 * 60 * 1000;

function signPanelToken(uid, mid) {
    const body = Buffer.from(JSON.stringify({ uid, mid, exp: Date.now() + PANEL_TOKEN_TTL_MS })).toString('base64url');
    return body + '.' + crypto.createHmac('sha256', PANEL_TOKEN_KEY).update(body).digest('base64url');
}

function readPanelToken(req) {
    const token = req.headers['x-panel-token'];
    if (typeof token !== 'string' || !token.includes('.')) return null;
    const [body, sig] = token.split('.');
    const expected = crypto.createHmac('sha256', PANEL_TOKEN_KEY).update(body).digest('base64url');
    if (!safeEqual(sig, expected)) return null;
    try {
        const data = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
        if (!data.exp || data.exp < Date.now()) return null;
        return { uid: data.uid, mid: data.mid };
    } catch (e) { return null; }
}

// Who may pause, connect, or end a session: the panel in that session's own
// meeting, or whoever holds its leader token (the browser that started it).
// Knowing the session code -- which every listener has -- is not enough.
function canControl(req, session, body) {
    if (!session) return false;
    const panel = readPanelToken(req);
    if (panel && panel.mid && session.meetingId && panel.mid === session.meetingId) return true;
    return safeEqual(body && body.leaderToken, session.leaderToken);
}

function sessionSummary(code) {
    const s = sessions[code];
    return { sessionCode: code, languages: s.languages, audio: s.audio, audioConnected: !!findPipelineBySession(code) };
}

// ---- Who may start a session, and with which Gemini key ---------------------
// In order:
//   1. An access code: one you issued (store.createCodeGroup), a group's
//      website code from the Zoom app, or the original ACCESS_CODE variable.
//   2. No code, from the Zoom panel: the key that Zoom user saved in Settings.
// Codes you issue without a key, and ACCESS_CODE, use the server's key.
function resolveCredentials(req, body) {
    const code = typeof body.accessCode === 'string' ? body.accessCode.trim() : '';
    if (code) {
        if (process.env.ACCESS_CODE && safeEqual(code, process.env.ACCESS_CODE)) {
            return { geminiKey: process.env.GEMINI_API_KEY || null };
        }
        const g = store.findByAccessCode(code);
        if (g && g.geminiKey) {
            const key = store.geminiKeyFor(g);
            return key ? { geminiKey: key } : { error: "This group's saved key could not be read. Save it again.", status: 409 };
        }
        if (g) return { geminiKey: process.env.GEMINI_API_KEY || null };
        return { error: 'Invalid access code', status: 401 };
    }
    const panel = readPanelToken(req);
    const g = panel && store.findByZoomUser(panel.uid);
    if (g && g.active && g.geminiKey) {
        const key = store.geminiKeyFor(g);
        if (key) return { geminiKey: key };
        return { error: 'Your saved key could not be read. Save it again in Settings.', status: 409 };
    }
    return { error: 'Enter an access code, or save your Gemini key in Settings.', status: 401 };
}

// Slows down guessing: 10 wrong codes from one address locks it out for 15
// minutes. (The first X-Forwarded-For entry can be faked, which only lets
// someone dodge the limit, not lock others out. Issued codes are 12 random
// characters, far too many to guess anyway.)
const failedAttempts = new Map(); // ip -> { count, first }
const FAIL_LIMIT = 10, FAIL_WINDOW_MS = 15 * 60 * 1000;
function clientIp(req) {
    return String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '?';
}
function isLockedOut(req) {
    const f = failedAttempts.get(clientIp(req));
    if (!f) return false;
    if (Date.now() - f.first > FAIL_WINDOW_MS) { failedAttempts.delete(clientIp(req)); return false; }
    return f.count >= FAIL_LIMIT;
}
function noteFailure(req) {
    const ip = clientIp(req);
    const f = failedAttempts.get(ip);
    if (!f || Date.now() - f.first > FAIL_WINDOW_MS) failedAttempts.set(ip, { count: 1, first: Date.now() });
    else f.count++;
}

// Checks a Gemini key with Google before saving it, so a typo shows up now,
// not as silence in the middle of a meeting. The key goes in a header, not
// the URL, so it can't end up in anyone's request logs.
//
// We don't guess at Google's key format -- it has changed before. We only
// remove whitespace a paste can carry (spaces, line breaks), then ask Google.
// Returns { ok, key } with the cleaned key, or { ok: false, error }.
async function validateGeminiKey(raw) {
    const key = typeof raw === 'string' ? raw.replace(/\s+/g, '') : '';
    if (key.length < 10 || key.length > 512) {
        return { ok: false, error: "That doesn't look like a Gemini API key. Copy it again from Google AI Studio." };
    }
    try {
        const r = await fetch('https://generativelanguage.googleapis.com/v1beta/models?pageSize=1', {
            headers: { 'x-goog-api-key': key }
        });
        if (r.ok) return { ok: true, key };
        if ([400, 401, 403].includes(r.status)) return { ok: false, error: 'Google rejected that key. Check it and try again.' };
        return { ok: false, error: `Google couldn't check the key right now (${r.status}). Try again in a minute.` };
    } catch (err) {
        return { ok: false, error: "Couldn't reach Google to check the key. Try again in a minute." };
    }
}

// Admin API for you, with ADMIN_TOKEN as a bearer token (see README).
function isAdmin(req) {
    const token = process.env.ADMIN_TOKEN;
    if (!token || token.length < 24) return false;
    const header = String(req.headers.authorization || '');
    return header.startsWith('Bearer ') && safeEqual(header.slice(7), token);
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
    req.on('data', chunk => {
        body += chunk;
        if (body.length > 64 * 1024) { body = ''; req.destroy(); } // nothing we accept is this big
    });
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
    '/privacy': ['privacy.html', 'text/html; charset=UTF-8', APP_PAGE_HEADERS],
    '/terms': ['terms.html', 'text/html; charset=UTF-8', APP_PAGE_HEADERS],
    '/support': ['support.html', 'text/html; charset=UTF-8', APP_PAGE_HEADERS],
    '/docs': ['docs.html', 'text/html; charset=UTF-8', APP_PAGE_HEADERS],
    '/captions.js': ['captions.js', 'application/javascript; charset=UTF-8'],
    '/languages.js': ['languages.js', 'application/javascript; charset=UTF-8'],
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

        // The Zoom panel. Zoom's signed context header says who and which meeting;
        // the page gets a panel token for it. Opened outside a Zoom meeting, the
    // token is empty and the panel says to open it from inside one.
    else if ((req.method === 'GET' || req.method === 'HEAD') && (pathname === '/zoom-app' || pathname === '/zoom-app.html')) {
        const ctx = readZoomAppContext(req);
        const token = ctx && ctx.mid ? signPanelToken(ctx.uid, ctx.mid) : '';
        if (ctx) console.log(`Zoom panel opened (meeting ${meetingTag(ctx.mid)})`);
        fs.readFile(path.join(__dirname, 'zoom-app.html'), 'utf8', (err, html) => {
            if (err) { res.writeHead(500); res.end(); return; }
            res.writeHead(200, {
                'Content-Type': 'text/html; charset=UTF-8',
                'Cache-Control': 'no-store',
                ...ZOOM_APP_HEADERS
            });
            res.end(req.method === 'HEAD' ? undefined
                : html.replace('<meta name="rt-panel-token" content=""/>', `<meta name="rt-panel-token" content="${token}"/>`));
        });
    }

    else if (req.method === 'POST' && pathname === '/session/create') {
        readJson(req, (body) => {
            if (isLockedOut(req)) { sendJson(res, 429, { error: 'Too many wrong codes. Try again in 15 minutes.' }); return; }
            const creds = resolveCredentials(req, body);
            if (creds.error) {
                if (creds.status === 401 && body.accessCode) noteFailure(req);
                sendJson(res, creds.status, { error: creds.error });
                return;
            }
            if (!creds.geminiKey) { sendJson(res, 500, { error: 'The server has no Gemini key set up.' }); return; }
            // Started from the Zoom panel: the session belongs to that meeting.
            const panel = readPanelToken(req);
            const code = createSession(parseLanguageChoice(body), body.audioSource, panel && panel.mid, creds.geminiKey);
            const s = sessions[code];
            const zoomAudio = s.audioSource === 'zoom' && s.meetingId ? connectZoomAudio(code) : undefined;
            sendJson(res, 200, { sessionCode: code, leaderToken: s.leaderToken, linkCode: s.linkCode, zoomAudio,
                languages: s.languages, audio: s.audio });
        });
    }

    // Live captions for one language, as server-sent events.
    else if (req.method === 'GET' && pathname.startsWith('/stream/')) {
        const [, , sessionCode, rawLanguage] = pathname.split('/');
        const session = sessions[sessionCode];
        const language = normalizeLanguage(rawLanguage);
        if (!session) { sendJson(res, 404, { error: 'Session not found' }); return; }
        if (!session.clients[language]) { sendJson(res, 400, { error: 'This session has no captions in that language' }); return; }
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

        // The Zoom panel's Settings screen: this Zoom user's saved settings.
    // Never returns the key itself -- only its last four characters.
    else if (req.method === 'GET' && pathname === '/zoom/config') {
        const panel = readPanelToken(req);
        if (!panel) { sendJson(res, 401, { error: 'Open this from inside a Zoom meeting.' }); return; }
        const g = store.findByZoomUser(panel.uid);
        sendJson(res, 200, {
            storeEnabled: store.enabled(),
            configured: !!(g && g.active && g.geminiKey),
            group: g ? store.describe(g) : null
        });
    }

        // Saves (or replaces) this Zoom user's Gemini key, after Google confirms
    // it works. The first save also returns a website access code -- once.
    else if (req.method === 'POST' && pathname === '/zoom/config') {
        readJson(req, async (body) => {
            const panel = readPanelToken(req);
            if (!panel) { sendJson(res, 401, { error: 'Open this from inside a Zoom meeting.' }); return; }
            if (!store.enabled()) { sendJson(res, 503, { error: "This server isn't set up to save keys." }); return; }
            const check = await validateGeminiKey(body.geminiKey);
            if (!check.ok) { sendJson(res, 400, { error: check.error }); return; }
            const result = store.saveZoomGroup(panel.uid, { label: body.label, geminiKey: check.key });
            console.log(`Settings saved from the Zoom app (group ${result.group.id})`);
            sendJson(res, 200, { configured: true, group: result.group, accessCode: result.accessCode });
        });
    }

    // "Remove saved key": deletes this Zoom user's record entirely.
    else if (req.method === 'POST' && pathname === '/zoom/config/delete') {
        const panel = readPanelToken(req);
        if (!panel) { sendJson(res, 401, { error: 'Open this from inside a Zoom meeting.' }); return; }
        const removed = store.deleteByZoomUser(panel.uid);
        console.log(`Settings removed from the Zoom app (${removed} record(s))`);
        sendJson(res, 200, { removed });
    }

    // ---- Admin API (Authorization: Bearer <ADMIN_TOKEN>) ----
    else if (pathname.startsWith('/admin/')) {
        if (isLockedOut(req)) { sendJson(res, 429, { error: 'Too many attempts' }); return; }
        if (!isAdmin(req)) { noteFailure(req); sendJson(res, 401, { error: 'Not authorized' }); return; }
        const parts = pathname.split('/').filter(Boolean); // ['admin','groups',id?,action?]
        if (parts[1] !== 'groups') { sendJson(res, 404, { error: 'Not found' }); return; }

        if (req.method === 'GET' && parts.length === 2) {
            sendJson(res, 200, { groups: store.list() });
        } else if (req.method === 'POST' && parts.length === 2) {
            readJson(req, async (body) => {
                if (!store.enabled()) { sendJson(res, 503, { error: 'Set KEY_ENCRYPTION_KEY first.' }); return; }
                let geminiKey = null;
                if (body.geminiKey) {
                    const check = await validateGeminiKey(body.geminiKey);
                    if (!check.ok) { sendJson(res, 400, { error: check.error }); return; }
                    geminiKey = check.key;
                }
                const result = store.createCodeGroup({ label: body.label, geminiKey });
                console.log(`Admin: created group ${result.group.id}`);
                sendJson(res, 200, result);
            });
        } else if (req.method === 'POST' && parts.length === 4 && ['enable', 'disable', 'delete'].includes(parts[3])) {
            const id = parts[2];
            const done = parts[3] === 'delete' ? store.remove(id) : store.setActive(id, parts[3] === 'enable');
            console.log(`Admin: ${parts[3]} group ${id}${done ? '' : ' (not found)'}`);
            sendJson(res, done ? 200 : 404, done ? { ok: true, group: done === true ? undefined : done } : { error: 'No such group' });
        } else {
            sendJson(res, 404, { error: 'Not found' });
        }
    }

    // Which session the Zoom panel should show: only its own meeting's.
    else if (req.method === 'GET' && pathname === '/zoom/current-session') {
        const panel = readPanelToken(req);
        if (!panel) { sendJson(res, 401, { error: 'Open this from inside a Zoom meeting.' }); return; }
        const code = currentZoomSessionCode(panel.mid);
        sendJson(res, 200, code ? sessionSummary(code) : { sessionCode: null });
    }

        // Links a session started in the browser to the meeting whose panel sent
    // the one-time link code the browser showed its leader.
    else if (req.method === 'POST' && pathname === '/zoom/link') {
        readJson(req, (body) => {
            const panel = readPanelToken(req);
            if (!panel || !panel.mid) { sendJson(res, 401, { error: 'Open this from inside a Zoom meeting.' }); return; }
            const linkCode = String(body.linkCode || '').trim().toUpperCase();
            const entry = linkCode && Object.entries(sessions)
                .find(([, s]) => !s.ended && s.linkCode && safeEqual(s.linkCode, linkCode));
            if (!entry) { sendJson(res, 404, { error: 'No session is waiting for that link code.' }); return; }
            const [code, s] = entry;
            s.meetingId = panel.mid;
            s.linkCode = null; // one use only
            console.log(`[${code}] linked to meeting ${meetingTag(panel.mid)}`);
            sendJson(res, 200, sessionSummary(code));
        });
    }

        // Public facts about a session: whether it's live, its languages (for the
        // listener's language list and the display columns), and whether audio is
    // flowing. Nothing here needs more than the session code.
    else if (req.method === 'GET' && (pathname === '/session/info' || pathname === '/zoom/session-status')) {
        const code = String(parsedUrl.searchParams.get('code') || '').toUpperCase();
        const s = sessions[code];
        const live = !!s && !s.ended;
        sendJson(res, 200, {
            exists: live,
            languages: live ? s.languages : [],
            audio: live ? s.audio : [],
            audioConnected: !!findPipelineBySession(code)
        });
    }

        // Panel's "Start captions": claim Zoom audio for this session (now, or
    // as soon as the stream starts).
    else if (req.method === 'POST' && pathname === '/zoom/connect') {
        readJson(req, (body) => {
            const s = sessions[body.sessionCode];
            if (!s || s.ended) { sendJson(res, 404, { status: 'no-session' }); return; }
            if (!canControl(req, s, body)) { sendJson(res, 403, { error: 'Not allowed' }); return; }
            sendJson(res, 200, { status: connectZoomAudio(body.sessionCode) });
        });
    }

        // Panel's "Stop captions": the stream stopping next is a pause, so the
    // session stays open for listeners.
    else if (req.method === 'POST' && pathname === '/zoom/pause') {
        readJson(req, (body) => {
            const s = sessions[body.sessionCode];
            if (!s || s.ended) { sendJson(res, 404, { paused: false }); return; }
            if (!canControl(req, s, body)) { sendJson(res, 403, { error: 'Not allowed' }); return; }
            s.zoomPaused = true;
            s.zoomWaiting = false;
            sendJson(res, 200, { paused: true });
        });
    }

    // Lets the Zoom panel end a session (listeners see "session ended").
    else if (req.method === 'POST' && pathname === '/zoom/end-session') {
        readJson(req, (body) => {
            const s = sessions[body.sessionCode];
            if (!s || s.ended) { sendJson(res, 404, { error: 'No active session with that code' }); return; }
            if (!canControl(req, s, body)) { sendJson(res, 403, { error: 'Not allowed' }); return; }
            endSession(body.sessionCode);
            sendJson(res, 200, { ended: true });
        });
    }

        // Zoom RTMS webhook:
        //   endpoint.url_validation -- Zoom's challenge when the URL is saved in
        //     the Zoom console; we echo back an HMAC-signed token.
        //   meeting.rtms_started / meeting.rtms_stopped -- join or leave the
        //     meeting's audio stream.
        //   app_deauthorized -- someone removed the app; delete their settings.
        //     Also accepted at /zoom/deauthorize, for Zoom's separate
    //     "Deauthorization Notification Endpoint" field.
    else if (req.method === 'POST' && (pathname === '/zoom/rtms-webhook' || pathname === '/zoom/deauthorize')) {
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

            // The app was removed from a Zoom account: delete that user's saved
            // settings (key and website access code) right away.
            if (payload.event === 'app_deauthorized') {
                const uid = payload.payload && payload.payload.user_id;
                const removed = uid ? store.deleteByZoomUser(String(uid)) : 0;
                console.log(`Zoom app removed by a user -- deleted ${removed} saved record(s)`);
                res.writeHead(200); res.end('ok');
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
                // The exchange completes Zoom's install flow. The server makes
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
//                    and can end the session. Needs ?token=<leaderToken>.
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
        // Only the browser that started the session can stream audio into it
        // or end it -- not anyone who knows the session code.
        if (!safeEqual(params.get('token'), session.leaderToken)) { ws.close(); return; }
        session.leaderSocket = ws;
        const pipelineKey = 'leader-' + sessionCode;
        // A reconnect (tab resume, network blip) cancels a pending teardown,
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
            // replaced by a reconnect must not tear anything down.
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
        // A language the session doesn't offer falls back to its first one.
        const asked = normalizeLanguage(params.get('language'));
        const language = session.languages.includes(asked) ? asked : session.languages[0];
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