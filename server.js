require('dotenv').config();
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');
const franc = require('franc-min');

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

// ---- Zoom audio <-> session pairing ------------------------------------
// A session started with "Zoom meeting" as its audio input waits for Zoom's
// audio stream; a Zoom stream that arrives first (e.g. RTMS auto-start)
// waits for a session. Whichever shows up second connects them. Until
// then, Zoom audio is ignored -- no OpenAI session, no cost.
// Pairs one meeting at a time: the most recent waiting session.
let zoomWaitingSession = null;           // { sessionCode, at }
const rtmsStreamInfo = new Map();        // streamId -> { sessionCode|null, meetingId, startedAt }
const ZOOM_WAIT_MAX_MS = 30 * 60 * 1000; // a waiting session expires after 30 min

function connectZoomAudio(sessionCode) {
    const session = sessions[sessionCode];
    if (!session || session.ended) return 'no-session';
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

// ---- Zoom OAuth tokens ----------------------------------------------------
// Saved to a file so they survive restarts and deploys. Zoom access tokens
// last one hour; we refresh them with the refresh token automatically.
// On Railway, attach a Volume (e.g. mounted at /data) and set
// ZOOM_TOKEN_FILE=/data/zoom-tokens.json -- otherwise the file is wiped on
// every deploy, just like the old in-memory variable was.
// Holds ONE Zoom user's tokens (the account that installed the app).
const ZOOM_TOKEN_FILE = process.env.ZOOM_TOKEN_FILE || path.join(__dirname, 'data', 'zoom-tokens.json');
let zoomTokens = loadZoomTokens();
let zoomRefreshInFlight = null;

function loadZoomTokens() {
    try {
        const t = JSON.parse(fs.readFileSync(ZOOM_TOKEN_FILE, 'utf8'));
        console.log(`Zoom tokens loaded from ${ZOOM_TOKEN_FILE}`);
        return t;
    } catch (e) {
        console.log(`No saved Zoom tokens at ${ZOOM_TOKEN_FILE} -- install/authorize the Zoom app to create them`);
        return null;
    }
}

function storeZoomTokens(data) {
    zoomTokens = {
        access_token: data.access_token,
        refresh_token: data.refresh_token,
        expires_at: Date.now() + (data.expires_in || 3600) * 1000,
        scope: data.scope
    };
    try {
        fs.mkdirSync(path.dirname(ZOOM_TOKEN_FILE), { recursive: true });
        fs.writeFileSync(ZOOM_TOKEN_FILE, JSON.stringify(zoomTokens), { mode: 0o600 });
    } catch (e) {
        console.error(`Could not save Zoom tokens to ${ZOOM_TOKEN_FILE}:`, e.message);
    }
}

// Returns a valid access token, refreshing it if it's expired or about to.
// forceRefresh is used when Zoom rejects a token we thought was valid.
async function getZoomAccessToken(forceRefresh = false) {
    if (!zoomTokens) return null;
    const fresh = zoomTokens.access_token && zoomTokens.expires_at - 60000 > Date.now();
    if (fresh && !forceRefresh) return zoomTokens.access_token;
    if (!zoomTokens.refresh_token) return null;
    // Zoom issues a NEW refresh token on every refresh and retires the old
    // one, so two refreshes at once would break the second. One at a time.
    if (!zoomRefreshInFlight) {
        zoomRefreshInFlight = refreshZoomToken().finally(() => { zoomRefreshInFlight = null; });
    }
    return zoomRefreshInFlight;
}

async function refreshZoomToken() {
    try {
        const basic = Buffer.from(`${process.env.ZOOM_CLIENT_ID}:${process.env.ZOOM_CLIENT_SECRET}`).toString('base64');
        const res = await fetch('https://zoom.us/oauth/token', {
            method: 'POST',
            headers: { 'Authorization': `Basic ${basic}`, 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: zoomTokens.refresh_token }).toString()
        });
        const data = await res.json();
        if (!res.ok) {
            console.error('Zoom token refresh FAILED -- reinstall/re-authorize the app:', JSON.stringify(data));
            return null;
        }
        storeZoomTokens(data);
        console.log('Zoom access token refreshed');
        return zoomTokens.access_token;
    } catch (err) {
        console.error('Zoom token refresh error:', err.message);
        return null;
    }
}

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
const CAPTION_MAX_CHARS = 250;

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

// Zoom gives a meeting ONE caption stream, with no per-viewer language
// choice. pipeline.zoomCaptionLang picks which language goes there:
// 'spanish' (default), 'english', or 'off'. Every caller goes through this
// gate, so nothing else needs to know the setting.
const ZOOM_CAPTION_LANG_CODES = { english: 'en-US', spanish: 'es-ES' };

function queueZoomCaption(pipeline, text, lang) {
    if (!pipeline.zoomCaptionUrl || !text) return;
    if (lang !== ZOOM_CAPTION_LANG_CODES[pipeline.zoomCaptionLang]) return;

    pipeline.captionBuffers = pipeline.captionBuffers || {};
    pipeline.captionFlushTimers = pipeline.captionFlushTimers || {};
    pipeline.captionBuffers[lang] = (pipeline.captionBuffers[lang] || '') + text;

    const buffered = pipeline.captionBuffers[lang];

    // Do NOT flush at sentence punctuation. Every POST/sequence can cause Zoom
    // to advance the native caption stream, so let Zoom wrap sentences within
    // the same caption chunk. Only force a flush if the chunk becomes large.
    if (buffered.length >= CAPTION_MAX_CHARS) {
        flushZoomCaption(pipeline, lang);
        return;
    }

    // Otherwise wait for a meaningful pause before advancing Zoom's caption
    // sequence. This lets multiple sentences wrap naturally in the same block.
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
        ws.send(JSON.stringify({
            type: 'session.update',
            session: {
                audio: {
                    input: {
                        transcription: {
                            model: 'gpt-realtime-whisper'
                        },
                        noise_reduction: {type: 'far_field'}
                    },
                    output: {
                        language: targetLanguage
                    }
                }
            }
        }));
    });

    ws.on('message', (raw) => {
        let ev;
        try { ev = JSON.parse(raw.toString()); } catch (e) { return; }
        const session = sessions[pipeline.sessionCode];
        const transcriptOnly = session && session.mode === 'transcript_only';

        if (ev.type === 'session.updated') {
            pipeline[readyKey] = true;
            console.log(`[${pipeline.sessionCode}] translate(${targetLanguage}): live`);
            return;
        }
        if (ev.type === 'error') {
            console.error(`[${pipeline.sessionCode}] translate(${targetLanguage}) ERROR:`, JSON.stringify(ev.error || ev));
            return;
        }

        // 1) Spanish captions: this session's translated output.
        if (ev.type === 'session.output_transcript.delta' && ev.delta && broadcastLanguage === 'spanish') {
            checkSourceTranscriptStall(pipeline);
            if (!transcriptOnly) {
                broadcast(pipeline.sessionCode, 'spanish', ev.delta);
                queueZoomCaption(pipeline, ev.delta, 'es-ES');
            }
            return;
        }

        // 2) Spanish audio: relayed to listeners who tapped "Hear translation".
        if (ev.type === 'session.output_audio.delta' && ev.delta && broadcastLanguage === 'spanish') {
            if (!pipeline.loggedFirstAudio) {
                pipeline.loggedFirstAudio = true;
                console.log(`[${pipeline.sessionCode}] AUDIO checkpoint 1: OpenAI is producing Spanish audio`);
            }
            if (!transcriptOnly) relayTranslatedAudio(pipeline.sessionCode, Buffer.from(ev.delta, 'base64'));
            return;
        }

        // 3) English captions: the INPUT transcript (whatever was actually
        //    spoken) goes through the franc chunker -- English passes
        //    through, Spanish chunks are translated to English.
        if (ev.type === 'session.input_transcript.delta' && ev.delta && wsKey === pipeline.sourceTranscriptWsKey) {
            console.log(ev.delta);
            handleSourceTranscriptDelta(pipeline, ev.delta);
            return;
        }
    });

    ws.on('error', (err) => console.error(`RTMS/OpenAI [${pipeline.sessionCode}] translate(${targetLanguage}) WS error:`, err.message));
    ws.on('close', (code) => console.log(`RTMS/OpenAI [${pipeline.sessionCode}] translate(${targetLanguage}): closed. code=${code}`));
}


// Google Cloud Translation (Basic, v2). Source language is auto-detected,
// so Spanish, English, or mixed chunks all come back as English.
const TRANSLATE_TIMEOUT_MS = 4000;

async function translateToEnglish(session, transcript, sourceLanguage) {
    const key = process.env.GOOGLE_TRANSLATE_API_KEY;
    if (!key) {
        console.error('text translation failed: GOOGLE_TRANSLATE_API_KEY is not set');
        return;
    }

    let response;
    try {
        response = await fetch('https://translation.googleapis.com/language/translate/v2', {
            // A hung request would block every English caption queued behind
            // it (they're released in order), so give up after a few seconds.
            signal: AbortSignal.timeout(TRANSLATE_TIMEOUT_MS),
            method: 'POST',
            headers: {
                'X-Goog-Api-Key': key, // header, so the key never appears in a URL or log
                'Content-Type': 'application/json'
            },
            // format: 'text' -- otherwise Google returns HTML-escaped text
            // (&#39; instead of an apostrophe).
            body: JSON.stringify({ q: transcript, target: 'en', format: 'text' })
        });
    } catch (err) {
        console.error(`text translation failed: ${err.name === 'TimeoutError' ? `no reply from Google in ${TRANSLATE_TIMEOUT_MS}ms` : err.message}`);
        return;
    }

    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
        console.error(`text translation failed (${response.status}):`, JSON.stringify(data).slice(0, 1000));
        return;
    }

    const t = data.data && data.data.translations && data.data.translations[0];
    if (!t) return;
    if (t.detectedSourceLanguage) console.log(`Google detected source language: ${t.detectedSourceLanguage}`);
    return (t.translatedText || '').trim();
}

// Translate a completed non-English transcript to English. This is deliberately
// text-to-text: it gives us glossary control and avoids asking the speech
// translation model to perform English -> English passthrough.
async function translateTranscriptToEnglish(pipeline, transcript, sourceLanguage) {
    const session = sessions[pipeline.sessionCode];

    if (!session || !transcript || !transcript.trim()) return;

    try {
        let eng = await translateToEnglish(session, transcript, sourceLanguage);
        if (eng) {
            broadcast(pipeline.sessionCode, 'english', eng + ' ');
            queueZoomCaption(pipeline, eng, 'en-US');
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
    const ws = new WebSocket('wss://api.openai.com/v1/realtime?intent=transcription', {
        headers: { 'Authorization': `Bearer ${apiKey}`, 'OpenAI-Safety-Identifier': 'recovery-translator' }
    });
    pipeline.transcribeWs = ws;
    pipeline.transcribeReady = false;

    ws.on('open', () => {
        console.log(`RTMS/OpenAI multilingual transcription: connected`);
        ws.send(JSON.stringify({
            type: 'session.update',
            session: {
                type: 'transcription',
                audio: {
                    input: {
                        format: {
                            type: 'audio/pcm',
                            rate: 24000
                        },
                        transcription: {
                            model: 'gpt-transcribe',
                            prompt: 'A live peer-recovery fellowship meeting. Transcribe exactly what the speaker says. Preserve recovery terminology, acronyms, names, Step/Tradition/Concept numbers, and code-switching.',
                            keywords: RECOVERY_KEYWORDS,
                            languages: ['en', 'es']
                        },
                        turn_detection: {
                            type: 'server_vad',
                            threshold: 0.1,
                            prefix_padding_ms: 300,
                            silence_duration_ms: 350
                        }
                    }
                },
                include: ["item.input_audio_transcription.logprobs"]
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
        console.log(transcript)
        const detected = Array.isArray(ev.languages) && ev.languages.length
            ? ev.languages[0].code
            : null;
        console.log(`RTMS/OpenAI [${pipeline.sessionCode}] completed transcript language=${detected || 'unknown'}: ${transcript.slice(0, 160)}`);
        if ((detected === 'en' || detected === 'eng')) {
            let lngDet = franc.francAll( transcript, { only: ['eng', 'spa'] });
            console.log('languageDetection: ', lngDet);
            if(lngDet[0][0] === 'eng') {
                pipeline.pendingEnglishTranslation = '';
                broadcast(pipeline.sessionCode, 'english', transcript + ' ');
                queueZoomCaption(pipeline, transcript, 'en-US');
            } else {
                translateTranscriptToEnglish(pipeline, transcript, detected);
            }
        } else if (detected) {
            // For Spanish/non-English speech, use the continuously-running
            // English speech translator. It is much better at cross-language
            // translation than English->English passthrough.
            const translated = (pipeline.pendingEnglishTranslation || '').trim();
            pipeline.pendingEnglishTranslation = '';
            if (translated) {
                broadcast(pipeline.sessionCode, 'english', translated + ' ');
                queueZoomCaption(pipeline, translated, 'en-US');
            } else {
                // Safety net if translation timing lags behind language detection.
                translateTranscriptToEnglish(pipeline, transcript, detected);
            }
        } else {
            // No confident language prediction: preserve the transcript rather
            // than guessing and accidentally replacing valid English.
            pipeline.pendingEnglishTranslation = '';
            broadcast(pipeline.sessionCode, 'english', transcript + ' ');
            queueZoomCaption(pipeline, transcript, 'en-US');
        }
    });

    ws.on('error', (err) =>
        console.error(`RTMS/OpenAI [${pipeline.sessionCode}] transcription WS error:`, err)
    );
    ws.on('close', (code) => console.log(`RTMS/OpenAI [${pipeline.sessionCode}] transcription closed. code=${code}`));
}

// Fetches the meeting's closed-caption token automatically so the host
// doesn't have to paste it every meeting. Zoom's UUIDs contain characters
// (/ + =) that MUST be double-URL-encoded -- skipping that is the documented
// cause of "3001 Meeting does not exist" errors on this endpoint.
async function fetchZoomCaptionUrl(pipeline, meetingId, isRetry = false) {
    const accessToken = await getZoomAccessToken(isRetry);
    if (!meetingId || !accessToken) {
        console.error(`CAPTION TOKEN [${pipeline.sessionCode}]: skipped -- meetingId=${!!meetingId} accessToken=${!!accessToken}` +
            (accessToken ? '' : ' (not authorized: open the app in Zoom and approve it, or check the token refresh log above)'));
        return false;
    }

    // Numeric meeting IDs can be encoded normally. UUIDs containing / + = need
    // Zoom's documented double encoding when used in the meeting path.
    const rawId = String(meetingId);
    const encoded = /^[0-9]+$/.test(rawId)
        ? encodeURIComponent(rawId)
        : encodeURIComponent(encodeURIComponent(rawId));

    const endpoint = `https://api.zoom.us/v2/meetings/${encoded}/token?type=closed_caption`;
    console.log(`CAPTION TOKEN [${pipeline.sessionCode}]: requesting token using meeting identifier ${JSON.stringify(rawId)} (${ /^[0-9]+$/.test(rawId) ? 'meeting_id' : 'uuid-like' })`);

    try {
        const res = await fetch(endpoint, {
            headers: { 'Authorization': `Bearer ${accessToken}` }
        });
        const raw = await res.text();
        let data = {};
        try { data = raw ? JSON.parse(raw) : {}; } catch (e) { data = { raw }; }

        console.log(`CAPTION TOKEN [${pipeline.sessionCode}]: HTTP ${res.status} response=${JSON.stringify(data).slice(0, 1000)}`);
        if (res.ok && data.token) {
            pipeline.zoomCaptionUrl = data.token;
            console.log(`CAPTION TOKEN [${pipeline.sessionCode}]: SUCCESS -- automatic Zoom caption URL obtained`);
            return true;
        }
        // 401 = token rejected (expired or revoked). Refresh once and retry.
        if (res.status === 401 && !isRetry) {
            console.log(`CAPTION TOKEN [${pipeline.sessionCode}]: 401 -- refreshing Zoom token and retrying once`);
            return fetchZoomCaptionUrl(pipeline, meetingId, true);
        }
        // Zoom's error message names any missing scope, e.g.
        // "Invalid access token, does not contain scopes: [...]".
        console.error(`CAPTION TOKEN [${pipeline.sessionCode}]: FAILED -- manual paste remains available`);
    } catch (err) {
        console.error(`CAPTION TOKEN [${pipeline.sessionCode}]: request error:`, err.message);
    }
    return false;
}

// ---- Shared caption pipeline --------------------------------------------
// Used by BOTH the real Zoom RTMS path and the local browser test path
// (/audio-test), so a local test exercises exactly the code Zoom runs.
// Both paths feed 16kHz mono PCM16 in 20ms frames.

function createCaptionPipeline(streamKey, sessionCode) {
    const pipeline = {
        sessionCode,
        transcribeWs: null, transcribeReady: false,
        enWs: null, enReady: false,
        esWs: null, esReady: false,
        zoomCaptionUrl: null,
        zoomCaptionLang: 'off', // captions are shown in our own Zoom panel instead
        captionSeq: 1,
        audioFrameCount: 0,
        sourceBuffer: '',
        sourceIdleTimer: null,
        englishEmitChain: null,
        // English captions come from THIS feed's input transcript.
        sourceTranscriptWsKey: 'esWs'
    };
    rtmsCaptionPipelines.set(streamKey, pipeline);

    // Which OpenAI sessions run -- one place to change it for Zoom AND tests.
    //connectCaptionTranscriptionWs(pipeline);
    //connectTranslateWs(pipeline, 'en', 'enWs', 'enReady', 'english');
    connectTranslateWs(pipeline, 'es', 'esWs', 'esReady', 'spanish');
    return pipeline;
}

function feedPipelineAudio(pipeline, data) {
    try {
        pipeline.audioFrameCount++;
        if (pipeline.audioFrameCount === 1) {
            console.log(`[${pipeline.sessionCode}] first audio frame -- byteLength=${data && data.byteLength}`);
        }

        const resampled = resamplePCM16(toAudioBuffer(data), 16000, 24000);
        const b64 = resampled.toString('base64');
        // The translation endpoint needs the 'session.' prefix (confirmed by a
        // live rejection from OpenAI); the standard transcription endpoint
        // uses the plain event name.
        const translateMsg = JSON.stringify({ type: 'session.input_audio_buffer.append', audio: b64 });

        if (pipeline.audioFrameCount % 250 === 1) {
            console.log(`[${pipeline.sessionCode}] frame ${pipeline.audioFrameCount}, transcribeWs=${pipeline.transcribeWs && pipeline.transcribeWs.readyState}, esWs=${pipeline.esWs && pipeline.esWs.readyState}`);
        }

        if (pipeline.transcribeWs && pipeline.transcribeWs.readyState === WebSocket.OPEN) {
            pipeline.transcribeWs.send(JSON.stringify({ type: 'input_audio_buffer.append', audio: b64 }));
        }
        if (pipeline.enWs && pipeline.enWs.readyState === WebSocket.OPEN) pipeline.enWs.send(translateMsg);
        if (pipeline.esWs && pipeline.esWs.readyState === WebSocket.OPEN) pipeline.esWs.send(translateMsg);
    } catch (err) {
        console.error(`[${pipeline.sessionCode}] audio processing error:`, err.message);
    }
}

function teardownCaptionPipeline(streamKey) {
    const pipeline = rtmsCaptionPipelines.get(streamKey);
    if (!pipeline) return;
    if (pipeline.sourceIdleTimer) clearTimeout(pipeline.sourceIdleTimer);
    if (pipeline.sourceBuffer) emitSourceChunk(pipeline, pipeline.sourceBuffer);
    // Flush buffered caption text so the last words spoken aren't dropped.
    if (pipeline.captionBuffers) {
        Object.keys(pipeline.captionBuffers).forEach(lang => flushZoomCaption(pipeline, lang));
    }
    if (pipeline.transcribeWs) pipeline.transcribeWs.close();
    if (pipeline.enWs) pipeline.enWs.close();
    if (pipeline.esWs) pipeline.esWs.close();
    rtmsCaptionPipelines.delete(streamKey);
}

function findPipelineBySession(sessionCode) {
    for (const p of rtmsCaptionPipelines.values()) if (p.sessionCode === sessionCode) return p;
    return null;
}

function createSession(apiKey, micDistance, mode, audioSource) {
    const code = Math.random().toString(36).substring(2, 8).toUpperCase();
    sessions[code] = {
        apiKey,
        micDistance: micDistance === 'near_field' ? 'near_field' : 'far_field',
        mode: mode === 'transcript_only' ? 'transcript_only' : 'bilingual',
        audioSource: audioSource === 'zoom' ? 'zoom' : 'device',
        clients: { english: [], spanish: [] },
        leaderSocket: null,
        listenerSockets: {},
        speakQueue: [],
        spanishTurn: false,
        createdAt: Date.now(),
        leaderEnglishTranscript: '',
        zoomToken: ''
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
// ---- English captions from the source transcript -----------------------
// The translate session's input transcript is whatever was spoken, in any
// language. We cut it into ~5-word chunks, let franc decide the language
// (gpt-transcribe's own language call was too unstable), pass English
// through, and translate Spanish chunks to English.
const SOURCE_CHUNK_MIN_WORDS = 5;
const SOURCE_IDLE_FLUSH_MS = 1500;
const SOURCE_ENDINGS = ['', ' ', '\n', ',', '.', '!', '?'];

function handleSourceTranscriptDelta(pipeline, delta) {
    const now = Date.now();
    if (pipeline.sourceStallWarned) {
        console.log(`[${pipeline.sessionCode}] SOURCE TRANSCRIPT resumed after ${((now - pipeline.lastSourceDeltaAt) / 1000).toFixed(1)}s of silence`);
        pipeline.sourceStallWarned = false;
    }
    pipeline.lastSourceDeltaAt = now;
    const words = pipeline.sourceBuffer.trim().split(/\s+/).filter(Boolean).length;
    if (words >= SOURCE_CHUNK_MIN_WORDS && delta[0] === ' ') {
        // A new word is starting: cut here. Snapshot and reset BEFORE any
        // async work, so later deltas can't re-send the same chunk.
        const chunk = pipeline.sourceBuffer;
        pipeline.sourceBuffer = delta;
        emitSourceChunk(pipeline, chunk);
    } else if (words >= SOURCE_CHUNK_MIN_WORDS && SOURCE_ENDINGS.includes(delta)) {
        const chunk = pipeline.sourceBuffer + delta;
        pipeline.sourceBuffer = '';
        emitSourceChunk(pipeline, chunk);
    } else {
        pipeline.sourceBuffer += delta;
    }

    // Short utterances ("Thank you.") never reach 5 words. Flush whatever
    // is buffered once the speaker pauses, so they aren't held indefinitely.
    if (pipeline.sourceIdleTimer) clearTimeout(pipeline.sourceIdleTimer);
    pipeline.sourceIdleTimer = setTimeout(() => {
        const chunk = pipeline.sourceBuffer;
        pipeline.sourceBuffer = '';
        emitSourceChunk(pipeline, chunk);
    }, SOURCE_IDLE_FLUSH_MS);
}

// franc scales scores so the winner is always 1.0; the runner-up's score
// says how close the call was. Translate when franc says Spanish, when it
// can't decide ('und' -- usually short or mixed text), or when the runner-up
// is this close. A wrong "translate" is cheap (English comes back as-is);
// a wrong "pass through" leaves Spanish in the English captions.
const FRANC_CLOSE_CALL = 0.85;

function classifyChunk(chunk) {
    const ranked = franc.francAll(chunk, { only: ['eng', 'spa'] });
    const top = ranked[0][0];
    const runnerUp = ranked[1] ? ranked[1][1] : 0;
    if (top === 'spa') return { translate: true, reason: 'spanish', ranked };
    if (top === 'und') return { translate: true, reason: 'undetermined', ranked };
    if (runnerUp >= FRANC_CLOSE_CALL) return { translate: true, reason: 'close call', ranked };
    return { translate: false, reason: 'english', ranked };
}

// Detects the one failure our code can't fix: OpenAI still translating
// (Spanish flowing) but its input transcript -- the English source -- has
// gone quiet. If this fires often, that's the case for a separate
// transcription session.
const SOURCE_STALL_MS = 8000;

function checkSourceTranscriptStall(pipeline) {
    const now = Date.now();
    if (!pipeline.lastSourceDeltaAt) pipeline.lastSourceDeltaAt = now;
    if (!pipeline.sourceStallWarned && now - pipeline.lastSourceDeltaAt > SOURCE_STALL_MS) {
        pipeline.sourceStallWarned = true;
        console.warn(`[${pipeline.sessionCode}] SOURCE TRANSCRIPT STALLED: Spanish is still arriving, but OpenAI has sent no English source text for ${((now - pipeline.lastSourceDeltaAt) / 1000).toFixed(1)}s`);
    }
}

function emitSourceChunk(pipeline, chunk) {
    if (!chunk || !chunk.trim()) return;
    const session = sessions[pipeline.sessionCode];
    if (!session) return;
    const verdict = classifyChunk(chunk);
    const scores = verdict.ranked.map(([l, s]) => `${l}=${s.toFixed(2)}`).join(' ');
    console.log(`[${pipeline.sessionCode}] chunk ${verdict.translate ? 'TRANSLATE' : 'pass'} (${verdict.reason}; ${scores}): ${chunk.trim()}`);

    if (!verdict.translate) {
        emitEnglishInOrder(pipeline, chunk);
        return;
    }
    const startedAt = Date.now();
    emitEnglishInOrder(pipeline,
        translateToEnglish(session, chunk, verdict.reason)
            .catch(() => null)
            .then(t => {
                if (!t) {
                    // Showing the untranslated words beats silently losing them.
                    console.log(`[${pipeline.sessionCode}] translation FAILED after ${Date.now() - startedAt}ms -- showing original: ${chunk.trim()}`);
                    return chunk;
                }
                console.log(`[${pipeline.sessionCode}] translated in ${Date.now() - startedAt}ms: ${chunk.trim()} -> ${t}`);
                return ' ' + t;
            }));
}

// Emits English strictly in the order chunks were cut, even though a
// Spanish chunk's translation returns later than English cut after it.
function emitEnglishInOrder(pipeline, textOrPromise) {
    const pending = Promise.resolve(textOrPromise);
    pipeline.englishEmitChain = (pipeline.englishEmitChain || Promise.resolve())
        .then(() => pending)
        .then(text => {
            if (!text) return;
            broadcast(pipeline.sessionCode, 'english', text);
            queueZoomCaption(pipeline, text, 'en-US');
        })
        .catch(err => console.error(`[${pipeline.sessionCode}] English caption emit failed:`, err.message));
}

// Spanish audio from OpenAI is PCM16 24kHz mono. Send it as binary to every
// listener who asked for it. No WebRTC, no STUN/TURN, no autoplay tricks.
function relayTranslatedAudio(sessionCode, pcmBuffer) {
    const session = sessions[sessionCode];
    if (!session) return;
    Object.entries(session.listenerSockets).forEach(([id, entry]) => {
        if (entry.wantsAudio && entry.ws.readyState === WebSocket.OPEN) {
            entry.ws.send(pcmBuffer, { binary: true });
            entry.audioChunksSent = (entry.audioChunksSent || 0) + 1;
            if (entry.audioChunksSent === 1 || entry.audioChunksSent % 100 === 0) {
                console.log(`[${sessionCode}] AUDIO checkpoint 3: sent ${entry.audioChunksSent} chunk(s) to listener ${id}`);
            }
        }
    });
}

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
    else if (pathname === '/audio-test' || pathname === '/audio-test.html') {
        serveFile(res, './audio-test.html', 'text/html; charset=UTF-8');
    }
    else if (pathname === '/zoom-app' || pathname === '/zoom-app.html') {
        serveFile(res, './zoom-app.html', 'text/html; charset=UTF-8', ZOOM_APP_SECURITY_HEADERS);
    }
    else if (pathname === '/logo.png') {
        serveFile(res, './recoveryTrans.png', 'image/png');
    }

    // One endpoint, called with targetLanguage 'es' (baseline) or 'en'
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
            const code = createSession(creds.apiKey, creds.micDistance, creds.mode, creds.audioSource);
            const zoomAudio = creds.audioSource === 'zoom' ? connectZoomAudio(code) : undefined;
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ sessionCode: code, zoomAudio }));
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
    // Which session the Zoom panel should show right now.
    else if (req.method === 'GET' && pathname === '/zoom/current-session') {
        const code = currentZoomSessionCode();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(code
            ? { sessionCode: code, mode: sessions[code].mode, audioConnected: !!findPipelineBySession(code) }
            : { sessionCode: null }));
    }

        // Panel's "Start captions": claim Zoom audio for this session (now, or
    // as soon as the stream starts).
    else if (req.method === 'POST' && pathname === '/zoom/connect') {
        let body = '';
        req.on('data', c => { body += c; });
        req.on('end', () => {
            let parsed = {};
            try { parsed = JSON.parse(body); } catch (e) {}
            const status = connectZoomAudio(parsed.sessionCode);
            res.writeHead(status === 'no-session' ? 404 : 200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ status }));
        });
    }

    else if (req.method === 'GET' && pathname === '/zoom/session-status') {
        const code = parsedUrl.searchParams.get('code');
        const s = sessions[code];
        const p = findPipelineBySession(code);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            exists: !!s && !s.ended,
            mode: s ? s.mode : null,
            audioConnected: !!p,
            zoomCaptionReady: !!(p && p.zoomCaptionUrl),
            zoomCaptionLang: p ? p.zoomCaptionLang : null
        }));
    }

    else if (req.method === 'POST' && pathname === '/zoom/caption-language') {
        let body = '';
        req.on('data', c => { body += c; });
        req.on('end', () => {
            let parsed = {};
            try { parsed = JSON.parse(body); } catch (e) {}
            const p = findPipelineBySession(parsed.sessionCode);
            if (!p || !['spanish', 'english', 'off'].includes(parsed.language)) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Need an active sessionCode and language spanish|english|off' }));
                return;
            }
            // Send anything half-buffered in the old language before switching.
            if (p.captionBuffers) Object.keys(p.captionBuffers).forEach(l => flushZoomCaption(p, l));
            p.zoomCaptionLang = parsed.language;
            console.log(`[${p.sessionCode}] Zoom caption bar language -> ${parsed.language}`);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ zoomCaptionLang: p.zoomCaptionLang }));
        });
    }

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

                teardownCaptionPipeline(streamId);
                rtmsStreamInfo.delete(streamId);
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

                    // Register the stream. It only gets a caption pipeline
                    // once a session claims it (see connectZoomAudio).
                    const meetingId = payload.payload && (payload.payload.meeting_id || payload.payload.meeting_uuid);
                    rtmsStreamInfo.set(streamId, { sessionCode: null, meetingId, startedAt: Date.now() });
                    const waitingCode = takeWaitingZoomSession();
                    if (waitingCode) {
                        attachRtmsStream(streamId, waitingCode);
                    } else {
                        console.log(`RTMS: stream ${streamId} started -- no session yet. Start one with "Zoom meeting" as the audio input.`);
                    }
                    const sessionCode = waitingCode || '(unclaimed)';

                    // Look the pipeline up per frame, so audio flows the
                    // moment a session claims this stream -- and stops if
                    // that session ends.
                    client.onAudioData((data, size, timestamp, metadata) => {
                        const pipeline = rtmsCaptionPipelines.get(streamId);
                        if (pipeline) feedPipelineAudio(pipeline, data);
                    });

                    // the SDK's DEFAULT audio format is compressed Opus at 48kHz stereo -- not the simple L16/16kHz/mono raw PCM
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

                storeZoomTokens(tokenData);
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

    // Local test harness: browser sends raw 16kHz PCM16 frames here, and we
    // feed them into the SAME pipeline Zoom RTMS uses. Access-code protected,
    // since it spends the server's OpenAI key.
    if (role === 'audio-test') {
        if (params.get('code') !== process.env.ACCESS_CODE) {
            ws.send(JSON.stringify({ type: 'error', error: 'Invalid access code' }));
            ws.close();
            return;
        }
        const testCode = createSession(process.env.OPENAI_API_KEY, params.get('mic') === 'near_field' ? 'near_field' : 'far_field', 'bilingual');
        const streamKey = 'audio-test-' + testCode;
        const pipeline = createCaptionPipeline(streamKey, testCode);
        console.log(`[${testCode}] audio-test pipeline started`);
        ws.send(JSON.stringify({ type: 'session', sessionCode: testCode }));

        ws.on('message', (data, isBinary) => {
            if (isBinary) feedPipelineAudio(pipeline, data);
        });
        ws.on('close', () => {
            teardownCaptionPipeline(streamKey);
            console.log(`[${testCode}] audio-test pipeline stopped`);
        });
        return;
    }

    const session = sessions[sessionCode];
    if (!session) { ws.close(); return; }

    if (role === 'leader') {
        session.leaderSocket = ws;
        const pipelineKey = 'leader-' + sessionCode;
        // A reconnect (tab resume, network blip) cancels a pending teardown,
        // so the same pipeline keeps running without losing its state.
        if (session.leaderTeardownTimer) {
            clearTimeout(session.leaderTeardownTimer);
            session.leaderTeardownTimer = null;
        }

        ws.on('message', (data, isBinary) => {
            // Binary = one 20ms frame of 16kHz mono PCM16 from the browser,
            // the same shape Zoom RTMS delivers. Same pipeline, same code.
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

            if (msg.type === 'end_session') {
                endSession(sessionCode);
            }
        });

        ws.on('close', () => {
            // Only the CURRENT leader socket closing matters; a stale one
            // replaced by a reconnect must not tear anything down.
            if (session.leaderSocket !== ws || session.ended) return;
            // Give the leader a minute to come back before stopping the
            // OpenAI sessions, so a brief drop doesn't cost the transcript.
            session.leaderTeardownTimer = setTimeout(() => {
                teardownCaptionPipeline(pipelineKey);
                console.log(`[${sessionCode}] leader gone 60s -- pipeline stopped`);
            }, 60000);
        });

    } else if (role === 'listener') {
        const id = Math.random().toString(36).substring(2, 8);
        const language = params.get('language') || 'english';
        ws.listenerId = id;
        session.listenerSockets[id] = { ws, language, wantsAudio: false };
        ws.send(JSON.stringify({ type: 'welcome', listenerId: id }));

        ws.on('message', (data) => {
            let msg;
            try { msg = JSON.parse(data); } catch (e) { return; }
            const entry = session.listenerSockets[id];
            if (!entry) return;
            // Translated audio is pushed as binary over this same socket.
            if (msg.type === 'request_audio') {
                entry.wantsAudio = true;
                console.log(`[${sessionCode}] AUDIO checkpoint 2: listener ${id} (${language}) asked for audio`);
            }
            if (msg.type === 'stop_audio') entry.wantsAudio = false;
        });

        ws.on('close', () => {
            delete session.listenerSockets[id];
        });
    }
});

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