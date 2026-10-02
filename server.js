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
                        // Only pay for this transcript when it's the English source.
                        ...(ENGLISH_SOURCE === 'translate' ? { transcription: { model: 'gpt-realtime-whisper' } } : {}),
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
// Dedicated transcription session: its only job is the transcript of what
// was actually said, in whatever language. Same settings that streamed well
// in the browser version: gpt-realtime-whisper, delay 'high', live deltas.
// No language is set, so Spanish comes back as Spanish for franc/Google.
// gpt-realtime-whisper has no server VAD (OpenAI rejects turn_detection for
// it), so we commit ourselves -- only after a pause, and only to keep the
// audio buffer bounded. Text streams without waiting for commits.
const TRANSCRIBE_COMMIT_PAUSE_MS = 2500;

function connectEnglishTranscriptionWs(pipeline) {
    const apiKey = sessions[pipeline.sessionCode].apiKey;
    const ws = new WebSocket('wss://api.openai.com/v1/realtime?intent=transcription', {
        headers: { 'Authorization': `Bearer ${apiKey}`, 'OpenAI-Safety-Identifier': 'recovery-translator' }
    });
    pipeline.transcribeWs = ws;
    pipeline.transcribeReady = false;

    ws.on('open', () => {
        console.log(`[${pipeline.sessionCode}] transcription: connected`);
        ws.send(JSON.stringify({
            type: 'session.update',
            session: {
                type: 'transcription',
                audio: {
                    input: {
                        format: { type: 'audio/pcm', rate: 24000 },
                        transcription: { model: 'gpt-realtime-whisper', delay: 'high' },
                        turn_detection: null
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
            console.log(`[${pipeline.sessionCode}] transcription: live`);
            return;
        }
        if (ev.type === 'error') {
            // An empty-buffer commit is harmless (nobody spoke since the last one).
            const msg = JSON.stringify(ev.error || ev);
            if (!/buffer too small|buffer is empty|input_audio_buffer_commit_empty/i.test(msg)) {
                console.error(`[${pipeline.sessionCode}] transcription ERROR:`, msg);
            }
            return;
        }
        if (ev.type === 'conversation.item.input_audio_transcription.delta' && ev.delta) {
            let delta = ev.delta;
            // The first words after a commit start a new segment and often
            // arrive without a leading space -- add one so words don't glue.
            if (pipeline.transcribeNeedsSpace) {
                if (!/^\s/.test(delta)) delta = ' ' + delta;
                pipeline.transcribeNeedsSpace = false;
            }
            handleSourceTranscriptDelta(pipeline, delta);

            // Commit after a pause, to keep the audio buffer bounded.
            if (pipeline.transcribeCommitTimer) clearTimeout(pipeline.transcribeCommitTimer);
            pipeline.transcribeCommitTimer = setTimeout(() => {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ type: 'input_audio_buffer.commit' }));
                    pipeline.transcribeNeedsSpace = true;
                }
            }, TRANSCRIBE_COMMIT_PAUSE_MS);
        }
    });

    ws.on('error', (err) => console.error(`[${pipeline.sessionCode}] transcription WS error:`, err.message));
    ws.on('close', (code) => {
        console.log(`[${pipeline.sessionCode}] transcription: closed (code ${code})`);
        if (pipeline.transcribeCommitTimer) clearTimeout(pipeline.transcribeCommitTimer);
    });
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

// ---- Gemini Live Translate -------------------------------------------------
// Speech-to-speech translation into Spanish. Gives us, from one connection:
//   * Spanish captions  (outputTranscription)
//   * Spanish audio     (modelTurn inlineData, PCM16 24kHz -- same format the
//                        listener player already handles)
//   * optionally the source transcript (inputTranscription), when
//     ENGLISH_SOURCE=gemini
// Input is PCM16 16kHz -- what Zoom RTMS and the browser already send -- so
// no resampling. Google recommends ~100ms chunks; our frames are 20ms, so
// five are batched per send.
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

function connectGeminiTranslator(pipeline) {
    pipeline.gemini = {
        active: null,          // the connection audio goes to
        opening: null,         // a connection being set up (start or hand-off)
        pending: Buffer.alloc(0),
        closed: false,
        retryMs: 1000,
        retryTimer: null,
        loggedChunks: 0
    };
    openGeminiConnection(pipeline, 'start');
}

function openGeminiConnection(pipeline, why) {
    const g = pipeline.gemini;
    if (!g || g.closed || g.opening) return;
    const key = process.env.GEMINI_API_KEY;
    if (!key) {
        console.error(`[${pipeline.sessionCode}] gemini: GEMINI_API_KEY is not set -- no Spanish translation`);
        return;
    }
    const session = sessions[pipeline.sessionCode];
    const wantSource = ENGLISH_SOURCE === 'gemini';
    // Key goes in the URL (Google's documented form) -- never log this URL.
    const ws = new WebSocket(`${GEMINI_WS_URL}?key=${encodeURIComponent(key)}`);
    const conn = { ws, ready: false, retiring: false, rotateTimer: null, openedAt: Date.now() };
    g.opening = conn;
    console.log(`[${pipeline.sessionCode}] gemini: connecting (${why})`);

    ws.on('open', () => {
        ws.send(JSON.stringify({
            setup: {
                model: `models/${GEMINI_TRANSLATE_MODEL}`,
                generationConfig: {
                    responseModalities: ['AUDIO'],
                    outputAudioTranscription: {},
                    ...(wantSource ? { inputAudioTranscription: {} } : {}),
                    translationConfig: {
                        targetLanguageCode: 'es',
                        // Spanish already being spoken is passed through in
                        // Spanish, so Spanish listeners hear every speaker.
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
            promoteGeminiConnection(pipeline, conn);
            return;
        }
        if (msg.goAway) {
            console.log(`[${pipeline.sessionCode}] gemini: goAway (time left ${msg.goAway.timeLeft || '?'}) -- handing off`);
            if (conn === g.active) openGeminiConnection(pipeline, 'goAway hand-off');
            return;
        }
        if (msg.error) {
            console.error(`[${pipeline.sessionCode}] gemini ERROR:`, JSON.stringify(msg.error));
            return;
        }

        const sc = msg.serverContent;
        if (!sc) return;
        const transcriptOnly = session && session.mode === 'transcript_only';

        // Spanish captions
        if (sc.outputTranscription && sc.outputTranscription.text) {
            const text = sc.outputTranscription.text;
            if (g.loggedChunks < 8) {
                // A few raw chunks, to check how Gemini spaces its text.
                g.loggedChunks++;
                console.log(`[${pipeline.sessionCode}] gemini caption chunk ${JSON.stringify(text)}`);
            }
            checkSourceTranscriptStall(pipeline);
            if (!transcriptOnly) {
                broadcast(pipeline.sessionCode, 'spanish', text);
                queueZoomCaption(pipeline, text, 'es-ES');
            }
        }

        // Spanish audio -> listeners who tapped "Hear translation"
        if (sc.modelTurn && sc.modelTurn.parts && !transcriptOnly) {
            for (const part of sc.modelTurn.parts) {
                if (part.inlineData && part.inlineData.data) {
                    if (!pipeline.loggedFirstAudio) {
                        pipeline.loggedFirstAudio = true;
                        console.log(`[${pipeline.sessionCode}] AUDIO checkpoint 1: Gemini is producing Spanish audio (${part.inlineData.mimeType || 'pcm'})`);
                    }
                    relayTranslatedAudio(pipeline.sessionCode, Buffer.from(part.inlineData.data, 'base64'));
                }
            }
        }

        // Source transcript, when Gemini is the English caption source
        if (wantSource && sc.inputTranscription && sc.inputTranscription.text) {
            handleSourceTranscriptDelta(pipeline, sc.inputTranscription.text);
        }
    });

    ws.on('error', (err) => console.error(`[${pipeline.sessionCode}] gemini WS error:`, err.message));

    ws.on('close', (code, reason) => {
        if (conn.rotateTimer) clearTimeout(conn.rotateTimer);
        const why = reason && reason.toString();
        if (g.closed || conn.retiring) return;
        console.warn(`[${pipeline.sessionCode}] gemini: connection closed (code ${code}${why ? `: ${why}` : ''})`);
        if (g.opening === conn) g.opening = null;
        if (g.active === conn) g.active = null;
        // Unexpected drop: reconnect, backing off if it keeps failing.
        if (!g.active && !g.opening && !g.retryTimer) {
            g.retryTimer = setTimeout(() => {
                g.retryTimer = null;
                openGeminiConnection(pipeline, 'reconnect');
            }, g.retryMs);
            g.retryMs = Math.min(g.retryMs * 2, GEMINI_RECONNECT_MAX_MS);
        }
    });
}

// A new connection is ready: send audio to it, and retire the old one.
function promoteGeminiConnection(pipeline, conn) {
    const g = pipeline.gemini;
    if (!g || g.closed) { conn.ws.close(); return; }
    conn.ready = true;
    if (g.opening === conn) g.opening = null;
    const old = g.active;
    g.active = conn;
    g.retryMs = 1000;
    console.log(`[${pipeline.sessionCode}] gemini: live${old ? ' (handed off)' : ''}`);
    if (old && old !== conn) retireGeminiConnection(old);
    // Hand off before Google's ~10-minute limit, even if goAway never comes.
    conn.rotateTimer = setTimeout(() => {
        if (g.active === conn) openGeminiConnection(pipeline, 'scheduled hand-off');
    }, GEMINI_ROTATE_MS);
    flushGeminiAudio(pipeline);
}

function retireGeminiConnection(conn) {
    conn.retiring = true;
    if (conn.rotateTimer) clearTimeout(conn.rotateTimer);
    // It gets no new audio; give it a moment to finish translating.
    setTimeout(() => { try { conn.ws.close(); } catch (e) {} }, GEMINI_HANDOFF_GRACE_MS);
}

function sendGeminiAudio(pipeline, pcm16k) {
    const g = pipeline.gemini;
    if (!g || g.closed) return;
    g.pending = Buffer.concat([g.pending, pcm16k]);
    flushGeminiAudio(pipeline);
}

function flushGeminiAudio(pipeline) {
    const g = pipeline.gemini;
    const ws = g.active && g.active.ready && g.active.ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        // Not connected right now: keep the most recent few seconds.
        if (g.pending.length > GEMINI_BUFFER_MAX_BYTES) {
            g.pending = g.pending.subarray(g.pending.length - GEMINI_BUFFER_MAX_BYTES);
        }
        return;
    }
    while (g.pending.length >= GEMINI_CHUNK_BYTES) {
        const chunk = g.pending.subarray(0, GEMINI_CHUNK_BYTES);
        g.pending = g.pending.subarray(GEMINI_CHUNK_BYTES);
        ws.send(JSON.stringify({
            realtimeInput: { audio: { data: chunk.toString('base64'), mimeType: 'audio/pcm;rate=16000' } }
        }));
    }
}

function closeGeminiTranslator(pipeline) {
    const g = pipeline.gemini;
    if (!g) return;
    g.closed = true;
    if (g.retryTimer) clearTimeout(g.retryTimer);
    for (const conn of [g.active, g.opening]) {
        if (!conn) continue;
        if (conn.rotateTimer) clearTimeout(conn.rotateTimer);
        try { conn.ws.close(); } catch (e) {}
    }
    g.active = g.opening = null;
}

// ---- Shared caption pipeline --------------------------------------------
// Used by BOTH the real Zoom RTMS path and the local browser test path
// (/audio-test), so a local test exercises exactly the code Zoom runs.
// Both paths feed 16kHz mono PCM16 in 20ms frames.

// Who translates into Spanish (captions + audio):
//   'gemini' (default) -- Gemini Live Translate. Needs GEMINI_API_KEY.
//   'openai'           -- OpenAI gpt-realtime-translate.
const TRANSLATE_PROVIDER = process.env.TRANSLATE_PROVIDER === 'openai' ? 'openai' : 'gemini';

// Where the English captions' source text comes from:
//   'dedicated' (default) -- OpenAI transcription session (gpt-realtime-whisper)
//                            whose only job is the transcript.
//   'gemini'              -- Gemini's own input transcript (TRANSLATE_PROVIDER=gemini).
//   'translate'           -- OpenAI translate session's input transcript
//                            (TRANSLATE_PROVIDER=openai; can stall for a minute).
let ENGLISH_SOURCE = ['dedicated', 'gemini', 'translate'].includes(process.env.ENGLISH_SOURCE)
    ? process.env.ENGLISH_SOURCE : 'dedicated';
if ((ENGLISH_SOURCE === 'gemini' && TRANSLATE_PROVIDER !== 'gemini') ||
    (ENGLISH_SOURCE === 'translate' && TRANSLATE_PROVIDER !== 'openai')) {
    console.warn(`ENGLISH_SOURCE=${ENGLISH_SOURCE} needs a different TRANSLATE_PROVIDER -- using 'dedicated'`);
    ENGLISH_SOURCE = 'dedicated';
}
console.log(`Translation: ${TRANSLATE_PROVIDER} · English caption source: ${ENGLISH_SOURCE}`);

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
        enLine: null,            // the English sentence still being spoken
        enMode: 'english',       // 'english' = stream live, 'spanish' = translate every chunk
        sentenceIdleTimer: null,
        // English captions come from THIS connection's transcript.
        sourceTranscriptWsKey: { dedicated: 'transcribeWs', translate: 'esWs', gemini: 'gemini' }[ENGLISH_SOURCE]
    };
    rtmsCaptionPipelines.set(streamKey, pipeline);

    // Which OpenAI sessions run -- one place to change it for Zoom AND tests.
    const englishOnly = sessions[sessionCode] && sessions[sessionCode].mode === 'transcript_only';
    if (ENGLISH_SOURCE === 'dedicated') connectEnglishTranscriptionWs(pipeline);
    // English-only sessions with a dedicated transcript don't need a
    // translator at all.
    if (!(englishOnly && ENGLISH_SOURCE === 'dedicated')) {
        if (TRANSLATE_PROVIDER === 'gemini') connectGeminiTranslator(pipeline);
        else connectTranslateWs(pipeline, 'es', 'esWs', 'esReady', 'spanish');
    }
    return pipeline;
}

function feedPipelineAudio(pipeline, data) {
    try {
        pipeline.audioFrameCount++;
        const pcm16k = toAudioBuffer(data);
        if (pipeline.audioFrameCount === 1) {
            console.log(`[${pipeline.sessionCode}] first audio frame -- byteLength=${pcm16k.length}`);
        }
        if (pipeline.audioFrameCount % 250 === 1) {
            const g = pipeline.gemini;
            console.log(`[${pipeline.sessionCode}] frame ${pipeline.audioFrameCount}, transcribeWs=${pipeline.transcribeWs && pipeline.transcribeWs.readyState}, ` +
                (g ? `gemini=${g.active ? 'live' : g.opening ? 'connecting' : 'down'}` : `esWs=${pipeline.esWs && pipeline.esWs.readyState}`));
        }

        // Gemini takes 16kHz as-is.
        if (pipeline.gemini) sendGeminiAudio(pipeline, pcm16k);

        // OpenAI needs 24kHz.
        const openaiTargets = [pipeline.transcribeWs, pipeline.enWs, pipeline.esWs]
            .filter(ws => ws && ws.readyState === WebSocket.OPEN);
        if (!openaiTargets.length) return;
        const b64 = resamplePCM16(pcm16k, 16000, 24000).toString('base64');
        // The translation endpoint needs the 'session.' prefix (confirmed by a
        // live rejection from OpenAI); the standard transcription endpoint
        // uses the plain event name.
        const translateMsg = JSON.stringify({ type: 'session.input_audio_buffer.append', audio: b64 });
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
    if (pipeline.enLine) finishSentence(pipeline, pipeline.enLine);
    // Flush buffered caption text so the last words spoken aren't dropped.
    if (pipeline.captionBuffers) {
        Object.keys(pipeline.captionBuffers).forEach(lang => flushZoomCaption(pipeline, lang));
    }
    if (pipeline.transcribeCommitTimer) clearTimeout(pipeline.transcribeCommitTimer);
    if (pipeline.transcribeWs) pipeline.transcribeWs.close();
    if (pipeline.enWs) pipeline.enWs.close();
    if (pipeline.esWs) pipeline.esWs.close();
    closeGeminiTranslator(pipeline);
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
        englishLines: new Map(),   // line id -> text, replayed to screens that join late
        nextLineId: 0,
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

// Sends a structured event (e.g. an English caption line) to one language's screens.
function broadcastEvent(sessionCode, language, obj) {
    const session = sessions[sessionCode];
    if (!session) return;
    const message = `data: ${JSON.stringify(obj)}\n\n`;
    session.clients[language].forEach(client => {
        try { client.write(message); } catch (e) {}
    });
}

// Sends a non-caption control event (e.g. spanish_turn) to every SSE client.
// ---- English captions: stream immediately, correct in place --------------
// Every word from the Whisper input transcript is shown the moment it
// arrives. Captions are sent as numbered LINES (one per sentence), so a line
// already on screen can be corrected later:
//   * every ~5 words, franc checks the latest chunk; if it looks Spanish,
//     Google translates just that chunk and it's swapped in place.
//   * when the sentence ends (. ! ?, a pause, or it runs long), franc
//     checks the whole sentence; if it isn't English, Google translates the
//     whole sentence and replaces the line.
// Screens receive {type:'line', id, text} and simply redraw line `id`.
// A slow translation only updates its own line, so it can't hold up others.
const CHUNK_WORDS = 5;
const SENTENCE_IDLE_MS = 1500;   // a pause this long ends the sentence
const SENTENCE_MAX_WORDS = 40;   // long run-ons are checked in pieces
const ENGLISH_HISTORY_LINES = 300; // kept for screens that join late

const wordCount = s => s.trim().split(/\s+/).filter(Boolean).length;
const leadingSpace = s => (/^\s/.test(s) ? ' ' : '');

// A line is a list of pieces (checked chunks) plus the words still arriving.
// Words still arriving are hidden while we're in Spanish mode.
function lineText(line) {
    return line.pieces.map(p => p.shown).join('') + (line.hideOpen ? '' : line.open);
}

function sendEnglishLine(pipeline, line) {
    const session = sessions[pipeline.sessionCode];
    if (!session) return;
    const text = lineText(line);
    session.englishLines.set(line.id, text);
    if (session.englishLines.size > ENGLISH_HISTORY_LINES) {
        session.englishLines.delete(session.englishLines.keys().next().value);
    }
    broadcastEvent(pipeline.sessionCode, 'english', { type: 'line', id: line.id, text });
}

// ---- Language mode -------------------------------------------------------
// A speaker who starts in Spanish usually keeps going in Spanish, so the
// pipeline remembers which language it's in:
//   'english' -- words stream to the screen live, as they arrive.
//   'spanish' -- raw words are held back; each ~5-word chunk appears only
//                once Google has translated it.
// English -> Spanish: franc's top answer is Spanish (chunk or sentence).
// Spanish -> English: franc says English AND Spanish scores below this.
// Anything in between keeps translating. Tune from the scores in the logs.
const SPANISH_EXIT_MAX = 0.6;

function spanishScore(verdict) {
    const spa = verdict.ranked.find(([lang]) => lang === 'spa');
    return spa ? spa[1] : 0;
}

// Updates the mode from a franc verdict and returns whether to translate.
function decideLanguage(pipeline, verdict, what) {
    const top = verdict.ranked[0][0];
    if (pipeline.enMode === 'spanish') {
        if (top === 'eng' && spanishScore(verdict) < SPANISH_EXIT_MAX) {
            setLanguageMode(pipeline, 'english', what, verdict);
            return false;
        }
        return true; // still Spanish, or not sure -- keep translating
    }
    if (top === 'spa') setLanguageMode(pipeline, 'spanish', what, verdict);
    return verdict.translate;
}

function setLanguageMode(pipeline, mode, what, verdict) {
    if (pipeline.enMode === mode) return;
    pipeline.enMode = mode;
    const scores = verdict.ranked.map(([l, s]) => `${l}=${s.toFixed(2)}`).join(' ');
    console.log(`[${pipeline.sessionCode}] LANGUAGE MODE -> ${mode.toUpperCase()} (${what}; ${scores})` +
        (mode === 'spanish' ? ' -- holding raw text, translating every chunk' : ' -- streaming live again'));
}

function handleSourceTranscriptDelta(pipeline, delta) {
    const session = sessions[pipeline.sessionCode];
    if (!session) return;
    const now = Date.now();
    if (pipeline.sourceStallWarned) {
        console.log(`[${pipeline.sessionCode}] SOURCE TRANSCRIPT resumed after ${((now - pipeline.lastSourceDeltaAt) / 1000).toFixed(1)}s of silence`);
        pipeline.sourceStallWarned = false;
    }
    pipeline.lastSourceDeltaAt = now;

    let line = pipeline.enLine;
    if (!line) {
        line = pipeline.enLine = { id: ++session.nextLineId, pieces: [], open: '', sentenceTranslated: false, hideOpen: false };
    }

    // A new word is starting and the unchecked words have reached a chunk:
    // set that chunk aside and check its language (this may switch modes).
    if (delta[0] === ' ' && wordCount(line.open) >= CHUNK_WORDS) {
        checkChunk(pipeline, line);
    }
    line.open += delta;
    line.hideOpen = pipeline.enMode === 'spanish';
    // English mode shows every word as it arrives. Spanish mode shows
    // nothing new until a chunk is translated (checkChunk sends it).
    if (!line.hideOpen) sendEnglishLine(pipeline, line);

    const total = wordCount(line.pieces.map(p => p.raw).join('') + line.open);
    if (/[.!?]["')\]]*\s*$/.test(delta) || total >= SENTENCE_MAX_WORDS) {
        finishSentence(pipeline, line);
    } else {
        if (pipeline.sentenceIdleTimer) clearTimeout(pipeline.sentenceIdleTimer);
        pipeline.sentenceIdleTimer = setTimeout(() => {
            if (pipeline.enLine === line) finishSentence(pipeline, line);
        }, SENTENCE_IDLE_MS);
    }
}

// Freezes the unchecked words into a piece and decides what to show:
// English -> the words themselves; otherwise Google's translation, swapped
// in when it returns. In Spanish mode the piece stays hidden until then.
function checkChunk(pipeline, line) {
    const hidden = line.hideOpen;
    const piece = { raw: line.open, shown: hidden ? '' : line.open };
    line.pieces.push(piece);
    line.open = '';
    const verdict = classifyChunk(piece.raw);
    logVerdict(pipeline, 'chunk', verdict, piece.raw);
    const translate = decideLanguage(pipeline, verdict, 'chunk');
    line.hideOpen = pipeline.enMode === 'spanish';

    if (!translate) {
        if (hidden) { piece.shown = piece.raw; sendEnglishLine(pipeline, line); }
        return;
    }
    const session = sessions[pipeline.sessionCode];
    const startedAt = Date.now();
    translateToEnglish(session, piece.raw, verdict.reason).catch(() => null).then(t => {
        // The whole-sentence translation wins if it already replaced this line.
        if (line.sentenceTranslated) return;
        if (t) console.log(`[${pipeline.sessionCode}] chunk translated in ${Date.now() - startedAt}ms: ${piece.raw.trim()} -> ${t}`);
        // If translation failed, show the original words rather than nothing.
        piece.shown = t ? leadingSpace(piece.raw) + t : piece.raw;
        sendEnglishLine(pipeline, line);
    });
}

// End of sentence: check the WHOLE sentence. English -> leave it. Otherwise
// translate the whole sentence and replace the line with it.
function finishSentence(pipeline, line) {
    if (pipeline.enLine === line) pipeline.enLine = null;
    if (pipeline.sentenceIdleTimer) { clearTimeout(pipeline.sentenceIdleTimer); pipeline.sentenceIdleTimer = null; }
    if (line.open) {
        if (line.hideOpen) {
            // Hidden tail in Spanish mode: run it through the chunk check so
            // it's revealed (translated or not) even if the sentence check
            // below decides the sentence is English.
            checkChunk(pipeline, line);
        } else {
            line.pieces.push({ raw: line.open, shown: line.open });
            line.open = '';
        }
    }
    const raw = line.pieces.map(p => p.raw).join('');
    if (!raw.trim()) return;
    const verdict = classifyChunk(raw);
    logVerdict(pipeline, 'sentence', verdict, raw);
    const translate = decideLanguage(pipeline, verdict, 'sentence');
    if (!translate) {
        queueZoomCaption(pipeline, lineText(line), 'en-US');
        return;
    }
    const session = sessions[pipeline.sessionCode];
    const startedAt = Date.now();
    translateToEnglish(session, raw, verdict.reason).catch(() => null).then(t => {
        if (!t) {
            // Keep whatever is on screen (chunk translations, or the original).
            console.log(`[${pipeline.sessionCode}] sentence translation FAILED after ${Date.now() - startedAt}ms -- keeping current line`);
        } else {
            console.log(`[${pipeline.sessionCode}] sentence translated in ${Date.now() - startedAt}ms: ${raw.trim()} -> ${t}`);
            line.sentenceTranslated = true;
            line.pieces = [{ raw, shown: leadingSpace(raw) + t }];
            sendEnglishLine(pipeline, line);
        }
        queueZoomCaption(pipeline, lineText(line), 'en-US');
    });
}

function logVerdict(pipeline, kind, verdict, text) {
    const scores = verdict.ranked.map(([l, s]) => `${l}=${s.toFixed(2)}`).join(' ');
    console.log(`[${pipeline.sessionCode}] ${kind} ${verdict.translate ? 'TRANSLATE' : 'english'} (${verdict.reason}; ${scores}): ${text.trim()}`);
}

// franc scales scores so the winner is always 1.0; the runner-up's score
// says how close the call was. Translate when franc says Spanish, when it
// can't decide ('und' -- usually short or mixed text), or when the runner-up
// is this close. A wrong "translate" is cheap (English comes back as-is);
// a wrong "english" leaves Spanish in the English captions.
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
    else if (pathname === '/captions.js') {
        serveFile(res, './captions.js', 'application/javascript; charset=UTF-8');
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

        // A screen joining late gets the English captions so far.
        if (language === 'english') {
            for (const [id, text] of sessions[sessionCode].englishLines) {
                try { res.write(`data: ${JSON.stringify({ type: 'line', id, text })}\n\n`); } catch (e) {}
            }
        }

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

    // Lets the Zoom panel end a session (listeners see "session ended").
    else if (req.method === 'POST' && pathname === '/zoom/end-session') {
        let body = '';
        req.on('data', c => { body += c; });
        req.on('end', () => {
            let parsed = {};
            try { parsed = JSON.parse(body); } catch (e) {}
            const s = sessions[parsed.sessionCode];
            if (!s || s.ended) {
                res.writeHead(404, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'No active session with that code' }));
                return;
            }
            endSession(parsed.sessionCode);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ended: true }));
        });
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