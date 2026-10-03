# Recovery Translator

Live English and Spanish captions, plus spoken translation, for recovery meetings, area committees, and service assemblies. It makes meetings accessible to everyone, whatever language they speak.

## How it works

The session leader starts a session and shares a QR code. Attendees open it on their phones; no account or app is needed.

Whatever language someone speaks, everyone gets live **English and Spanish captions**. Depending on the session type, listeners can also tap a button to **hear** a live spoken translation:

| Session type | English | Spanish |
|---|---|---|
| Spanish audio (default) | text | text & audio |
| English audio | text & audio | text |
| Both | text & audio | text & audio |

Audio can come from:

- **This computer:** a microphone, or the computer's own sound through [BlackHole](https://existential.audio/blackhole/).
- **A Zoom meeting:** through the Recovery Translator Zoom app, using Zoom's real-time media streams (RTMS).

Translation runs on Google's Gemini Live Translate model (`gemini-3.5-live-translate-preview`). Each session runs two translators over the same audio, one into English and one into Spanish.

## Using it

**Session leaders**

1. Open the site, tap **I am a session leader**, and enter your access code.
2. Choose the session type and where the audio comes from, then tap **Start session**.
3. For Zoom, open **Apps → Recovery Translator** in the meeting and tap **Start captions**. You can also start the session from the Zoom app and post the join link to the meeting chat.
4. Share the QR code. **Open big display screen** shows both languages side by side for a projector.
5. Tap **End session** when you're done. A Zoom session also ends when the meeting's stream stops. **Pause** in the Zoom app keeps the session open.

**Listeners**

1. Scan the QR code, pick English or Español, and tap **Join**.
2. If the session offers audio in your language, tap **🔊 Listen in English** or **🔊 Escuchar en español**.

To request an access code, contact dynontricia@gmail.com.

## Pages

| Path | What it is |
|---|---|
| `/` | Leader and listener app (`index.html`) |
| `/display?session=CODE` | Big-screen view: QR code plus both caption columns |
| `/transcript?session=CODE&lang=english` | Plain pop-out caption window |
| `/zoom-app` | Panel that runs inside Zoom |

## Running it

```bash
npm install
npm start          # http://localhost:3000
```

Environment variables (put them in `.env` locally):

| Variable | Needed for |
|---|---|
| `ACCESS_CODE` | Starting a session |
| `GEMINI_API_KEY` | Translation |
| `ZOOM_CLIENT_ID`, `ZOOM_CLIENT_SECRET` | Zoom app authorization |
| `ZM_RTMS_CLIENT`, `ZM_RTMS_SECRET` | Joining Zoom audio streams (read by `@zoom/rtms`) |
| `ZOOM_WEBHOOK_SECRET_TOKEN` | Verifying the Zoom webhook URL |
| `ZOOM_TOKEN_FILE` | Optional. Where Zoom OAuth tokens are saved (default `data/zoom-tokens.json`) |

Zoom app settings point at:

- Home URL: `/zoom-app`
- OAuth redirect: `/zoom/oauth/callback`
- Event webhook (RTMS started/stopped): `/zoom/rtms-webhook`

Sessions live in memory only, so restarting the server ends every session.

## Privacy

No accounts or names are collected, and no audio, captions, or translations are stored on the server. Audio is sent to the **paid** Gemini API, so Google does not use it to improve its products. Google keeps prompts and responses for up to 55 days, only for abuse monitoring. Server logs hold only session codes, connection events and Zoom stream IDs, never speech or caption text. The full policy is under **Privacy Policy** on the site (`index.html`).

## Built for recovery

Built by and for the recovery community, with anonymity and accessibility in mind.
