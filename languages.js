// The languages Recovery Translator offers, shared by the server and every
// page (served at /languages.js). To add a language: add an entry here with
// its Gemini language code as the key, and add it to LANGUAGE_ORDER.
//
// Listener-facing text is in each language. Have a native speaker check new
// entries before they go live.
(function (root) {
    const LANGUAGES = {
        en: {
            name: 'English', native: 'English',
            listen: '🔊 Listen in English', stop: '🔇 Stop audio',
            listening: 'Listening — audio starts with the next sentence.',
            ended: 'This session has ended. Thank you!',
            connected: 'Connected', reconnecting: 'Reconnecting…',
            testSound: 'No sound? Play a test sound',
            chat: 'Live captions in English'
        },
        es: {
            name: 'Spanish', native: 'Español',
            listen: '🔊 Escuchar en español', stop: '🔇 Detener audio',
            listening: 'Escuchando — el audio empieza con la próxima frase.',
            ended: 'Esta sesión ha terminado. ¡Gracias!',
            connected: 'Conectado', reconnecting: 'Reconectando…',
            testSound: '¿No hay sonido? Reproducir un sonido de prueba',
            chat: 'Subtítulos en vivo en español'
        },
        pl: {
            name: 'Polish', native: 'Polski',
            listen: '🔊 Słuchaj po polsku', stop: '🔇 Wyłącz dźwięk',
            listening: 'Słuchasz — dźwięk zacznie się od następnego zdania.',
            ended: 'Ta sesja się zakończyła. Dziękujemy!',
            connected: 'Połączono', reconnecting: 'Ponowne łączenie…',
            testSound: 'Nie słychać? Odtwórz dźwięk testowy',
            chat: 'Napisy na żywo po polsku'
        },
        fr: {
            name: 'French', native: 'Français',
            listen: '🔊 Écouter en français', stop: '🔇 Couper le son',
            listening: 'Écoute en cours — le son commence à la phrase suivante.',
            ended: 'Cette session est terminée. Merci !',
            connected: 'Connecté', reconnecting: 'Reconnexion…',
            testSound: 'Pas de son ? Jouer un son test',
            chat: 'Sous-titres en direct en français'
        }
    };
    const LANGUAGE_ORDER = ['en', 'es', 'pl', 'fr'];

    // Old links and pages used full names.
    const ALIASES = { english: 'en', spanish: 'es' };
    function normalizeLanguage(code) {
        const c = String(code || '').toLowerCase();
        return LANGUAGES[c] ? c : (ALIASES[c] || null);
    }

    const api = { LANGUAGES, LANGUAGE_ORDER, normalizeLanguage };
    if (typeof module !== 'undefined' && module.exports) module.exports = api;
    else Object.assign(root, api);
})(this);

// ---- Browser only: the leader's language picker (index.html, Zoom panel) ----
// A small table: one row per language, a Captions and an Audio checkbox.
// Audio implies captions. The last choice is remembered on this device.
(function (root) {
    if (typeof document === 'undefined') return;
    const KEY = 'rt-language-choice';
    const DEFAULT = { languages: ['en', 'es'], audio: ['es'] };

    function savedChoice() {
        try {
            const c = JSON.parse(localStorage.getItem(KEY));
            if (c && Array.isArray(c.languages) && c.languages.length) return c;
        } catch (e) {}
        return DEFAULT;
    }

    root.renderLanguagePicker = function (container) {
        const choice = savedChoice();
        const rows = root.LANGUAGE_ORDER.map(code => {
            const L = root.LANGUAGES[code];
            const label = L.name === L.native ? L.name : `${L.name} · ${L.native}`;
            return `<tr><td>${label}</td>` +
                `<td><input type="checkbox" aria-label="${L.name} captions" data-lang="${code}" data-kind="captions"${choice.languages.includes(code) ? ' checked' : ''}></td>` +
                `<td><input type="checkbox" aria-label="${L.name} audio" data-lang="${code}" data-kind="audio"${choice.audio.includes(code) ? ' checked' : ''}></td></tr>`;
        }).join('');
        container.innerHTML = `<table class="lang-picker"><thead><tr><th>Language</th><th>Captions</th><th>Audio</th></tr></thead><tbody>${rows}</tbody></table>`;
        container.addEventListener('change', (e) => {
            const box = e.target;
            if (!box.dataset || !box.dataset.lang) return;
            const other = container.querySelector(`input[data-lang="${box.dataset.lang}"][data-kind="${box.dataset.kind === 'audio' ? 'captions' : 'audio'}"]`);
            if (box.dataset.kind === 'audio' && box.checked) other.checked = true;    // audio needs captions
            if (box.dataset.kind === 'captions' && !box.checked) other.checked = false;
        });
    };

    // { languages, audio } from the picker, and remembers it.
    root.readLanguagePicker = function (container) {
        const picked = kind => [...container.querySelectorAll(`input[data-kind="${kind}"]:checked`)].map(b => b.dataset.lang);
        const choice = { languages: picked('captions'), audio: picked('audio') };
        try { localStorage.setItem(KEY, JSON.stringify(choice)); } catch (e) {}
        return choice;
    };

    // "English, Spanish, ..."
    root.languageListText = function (codes) {
        return codes.map(c => root.LANGUAGES[c] ? root.LANGUAGES[c].name : c).join(', ');
    };
})(this);
