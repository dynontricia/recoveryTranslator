// Stored group settings: who may start sessions, and with which Gemini key.
//
// One small JSON file (STORE_PATH, on a Railway volume in production). Only
// this module touches it, so moving to a database later means rewriting this
// file and nothing else.
//
// What's in a group record:
//   id, label, active, createdAt
//   accessCodeHash -- HMAC of the group's access code. The code itself is
//                     shown once, when it's made, and never stored.
//   zoomUserHash   -- HMAC of the Zoom user ID that saved the settings from
//                     the Zoom app. Lets the panel find its own settings, and
//                     lets uninstall find what to delete. The raw Zoom ID is
//                     never stored.
//   geminiKey      -- the group's Gemini API key, encrypted (AES-256-GCM).
//                     null means "use the server's own key" (codes you issue).
//   slug           -- optional permanent link name, e.g. "area61" for
//                     app.recoverytranslator.com/area61. Set only by the admin.
//
// Both secrets come from KEY_ENCRYPTION_KEY (32 random bytes, base64), kept
// in a Railway variable -- never in the store file -- so a copy of the file
// alone reveals no keys and no access codes.
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const STORE_PATH = process.env.STORE_PATH || path.join(__dirname, 'data', 'store.json');

let encKey = null, hashKey = null;
(function loadKeys() {
    const raw = process.env.KEY_ENCRYPTION_KEY;
    if (!raw) return;
    const master = Buffer.from(raw, 'base64');
    if (master.length !== 32) {
        console.error('KEY_ENCRYPTION_KEY must be 32 bytes, base64-encoded -- stored settings are disabled');
        return;
    }
    // Two separate keys from one secret: one encrypts, one hashes.
    encKey = Buffer.from(crypto.hkdfSync('sha256', master, Buffer.alloc(0), 'rt-encrypt', 32));
    hashKey = Buffer.from(crypto.hkdfSync('sha256', master, Buffer.alloc(0), 'rt-hash', 32));
})();

function enabled() { return !!encKey; }

let data = { version: 1, groups: [] };
(function load() {
    try {
        data = JSON.parse(fs.readFileSync(STORE_PATH, 'utf8'));
        if (!Array.isArray(data.groups)) data.groups = [];
    } catch (err) {
        if (err.code !== 'ENOENT') console.error('Could not read store file:', err.message);
    }
    console.log(`Store: ${STORE_PATH} (${data.groups.length} group(s))${enabled() ? '' : ' -- KEY_ENCRYPTION_KEY not set, saving is disabled'}`);
})();

// Write to a temp file, then rename over the old one, so a crash mid-write
// never leaves a half-written store.
function save() {
    fs.mkdirSync(path.dirname(STORE_PATH), { recursive: true });
    const tmp = STORE_PATH + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(data, null, 2), { mode: 0o600 });
    fs.renameSync(tmp, STORE_PATH);
}

function hmac(value) {
    return crypto.createHmac('sha256', hashKey).update(String(value)).digest('hex');
}

function encrypt(plain) {
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', encKey, iv);
    const ct = Buffer.concat([cipher.update(plain, 'utf8'), cipher.final()]);
    return { iv: iv.toString('base64'), tag: cipher.getAuthTag().toString('base64'), ct: ct.toString('base64') };
}

function decrypt(box) {
    const decipher = crypto.createDecipheriv('aes-256-gcm', encKey, Buffer.from(box.iv, 'base64'));
    decipher.setAuthTag(Buffer.from(box.tag, 'base64'));
    return Buffer.concat([decipher.update(Buffer.from(box.ct, 'base64')), decipher.final()]).toString('utf8');
}

// Access codes: 12 characters, no look-alikes (0/O, 1/I/L), shown as
// XXXX-XXXX-XXXX. Dashes, spaces and case don't matter when typed.
const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
function newAccessCode() {
    let c = '';
    for (let i = 0; i < 12; i++) c += CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
    return c.match(/.{4}/g).join('-');
}
function normalizeCode(code) { return String(code || '').replace(/[\s-]/g, '').toUpperCase(); }

// What's safe to show (the admin list, the panel): never a key or a hash.
function describe(g) {
    return {
        id: g.id,
        label: g.label,
        active: g.active,
        createdAt: g.createdAt,
        fromZoomApp: !!g.zoomUserHash,
        hasAccessCode: !!g.accessCodeHash,
        usesOwnKey: !!g.geminiKey,
        keyHint: g.keyHint || null,
        slug: g.slug || null
    };
}

// null if there's no key -- or if it can't be decrypted, which means
// KEY_ENCRYPTION_KEY changed since it was saved. That group has to save its
// key again; nobody else is affected.
function geminiKeyFor(g) {
    if (!g || !g.geminiKey) return null;
    try { return decrypt(g.geminiKey); } catch (err) {
        console.error(`Store: can't decrypt the key for group ${g.id} -- was KEY_ENCRYPTION_KEY changed?`);
        return null;
    }
}

function findByAccessCode(code) {
    if (!enabled()) return null;
    const norm = normalizeCode(code);
    if (!norm) return null;
    const h = hmac('code:' + norm);
    return data.groups.find(g => g.active && g.accessCodeHash && crypto.timingSafeEqual(
        Buffer.from(g.accessCodeHash, 'hex'), Buffer.from(h, 'hex'))) || null;
}

function findByZoomUser(uid) {
    if (!enabled() || !uid) return null;
    const h = hmac('zoom:' + uid);
    return data.groups.find(g => g.zoomUserHash === h) || null;
}

function keyHint(key) { return '…' + key.slice(-4); }

// Permanent link names: 3-32 lowercase letters, digits, or dashes.
const SLUG_PATTERN = /^[a-z0-9](?:[a-z0-9-]{1,30}[a-z0-9])$/;
function normalizeSlug(slug) { return String(slug || '').trim().toLowerCase(); }

function findBySlug(slug) {
    const s = normalizeSlug(slug);
    return s ? data.groups.find(g => g.active && g.slug === s) || null : null;
}

// Sets (or, with an empty slug, removes) a group's link name. Returns
// { group } or { error }. The caller rules out names its own pages use.
function setSlug(id, slug) {
    const g = data.groups.find(x => x.id === id);
    if (!g) return { error: 'No such group' };
    const s = normalizeSlug(slug);
    if (!s) { g.slug = null; save(); return { group: describe(g) }; }
    if (!SLUG_PATTERN.test(s)) return { error: 'Use 3-32 lowercase letters, numbers, or dashes.' };
    if (data.groups.some(x => x.id !== id && x.slug === s)) return { error: 'Another group already has that link.' };
    g.slug = s;
    save();
    return { group: describe(g) };
}

// From the Zoom app's Settings screen: save (or replace) this Zoom user's
// key. A new website access code comes back the first time; it's only ever
// shown then.
function saveZoomGroup(uid, { label, geminiKey }) {
    let g = findByZoomUser(uid);
    let accessCode = null;
    if (!g) {
        accessCode = newAccessCode();
        g = {
            id: crypto.randomUUID(),
            createdAt: new Date().toISOString(),
            active: true,
            zoomUserHash: hmac('zoom:' + uid),
            accessCodeHash: hmac('code:' + normalizeCode(accessCode))
        };
        data.groups.push(g);
    }
    g.label = String(label || g.label || 'Zoom group').slice(0, 80);
    g.geminiKey = encrypt(geminiKey);
    g.keyHint = keyHint(geminiKey);
    save();
    return { group: describe(g), accessCode };
}

// From you (the admin API): a group with an access code, using either its
// own Gemini key or, with none, the server's.
function createCodeGroup({ label, geminiKey }) {
    const accessCode = newAccessCode();
    const g = {
        id: crypto.randomUUID(),
        createdAt: new Date().toISOString(),
        active: true,
        label: String(label || 'Group').slice(0, 80),
        zoomUserHash: null,
        accessCodeHash: hmac('code:' + normalizeCode(accessCode)),
        geminiKey: geminiKey ? encrypt(geminiKey) : null,
        keyHint: geminiKey ? keyHint(geminiKey) : null
    };
    data.groups.push(g);
    save();
    return { group: describe(g), accessCode };
}

// Uninstall, or "Remove saved key" in the panel: delete the record entirely.
function deleteByZoomUser(uid) {
    if (!enabled() || !uid) return 0;
    const h = hmac('zoom:' + uid);
    const before = data.groups.length;
    data.groups = data.groups.filter(g => g.zoomUserHash !== h);
    const removed = before - data.groups.length;
    if (removed) save();
    return removed;
}

function list() { return data.groups.map(describe); }

function setActive(id, active) {
    const g = data.groups.find(x => x.id === id);
    if (!g) return null;
    g.active = !!active;
    save();
    return describe(g);
}

function remove(id) {
    const before = data.groups.length;
    data.groups = data.groups.filter(g => g.id !== id);
    if (data.groups.length !== before) { save(); return true; }
    return false;
}

module.exports = {
    STORE_PATH, enabled, describe, geminiKeyFor, findBySlug, setSlug, normalizeSlug,
    findByAccessCode, findByZoomUser, saveZoomGroup, createCodeGroup,
    deleteByZoomUser, list, setActive, remove
};
