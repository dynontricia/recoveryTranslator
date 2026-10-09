import re, html
idx = open('index.html').read()

def modal_body(mid):
    s = idx.index(f'id="{mid}"')
    s = idx.index('<h2>', s)
    e = idx.index('</div>\n</div>', s)
    body = idx[s:e].strip()
    return re.sub(r'<h2>.*?</h2>\s*', '', body, count=1)

privacy = modal_body('modal-privacy')
instructions = modal_body('modal-instructions')

EMAIL = 'dynontricia@gmail.com'
UPDATED = 'October 8, 2026'

def page(slug, title, intro, body):
    nav = ''.join(
        f'<a href="/{s}"{" aria-current=\"page\"" if s == slug else ""}>{t}</a>'
        for s, t in [('docs', 'Documentation'), ('support', 'Support'), ('privacy', 'Privacy'), ('terms', 'Terms')])
    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>{title} — Recovery Translator</title>
<link rel="icon" href="/logo.png"/>
<style>
  :root {{ --ink:#1a202c; --muted:#4a5568; --line:#e2e8f0; --accent:#4f46e5; --bg:#f7fafc; }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; color:var(--ink); background:var(--bg); line-height:1.6; }}
  header {{ background:#fff; border-bottom:1px solid var(--line); }}
  .bar {{ max-width:820px; margin:0 auto; padding:14px 20px; display:flex; flex-wrap:wrap; gap:8px 18px; align-items:center; }}
  .brand {{ font-weight:700; color:var(--ink); text-decoration:none; margin-right:auto; }}
  nav a {{ color:var(--muted); text-decoration:none; font-size:15px; margin-left:14px; }}
  nav a[aria-current] {{ color:var(--accent); font-weight:600; }}
  main {{ max-width:820px; margin:0 auto; padding:28px 20px 60px; }}
  h1 {{ font-size:30px; margin:0 0 4px; }}
  .date {{ color:var(--muted); font-size:14px; margin:0 0 20px; }}
  .intro {{ font-size:17px; }}
  h2 {{ font-size:21px; margin:34px 0 8px; padding-top:10px; border-top:1px solid var(--line); }}
  h3 {{ font-size:17px; margin:24px 0 6px; }}
  a {{ color:var(--accent); }}
  .card {{ background:#fff; border:1px solid var(--line); border-radius:10px; padding:16px 18px; margin:14px 0; }}
  footer {{ max-width:820px; margin:0 auto; padding:0 20px 40px; color:var(--muted); font-size:14px; }}
</style>
</head>
<body>
<header><div class="bar"><a class="brand" href="/">Recovery Translator</a><nav>{nav}</nav></div></header>
<main>
<h1>{title}</h1>
<p class="date">Last updated: {UPDATED}</p>
<p class="intro">{intro}</p>
{body}
</main>
<footer>Questions? Email <a href="mailto:{EMAIL}">{EMAIL}</a>.</footer>
</body>
</html>
'''

# ---------- Privacy (same text as the site's policy) ----------
privacy_body = re.sub(r'<p class="modal-date">.*?</p>\s*', '', privacy, count=1)
first_p = re.search(r'<p>(.*?)</p>', privacy_body, re.S)
privacy_intro = first_p.group(1)
privacy_body = privacy_body[first_p.end():].replace('<h3>', '<h2>').replace('</h3>', '</h2>')
privacy_body += f'''
<h2>Zoom app: removing it and your data</h2>
<p>Recovery Translator stores no Zoom user data, so there is nothing to delete when the app is removed. Removing it stops all access immediately. See <a href="/docs#remove">how to remove the app</a>.</p>'''

# ---------- Documentation ----------
docs_body = f'''
<h2 id="what">What it does</h2>
<p>Recovery Translator gives a meeting live English and Spanish captions, whatever language someone speaks, plus an optional spoken translation people can listen to on their own phone. Listeners need no account or app; they join with a 6-character session code or QR code.</p>
<p>Audio can come from a computer's microphone (or its system audio), or from a Zoom meeting through the Recovery Translator Zoom app. Translation is done by Google's Gemini Live API.</p>

<h2 id="requirements">Requirements</h2>
<ul>
  <li>An access code to start sessions. Listeners don't need one. To request an access code, email <a href="mailto:{EMAIL}">{EMAIL}</a>.</li>
  <li>For the Zoom app: a Zoom account that can host meetings, the Zoom desktop client, and Zoom's <strong>Realtime Media Streams</strong> enabled for your account. You'll be asked to allow the app to access meeting audio when you start captions.</li>
  <li>Listeners: any phone, tablet, or computer with a current web browser.</li>
</ul>

<h2 id="add">Adding the Zoom app</h2>
<ol>
  <li>Sign in to the Zoom App Marketplace and open the <strong>Recovery Translator</strong> listing.</li>
  <li>Click <strong>Add</strong>, review the permissions, and click <strong>Allow</strong>.</li>
  <li>The app now appears under <strong>Apps</strong> in your Zoom meetings.</li>
</ol>

<h2 id="zoom">Using it in a Zoom meeting</h2>
<ol>
  <li>Start your meeting and open <strong>Apps → Recovery Translator</strong>.</li>
  <li>Enter your access code, choose the session type, and tap <strong>Start session</strong>. (Or start the session on the website, choose <strong>A Zoom meeting</strong> as the audio source, and the panel will pick it up.)</li>
  <li>Allow Zoom to share meeting audio with the app when prompted. Captions appear in the panel within a few seconds.</li>
  <li>Tap <strong>Post join link to meeting chat</strong> so participants can follow along on their own device, in English or Spanish.</li>
  <li>Use <strong>Pause</strong> to stop sending audio for part of a meeting, and <strong>End session</strong> when you're done. The session also ends when the meeting ends.</li>
</ol>
<p>The panel's text-size buttons (A− / A+) and the <strong>English</strong> / <strong>English + Español</strong> switch change only your own view.</p>

<h2 id="browser">Using it without Zoom</h2>
{instructions.replace('<h3>', '<h3>').replace('Session Leaders', 'Session leaders')}

<h2 id="remove">Removing the Zoom app</h2>
<ol>
  <li>Sign in to the Zoom App Marketplace.</li>
  <li>Click <strong>Manage → Added Apps</strong>.</li>
  <li>Find <strong>Recovery Translator</strong> and click <strong>Remove</strong>.</li>
</ol>
<p>Recovery Translator does not store any Zoom user data, so nothing remains with us after removal. See the <a href="/privacy">Privacy Policy</a>.</p>

<h2 id="data">Data and privacy, in brief</h2>
<ul>
  <li>No accounts or names are collected, and no audio, captions, or translations are saved.</li>
  <li>Meeting audio is sent over an encrypted connection to Google's paid Gemini API for translation. Google does not use it to improve its products, and keeps it for up to 55 days for abuse monitoring only.</li>
  <li>Everything said while captions are running is sent for translation. Leaders should tell the group at the start of the meeting.</li>
</ul>
<p>Full details: <a href="/privacy">Privacy Policy</a>.</p>

<h2 id="troubleshooting">Troubleshooting</h2>
<p>See <a href="/support#troubleshooting">Support</a>.</p>
'''

# ---------- Support ----------
support_body = f'''
<div class="card">
  <h3 style="margin-top:0">Contact</h3>
  <p style="margin:0">Email <a href="mailto:{EMAIL}">{EMAIL}</a>. Include your session code if you have one, and what you saw. We usually reply within 2 business days.</p>
</div>
<p>For access codes, questions about privacy, or reporting a security concern, use the same address.</p>

<h2 id="troubleshooting">Troubleshooting</h2>
<h3>The Zoom panel says no session is running</h3>
<p>Start one in the panel with your access code, or on the website with <strong>A Zoom meeting</strong> as the audio source.</p>
<h3>Captions never start in Zoom</h3>
<ul>
  <li>Make sure you allowed Zoom to share meeting audio with the app when prompted.</li>
  <li>Your Zoom account admin may need to enable <strong>Realtime Media Streams</strong> for the account.</li>
  <li>Tap <strong>Pause</strong>, then <strong>Start captions</strong> again.</li>
</ul>
<h3>Captions stopped partway through</h3>
<p>Check your internet connection. Captions reconnect on their own after a short drop. If they don't come back within a minute, tap <strong>Start captions</strong> again.</p>
<h3>I can't hear the spoken translation</h3>
<ul>
  <li>Tap <strong>No sound? Play a test sound</strong> on the listener screen. If you don't hear the beep, check your device's volume, silent switch, and Bluetooth.</li>
  <li>Spoken translation is only offered in the language(s) the leader chose when starting the session.</li>
  <li>On a phone, keep the page open; some phones stop audio when the screen locks.</li>
</ul>
<h3>Translations are inaccurate</h3>
<p>Translation is automatic and works best with one person speaking at a time, close to the microphone. Background noise and crosstalk lower accuracy.</p>
'''

# ---------- Terms ----------
terms_body = f'''
<h2>Using Recovery Translator</h2>
<p>Recovery Translator is a free service that provides automatic live captions and spoken translation for recovery meetings and related service meetings. By using it, including the Recovery Translator Zoom app, you agree to these terms.</p>

<h2>Access</h2>
<ul>
  <li>Starting a session requires an access code. Don't share it outside your group's trusted servants. We may change or revoke access codes at any time.</li>
  <li>Listeners may join any session they have the code for, at the leader's invitation.</li>
</ul>

<h2>Your responsibilities</h2>
<ul>
  <li>Leaders should tell participants that the meeting is being captioned and translated by an automated AI service before captions start.</li>
  <li>Use the service only for meetings you're entitled to caption, and follow your group's and Zoom's rules.</li>
  <li>Don't use the service for anything unlawful, to harass anyone, or in any way that violates Google's <a href="https://policies.google.com/terms/generative-ai/use-policy">Generative AI Prohibited Use Policy</a>, since audio is processed by Google.</li>
  <li>Don't try to disrupt, overload, or gain unauthorized access to the service.</li>
</ul>

<h2>Translations are automatic</h2>
<p>Captions and translations are produced by machine and can be incomplete or wrong. Don't rely on them where accuracy is critical, such as medical, legal, financial, or emergency situations.</p>

<h2>Privacy</h2>
<p>How meeting audio is handled is described in the <a href="/privacy">Privacy Policy</a>, which is part of these terms.</p>

<h2>No warranty</h2>
<p>The service is provided "as is" and "as available," without warranties of any kind, to the extent the law allows. We don't guarantee it will be available, uninterrupted, or error-free.</p>

<h2>Limitation of liability</h2>
<p>To the extent the law allows, we are not liable for any indirect, incidental, or consequential damages, or for any loss arising from the use of, or inability to use, the service or its translations.</p>

<h2>Changes and ending access</h2>
<p>We may change or discontinue the service, or these terms, at any time. If we change the terms, we'll update the date at the top of this page. You can stop using the service at any time, and remove the Zoom app as described in the <a href="/docs#remove">documentation</a>.</p>

<h2>Third-party services</h2>
<p>The service relies on Zoom and Google. Their own terms and policies apply to your use of their services.</p>
'''

pages = {
    'privacy': page('privacy', 'Privacy Policy', privacy_intro, privacy_body),
    'terms': page('terms', 'Terms of Use', 'These terms cover your use of Recovery Translator, including its website and Zoom app.', terms_body),
    'support': page('support', 'Support', 'Help with Recovery Translator, on the website or in Zoom.', support_body),
    'docs': page('docs', 'Documentation', 'How to add, use, and remove Recovery Translator.', docs_body),
}
for name, content in pages.items():
    open(f'{name}.html', 'w').write(content)
    print(name, len(content))
