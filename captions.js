// Shared caption renderer for every page that shows live captions.
//
// The server sends two kinds of messages on /stream/<code>/<language>:
//   {type:'line', id, text} -- English: the current text of sentence `id`.
//                              A later message with the same id REPLACES it
//                              (used to swap in translations after the fact).
//   {text}                  -- Spanish: plain text to append.
//   {type:'session_ended'}  -- the session is over.
//
// Usage: const stream = attachCaptions(url, boxElement, { onEnded });
//        stream.close() to stop.
function attachCaptions(url, box, opts) {
    opts = opts || {};
    const lines = new Map();
    const source = new EventSource(url);

    // Only follow new text if the reader is already at the bottom, so
    // someone scrolling back to reread isn't yanked down.
    const nearBottom = () => box.scrollHeight - box.scrollTop - box.clientHeight < 40;

    source.onmessage = (e) => {
        let data;
        try { data = JSON.parse(e.data); } catch (err) { return; }
        if (data.type === 'session_ended') {
            source.close();
            if (opts.onEnded) opts.onEnded();
            return;
        }
        const follow = nearBottom();
        if (data.type === 'line') {
            let el = lines.get(data.id);
            if (!el) {
                el = document.createElement('span');
                lines.set(data.id, el);
                box.appendChild(el);
            }
            el.textContent = data.text;
        } else if (data.text) {
            box.appendChild(document.createTextNode(data.text));
        } else {
            return;
        }
        if (follow) box.scrollTop = box.scrollHeight;
    };
    if (opts.onOpen) source.onopen = opts.onOpen;
    if (opts.onError) source.onerror = opts.onError;
    return source;
}
