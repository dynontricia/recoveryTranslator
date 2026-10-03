// Shared caption renderer for every page that shows live captions.
//
// The server sends two kinds of messages on /stream/<code>/<language>:
//   {text}                  -- caption text to append.
//   {type:'session_ended'}  -- the session is over.
//
// Usage: const stream = attachCaptions(url, boxElement, { onEnded, onOpen, onError });
//        stream.close() to stop.
function attachCaptions(url, box, opts) {
    opts = opts || {};
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
        if (!data.text) return;
        const follow = nearBottom();
        box.appendChild(document.createTextNode(data.text));
        if (follow) box.scrollTop = box.scrollHeight;
    };
    if (opts.onOpen) source.onopen = opts.onOpen;
    if (opts.onError) source.onerror = opts.onError;
    return source;
}
