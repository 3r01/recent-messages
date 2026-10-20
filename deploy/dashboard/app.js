const nodes = document.querySelector('#nodes');
const number = (n, digits = 0) => n == null ? '—' : Number(n).toLocaleString(undefined, {maximumFractionDigits: digits});
const gib = n => n == null ? '—' : `${number(n / 1073741824, 2)} GiB`;
function element(tag, text, cls) { const e = document.createElement(tag); if (text != null) e.textContent = text; if (cls) e.className = cls; return e; }
function field(dl, label, value) { const wrap = element('div'); wrap.append(element('dt', label), element('dd', value)); dl.append(wrap); }
async function refresh() {
  try {
    const response = await fetch('api/status', {cache: 'no-store'});
    if (!response.ok) throw Error('Dashboard unavailable');
    const data = await response.json();
    const stale = Date.now() / 1000 - data.sample_time > 30;
    document.querySelector('#health').textContent = `${data.nodes.filter(n => n.healthy && !stale).length} / ${data.nodes.length} ready${stale ? ' · readings stale' : ''}`;
    document.querySelector('#timestamp').textContent = `Checked ${new Date(data.sample_time * 1000).toLocaleTimeString()}`;
    nodes.replaceChildren();
    for (const n of data.nodes) {
      const card = element('article', null, `node${n.healthy && !stale ? '' : ' bad'}`);
      const title = element('div', null, 'title');
      title.append(element('h2', n.name), element('span', stale ? 'STALE' : n.healthy ? 'READY' : 'UNAVAILABLE', 'badge'));
      const dl = element('dl');
      field(dl, 'Process memory', gib(n.rss_bytes)); field(dl, 'Cache / budget', `${gib(n.cache_bytes)} / ${gib(n.cache_limit_bytes)}`);
      field(dl, 'CPU cores used', number(n.cpu_cores, 2)); field(dl, 'Requests / second', number(n.requests_per_second, 2));
      field(dl, 'Persisted messages', number(n.messages)); field(dl, 'Ingest queue', number(n.queue));
      const bar = element('progress'); bar.max = n.cache_limit_bytes || 1; bar.value = n.cache_bytes || 0; bar.setAttribute('aria-label', 'Cache budget used');
      const sources = Object.values(n.sources || {});
      const fresh = sources.filter(s => s.connected && s.last_event > 0 && data.sample_time - s.last_event < 300).length;
      card.append(title, dl, bar, element('p', `${fresh}/${sources.length} fresh feeds · ${number(n.channels)} persisted channels · ${number(n.ingest_per_second)} accepted/s`, 'detail'));
      if (n.error || stale) card.append(element('p', `${n.error || 'Collector readings are stale'}${n.last_success ? ` · last measured ${new Date(n.last_success * 1000).toLocaleTimeString()}` : ''}`, 'detail warn'));
      nodes.append(card);
    }
    document.querySelector('#error').textContent = '';
  } catch (error) { document.querySelector('#error').textContent = 'Status refresh failed. Displayed readings may be stale.'; }
}
document.querySelector('#refresh').addEventListener('click', refresh);
refresh(); setInterval(refresh, 10000);
