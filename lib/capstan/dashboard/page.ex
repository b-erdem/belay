defmodule Capstan.Dashboard.Page do
  @moduledoc false

  # The entire dashboard UI: one HTML document, vanilla JS, no build step.
  # Interpolation is disabled (~S) so JS template literals pass through.

  @html ~S"""
  <!doctype html>
  <html lang="en">
  <head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Capstan</title>
  <style>
    :root{--bg:#0b0e14;--panel:#131722;--panel2:#1a2030;--line:#232b3d;--text:#dce3f0;
      --dim:#8b96ad;--accent:#5eead4;--ready:#60a5fa;--running:#fbbf24;--succeeded:#34d399;
      --failed:#f87171;--cancelled:#94a3b8;--awaiting:#c084fc;--held:#f0abfc;--paused:#64748b}
    *{box-sizing:border-box;margin:0}
    body{background:var(--bg);color:var(--text);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,sans-serif}
    header{display:flex;align-items:baseline;gap:14px;padding:14px 22px;border-bottom:1px solid var(--line)}
    header h1{font-size:17px;letter-spacing:.4px}
    header .inst{color:var(--dim);font-family:ui-monospace,monospace;font-size:12px}
    header .live{margin-left:auto;color:var(--accent);font-size:12px}
    main{padding:18px 22px;max-width:1200px;margin:0 auto}
    .tiles{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:12px;margin-bottom:18px}
    .tile{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
    .tile h3{font-size:13px;display:flex;gap:8px;align-items:baseline}
    .tile h3 .caps{color:var(--dim);font-size:11px;font-weight:400}
    .tile .counts{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px}
    .badge{padding:1px 8px;border-radius:99px;font-size:11px;font-family:ui-monospace,monospace;
      background:var(--panel2);border:1px solid var(--line)}
    .b-ready{color:var(--ready)} .b-running{color:var(--running)} .b-succeeded{color:var(--succeeded)}
    .b-failed{color:var(--failed)} .b-cancelled{color:var(--cancelled)} .b-awaiting{color:var(--awaiting)}
    .b-held{color:var(--held)} .b-paused{color:var(--paused)}
    .filters{display:flex;gap:8px;margin-bottom:10px;align-items:center}
    select,button,input{background:var(--panel2);color:var(--text);border:1px solid var(--line);
      border-radius:7px;padding:5px 10px;font-size:13px}
    button{cursor:pointer} button:hover{border-color:var(--accent)}
    button.danger:hover{border-color:var(--failed)}
    table{width:100%;border-collapse:collapse;background:var(--panel);border:1px solid var(--line);border-radius:10px;overflow:hidden}
    th,td{text-align:left;padding:7px 12px;border-bottom:1px solid var(--line);font-size:13px}
    th{color:var(--dim);font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.6px}
    tbody tr{cursor:pointer} tbody tr:hover{background:var(--panel2)}
    td.mono, .mono{font-family:ui-monospace,monospace;font-size:12px}
    #drawer{position:fixed;top:0;right:-680px;width:660px;max-width:95vw;height:100vh;overflow:auto;
      background:var(--panel);border-left:1px solid var(--line);transition:right .18s;padding:18px;z-index:5}
    #drawer.open{right:0}
    #drawer h2{font-size:15px;display:flex;gap:10px;align-items:center;margin-bottom:10px}
    #drawer section{margin:14px 0} #drawer h4{color:var(--dim);font-size:11px;text-transform:uppercase;
      letter-spacing:.6px;margin-bottom:6px}
    pre{background:var(--bg);border:1px solid var(--line);border-radius:8px;padding:10px;overflow:auto;
      font-size:12px;max-height:220px}
    .actions{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0}
    .stepline{display:flex;gap:10px;padding:5px 0;border-bottom:1px dashed var(--line);font-size:12.5px}
    .stepline .cost{color:var(--dim);margin-left:auto;font-family:ui-monospace,monospace}
    #dag{background:var(--panel);border:1px solid var(--line);border-radius:10px;margin-top:10px;width:100%}
    #dag text{fill:var(--text);font:11px ui-monospace,monospace}
    .close{margin-left:auto}
    a{color:var(--accent);cursor:pointer;text-decoration:none}
    .empty{color:var(--dim);padding:18px;text-align:center}
  </style>
  </head>
  <body>
  <header>
    <h1>⚓ Capstan</h1><span class="inst" id="inst"></span><span class="live" id="live">● live</span>
  </header>
  <main>
    <div class="tiles" id="tiles"></div>
    <div class="filters">
      <select id="f-state"><option value="">all states</option>
        <option>ready</option><option>running</option><option>awaiting</option><option>held</option>
        <option>succeeded</option><option>failed</option><option>cancelled</option><option>paused</option>
      </select>
      <select id="f-queue"><option value="">all queues</option></select>
      <button onclick="loadJobs()">refresh</button>
    </div>
    <table>
      <thead><tr><th>id</th><th>worker</th><th>queue</th><th>state</th><th>att</th><th>workflow</th><th>inserted</th></tr></thead>
      <tbody id="rows"></tbody>
    </table>
  </main>
  <div id="drawer"></div>
  <script>
  const qs = new URLSearchParams(location.search);
  const TOKEN = qs.get('token');
  const auth = p => TOKEN ? p + (p.includes('?') ? '&' : '?') + 'token=' + TOKEN : p;
  const j = async (p, opts) => { const r = await fetch(auth(p), opts); return r.json(); };
  const post = (p, body) => j(p, {method:'POST', body: JSON.stringify(body || {})});
  const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
  const badge = s => `<span class="badge b-${esc(s)}">${esc(s)}</span>`;
  const fmtT = t => t ? new Date(t).toLocaleTimeString() : '';

  let overview = null;

  function renderOverview(o) {
    overview = o;
    document.getElementById('inst').textContent = o.instance;
    const tiles = document.getElementById('tiles');
    const queues = Object.keys(o.queues || {}).sort();
    const fq = document.getElementById('f-queue');
    if (fq.options.length <= 1) queues.forEach(q => fq.add(new Option(q, q)));
    tiles.innerHTML = queues.map(q => {
      const spec = o.queues[q] || {}, counts = (o.stats || {})[q] || {};
      const caps = [spec.limit && 'limit ' + spec.limit, spec.global_limit && 'global ' + spec.global_limit,
                    spec.rate && 'rate ' + spec.rate.allowed + '/' + spec.rate.period + 's',
                    spec.partition && 'per-' + spec.partition[1], spec.dynamic && 'dynamic',
                    spec.manual && 'manual'].filter(Boolean).join(' · ');
      const badges = Object.entries(counts).map(([s, n]) => `<span class="badge b-${esc(s)}">${esc(s)} ${n}</span>`).join('');
      return `<div class="tile"><h3>${esc(q)} <span class="caps">${esc(caps)}</span></h3>
              <div class="counts">${badges || '<span class="badge">idle</span>'}</div></div>`;
    }).join('');
  }

  function rowsHtml(jobs) {
    if (!jobs.length) return '<tr><td colspan="7" class="empty">no jobs</td></tr>';
    return jobs.map(x => `<tr onclick="openJob(${x.id})">
      <td class="mono">${x.id}</td><td class="mono">${esc(x.worker)}</td><td>${esc(x.queue)}</td>
      <td>${badge(x.state)}</td><td class="mono">${x.attempt}</td>
      <td>${x.workflow ? `<a onclick="event.stopPropagation();openWorkflow('${esc(x.workflow.id)}')">${esc(x.workflow.name || 'dag')}</a>` : ''}</td>
      <td class="mono">${fmtT(x.inserted_at)}</td></tr>`).join('');
  }

  let lastRows = null;
  async function loadJobs() {
    const s = document.getElementById('f-state').value, q = document.getElementById('f-queue').value;
    const p = new URLSearchParams(); if (s) p.set('state', s); if (q) p.set('queue', q); p.set('limit', 50);
    const data = await j('/api/jobs?' + p);
    const html = rowsHtml(data.jobs);
    if (html !== lastRows) {  // don't churn the DOM (and in-flight clicks) for identical data
      lastRows = html;
      document.getElementById('rows').innerHTML = html;
    }
  }

  async function openJob(id) {
    const d = await j('/api/jobs/' + id);
    const money = m => (m == null) ? '—' : '$' + (m / 1e6).toFixed(4);
    const steps = (d.steps || []).map(s => `<div class="stepline"><span class="mono">${s.seq}. ${esc(s.name)}</span>
      <span class="mono">${esc(s.value || '')}</span>
      <span class="cost">${money(s.usd_micros)} · ${s.tokens} tok</span></div>`).join('');
    const events = (d.events || []).map(e => `<div class="stepline"><span class="mono">${e.seq}</span>
      <span class="mono">${esc(JSON.stringify(e.payload))}</span><span class="cost">${fmtT(e.at)}</span></div>`).join('');
    const kids = (d.children || []).map(c => `<a onclick="openJob(${c.id})">#${c.id} ${badge(c.state)}</a>`).join(' ');
    const drawer = document.getElementById('drawer');
    drawer.innerHTML = `
      <h2>#${d.id} <span class="mono">${esc(d.worker)}</span> ${badge(d.state)}
          <button class="close" onclick="closeDrawer()">✕</button></h2>
      <div class="mono" style="color:var(--dim)">queue ${esc(d.queue)} · attempt ${d.attempt}/${d.max_attempts}
        · spent ${money(d.spent.usd_micros)} / ${d.spent.tokens} tok
        ${d.budget.usd_micros ? '· budget ' + money(d.budget.usd_micros) : ''}</div>
      <div class="actions">
        <button onclick="act(${d.id},'retry')">retry</button>
        <button class="danger" onclick="act(${d.id},'cancel')">cancel</button>
        <button onclick="steer(${d.id})">steer</button>
        ${d.await ? `<button onclick="signal('${esc(d.await.scope)}','${esc(d.await.name)}')">signal ${esc(d.await.name)}</button>` : ''}
        ${d.workflow ? `<button onclick="openWorkflow('${esc(d.workflow.id)}')">workflow dag</button>` : ''}
      </div>
      <section><h4>input</h4><pre>${esc(JSON.stringify(d.input, null, 2))}</pre></section>
      ${d.result ? `<section><h4>result</h4><pre>${esc(d.result)}</pre></section>` : ''}
      ${steps ? `<section><h4>journal (steps)</h4>${steps}</section>` : ''}
      ${events ? `<section><h4>events</h4>${events}</section>` : ''}
      ${kids ? `<section><h4>children</h4>${kids}</section>` : ''}
      ${(d.errors || []).length ? `<section><h4>errors</h4><pre>${esc(JSON.stringify(d.errors, null, 2))}</pre></section>` : ''}`;
    drawer.classList.add('open');
  }

  async function openWorkflow(id) {
    const d = await j('/api/workflows/' + id);
    const jobs = d.jobs || [];
    const byName = {}; jobs.forEach(x => byName[x.workflow.name] = x);
    const depth = {}, order = [];
    const depthOf = n => {
      if (depth[n] != null) return depth[n];
      const deps = (byName[n] && byName[n].workflow.deps) || [];
      depth[n] = deps.length ? 1 + Math.max(...deps.map(depthOf)) : 0;
      return depth[n];
    };
    jobs.forEach(x => { depthOf(x.workflow.name); order.push(x.workflow.name); });
    const cols = {}; order.forEach(n => { (cols[depth[n]] = cols[depth[n]] || []).push(n); });
    const colKeys = Object.keys(cols).map(Number).sort((a, b) => a - b);
    const W = 190, H = 64, pos = {};
    colKeys.forEach(c => cols[c].forEach((n, i) => pos[n] = {x: 30 + c * W, y: 30 + i * H}));
    const maxY = Math.max(...Object.values(pos).map(p => p.y)) + 70;
    const maxX = 30 + colKeys.length * W;
    const colorFor = s => getComputedStyle(document.body).getPropertyValue('--' + s) || '#888';
    let edges = '', nodes = '';
    jobs.forEach(x => {
      const n = x.workflow.name, p = pos[n];
      (x.workflow.deps || []).forEach(dep => {
        const q = pos[dep];
        if (q) edges += `<path d="M ${q.x + 140} ${q.y + 18} C ${q.x + 170} ${q.y + 18}, ${p.x - 30} ${p.y + 18}, ${p.x} ${p.y + 18}"
                         fill="none" stroke="#3a4358" stroke-width="1.5"/>`;
      });
      nodes += `<g onclick="openJob(${x.id})" style="cursor:pointer">
        <rect x="${p.x}" y="${p.y}" rx="8" width="140" height="36" fill="var(--panel2)"
              stroke="${colorFor(x.state)}" stroke-width="1.6"/>
        <text x="${p.x + 10}" y="${p.y + 15}">${esc(n)}</text>
        <text x="${p.x + 10}" y="${p.y + 29}" style="fill:${colorFor(x.state)}">${esc(x.state)}</text></g>`;
    });
    const drawer = document.getElementById('drawer');
    drawer.innerHTML = `<h2>workflow <span class="mono">${esc(id).slice(0, 12)}…</span>
        ${d.done ? badge('succeeded') : badge('running')}
        <button class="close" onclick="closeDrawer()">✕</button></h2>
      <svg id="dag" viewBox="0 0 ${maxX} ${maxY}" height="${Math.min(maxY, 560)}">${edges}${nodes}</svg>`;
    drawer.classList.add('open');
  }

  const closeDrawer = () => document.getElementById('drawer').classList.remove('open');
  async function act(id, action) { await post(`/api/jobs/${id}/${action}`); openJob(id); loadJobs(); }
  async function steer(id) {
    const raw = prompt('steering payload (JSON)', '{"instruction": ""}');
    if (raw) { await post(`/api/jobs/${id}/steer`, {payload: JSON.parse(raw)}); openJob(id); }
  }
  async function signal(scope, name) {
    const raw = prompt(`signal ${name} payload (JSON)`, '{"approved": true}');
    if (raw) { await post('/api/signals', {scope, name, payload: JSON.parse(raw)}); loadJobs(); }
  }

  function connect() {
    const es = new EventSource(auth('/api/sse'));
    es.onmessage = e => { renderOverview(JSON.parse(e.data)); document.getElementById('live').style.opacity = 1; };
    es.onerror = () => { document.getElementById('live').style.opacity = .3; };
  }
  // Deep links: #job=123 / #workflow=<id> open the drawer on load — paste
  // them straight from alerts or logs.
  function openFromHash() {
    const m = location.hash.match(/^#(job|workflow)=(.+)$/);
    if (m) (m[1] === 'job' ? openJob : openWorkflow)(m[1] === 'job' ? +m[2] : m[2]);
  }
  window.addEventListener('hashchange', openFromHash);

  j('/api/overview').then(renderOverview).then(loadJobs).then(openFromHash);
  connect();
  setInterval(loadJobs, 3000);
  </script>
  </body>
  </html>
  """

  def html, do: @html
end
