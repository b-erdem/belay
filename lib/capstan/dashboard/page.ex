defmodule Capstan.Dashboard.Page do
  @moduledoc false

  # The entire dashboard UI: one HTML document, vanilla JS, no build step.
  # Interpolation is disabled (~S) so JS template literals pass through.
  #
  # Layout: KPI strip (throughput + spend rates from trailing-window
  # snapshots persisted in localStorage, so rates survive reloads), queue
  # rows with limit-utilization bars, a two-line job list with state-chip
  # filters, and the drawer (journal timeline, events, actions, DAG).

  @html ~S"""
  <!doctype html>
  <html lang="en">
  <head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Capstan</title>
  <style>
    :root{--bg:#0a0d13;--panel:#10141c;--panel2:#171d28;--line:#1f2734;--text:#dde4ef;
      --dim:#8792a5;--accent:#5eead4;--ready:#5aa7ff;--running:#f2b63c;--succeeded:#3ecf8e;
      --failed:#ff6363;--cancelled:#77839a;--awaiting:#b78cff;--held:#e59ae2;--paused:#64748b}
    *{box-sizing:border-box;margin:0}
    html{scrollbar-color:var(--line) var(--bg)}
    body{background:var(--bg);color:var(--text);font:13.5px/1.45 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif}
    .mono{font-family:ui-monospace,"SF Mono",Menlo,monospace}
    header{display:flex;align-items:center;gap:12px;padding:11px 22px;border-bottom:1px solid var(--line)}
    header h1{font-size:15px;letter-spacing:.3px}
    header .inst{color:var(--dim);font-size:12px}
    header .live{margin-left:auto;color:var(--accent);font-size:11px;letter-spacing:.4px}
    main{padding:16px 22px 40px;max-width:1180px;margin:0 auto}

    /* KPI strip */
    .kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;margin-bottom:16px}
    .kpi{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px 12px 8px;position:relative;overflow:hidden}
    .kpi label{display:block;color:var(--dim);font-size:10.5px;text-transform:uppercase;letter-spacing:.7px}
    .kpi .v{font-size:21px;font-weight:600;font-variant-numeric:tabular-nums;margin-top:2px}
    .kpi .v small{font-size:11px;color:var(--dim);font-weight:400;margin-left:3px}
    .kpi svg{position:absolute;right:0;bottom:0;left:0;height:26px;width:100%;opacity:.9}
    .kpi.pad{padding-bottom:30px}

    /* Queues */
    h2.sect{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.8px;margin:18px 0 8px}
    .queues{display:flex;flex-direction:column;gap:6px}
    .queue{display:grid;grid-template-columns:180px 1fr auto;gap:14px;align-items:center;
      background:var(--panel);border:1px solid var(--line);border-radius:9px;padding:8px 14px}
    .queue .name{font-weight:600;font-size:13px}
    .queue .caps{color:var(--dim);font-size:11px;margin-top:1px}
    .util{height:5px;background:var(--panel2);border-radius:99px;overflow:hidden;min-width:120px}
    .util i{display:block;height:100%;background:var(--running);border-radius:99px;transition:width .4s}
    .util-wrap{display:flex;flex-direction:column;gap:5px}
    .util-label{color:var(--dim);font-size:10.5px}
    .chips{display:flex;gap:5px;flex-wrap:wrap;justify-content:flex-end}
    .badge{padding:1px 8px;border-radius:99px;font-size:10.5px;font-variant-numeric:tabular-nums;
      background:var(--panel2);border:1px solid var(--line);white-space:nowrap}
    .b-ready{color:var(--ready)} .b-running{color:var(--running)} .b-succeeded{color:var(--succeeded)}
    .b-failed{color:var(--failed)} .b-cancelled{color:var(--cancelled)} .b-awaiting{color:var(--awaiting)}
    .b-held{color:var(--held)} .b-paused{color:var(--paused)}

    /* Filters */
    .filters{display:flex;gap:6px;margin:16px 0 8px;align-items:center;flex-wrap:wrap}
    .fchip{padding:3px 11px;border-radius:99px;font-size:12px;background:var(--panel);border:1px solid var(--line);
      cursor:pointer;color:var(--dim);font-variant-numeric:tabular-nums}
    .fchip:hover{border-color:var(--accent);color:var(--text)}
    .fchip.on{background:var(--panel2);color:var(--text);border-color:var(--accent)}
    select,button,input{background:var(--panel2);color:var(--text);border:1px solid var(--line);
      border-radius:7px;padding:4px 10px;font-size:12.5px}
    button{cursor:pointer} button:hover{border-color:var(--accent)}
    button.danger:hover{border-color:var(--failed)}
    .filters select{margin-left:auto}

    /* Job rows */
    .jobs{display:flex;flex-direction:column;background:var(--panel);border:1px solid var(--line);border-radius:10px;overflow:hidden}
    .job{display:flex;align-items:center;gap:12px;padding:8px 14px;border-bottom:1px solid var(--line);cursor:pointer}
    .job:last-child{border-bottom:0}
    .job:hover{background:var(--panel2)}
    .dot{width:8px;height:8px;border-radius:99px;flex:none}
    .d-ready{background:var(--ready)} .d-running{background:var(--running);box-shadow:0 0 6px var(--running)}
    .d-succeeded{background:var(--succeeded)} .d-failed{background:var(--failed)}
    .d-cancelled{background:var(--cancelled)} .d-awaiting{background:var(--awaiting)}
    .d-held{background:var(--held)} .d-paused{background:var(--paused)}
    .job .main{min-width:0;flex:1}
    .job .kind{font-weight:600;font-size:13px}
    .job .sub{color:var(--dim);font-size:11.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-top:1px}
    .job .side{display:flex;flex-direction:column;align-items:flex-end;gap:2px;flex:none}
    .job .when{font-size:11.5px;color:var(--dim);font-variant-numeric:tabular-nums}
    .tag{padding:0 7px;border-radius:99px;font-size:10.5px;background:var(--panel2);border:1px solid var(--line);color:var(--dim)}
    .cost{color:var(--accent);font-size:10.5px;font-variant-numeric:tabular-nums}
    .empty{color:var(--dim);padding:22px;text-align:center}
    a{color:var(--accent);cursor:pointer;text-decoration:none}

    /* Drawer */
    #drawer{position:fixed;top:0;right:-700px;width:680px;max-width:96vw;height:100vh;overflow:auto;
      background:var(--panel);border-left:1px solid var(--line);transition:right .18s;padding:18px 20px;z-index:5;
      box-shadow:-18px 0 40px rgba(0,0,0,.35)}
    #drawer.open{right:0}
    #drawer h2{font-size:14.5px;display:flex;gap:10px;align-items:center;margin-bottom:8px}
    #drawer .meta{color:var(--dim);font-size:12px}
    #drawer section{margin:16px 0} 
    #drawer h4{color:var(--dim);font-size:10.5px;text-transform:uppercase;letter-spacing:.7px;margin-bottom:7px}
    pre{background:var(--bg);border:1px solid var(--line);border-radius:8px;padding:10px;overflow:auto;
      font-size:12px;max-height:220px;font-family:ui-monospace,monospace}
    .actions{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}
    .step{display:flex;gap:10px;padding:6px 0 6px 12px;border-left:2px solid var(--line);margin-left:4px;
      font-size:12.5px;align-items:baseline}
    .step:hover{border-left-color:var(--accent)}
    .step .nm{font-weight:600}
    .step .val{color:var(--dim);font-family:ui-monospace,monospace;font-size:11.5px;
      white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:280px}
    .step .cost{margin-left:auto;font-size:11px}
    #dag{background:var(--bg);border:1px solid var(--line);border-radius:10px;margin-top:10px;width:100%}
    #dag text{fill:var(--text);font:11px ui-monospace,monospace}
    .close{margin-left:auto}
  </style>
  </head>
  <body>
  <header>
    <h1>⚓ Capstan</h1><span class="inst mono" id="inst"></span><span class="live" id="live">● live</span>
  </header>
  <main>
    <div class="kpis" id="kpis"></div>
    <h2 class="sect">Queues</h2>
    <div class="queues" id="queues"></div>
    <div class="filters" id="filters"></div>
    <div class="jobs" id="rows"></div>
  </main>
  <div id="drawer"></div>
  <script>
  const qs = new URLSearchParams(location.search);
  const TOKEN = qs.get('token');
  const auth = p => TOKEN ? p + (p.includes('?') ? '&' : '?') + 'token=' + TOKEN : p;
  const j = async (p, opts) => { const r = await fetch(auth(p), opts); return r.json(); };
  const post = (p, body) => j(p, {method:'POST', body: JSON.stringify(body || {})});
  const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
  // For values interpolated into a JS string literal inside an inline
  // handler (onclick="f('HERE')"). esc alone leaves ' unescaped, which
  // breaks out of the JS string; hex-escaping every non-word char is
  // bulletproof in both the HTML-attribute and JS-string layers.
  const jstr = s => String(s ?? '').replace(/[^\w]/g, c => {
    const h = c.charCodeAt(0);
    return h < 256 ? '\\x' + h.toString(16).padStart(2, '0') : '\\u' + h.toString(16).padStart(4, '0');
  });
  const badge = s => `<span class="badge b-${esc(s)}">${esc(s)}</span>`;
  const money = m => '$' + (m / 1e6).toFixed(m >= 1e6 ? 2 : 4);
  const rel = t => {
    if (!t) return '';
    const s = Math.max(0, (Date.now() - new Date(t)) / 1000);
    return s < 60 ? Math.floor(s) + 's ago' : s < 3600 ? Math.floor(s / 60) + 'm ago' : Math.floor(s / 3600) + 'h ago';
  };
  const dur = x => {
    if (!x.started_at) return '';
    const end = x.finished_at ? new Date(x.finished_at) : Date.now();
    const ms = end - new Date(x.started_at);
    if (ms < 0) return '';
    return ms < 1000 ? ms + 'ms' : ms < 60000 ? (ms / 1000).toFixed(1) + 's' : Math.floor(ms / 60000) + 'm' + Math.floor(ms % 60000 / 1000) + 's';
  };

  const STATES = ['ready','running','awaiting','held','succeeded','failed','cancelled'];
  let overview = null, fState = '', fQueue = '';

  // -- Rate tracking: snapshots {t, succ, usd} persisted per-instance so
  // rates survive reloads; rate = delta over the trailing window.
  const histKey = () => 'capstan:' + (overview ? overview.instance : '');
  function pushSnapshot(o) {
    const totals = Object.values(o.stats || {}).reduce((a, q) => a + (q.succeeded || 0), 0);
    const usd = (o.spend || {}).usd_micros || 0;
    let h = [];
    try { h = JSON.parse(localStorage.getItem(histKey()) || '[]'); } catch (e) {}
    h.push({t: Date.now(), succ: totals, usd});
    h = h.filter(p => Date.now() - p.t < 5 * 60 * 1000).slice(-400);
    try { localStorage.setItem(histKey(), JSON.stringify(h)); } catch (e) {}
    return h;
  }
  function rate(h, field, windowMs) {
    const now = Date.now(), start = now - windowMs;
    const win = h.filter(p => p.t >= start);
    if (win.length < 2) return {per: 0, series: win.map(p => 0)};
    const spanMin = (win[win.length - 1].t - win[0].t) / 60000;
    const delta = Math.max(0, win[win.length - 1][field] - win[0][field]);
    // Per-sample deltas for the sparkline.
    const series = win.slice(1).map((p, i) => Math.max(0, p[field] - win[i][field]));
    return {per: spanMin > 0 ? delta / spanMin : 0, series};
  }
  const spark = (series, color) => {
    if (series.length < 2) return '';
    const max = Math.max(...series, 1), n = series.length;
    const pts = series.map((v, i) => `${(i / (n - 1)) * 100},${26 - (v / max) * 22}`).join(' ');
    return `<svg preserveAspectRatio="none" viewBox="0 0 100 26">
      <polyline points="${pts}" fill="none" stroke="${color}" stroke-width="1.5" vector-effect="non-scaling-stroke"/></svg>`;
  };

  function renderOverview(o) {
    overview = o;
    document.getElementById('inst').textContent = o.instance;
    const h = pushSnapshot(o);
    const totals = {};
    STATES.forEach(s => totals[s] = 0);
    Object.values(o.stats || {}).forEach(q => Object.entries(q).forEach(([s, n]) => totals[s] = (totals[s] || 0) + n));

    const thr = rate(h, 'succ', 60 * 1000);
    const usd = rate(h, 'usd', 60 * 1000);
    document.getElementById('kpis').innerHTML = `
      <div class="kpi pad"><label>Throughput</label>
        <div class="v">${thr.per.toFixed(thr.per >= 10 ? 0 : 1)}<small>jobs/min</small></div>
        ${spark(thr.series, 'var(--succeeded)')}</div>
      <div class="kpi pad"><label>Spend rate</label>
        <div class="v">$${(usd.per * 60 / 1e6).toFixed(2)}<small>/hour</small></div>
        ${spark(usd.series, 'var(--accent)')}</div>
      <div class="kpi"><label>Running</label><div class="v" style="color:var(--running)">${totals.running || 0}</div></div>
      <div class="kpi"><label>Backlog</label><div class="v" style="color:var(--ready)">${(totals.ready || 0) + (totals.held || 0)}</div>
        <label style="margin-top:2px">${totals.awaiting || 0} awaiting</label></div>
      <div class="kpi"><label>Failed</label><div class="v" style="color:${totals.failed ? 'var(--failed)' : 'inherit'}">${totals.failed || 0}</div></div>`;

    const queues = Object.keys(o.queues || {}).sort();
    document.getElementById('queues').innerHTML = queues.map(q => {
      const spec = o.queues[q] || {}, counts = (o.stats || {})[q] || {};
      const limit = spec.limit || 1, running = counts.running || 0;
      const limitLabel = spec.limit_min ? `${spec.limit_min}–${spec.limit}` : spec.limit;
      const caps = [spec.global_limit && 'global ' + spec.global_limit,
                    spec.rate && 'rate ' + spec.rate.allowed + '/' + spec.rate.period + 's' +
                      (spec.rate.resource ? ' (' + spec.rate.resource + ')' : ''),
                    spec.partition && 'per-' + spec.partition[1], spec.dynamic && 'dynamic',
                    spec.manual && 'manual'].filter(Boolean).join(' · ');
      const chips = Object.entries(counts).filter(([, n]) => n > 0)
        .map(([s, n]) => `<span class="badge b-${esc(s)}">${esc(s)} ${n}</span>`).join('');
      return `<div class="queue">
        <div><div class="name">${esc(q)}</div><div class="caps">${esc(caps) || '&nbsp;'}</div></div>
        <div class="util-wrap"><div class="util"><i style="width:${Math.min(100, running / limit * 100)}%"></i></div>
          <span class="util-label">${running}/${limitLabel} slots</span></div>
        <div class="chips">${chips || '<span class="badge">idle</span>'}</div></div>`;
    }).join('');

    const fs = document.getElementById('filters');
    const chip = (label, val, count) =>
      `<span class="fchip${fState === val ? ' on' : ''}" onclick="setState('${val}')">${label}${count != null ? ' ' + count : ''}</span>`;
    fs.innerHTML = chip('all', '') +
      STATES.map(s => chip(s, s, totals[s] || 0)).join('') +
      `<select id="f-queue" onchange="setQueue(this.value)">
         <option value="">all queues</option>
         ${queues.map(q => `<option${fQueue === q ? ' selected' : ''}>${esc(q)}</option>`).join('')}
       </select>`;
  }

  function setState(s) { fState = s; renderOverview(overview); loadJobs(); }
  function setQueue(q) { fQueue = q; loadJobs(); }

  function rowsHtml(jobs) {
    if (!jobs.length) return '<div class="empty">no jobs</div>';
    return jobs.map(x => {
      const wf = x.workflow ? `<a onclick="event.stopPropagation();openWorkflow('${jstr(x.workflow.id)}')">${esc(x.workflow.name || 'dag')}</a> · ` : '';
      const cost = x.spent_usd_micros > 0 ? `<span class="cost">${money(x.spent_usd_micros)}</span>` : '';
      const d = dur(x);
      return `<div class="job" onclick="openJob(${x.id})">
        <span class="dot d-${esc(x.state)}"></span>
        <div class="main">
          <div class="kind">${esc(x.worker)}</div>
          <div class="sub mono">#${x.id} · ${x.attempt}/${x.max_attempts} · ${wf}${esc(x.input_preview || '')}</div>
        </div>
        <div class="side">
          <span class="when">${d ? d + ' · ' : ''}${rel(x.inserted_at)}</span>
          <span>${cost} <span class="tag">${esc(x.queue)}</span></span>
        </div></div>`;
    }).join('');
  }

  let lastRows = null;
  async function loadJobs() {
    const p = new URLSearchParams();
    if (fState) p.set('state', fState); if (fQueue) p.set('queue', fQueue); p.set('limit', 50);
    const data = await j('/api/jobs?' + p);
    const html = rowsHtml(data.jobs);
    if (html !== lastRows) {  // don't churn the DOM (and in-flight clicks) for identical data
      lastRows = html;
      document.getElementById('rows').innerHTML = html;
    }
  }

  async function openJob(id) {
    const d = await j('/api/jobs/' + id);
    const m = x => (x == null) ? '—' : money(x);
    const steps = (d.steps || []).map(s => `<div class="step"><span class="nm mono">${s.seq}. ${esc(s.name)}</span>
      <span class="val">${esc(s.value || '')}</span>
      <span class="cost mono">${m(s.usd_micros)} · ${s.tokens} tok</span></div>`).join('');
    const events = (d.events || []).map(e => `<div class="step"><span class="nm mono">${e.seq}</span>
      <span class="val">${esc(JSON.stringify(e.payload))}</span><span class="cost mono">${rel(e.at)}</span></div>`).join('');
    const kids = (d.children || []).map(c => `<a onclick="openJob(${c.id})">#${c.id} ${badge(c.state)}</a>`).join(' ');
    const drawer = document.getElementById('drawer');
    drawer.innerHTML = `
      <h2><span class="mono" style="color:var(--dim)">#${d.id}</span> ${esc(d.worker)} ${badge(d.state)}
          <button class="close" onclick="closeDrawer()">✕</button></h2>
      <div class="meta mono">queue ${esc(d.queue)} · attempt ${d.attempt}/${d.max_attempts}
        · spent ${m(d.spent.usd_micros)} / ${d.spent.tokens} tok
        ${d.budget.usd_micros ? '· budget ' + m(d.budget.usd_micros) : ''}</div>
      <div class="actions">
        <button onclick="act(${d.id},'retry')">retry</button>
        <button class="danger" onclick="act(${d.id},'cancel')">cancel</button>
        <button onclick="steer(${d.id})">steer</button>
        ${d.await ? `<button onclick="signal('${jstr(d.await.scope)}','${jstr(d.await.name)}')">signal ${esc(d.await.name)}</button>` : ''}
        ${d.workflow ? `<button onclick="openWorkflow('${jstr(d.workflow.id)}')">workflow dag</button>` : ''}
      </div>
      <section><h4>input</h4><pre>${esc(JSON.stringify(d.input, null, 2))}</pre></section>
      ${d.result ? `<section><h4>result</h4><pre>${esc(d.result)}</pre></section>` : ''}
      ${steps ? `<section><h4>journal</h4>${steps}</section>` : ''}
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
                         fill="none" stroke="#39435a" stroke-width="1.5"/>`;
      });
      nodes += `<g onclick="openJob(${x.id})" style="cursor:pointer">
        <rect x="${p.x}" y="${p.y}" rx="9" width="140" height="36" fill="var(--panel2)"
              stroke="${colorFor(x.state)}" stroke-width="1.7"/>
        <text x="${p.x + 10}" y="${p.y + 15}">${esc(n)}</text>
        <text x="${p.x + 10}" y="${p.y + 29}" style="fill:${colorFor(x.state)}">${esc(x.state)}</text></g>`;
    });
    const drawer = document.getElementById('drawer');
    drawer.innerHTML = `<h2>workflow <span class="mono" style="color:var(--dim)">${esc(id).slice(0, 12)}…</span>
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
    // ?sse=0 renders a static snapshot — for screenshot/automation tooling
    // that waits on network idle (headless browsers hang on open streams).
    if (new URLSearchParams(location.search).get('sse') === '0') return;
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
