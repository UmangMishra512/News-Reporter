/**
 * dashboard/app.js — Real-time dashboard frontend.
 * Polls the platform's REST API every 30 seconds.
 * Draws a performance chart using Canvas API (no external deps).
 */

const API = {
  stats:     '/api/stats',
  stories:   '/api/stories',
  incidents: '/api/incidents',
  health:    '/api/health',
};

const AGENT_ICONS = {
  supervisor:        '👑',
  ops_manager:       '📋',
  source_discovery:  '🔍',
  rss_worker:        '📡',
  api_worker:        '🌐',
  browser_worker:    '🌍',
  dedup_agent:       '🧬',
  fact_verifier:     '✅',
  summarizer:        '✍️',
  ranker:            '📊',
  qa_agent:          '🎯',
  health_monitor:    '🩺',
  watchdog:          '🐕',
  failure_analyzer:  '⚠️',
  website_knowledge: '📚',
  discord_bot:       '💬',
};

const CATEGORY_COLORS = {
  politics:          '#3b82f6',
  finance:           '#f59e0b',
  economy:           '#8b5cf6',
  business:          '#06b6d4',
  stock_market:      '#22c55e',
  technology:        '#6366f1',
  ai:                '#ec4899',
  startups:          '#f97316',
  sports:            '#ef4444',
  national_security: '#1f2937',
  government:        '#4f46e5',
  crime:             '#dc2626',
  accidents:         '#b91c1c',
  natural_disasters: '#0369a1',
  international:     '#047857',
  general:           '#6b7280',
};

let perfData = [];

// ── Fetch helpers ─────────────────────────────────────────────

async function fetchJSON(url) {
  try {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } catch (e) {
    return null;
  }
}

// ── System status ─────────────────────────────────────────────

function setSystemStatus(online) {
  const dot  = document.getElementById('system-status-dot');
  const text = document.getElementById('system-status-text');
  dot.className  = 'status-dot ' + (online ? 'online' : 'offline');
  text.textContent = online ? 'System Online' : 'Cannot reach API';
}

function updateLastUpdated() {
  const el = document.getElementById('last-updated');
  el.textContent = 'Updated ' + new Date().toLocaleTimeString('en-IN');
}

// ── Stats cards ───────────────────────────────────────────────

function renderStats(metrics) {
  if (!metrics || !metrics.length) return;
  const last = metrics[0];
  document.getElementById('stat-fetched').textContent    = last.articles_fetched ?? '–';
  document.getElementById('stat-deduped').textContent    = last.articles_after_dedup ?? '–';
  document.getElementById('stat-published').textContent  = last.articles_published ?? '–';
  const dur = last.total_duration_seconds;
  document.getElementById('stat-duration').textContent   = dur ? `${Math.round(dur)}s` : '–';
}

// ── Agent health ──────────────────────────────────────────────

function agentStatusClass(status) {
  if (!status) return 'unknown';
  if (['running', 'starting'].includes(status)) return 'running';
  if (status === 'idle') return 'idle';
  if (['failed', 'stopped'].includes(status)) return 'failed';
  return 'unknown';
}

function timeAgo(ts) {
  if (!ts) return '?';
  const diff = (Date.now() - new Date(ts + 'Z').getTime()) / 1000;
  if (diff < 60) return `${Math.round(diff)}s ago`;
  if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
  return `${Math.round(diff / 3600)}h ago`;
}

function renderAgents(heartbeats) {
  const list = document.getElementById('agent-list');
  const badge = document.getElementById('agents-online-badge');

  if (!heartbeats || !heartbeats.length) {
    list.innerHTML = '<div class="empty-state"><div class="empty-icon">🤖</div>No agents reporting yet</div>';
    return;
  }

  const onlineCount = heartbeats.filter(h => ['running', 'starting'].includes(h.status)).length;
  badge.textContent = `${onlineCount} Online`;

  const html = heartbeats.map(h => {
    const cls   = agentStatusClass(h.status);
    const icon  = AGENT_ICONS[h.agent_name] || '🔧';
    const ago   = timeAgo(h.timestamp);
    const task  = h.current_task ? h.current_task.substring(0, 20) + '…' : '';
    return `
      <div class="agent-item">
        <div class="agent-dot ${cls}"></div>
        <span class="agent-name">${icon} ${h.agent_name}</span>
        <span class="agent-meta">${ago}${task ? ' · ' + task : ''}</span>
      </div>
    `;
  }).join('');
  list.innerHTML = html;
}

// ── Stories ───────────────────────────────────────────────────

function renderStories(stories) {
  const list  = document.getElementById('story-list');
  const badge = document.getElementById('stories-count-badge');

  if (!stories || !stories.length) {
    list.innerHTML = '<div class="empty-state"><div class="empty-icon">📰</div>No stories published yet today</div>';
    badge.textContent = '0 Today';
    return;
  }

  badge.textContent = `${stories.length} Today`;

  const html = stories.slice(0, 10).map((s, i) => {
    const catColor = CATEGORY_COLORS[s.category] || '#6b7280';
    const pub = s.published_at ? new Date(s.published_at + 'Z').toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }) : '';
    return `
      <div class="story-item" style="border-left-color: ${catColor}">
        <div class="story-rank">#${s.rank || (i + 1)}</div>
        <div class="story-headline">${escapeHtml(s.headline || '')}</div>
        <div class="story-meta">
          <span class="story-cat">${s.category || 'general'}</span>
          <span>${escapeHtml(s.publisher || '')}</span>
          ${pub ? '<span>' + pub + '</span>' : ''}
          ${s.source_url ? `<a class="story-link" href="${s.source_url}" target="_blank" rel="noopener">🔗 Read</a>` : ''}
        </div>
      </div>
    `;
  }).join('');
  list.innerHTML = html;
}

// ── Incidents ─────────────────────────────────────────────────

function renderIncidents(incidents) {
  const list  = document.getElementById('incident-list');
  const badge = document.getElementById('incidents-badge');

  if (!incidents || !incidents.length) {
    list.innerHTML = '<div class="empty-state"><div class="empty-icon">✅</div>No open incidents</div>';
    badge.textContent = '0';
    return;
  }

  badge.textContent = incidents.length;

  const html = incidents.slice(0, 10).map(inc => {
    const sev = inc.severity || 'medium';
    const ts  = inc.timestamp ? new Date(inc.timestamp + 'Z').toLocaleString('en-IN') : '';
    return `
      <div class="incident-item">
        <span class="incident-sev ${sev}">${sev.toUpperCase()}</span>
        <div class="incident-body">
          <div class="incident-agent">${inc.agent_name || '?'} — ${inc.error_type || '?'}</div>
          <div class="incident-msg">${escapeHtml((inc.error_message || '').substring(0, 120))}</div>
          <div class="incident-time">${ts}</div>
        </div>
      </div>
    `;
  }).join('');
  list.innerHTML = html;
}

// ── Performance chart ─────────────────────────────────────────

function drawChart(metrics) {
  const canvas = document.getElementById('perf-chart');
  if (!canvas || !metrics || !metrics.length) return;

  const ctx  = canvas.getContext('2d');
  const data = [...metrics].reverse(); // oldest → newest
  const W    = canvas.parentElement.clientWidth;
  const H    = 180;
  canvas.width  = W;
  canvas.height = H;

  const PAD = { t: 20, r: 20, b: 40, l: 50 };
  const cW   = W - PAD.l - PAD.r;
  const cH   = H - PAD.t - PAD.b;

  const maxFetch = Math.max(...data.map(d => d.articles_fetched || 0), 1);
  const maxPub   = Math.max(...data.map(d => d.articles_published || 0), 1);
  const n        = data.length;

  const xScale = i => PAD.l + (i / Math.max(n - 1, 1)) * cW;
  const yScale = (v, max) => PAD.t + cH - (v / max) * cH;

  ctx.clearRect(0, 0, W, H);

  // Grid lines
  ctx.strokeStyle = 'rgba(255,255,255,0.05)';
  ctx.lineWidth   = 1;
  for (let i = 0; i <= 4; i++) {
    const y = PAD.t + (i / 4) * cH;
    ctx.beginPath();
    ctx.moveTo(PAD.l, y);
    ctx.lineTo(PAD.l + cW, y);
    ctx.stroke();
  }

  // Articles fetched line
  function drawLine(arr, color, yMax) {
    if (arr.length < 2) return;
    ctx.beginPath();
    ctx.strokeStyle = color;
    ctx.lineWidth   = 2;
    ctx.lineJoin    = 'round';
    arr.forEach((d, i) => {
      const x = xScale(i);
      const y = yScale(d, yMax);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.stroke();

    // Dots
    ctx.fillStyle = color;
    arr.forEach((d, i) => {
      ctx.beginPath();
      ctx.arc(xScale(i), yScale(d, yMax), 3, 0, Math.PI * 2);
      ctx.fill();
    });
  }

  drawLine(data.map(d => d.articles_fetched || 0),   'rgba(255,153,51,0.8)',   maxFetch);
  drawLine(data.map(d => d.articles_published || 0), 'rgba(34,197,94,0.8)',   maxFetch);

  // X labels (run timestamps)
  ctx.fillStyle  = 'rgba(144,144,176,0.8)';
  ctx.font       = '10px JetBrains Mono, monospace';
  ctx.textAlign  = 'center';
  data.forEach((d, i) => {
    if (i % Math.ceil(n / 5) === 0 || i === n - 1) {
      const label = d.timestamp ? d.timestamp.substring(5, 16) : `Run ${i + 1}`;
      ctx.fillText(label, xScale(i), H - 10);
    }
  });

  // Legend
  ctx.textAlign = 'left';
  ctx.fillStyle = 'rgba(255,153,51,0.9)';
  ctx.fillRect(PAD.l, 5, 12, 4);
  ctx.fillStyle = 'rgba(255,255,255,0.6)';
  ctx.fillText('Fetched', PAD.l + 16, 12);

  ctx.fillStyle = 'rgba(34,197,94,0.9)';
  ctx.fillRect(PAD.l + 90, 5, 12, 4);
  ctx.fillStyle = 'rgba(255,255,255,0.6)';
  ctx.fillText('Published', PAD.l + 106, 12);
}

// ── Helpers ───────────────────────────────────────────────────

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

function updateFooterTime() {
  const el = document.getElementById('footer-time');
  el.textContent = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) + ' IST';
}

// ── Main refresh ──────────────────────────────────────────────

async function refresh() {
  const [metrics, stories, incidents, health] = await Promise.all([
    fetchJSON(API.stats),
    fetchJSON(API.stories),
    fetchJSON(API.incidents),
    fetchJSON(API.health),
  ]);

  const online = metrics !== null;
  setSystemStatus(online);

  if (metrics)   { renderStats(metrics);     perfData = metrics; drawChart(metrics); }
  if (stories)   { renderStories(stories); }
  if (incidents) { renderIncidents(incidents); }
  if (health)    { renderAgents(health); }

  updateLastUpdated();
}

// ── Init ──────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  updateFooterTime();
  setInterval(updateFooterTime, 1000);
  refresh();
  setInterval(refresh, 30_000);  // auto-refresh every 30s
  window.addEventListener('resize', () => drawChart(perfData));
});
// ── Command Centre ────────────────────────────────────────────

function addChatMessage(text, type) {
  const history = document.getElementById('chat-history');
  if (!history) return;
  const msgDiv = document.createElement('div');
  msgDiv.className = `chat-message ${type}`;
  msgDiv.textContent = text;
  history.appendChild(msgDiv);
  history.scrollTop = history.scrollHeight;
}

async function sendCommand() {
  const inputEl = document.getElementById('command-input');
  const btnEl = document.getElementById('command-send-btn');
  const command = inputEl.value.trim();
  if (!command) return;

  // Add user message
  addChatMessage(`${command}`, 'user-message');
  inputEl.value = '';
  btnEl.disabled = true;

  try {
    const res = await fetch('/api/command', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ command })
    });
    
    let data;
    try {
      data = await res.json();
    } catch(e) {
      addChatMessage(`Error parsing response`, 'error-message');
      return;
    }

    if (res.ok && data.status === 'success') {
      addChatMessage(`${data.message}`, 'response-message');
    } else {
      addChatMessage(`${data.message || 'Unknown error'}`, 'error-message');
    }
  } catch (error) {
    addChatMessage(`${error.message}`, 'error-message');
  } finally {
    btnEl.disabled = false;
    inputEl.focus();
  }
}

document.addEventListener('DOMContentLoaded', () => {
  const inputEl = document.getElementById('command-input');
  const btnEl = document.getElementById('command-send-btn');
  
  if (btnEl && inputEl) {
    btnEl.addEventListener('click', sendCommand);
    inputEl.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') sendCommand();
    });
  }
});
