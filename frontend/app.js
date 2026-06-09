/* ============================================================
   Prophunt AI Dashboard — app.js
   Navigation + API logic (fully wired)
   ============================================================ */

const runtimeConfig = (typeof window !== 'undefined' && window.__APP_CONFIG__) || {};
const API_BASE = runtimeConfig.API_BASE_URL || '/api';
const INTERNAL_TOKEN = runtimeConfig.INTERNAL_TOKEN || 'local-dev-internal-token';
// Orchestrator endpoints route separately via vercel.json (not through platform-api)
const ORCH_BASE = '';

// ─── State ───────────────────────────────────────────────────
const state = {
  activeView: 'dashboard',
  callSid: null,
  pollInterval: null,
  liveCallsInterval: null,
  analyticsRange: 'today',
  leadsFilter: 'all',
};

const NAV_ITEMS = ['dashboard','leads','campaigns','voice-agent','agents','analytics','call-history','knowledge-base','contacts','crm','phone-numbers','inbound','integrations','settings','profile'];
const PAGE_TITLES = {
  'dashboard':      'Dashboard',
  'leads':          'Leads Queue',
  'campaigns':      'Campaigns',
  'voice-agent':    'Voice Agent',
  'agents':         'AI Agents',
  'analytics':      'Analytics',
  'call-history':   'Call History',
  'knowledge-base': 'Knowledge Base',
  'contacts':       'Contacts',
  'crm':            'CRM Integration',
  'phone-numbers':  'Phone Numbers',
  'inbound':        'Inbound Calls',
  'integrations':   'Integrations',
  'settings':       'Settings',
  'profile':        'Profile',
};

// ─── Navigation ──────────────────────────────────────────────
// Reads the active view from the URL hash (#/voice-agent) with localStorage fallback.
// Using the hash means the browser preserves it on refresh automatically.
function getInitialView() {
  const hash = window.location.hash.replace(/^#\/?/, '').trim();
  if (hash && NAV_ITEMS.includes(hash)) return hash;
  const saved = localStorage.getItem('prophunt_active_view');
  if (saved && NAV_ITEMS.includes(saved)) return saved;
  return 'dashboard';
}

function navigate(view) {
  if (!NAV_ITEMS.includes(view)) return;
  state.activeView = view;
  // Encode view in hash so browser preserves it on refresh (no server-side routing needed)
  if (window.location.hash !== '#/' + view) {
    history.replaceState(null, '', '#/' + view);
  }
  localStorage.setItem('prophunt_active_view', view); // secondary fallback
  showPage(view);
  setActiveNav(view);
  updatePageTitle(PAGE_TITLES[view] || view);

  // Stop live-sessions polling when leaving voice-agent
  if (view !== 'voice-agent' && _liveSessionsPollTimer) {
    clearInterval(_liveSessionsPollTimer);
    _liveSessionsPollTimer = null;
  }
  if (view === 'voice-agent')    initVoiceAgentPage();
  if (view === 'analytics')      initAnalyticsPage();
  if (view === 'dashboard')      initDashboardPage();
  if (view === 'settings')       initSettingsPage();
  if (view === 'crm')            initCrmPage();
  if (view === 'campaigns')      initCampaignsPage();
  if (view === 'leads')          initLeadsPage();
  if (view === 'profile')        updateProfileViews();
  if (view === 'call-history')   initCallHistoryPage();
  if (view === 'knowledge-base') initKnowledgeBasePage();
  if (view === 'agents')         initAgentsPage();
  if (view === 'contacts')       initContactsPage();
  if (view === 'phone-numbers')  initPhoneNumbersPage();
  if (view === 'inbound')        initInboundPage();
  if (view === 'integrations')   initIntegrationsPage();
}

function showPage(id) {
  document.querySelectorAll('.page-section').forEach(el => el.classList.remove('active'));
  const target = document.getElementById('page-' + id);
  if (target) target.classList.add('active');
}

function setActiveNav(view) {
  document.querySelectorAll('#sidebar-nav .nav-item').forEach(el => {
    const v = el.getAttribute('data-view');
    el.classList.toggle('nav-item-active', v === view);
  });
}

function updatePageTitle(title) {
  const el = document.getElementById('page-title');
  if (el) el.textContent = title;
}

// ─── Theme ───────────────────────────────────────────────────
// Returns the right cyan colour for the current theme — use this in JS-rendered HTML
// so the colour is baked in at render time and never fights CSS specificity.
function cyanColor() {
  return document.documentElement.getAttribute('data-theme') === 'light' ? '#1565c0' : '#22d3ee';
}

function toggleTheme() {
  const html = document.documentElement;
  const current = html.getAttribute('data-theme') || 'dark';
  const next = current === 'dark' ? 'light' : 'dark';
  html.setAttribute('data-theme', next);
  localStorage.setItem('prophunt_theme', next);
  const icon = document.getElementById('theme-icon');
  if (icon) icon.textContent = next === 'dark' ? 'dark_mode' : 'light_mode';
  // Re-render tables so JS-baked colours update immediately
  if (typeof renderRecentCalls === 'function') renderRecentCalls(window._lastRecentCalls || []);
}

function restoreTheme() {
  const saved = localStorage.getItem('prophunt_theme') || 'dark';
  document.documentElement.setAttribute('data-theme', saved);
  const icon = document.getElementById('theme-icon');
  if (icon) icon.textContent = saved === 'dark' ? 'dark_mode' : 'light_mode';
}

// ─── Toast ───────────────────────────────────────────────────
function showToast(msg, type = 'success') {
  const toast = document.getElementById('toast');
  const icon  = document.getElementById('toast-icon');
  const msgEl = document.getElementById('toast-msg');
  if (!toast) return;
  toast.style.borderColor = type === 'error' ? 'rgba(255,100,100,0.3)' : 'rgba(177,236,62,0.3)';
  if (icon) icon.textContent = type === 'error' ? 'error' : 'check_circle';
  if (msgEl) msgEl.textContent = msg;
  toast.style.display = 'flex';
  setTimeout(() => { toast.style.display = 'none'; }, 3000);
}

// ─── Helpers ─────────────────────────────────────────────────
function formatDuration(sec) {
  if (!sec || isNaN(sec)) return '—';
  const m = Math.floor(sec / 60);
  const s = String(Math.floor(sec % 60)).padStart(2, '0');
  return `${m}:${s}`;
}

function formatTime(iso) {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  } catch { return iso; }
}

function statusChip(outcome) {
  const map = {
    'qualified':      ['chip-green',  'Qualified'],
    'callback':       ['chip-amber',  'Callback'],
    'not_interested': ['chip-slate',  'Not Interested'],
    'no_answer':      ['chip-slate',  'No Answer'],
    'dropped':        ['chip-error',  'Dropped'],
    'active':         ['chip-cyan',   'Active'],
    'completed':      ['chip-slate',  'Completed'],
    'failed':         ['chip-error',  'Failed'],
    'timeout':        ['chip-error',  'Timeout'],
  };
  const [cls, label] = map[outcome] || ['chip-slate', outcome || 'Unknown'];
  return `<span class="chip ${cls}">${label}</span>`;
}

function toggleAudio(id, btn) {
  document.querySelectorAll('audio').forEach(a => {
    if (a.id !== id && !a.paused) {
      a.pause();
      const b = document.querySelector(`button[onclick*="${a.id}"]`);
      if (b) b.innerHTML = '<span class="material-symbols-outlined" style="font-size:15px;">play_circle</span><span>Play</span>';
    }
  });
  const audio = document.getElementById(id);
  if (!audio) return;
  if (audio.paused) {
    audio.play().catch(err => showToast('Audio error: ' + err.message, 'error'));
    btn.innerHTML = '<span class="material-symbols-outlined" style="font-size:15px;">pause_circle</span><span>Pause</span>';
    audio.onended = () => {
      btn.innerHTML = '<span class="material-symbols-outlined" style="font-size:15px;">play_circle</span><span>Play</span>';
    };
  } else {
    audio.pause();
    btn.innerHTML = '<span class="material-symbols-outlined" style="font-size:15px;">play_circle</span><span>Play</span>';
  }
}

function copyToClipboard(text) {
  navigator.clipboard.writeText(text)
    .then(() => showToast('Copied to clipboard'))
    .catch(() => {
      const ta = document.createElement('textarea');
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
      showToast('Copied to clipboard');
    });
}

function toggleMask(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.type = el.type === 'password' ? 'text' : 'password';
}

function appendLog(msg) {
  const log = document.getElementById('status-log');
  if (!log) return;
  const ts = new Date().toLocaleTimeString();
  log.textContent += `[${ts}] ${msg}\n`;
  log.scrollTop = log.scrollHeight;
}

// ─── Voice metadata (ElevenLabs) ─────────────────────────────
let _cachedELVoices = null; // populated by fetchElevenLabsVoices()
function getVoiceMeta(voiceId) {
  // Look up from ElevenLabs cache if available
  if (_cachedELVoices) {
    const v = _cachedELVoices.find(v => v.voice_id === voiceId);
    if (v) return { id: voiceId, gender: v.gender || 'female', lang: v.language || 'en' };
  }
  return { id: voiceId, gender: 'female', lang: 'en' };
}

// ─── Settings (localStorage) ──────────────────────────────────
function loadSettings() {
  try {
    return JSON.parse(localStorage.getItem('prophunt_settings') || '{}');
  } catch { return {}; }
}

function saveSettings() {
  const s = {
    voice:       document.getElementById('setting-voice')?.value    || '',
    language:    document.getElementById('setting-language')?.value || 'English',
    openingLine: document.getElementById('setting-opening')?.value  || '',
  };
  // Sync to Voice Agent config bar if it exists
  const av = document.getElementById('agent-voice');
  const al = document.getElementById('agent-language');
  const ao = document.getElementById('agent-opening');
  if (av) av.value = s.voice;
  if (al) al.value = s.language;
  if (ao) ao.value = s.openingLine;

  localStorage.setItem('prophunt_settings', JSON.stringify(s));

  // Show inline saved message
  const msg = document.getElementById('settings-saved-msg');
  if (msg) {
    msg.style.display = 'inline-flex';
    setTimeout(() => { msg.style.display = 'none'; }, 3000);
  }
  showToast('Settings saved');
}

function applyAgentConfig() {
  // Called when Voice Agent config bar changes — save to localStorage immediately
  const s = loadSettings();
  s.voice       = document.getElementById('agent-voice')?.value    || s.voice;
  s.language    = document.getElementById('agent-language')?.value || s.language;
  s.openingLine = document.getElementById('agent-opening')?.value  || s.openingLine;
  localStorage.setItem('prophunt_settings', JSON.stringify(s));

  // Sync language to call form
  const langEl = document.getElementById('call-language');
  if (langEl && s.language) langEl.value = s.language;

  // Flash saved indicator
  const saved = document.getElementById('agent-config-saved');
  if (saved) {
    saved.style.display = 'flex';
    setTimeout(() => { saved.style.display = 'none'; }, 2000);
  }
}

function initSettingsPage() {
  const s = loadSettings();
  const sl = document.getElementById('setting-language');
  const so = document.getElementById('setting-opening');
  if (sl && s.language)    sl.value = s.language;
  if (so && s.openingLine) so.value = s.openingLine;
  // Populate ElevenLabs voices (replaces hardcoded Sarvam list)
  populateVoiceSelect('setting-voice', s.voice || '');

  // Check orchestrator health
  checkOrchestratorStatus();
}

async function checkOrchestratorStatus() {
  const dot  = document.getElementById('orch-status-dot');
  const text = document.getElementById('orch-status-text');
  try {
    const res = await fetch(`${ORCH_BASE}/health`, { signal: AbortSignal.timeout(5000) });
    if (res.ok) {
      if (dot)  { dot.style.background = '#b1ec3e'; }
      if (text) text.textContent = 'Online — Railway production';
    } else {
      if (dot)  { dot.style.background = '#ffac36'; }
      if (text) text.textContent = `Degraded (HTTP ${res.status})`;
    }
  } catch (err) {
    if (dot)  { dot.style.background = '#e74c3c'; }
    if (text) text.textContent = 'Unreachable';
  }
}

// ─── Opening Line per Language ────────────────────────────────
// Replace all supported placeholder formats with actual values
function interpolateOpeningLine(text, leadName, projectName) {
  const name    = leadName    || 'ji';
  const project = projectName || 'our project';
  return text
    .replace(/\{[\s]*lead[\s_]*name[\s]*\}/gi, name)
    .replace(/\{[\s]*name[\s]*\}/gi, name)
    .replace(/\{[\s]*lead[\s]*\}/gi, name)
    .replace(/\[Lead Name\]/gi, name)
    .replace(/\{[\s]*project[\s_]*name[\s]*\}/gi, project)
    .replace(/\{[\s]*project[\s]*\}/gi, project)
    .replace(/\[Project Name\]/gi, project);
}

// getOpeningLine removed — opening line is optional; backend generates greeting when blank

// ─── Opening line live preview ─────────────────────────────────
function updateOpeningPreview() {
  const name    = document.getElementById('call-name')?.value.trim()    || '';
  const project = document.getElementById('call-project')?.value.trim() || '';
  const template = document.getElementById('call-opening')?.value.trim() || '';
  const preview  = document.getElementById('opening-preview');
  if (!preview) return;
  if (!template) { preview.style.display = 'none'; return; }
  const interpolated = interpolateOpeningLine(template, name, project);
  preview.style.display = 'block';
  preview.textContent = '▶ ' + interpolated;
}

// ─── Test Call Form ───────────────────────────────────────────
function initTestCallForm() {
  const form = document.getElementById('test-call-form');
  if (!form || form._bound) return;
  form._bound = true;

  // Pre-fill language from settings
  const s = loadSettings();
  const langEl = document.getElementById('call-language');
  if (langEl && s.language) langEl.value = s.language;

  // Live preview: update whenever name, project, or opening line changes
  ['call-name', 'call-project', 'call-opening'].forEach(id => {
    document.getElementById(id)?.addEventListener('input', updateOpeningPreview);
  });
  updateOpeningPreview();

  form.addEventListener('submit', async (e) => {
    e.preventDefault();

    const phone    = document.getElementById('call-phone').value.trim();
    const name     = document.getElementById('call-name').value.trim();
    const language = document.getElementById('call-language').value;
    const project  = document.getElementById('call-project').value.trim();
    const custom   = document.getElementById('call-opening').value.trim();
    const kbId     = document.getElementById('call-kb')?.value || '';
    const settings = loadSettings();
    const voice    = settings.voice || '';
    const voiceMeta = getVoiceMeta(voice);

    // Opening line is optional — if blank, backend generates a natural greeting
    const openingLine = custom ? interpolateOpeningLine(custom, name, project) : undefined;
    const kbContext   = getKBContext(kbId);

    const statusPanel   = document.getElementById('test-call-status');
    const statusDot     = document.getElementById('status-dot');
    const statusLabel   = document.getElementById('status-label');
    const statusSid     = document.getElementById('status-sid');
    const statusState   = document.getElementById('status-state');
    const statusGreeting= document.getElementById('status-greeting');
    const statusLog     = document.getElementById('status-log');

    statusPanel.style.display = 'block';
    statusDot.style.background = '#ffac36';
    statusDot.className = 'pulse-dot';
    statusLabel.textContent = 'Initiating call...';
    statusSid.textContent = '—';
    statusState.textContent = '—';
    statusGreeting.textContent = openingLine || '(AI-generated greeting)';
    if (statusLog) statusLog.textContent = '';

    appendLog(`Calling ${phone} (${language}, voice: ${voice} ${voiceMeta.gender})...`);
    if (kbContext) appendLog(`KB attached: "${document.getElementById('call-kb')?.selectedOptions[0]?.text}"`);

    // Pull active agent config for system prompt customisation
    const activeAgentConfig = (() => {
      const agents = loadAgents();
      const active = agents.find(a => a.status === 'active') || agents[0];
      const sess   = getCurrentSession();
      const orgName = sess?.tenantName || localStorage.getItem('prophunt_tenant_name') || 'Prophunt';
      if (!active) return { companyName: orgName };
      return {
        agentName:      active.name?.split('—')[0]?.trim() || 'Maya',
        companyName:    orgName,
        pitchTone:      active.pitchTone      || 'balanced',
        langStrictness: active.langStrictness || 'pure-hindi',
        wordCap:        active.wordCap        || 30,
        escalationLine: active.escalationLine || '',
      };
    })();

    const payload = {
      lead:     { name, phone, language, project },
      // voice_gender goes into campaign so backend uses it when resolving language-switched voices
      // voice_id is intentionally NOT set here → lets language auto-detection pick correct voice per language
      campaign: { name: project, openingLine: openingLine || undefined, voice_gender: voiceMeta.gender },
      provider: 'enablex',
      opening_line: openingLine || undefined,
      // KB context injected as dynamic variable for LLM system prompt
      dynamic_variables: kbContext ? { knowledge_base: kbContext, lead_name: name, project_name: project } : { lead_name: name, project_name: project },
      kb_id: kbId || undefined,
      // Agent config — pitch tone, word cap, language strictness, escalation line
      agent_config: activeAgentConfig,
    };

    try {
      const dialCtrl = new AbortController();
      const dialTimer = setTimeout(() => dialCtrl.abort(), 55000);
      let res;
      try {
        res = await fetch(`${ORCH_BASE}/call/dial`, {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify(payload),
          signal:  dialCtrl.signal,
        });
      } finally {
        clearTimeout(dialTimer);
      }

      if (!res.ok) {
        const errBody = await res.text().catch(() => '');
        throw new Error(`HTTP ${res.status}: ${errBody.slice(0, 200)}`);
      }

      const data = await res.json();
      const callSid = data.callSid || data.call_sid || data.sid || data.sessionId || null;

      if (callSid) {
        statusSid.textContent = callSid;
        statusLabel.textContent = 'Call placed — connecting...';
        appendLog(`Call SID: ${callSid}`);
        if (data.provider) appendLog(`Provider: ${data.provider.toUpperCase()}`);
        if (data.session_id) appendLog(`Agni session: ${data.session_id}`);
        if (data.kb_attached) appendLog(`✓ KB injected: ${data.kb_chars} chars of project context`);
        else appendLog(`⚠ No KB selected — agent has no project data`);
        if (data.livekit_url) appendLog(`LiveKit ready — agent is live`);
        startPolling(callSid);
      } else {
        statusLabel.textContent = 'Call dispatched';
        statusDot.style.background = '#b1ec3e';
        appendLog('Call dispatched. Response: ' + JSON.stringify(data).slice(0, 100));
      }
    } catch (err) {
      appendLog(`Error: ${err.message}`);
      statusLabel.textContent = 'Call failed — see log';
      statusDot.style.background = '#e74c3c';
      statusDot.classList.remove('pulse-dot');
    }
  });
}

// ─── Poll session status ──────────────────────────────────────
function startPolling(sid) {
  if (state.pollInterval) clearInterval(state.pollInterval);
  state.callSid = sid;

  const markCallEnded = (finalStatus = 'completed') => {
    const statusDot   = document.getElementById('status-dot');
    const statusLabel = document.getElementById('status-label');
    const statusState = document.getElementById('status-state');
    if (statusDot)   { statusDot.style.background = '#b1ec3e'; statusDot.classList.remove('pulse-dot'); }
    if (statusLabel) statusLabel.textContent = 'Call ended';
    if (statusState) statusState.textContent = finalStatus;
    appendLog(`Call ended — status: ${finalStatus}`);
    clearInterval(state.pollInterval);
    state.pollInterval = null;
    // Refresh recent calls + dashboard stats after call ends
    setTimeout(() => { loadRecentCalls(); loadDashboardStats(); }, 3000);
  };

  let lastStatus = '';
  let lastTurnCount = 0;
  let kbLoggedOnce = false;
  let lastDetectedLang = '';

  state.pollInterval = setInterval(async () => {
    try {
      const res = await fetch(`${ORCH_BASE}/sessions/${sid}`);

      // 404 = session deleted = call ended normally
      if (res.status === 404) {
        markCallEnded('completed');
        return;
      }
      if (!res.ok) return;

      const data = await res.json();
      const statusState = document.getElementById('status-state');
      const statusDot   = document.getElementById('status-dot');
      const statusLabel = document.getElementById('status-label');

      const s = data.status || data.state || '';
      if (statusState) statusState.textContent = s + (data.turn_count ? ` (${data.turn_count} turns)` : '');

      // Log KB status once when confirmed
      if (data.kb_loaded && !kbLoggedOnce) {
        kbLoggedOnce = true;
        appendLog(`✓ Knowledge Base confirmed active on agent`);
      }

      // Log language detection switches
      if (data.detected_language && data.detected_language !== lastDetectedLang && data.turn_count > 0) {
        const langNames = { auto: 'Multilingual', hi: 'Hindi', en: 'English', mr: 'Marathi', ta: 'Tamil', te: 'Telugu', pa: 'Punjabi', bn: 'Bengali', gu: 'Gujarati', kn: 'Kannada' };
        const langName = langNames[data.detected_language] || data.detected_language.toUpperCase();
        appendLog(`🌐 Language detected: ${langName} — voice switched to ${data.detected_language} (${data.voice_gender})`);
        lastDetectedLang = data.detected_language;
      }

      // Log new turns as they happen
      const currentTurns = data.turn_count || 0;
      if (currentTurns > lastTurnCount) {
        lastTurnCount = currentTurns;
        if (data.last_agent_reply) {
          appendLog(`Agent (turn ${currentTurns}): "${data.last_agent_reply.slice(0, 80)}${data.last_agent_reply.length > 80 ? '...' : ''}"`);
        }
      }

      if (s === 'completed' || s === 'failed' || s === 'ended' || s === 'timeout' || s === 'not_found') {
        markCallEnded(s);
      } else {
        if (statusDot) statusDot.style.background = '#ffac36';
        if (s && s !== lastStatus) {
          lastStatus = s;
          appendLog(`Status: ${s}`);
        }
      }
    } catch (err) {
      appendLog(`Poll error: ${err.message}`);
    }
  }, 3000);
}

// ─── Live calls counter ───────────────────────────────────────
async function refreshLiveCalls() {
  try {
    const res = await fetch(`${ORCH_BASE}/sessions`);
    if (!res.ok) return;
    const data = await res.json();
    const sessions = data.sessions || (Array.isArray(data) ? data : []);
    const count = sessions.filter(s => !s.closed && (s.status === 'active' || s.status === 'in-progress' || s.status === 'initiated')).length;

    ['live-calls-count', 'va-live-calls', 'stat-active-agents'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.textContent = count;
    });
  } catch (_) {}
}

// ─── Dashboard Stats ──────────────────────────────────────────
async function loadDashboardStats() {
  try {
    const res = await fetch(`${API_BASE}/internal/calls?limit=200`, {
      headers: { 'X-Internal-Token': INTERNAL_TOKEN },
    });
    if (!res.ok) return;
    const data = await res.json();
    const calls = data.calls || [];

    const total     = calls.length;
    const connected = calls.filter(c => (c.duration || 0) > 10).length;
    const callbacks = calls.filter(c => c.outcome === 'callback').length;

    const setEl = (id, val) => {
      const el = document.getElementById(id);
      if (el) el.textContent = val;
    };

    setEl('stat-total-calls',     total);
    setEl('stat-connected-calls', connected);
    setEl('stat-callbacks',       callbacks);

    // Update connect rate text under connected calls card
    if (total > 0) {
      const rate = Math.round((connected / total) * 100);
      const rateEl = document.querySelector('#stat-connected-calls + div');
      // Can't easily target this — skip, just update the number
    }

    // Populate recent activity feed
    renderDashboardActivity(calls.slice(0, 7));
  } catch (_) {}
}

function renderDashboardActivity(calls) {
  const feed = document.getElementById('activity-feed');
  if (!feed) return;
  if (!calls.length) {
    feed.innerHTML = `
      <div style="color:var(--text-3);font-size:13px;padding:18px;border:1px dashed var(--border-md);border-radius:10px;">
        No call activity yet. Live calls and CRM updates will appear here after the first real campaign run.
      </div>`;
    return;
  }

  feed.innerHTML = calls.map(c => {
    const outcome = c.outcome || c.status || 'completed';
    const icons = { qualified:'star', callback:'schedule', not_interested:'thumb_down', no_answer:'phone_missed', dropped:'call_end', completed:'check_circle', failed:'error' };
    // Use CSS variable names for theme-aware colors
    const colorMap = {
      qualified:      'var(--accent)',
      callback:       'var(--amber)',
      not_interested: 'var(--text-3)',
      no_answer:      'var(--text-3)',
      dropped:        'var(--red)',
      completed:      'var(--cyan)',
      failed:         'var(--red)',
    };
    const icon  = icons[outcome]  || 'call';
    const color = colorMap[outcome] || 'var(--text-3)';
    return `
      <div style="display:flex;align-items:flex-start;gap:12px;padding:10px 0;border-bottom:1px solid var(--border);">
        <div style="width:32px;height:32px;border-radius:50%;background:var(--surface-2);border:1px solid var(--border);display:flex;align-items:center;justify-content:center;flex-shrink:0;margin-top:2px;">
          <span class="material-symbols-outlined" style="font-size:15px;color:${color};">${icon}</span>
        </div>
        <div style="flex:1;min-width:0;">
          <div style="font-size:13px;font-weight:500;color:var(--text-1);">${c.lead_name || c.phone || 'Unknown'}</div>
          <div style="font-size:11px;color:var(--text-2);">${c.phone || ''} · ${formatTime(c.created_at)}</div>
        </div>
        ${statusChip(outcome)}
      </div>`;
  }).join('');
}

// ─── Recent Calls ─────────────────────────────────────────────
async function loadRecentCalls() {
  const tbody = document.getElementById('recent-calls-tbody');
  if (tbody) tbody.innerHTML = `<tr><td colspan="6" style="text-align:center;color:var(--text-3);padding:24px;">Loading...</td></tr>`;

  let calls = [];
  try {
    const res = await fetch(`${API_BASE}/internal/calls?limit=20`, {
      headers: { 'X-Internal-Token': INTERNAL_TOKEN },
    });
    if (res.ok) {
      const data = await res.json();
      calls = Array.isArray(data) ? data : (data.calls || []);
    }
  } catch (err) {
    appendLog(`Calls fetch error: ${err.message}`);
  }
  renderRecentCalls(calls);
}

function renderRecentCalls(calls) {
  window._lastRecentCalls = calls; // cache for theme-toggle re-render
  const tbody   = document.getElementById('recent-calls-tbody');
  const countEl = document.getElementById('recent-calls-count');
  if (!tbody) return;

  if (countEl) countEl.textContent = calls.length ? `${calls.length} calls` : 'No calls yet';

  if (!calls.length) {
    tbody.innerHTML = `<tr><td colspan="6" style="text-align:center;color:var(--text-3);padding:24px;">No calls yet — place your first test call above.</td></tr>`;
    return;
  }

  tbody.innerHTML = calls.map(c => {
    // Convert full orchestrator URL to Vercel-proxied path to avoid CORS issues
    const rawUrl = c.recording_url || c.recordingUrl || c.mixed_url || null;
    const recUrl = rawUrl
      ? rawUrl.replace(/^https?:\/\/orchestrator-[^/]+\.up\.railway\.app/, '')
      : null;
    const rid    = 'rec-' + (c.call_sid || c.sid || Math.random().toString(36).substr(2, 8));
    return `
    <tr>
      <td class="mono" style="font-size:11px;color:${cyanColor()};">${(c.call_sid || c.sid || '—').slice(0, 18)}</td>
      <td style="color:var(--text-2);">${c.phone || '—'}</td>
      <td style="color:var(--text-2);">${c.lead_name || '—'}</td>
      <td>${formatDuration(c.duration || c.duration_sec)}</td>
      <td>${statusChip(c.outcome || c.status || 'completed')}</td>
      <td style="min-width:160px;">
        ${recUrl
          ? `<div style="display:flex;align-items:center;gap:6px;">
               <audio id="${rid}" src="${recUrl}" preload="none" style="display:none;"></audio>
               <button class="btn-ghost" style="padding:4px 10px;font-size:11px;display:flex;align-items:center;gap:4px;"
                 onclick="toggleAudio('${rid}',this)">
                 <span class="material-symbols-outlined" style="font-size:15px;">play_circle</span>
                 <span>Play</span>
               </button>
               <a href="${recUrl}" download style="color:${cyanColor()};font-size:11px;" title="Download">
                 <span class="material-symbols-outlined" style="font-size:15px;vertical-align:middle;">download</span>
               </a>
             </div>`
          : `<span style="font-size:11px;color:var(--text-3);">No recording</span>`
        }
      </td>
    </tr>`;
  }).join('');
}

// ─── Analytics ────────────────────────────────────────────────
async function loadAnalytics() {
  let calls = [];
  try {
    const res = await fetch(`${API_BASE}/internal/calls?limit=200`, {
      headers: { 'X-Internal-Token': INTERNAL_TOKEN },
    });
    if (res.ok) {
      const data = await res.json();
      calls = data.calls || [];

      const total     = calls.length;
      const connected = calls.filter(c => (c.duration || 0) > 10).length;
      const callbacks = calls.filter(c => c.outcome === 'callback').length;
      const totalDur  = calls.reduce((sum, c) => sum + (c.duration || 0), 0);
      const avgDur    = total ? Math.round(totalDur / total) : 0;

      const setEl = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
      setEl('an-calls',    total);
      setEl('an-connect',  total ? Math.round((connected / total) * 100) + '%' : '—');
      setEl('an-callback', total ? Math.round((callbacks / total) * 100) + '%' : '—');
      setEl('an-duration', formatDuration(avgDur));
    }
  } catch (_) {}
  renderAnalyticsCalls(calls);
  renderAnalyticsCharts(calls);
}

function renderAnalyticsCalls(calls) {
  const tbody = document.getElementById('analytics-calls-tbody');
  if (!tbody) return;

  if (!calls.length) {
    tbody.innerHTML = `<tr><td colspan="7" style="text-align:center;color:var(--text-3);padding:24px;">No call data yet.</td></tr>`;
    return;
  }

  tbody.innerHTML = calls.map(c => `
    <tr>
      <td class="mono" style="font-size:11px;color:${cyanColor()};">${(c.call_sid || '—').slice(0, 20)}</td>
      <td style="color:var(--text-2);">${c.phone || '—'}</td>
      <td>${c.lead_name || '—'}</td>
      <td>${c.campaign || '—'}</td>
      <td>${formatDuration(c.duration || c.duration_sec)}</td>
      <td>${statusChip(c.outcome || c.status || 'completed')}</td>
      <td style="color:var(--text-3);font-size:12px;">${formatTime(c.created_at)}</td>
    </tr>
  `).join('');
}

function setDateRange(range) {
  state.analyticsRange = range;
  document.querySelectorAll('#date-range-btns button[data-range]').forEach(btn => {
    const active = btn.getAttribute('data-range') === range;
    btn.className = active ? 'chip chip-green' : 'chip chip-slate';
    btn.style.cssText = 'cursor:pointer;border:none;padding:7px 16px;';
  });
  loadAnalytics();
}

// ─── Leads ───────────────────────────────────────────────────

// Live workspace auth / Arthaleads helpers
function getAuthSession() {
  let stored = {};
  for (const key of ['prophunt_auth', 'voiceai_auth', 'auth', 'session']) {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) continue;
      stored = JSON.parse(raw) || {};
      break;
    } catch (_) {}
  }
  const directToken = localStorage.getItem('access_token') || localStorage.getItem('prophunt_access_token');
  const directTenant = localStorage.getItem('tenant_id') || localStorage.getItem('prophunt_tenant_id');
  const storedToken = stored.access_token || stored.accessToken || stored.token || stored.jwt;
  const storedTenant = stored.tenant_id || stored.tenantId || stored.tenant?.id || stored.user?.tenant_id;
  const storedEmail = stored.email || stored.user?.email || localStorage.getItem('prophunt_email');
  const storedTenantName = stored.tenant_name || stored.tenantName || stored.tenant?.name || localStorage.getItem('prophunt_tenant_name');
  if (directToken || directTenant || storedToken || storedTenant) {
    return {
      accessToken: directToken || storedToken || null,
      tenantId: directTenant || storedTenant || null,
      email: storedEmail || null,
      tenantName: storedTenantName || null,
    };
  }
  for (const key of ['prophunt_auth', 'voiceai_auth', 'auth', 'session']) {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) continue;
      const parsed = JSON.parse(raw);
      const accessToken = parsed.access_token || parsed.accessToken || parsed.token || parsed.jwt;
      const tenantId = parsed.tenant_id || parsed.tenantId || parsed.tenant?.id || parsed.user?.tenant_id;
      const email = parsed.email || parsed.user?.email || null;
      const tenantName = parsed.tenant_name || parsed.tenantName || parsed.tenant?.name || null;
      if (accessToken || tenantId) return { accessToken, tenantId, email, tenantName };
    } catch (_) {}
  }
  return { accessToken: null, tenantId: null, email: null, tenantName: null };
}

function getAuthHeaders(extra = {}) {
  const { accessToken } = getAuthSession();
  return {
    'Content-Type': 'application/json',
    ...(accessToken ? { Authorization: 'Bearer ' + accessToken } : {}),
    ...extra,
  };
}

function requireWorkspaceSession() {
  const session = getAuthSession();
  if (!session.accessToken || !session.tenantId) {
    showToast('Sign in to the workspace before syncing Arthaleads data.', 'error');
    return null;
  }
  return session;
}

function storeAuthSession(payload, fallback = {}) {
  const accessToken = payload.access_token || payload.accessToken || payload.token || payload.jwt;
  const tenantId = payload.tenant_id || payload.tenantId || payload.tenant?.id || fallback.tenantId;
  const email = payload.email || fallback.email || '';
  const tenantName = payload.tenant_name || payload.tenantName || payload.tenant?.name || fallback.tenantName || '';
  if (!accessToken || !tenantId) throw new Error('Auth response did not include a token and tenant.');
  const session = { accessToken, tenantId, email, tenantName };
  localStorage.setItem('prophunt_auth', JSON.stringify(session));
  localStorage.setItem('prophunt_workspace_ready', 'true');
  localStorage.setItem('access_token', accessToken);
  localStorage.setItem('prophunt_access_token', accessToken);
  localStorage.setItem('tenant_id', tenantId);
  localStorage.setItem('prophunt_tenant_id', tenantId);
  if (email) localStorage.setItem('prophunt_email', email);
  if (tenantName) localStorage.setItem('prophunt_tenant_name', tenantName);
  return session;
}

function clearAuthSession() {
  ['prophunt_auth', 'voiceai_auth', 'auth', 'session', 'access_token', 'prophunt_access_token', 'tenant_id', 'prophunt_tenant_id', 'prophunt_email', 'prophunt_tenant_name', 'prophunt_workspace_ready'].forEach(key => {
    localStorage.removeItem(key);
  });
}

function hasWorkspaceGateSession() {
  return localStorage.getItem('prophunt_workspace_ready') === 'true';
}

function getProfileInitial(email = '') {
  return (email || 'P').trim().charAt(0).toUpperCase() || 'P';
}

function updateProfileViews() {
  const session = getAuthSession();
  const email = session.email || localStorage.getItem('prophunt_email') || 'admin@prophunt.com';
  const tenantName = session.tenantName || localStorage.getItem('prophunt_tenant_name') || 'Prophunt workspace';
  const tenantId = session.tenantId || '-';
  const initial = getProfileInitial(email || tenantName);

  const avatar = document.getElementById('profile-button');
  if (avatar) avatar.textContent = initial;
  const largeAvatar = document.getElementById('profile-avatar-large');
  if (largeAvatar) largeAvatar.textContent = initial;
  const title = document.getElementById('profile-email-title');
  if (title) title.textContent = email;
  const subtitle = document.getElementById('profile-tenant-subtitle');
  if (subtitle) subtitle.textContent = tenantName;
  const id = document.getElementById('profile-tenant-id');
  if (id) id.textContent = tenantId;
  const emailValue = document.getElementById('profile-email-value');
  if (emailValue) emailValue.textContent = email;
}

function logoutWorkspace() {
  if (state.pollInterval) {
    clearInterval(state.pollInterval);
    state.pollInterval = null;
  }
  if (state.liveCallsInterval) {
    clearInterval(state.liveCallsInterval);
    state.liveCallsInterval = null;
  }
  state.callSid = null;
  clearAuthSession();
  state.activeView = 'dashboard';
  showToast('Logged out');
  showAuth('login');
}

function showAuth(mode = 'login') {
  const authScreen = document.getElementById('auth-screen');
  const appShell = document.getElementById('app-shell');
  if (authScreen) authScreen.style.display = 'flex';
  if (appShell) appShell.style.display = 'none';
  switchAuthMode(mode);
}

function showApp() {
  const authScreen = document.getElementById('auth-screen');
  const appShell = document.getElementById('app-shell');
  if (authScreen) authScreen.style.display = 'none';
  if (appShell) appShell.style.display = 'block';
  updateProfileViews();
}

function switchAuthMode(mode = 'login') {
  const isRegister = mode === 'register';
  document.getElementById('login-form')?.classList.toggle('active', !isRegister);
  document.getElementById('register-form')?.classList.toggle('active', isRegister);

  const title = document.getElementById('auth-copy-title');
  const text = document.getElementById('auth-copy-text');
  const action = document.getElementById('auth-copy-action');
  if (title) title.textContent = isRegister ? 'Welcome back to the workspace.' : 'Launch real-estate calling from one premium workspace.';
  if (text) {
    text.textContent = isRegister
      ? 'Already have a Prophunt workspace? Sign in and continue CRM setup, campaigns, and voice testing.'
      : 'Connect Arthaleads CRM, prepare campaigns, and test Sarvam voice agents after your team signs in.';
  }
  if (action) {
    action.textContent = isRegister ? 'Sign In' : 'Sign Up';
    action.onclick = () => switchAuthMode(isRegister ? 'login' : 'register');
  }
}

async function parseApiError(res) {
  const text = await res.text();
  try {
    const json = JSON.parse(text);
    return json.detail || json.message || text;
  } catch (_) {
    return text || res.statusText || 'Request failed';
  }
}

async function handleLogin(event) {
  event.preventDefault();
  const email = document.getElementById('login-email')?.value.trim();
  const password = document.getElementById('login-password')?.value || '';
  if (!email || !password) return showToast('Enter your workspace email and password.', 'error');
  try {
    const res = await fetch(API_BASE + '/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });
    if (!res.ok) throw new Error(await parseApiError(res));
    const data = await res.json();
    storeAuthSession(data, { email });
    showToast('Workspace unlocked');
    openWorkspace();
  } catch (err) {
    showToast('Sign in failed: ' + String(err.message || err).slice(0, 120), 'error');
  }
}

async function handleRegister(event) {
  event.preventDefault();
  const tenantName = document.getElementById('register-tenant-name')?.value.trim();
  const email = document.getElementById('register-email')?.value.trim();
  const password = document.getElementById('register-password')?.value || '';
  if (!tenantName || !email || !password) return showToast('Fill tenant name, email, and password.', 'error');
  try {
    const res = await fetch(API_BASE + '/auth/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tenant_name: tenantName, email, password }),
    });
    if (!res.ok) throw new Error(await parseApiError(res));
    const data = await res.json();
    storeAuthSession(data, { email, tenantName });
    showToast('Tenant created');
    openWorkspace();
  } catch (err) {
    showToast('Registration failed: ' + String(err.message || err).slice(0, 120), 'error');
  }
}

function bindAuthForms() {
  document.getElementById('login-form')?.addEventListener('submit', handleLogin);
  document.getElementById('register-form')?.addEventListener('submit', handleRegister);
  document.getElementById('show-register-button')?.addEventListener('click', () => switchAuthMode('register'));
  document.getElementById('show-login-button')?.addEventListener('click', () => switchAuthMode('login'));
}

function startLivePolling() {
  refreshLiveCalls();
  if (!state.liveCallsInterval) {
    state.liveCallsInterval = setInterval(refreshLiveCalls, 10000);
  }
}

function openWorkspace() {
  showApp();
  kbSeedIfNeeded(); // ensure KB is seeded regardless of which page is visited first
  navigate(state.activeView || 'dashboard');
  startLivePolling();
}

function getArthaleadsLocalConfig() {
  try { return JSON.parse(localStorage.getItem('arthaleads_crm') || '{}'); } catch { return {}; }
}

function saveArthaleadsLocalConfig() {
  const old = getArthaleadsLocalConfig();
  const cfg = {
    baseUrl: document.getElementById('arthaleads-base-url')?.value.trim() || old.baseUrl || 'https://api.arthaleads.com',
    headerName: document.getElementById('arthaleads-header-name')?.value.trim() || old.headerName || 'X-Api-Key',
    campaignId: document.getElementById('arthaleads-campaign-id')?.value.trim() || old.campaignId || '',
    apiKey: document.getElementById('arthaleads-api-key')?.value.trim() || old.apiKey || '',
  };
  localStorage.setItem('arthaleads_crm', JSON.stringify(cfg));
  return cfg;
}

function hydrateArthaleadsForm() {
  const cfg = getArthaleadsLocalConfig();
  const set = (id, value) => { const el = document.getElementById(id); if (el && value) el.value = value; };
  set('arthaleads-base-url', cfg.baseUrl || 'https://api.arthaleads.com');
  set('arthaleads-header-name', cfg.headerName || 'X-Api-Key');
  set('arthaleads-campaign-id', cfg.campaignId || '');
  set('arthaleads-api-key', cfg.apiKey || '');
}

function buildArthaleadsConnectionConfig() {
  const cfg = saveArthaleadsLocalConfig();
  return {
    crm_type: 'custom_rest',
    label: 'Arthaleads',
    base_url: cfg.baseUrl.replace(/\/$/, ''),
    auth_type: 'api_key',
    credentials: { api_key: cfg.apiKey, api_key_header: cfg.headerName || 'X-Api-Key' },
    endpoints: {
      health: '/health',
      fetch_dialable: '/api/voice/leads',
      by_phone: '/api/voice/leads/search',
    },
    field_map: {
      id: '_id', name: 'name', phone: 'phone', email: 'email', status: 'status', source: 'source',
      project: 'leadSourceLabel', priority: 'priority', assigned_to: 'assignedToName', created_at: 'createdAt', last_called_at: 'followUpDate',
    },
    settings: { campaign_id: cfg.campaignId || undefined },
  };
}

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[ch]));
}

function normalizeLead(lead = {}) {
  return {
    id: lead.id || lead._id || lead.lead_id || '',
    name: lead.name || lead.lead_name || 'Unnamed lead',
    phone: lead.phone || lead.mobile || lead.phone_number || '',
    email: lead.email || '',
    status: lead.status || lead.call_status || 'New',
    source: lead.source || lead.leadSource || lead.lead_source || 'Arthaleads',
    project: lead.project || lead.project_name || lead.leadSourceLabel || lead.campaign_id || '-',
    language: lead.language || lead.language_preference || lead.preferred_language || 'English',
    calls: Number(lead.call_attempts || lead.calls || lead.attempts || 0),
    city: lead.city || lead.location || '',
    config: lead.configuration || lead.unit || lead.requirement || '',
    raw: lead,
  };
}

function leadStatusKind(status = '') {
  const s = status.toLowerCase();
  if (s.includes('follow')) return 'followup';
  if (s.includes('review') || s.includes('contacted')) return 'review';
  return 'ai-ready';
}

function updateLeadCounts(leads = []) {
  const counts = { 'ai-ready': 0, review: 0, followup: 0 };
  leads.forEach(lead => { counts[leadStatusKind(normalizeLead(lead).status)] += 1; });
  const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
  set('count-ai-ready', counts['ai-ready']);
  set('count-review', counts.review);
  set('count-followup', counts.followup);
}

function renderCrmPreview(leads = []) {
  const tbody = document.getElementById('crm-preview-tbody');
  if (!tbody) return;
  if (!leads.length) {
    tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text-3);padding:28px;">No Arthaleads preview loaded yet.</td></tr>';
    return;
  }
  tbody.innerHTML = leads.slice(0, 8).map(lead => {
    const n = normalizeLead(lead);
    return '<tr>' +
      '<td style="font-weight:500;">' + escapeHtml(n.name || '-') + '</td>' +
      '<td style="color:var(--text-2);">' + escapeHtml(n.phone || '-') + '</td>' +
      '<td>' + escapeHtml(n.project || '-') + '</td>' +
      '<td>' + escapeHtml(n.language || '-') + '</td>' +
      '<td><span class="chip chip-slate">' + escapeHtml(n.source || 'Arthaleads') + '</span></td>' +
    '</tr>';
  }).join('');
}

function setArthaleadsStatus(connected, detail) {
  const chip = document.getElementById('arthaleads-status-chip');
  if (chip) {
    chip.className = connected ? 'chip chip-green' : 'chip chip-slate';
    chip.textContent = connected ? 'Connected' : 'Not connected';
  }
  const summary = document.getElementById('arthaleads-summary');
  if (summary && detail) summary.innerHTML = detail;
}

function initLeadsPage() {
  loadLeadsFromArthaleads(false);
}

async function loadLeadsFromArthaleads(showSuccess = false) {
  const tbody = document.getElementById('leads-tbody');
  if (tbody) tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--text-3);padding:28px;">Loading Arthaleads leads...</td></tr>';
  const session = getAuthSession();
  if (!session.accessToken || !session.tenantId) {
    renderLeads([]);
    if (showSuccess) showToast('Sign in before fetching Arthaleads leads.', 'error');
    return;
  }
  const cfg = saveArthaleadsLocalConfig();
  const campaignParam = cfg.campaignId ? '&campaign_id=' + encodeURIComponent(cfg.campaignId) : '';
  try {
    const res = await fetch(API_BASE + '/tenants/' + encodeURIComponent(session.tenantId) + '/leads?limit=100' + campaignParam, { headers: getAuthHeaders() });
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();
    const leads = Array.isArray(data) ? data : (data.leads || data.data || []);
    renderLeads(leads);
    renderCrmPreview(leads);
    localStorage.setItem('prophunt_last_import', new Date().toISOString());
    if (showSuccess) showToast('Arthaleads leads refreshed');
  } catch (err) {
    renderLeads([]);
    showToast('Could not fetch Arthaleads leads: ' + String(err.message || err).slice(0, 100), 'error');
  }
}

function renderLeads(leads = []) {
  const tbody = document.getElementById('leads-tbody');
  updateLeadCounts(leads);
  // Populate project filter dropdown
  const projSel = document.getElementById('leads-project-filter');
  if (projSel) {
    const projects = [...new Set(leads.map(l => normalizeLead(l).project).filter(p => p && p !== '-'))];
    const cur = projSel.value;
    while (projSel.options.length > 1) projSel.remove(1);
    projects.forEach(p => { const o = document.createElement('option'); o.value = p.toLowerCase(); o.textContent = p; projSel.appendChild(o); });
    if (cur) projSel.value = cur;
  }
  if (!tbody) return;
  if (!leads.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text-3);padding:28px;">No leads yet. Click "Import Live" to fetch from Arthaleads.</td></tr>';
    return;
  }
  const sourceIcon = src => {
    const s = (src||'').toLowerCase();
    if (s.includes('facebook') || s.includes('fb')) return 'thumb_up';
    if (s.includes('google')) return 'search';
    if (s.includes('wordpress') || s.includes('wp')) return 'language';
    if (s.includes('organic')) return 'eco';
    if (s.includes('referral')) return 'person_add';
    return 'database';
  };
  const sourceColor = src => {
    const s = (src||'').toLowerCase();
    if (s.includes('facebook') || s.includes('fb')) return 'var(--cyan)';
    if (s.includes('google')) return 'var(--amber)';
    if (s.includes('wordpress') || s.includes('wp')) return '#7c3aed';
    if (s.includes('organic')) return 'var(--accent)';
    return 'var(--text-3)';
  };
  tbody.innerHTML = leads.map(lead => {
    const n = normalizeLead(lead);
    const kind = leadStatusKind(n.status);
    const chipClass = kind === 'followup' ? 'chip-cyan' : kind === 'review' ? 'chip-amber' : 'chip-green';
    const sub = [n.city, n.config].filter(Boolean).join(', ');
    const callArgs = [n.phone, n.name, n.language, n.project].map(value => JSON.stringify(String(value || ''))).join(',');
    const srcNorm = (n.source||'arthaleads').toLowerCase();
    return '<tr data-lead-status="' + kind + '" data-lead-source="' + srcNorm + '" data-lead-project="' + (n.project||'').toLowerCase() + '">' +
      '<td><div style="font-weight:500;">' + escapeHtml(n.name) + '</div><div style="font-size:11px;color:var(--text-3);">' + escapeHtml(sub || '') + '</div></td>' +
      '<td style="color:var(--text-2);">' + escapeHtml(n.phone || '-') + '</td>' +
      '<td>' + escapeHtml(n.project || '-') + '</td>' +
      '<td><span style="display:inline-flex;align-items:center;gap:4px;font-size:11px;color:' + sourceColor(n.source) + '"><span class="material-symbols-outlined" style="font-size:13px;">' + sourceIcon(n.source) + '</span>' + escapeHtml(n.source || 'Arthaleads') + '</span></td>' +
      '<td><span class="chip chip-slate">' + escapeHtml(n.language || 'English') + '</span></td>' +
      '<td>' + n.calls + '</td>' +
      '<td><span class="chip ' + chipClass + '">' + escapeHtml(n.status || 'New') + '</span></td>' +
      '<td><button class="btn-primary" style="padding:6px 14px;font-size:10px;" onclick=\'startCallFromLead(' + callArgs + ')\'><span class="material-symbols-outlined" style="font-size:14px;">call</span>Call Now</button></td>' +
    '</tr>';
  }).join('');
  filterLeads();
}

function filterLeads(filter) {
  if (filter !== undefined) state.leadsFilter = filter;
  document.querySelectorAll('#filter-chips button[data-filter]').forEach(btn => {
    const active = btn.getAttribute('data-filter') === state.leadsFilter;
    btn.className = active ? 'chip chip-green' : 'chip chip-slate';
    btn.style.cssText = 'cursor:pointer;border:none;padding:6px 14px;';
  });
  const search    = (document.getElementById('leads-search')?.value || '').toLowerCase();
  const srcFilter = (document.getElementById('leads-source-filter')?.value || '').toLowerCase();
  const projFilter= (document.getElementById('leads-project-filter')?.value || '').toLowerCase();
  document.querySelectorAll('#leads-tbody tr').forEach(row => {
    const text    = row.textContent.toLowerCase();
    const status  = row.getAttribute('data-lead-status') || '';
    const source  = row.getAttribute('data-lead-source') || '';
    const project = row.getAttribute('data-lead-project') || '';
    const matchSearch  = !search    || text.includes(search);
    const matchFilter  = state.leadsFilter === 'all' || !status || status === state.leadsFilter;
    const matchSource  = !srcFilter  || source.includes(srcFilter);
    const matchProject = !projFilter || project.includes(projFilter);
    row.style.display = matchSearch && matchFilter && matchSource && matchProject ? '' : 'none';
  });
}

function startCallFromLead(phone, name, lang, project) {
  navigate('voice-agent');
  setTimeout(() => {
    const set = (id, val) => { const el = document.getElementById(id); if (el) el.value = val; };
    set('call-phone', phone);
    set('call-name', name);
    set('call-language', lang);
    set('call-project', project);
  }, 150);
}

function initCampaignsPage() {
  loadCampaigns();
}

function openNewCampaignModal() {
  const modal = document.getElementById('campaign-modal');
  if (modal) modal.style.display = 'flex';
}

function closeNewCampaignModal() {
  const modal = document.getElementById('campaign-modal');
  if (modal) modal.style.display = 'none';
}

async function loadCampaigns() {
  const session = getAuthSession();
  if (!session.accessToken || !session.tenantId) {
    renderCampaigns([]);
    return;
  }
  try {
    const res = await fetch(API_BASE + '/tenants/' + encodeURIComponent(session.tenantId) + '/campaigns', { headers: getAuthHeaders() });
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();
    renderCampaigns(Array.isArray(data) ? data : (data.campaigns || []));
  } catch (err) {
    renderCampaigns([]);
    showToast('Could not load campaigns: ' + String(err.message || err).slice(0, 100), 'error');
  }
}

function renderCampaigns(campaigns = []) {
  const grid = document.getElementById('campaigns-grid');
  const stat = document.getElementById('stat-campaigns');
  if (stat) stat.textContent = campaigns.length;
  if (!grid) return;
  if (!campaigns.length) {
    grid.innerHTML = '<div class="glass-panel" style="grid-column:1 / -1;border-radius:24px;padding:28px;text-align:center;color:var(--text-3);">No campaigns created yet. Use New Campaign to launch an Arthaleads outbound flow.</div>';
    return;
  }
  grid.innerHTML = campaigns.map(c => {
    const status = c.status || 'draft';
    const active = ['active', 'running'].includes(status.toLowerCase());
    const action = active ? 'pause' : 'start';
    const project = c.project_id || c.lead_filters?.campaign_id || '-';
    const campaignActionArgs = [c.id, action].map(value => JSON.stringify(String(value || ''))).join(',');
    return '<div class="glass-panel" style="border-radius:24px;padding:24px;display:flex;flex-direction:column;gap:16px;">' +
      '<div style="display:flex;align-items:flex-start;justify-content:space-between;"><div><div style="font-family:\'Space Grotesk\',sans-serif;font-size:18px;font-weight:700;color:var(--text-1);margin-bottom:4px;">' + escapeHtml(c.name || c.id) + '</div><div style="font-size:12px;color:var(--text-3);">Arthaleads campaign_id: ' + escapeHtml(project) + '</div></div><span class="chip ' + (active ? 'chip-green' : 'chip-slate') + '">' + escapeHtml(status) + '</span></div>' +
      '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;text-align:center;"><div style="background:rgba(255,255,255,0.03);border-radius:12px;padding:10px;"><div style="font-family:\'Space Grotesk\',sans-serif;font-size:20px;font-weight:700;color:#b1ec3e;">0</div><div class="section-title" style="margin-top:2px;">Leads</div></div><div style="background:rgba(255,255,255,0.03);border-radius:12px;padding:10px;"><div style="font-family:\'Space Grotesk\',sans-serif;font-size:20px;font-weight:700;color:var(--cyan);">0</div><div class="section-title" style="margin-top:2px;">Calls</div></div><div style="background:rgba(255,255,255,0.03);border-radius:12px;padding:10px;"><div style="font-family:\'Space Grotesk\',sans-serif;font-size:20px;font-weight:700;color:#ffac36;">-</div><div class="section-title" style="margin-top:2px;">Success</div></div></div>' +
      '<div style="display:flex;gap:8px;"><button class="btn-ghost" style="flex:1;justify-content:center;" onclick="showToast(\'Campaign details will show once calls start.\')"><span class="material-symbols-outlined" style="font-size:15px;">visibility</span>View</button><button class="btn-ghost" style="flex:1;justify-content:center;" onclick=\'runCampaignAction(' + campaignActionArgs + ')\'><span class="material-symbols-outlined" style="font-size:15px;">' + (active ? 'pause' : 'play_arrow') + '</span>' + (active ? 'Pause' : 'Start') + '</button></div>' +
    '</div>';
  }).join('');
}

async function createCampaignFromForm(event) {
  event.preventDefault();
  const session = requireWorkspaceSession();
  if (!session) return;
  const name = document.getElementById('campaign-name')?.value.trim();
  const projectId = document.getElementById('campaign-project-id')?.value.trim();
  const gender = document.getElementById('campaign-voice-gender')?.value || 'female';
  const payload = {
    id: 'arthaleads-' + (projectId || name || Date.now()).toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, ''),
    name,
    project_id: projectId,
    crm_source: 'arthaleads',
    crm_config_id: 'arthaleads',
    voice_gender: gender,
    voice_id: gender === 'male' ? 'rahul' : 'ritu',
    language: document.getElementById('campaign-language')?.value || 'English',
    calling_schedule: { timezone: 'Asia/Kolkata', active_days: ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'], start_time: document.getElementById('campaign-start-time')?.value || '10:00', end_time: document.getElementById('campaign-end-time')?.value || '18:00' },
    lead_filters: { campaign_id: projectId },
    max_concurrent: Number(document.getElementById('campaign-max-concurrent')?.value || 1),
    max_attempts: Number(document.getElementById('campaign-max-attempts')?.value || 3),
    retry_interval_hours: 4,
    status: 'draft',
    opening_line: document.getElementById('campaign-opening-line')?.value.trim() || undefined,
  };
  try {
    const res = await fetch(API_BASE + '/campaigns', { method: 'POST', headers: getAuthHeaders(), body: JSON.stringify(payload) });
    if (!res.ok) throw new Error(await res.text());
    closeNewCampaignModal();
    document.getElementById('campaign-form')?.reset();
    showToast('Campaign created');
    loadCampaigns();
  } catch (err) {
    showToast('Could not create campaign: ' + String(err.message || err).slice(0, 120), 'error');
  }
}

async function runCampaignAction(campaignId, action) {
  const session = requireWorkspaceSession();
  if (!session) return;
  try {
    const res = await fetch(API_BASE + '/campaigns/' + encodeURIComponent(campaignId) + '/' + action, { method: 'POST', headers: getAuthHeaders(), body: '{}' });
    if (!res.ok) throw new Error(await res.text());
    showToast('Campaign ' + action + ' requested');
    loadCampaigns();
  } catch (err) {
    showToast('Campaign action failed: ' + String(err.message || err).slice(0, 120), 'error');
  }
}

async function initCrmPage() {
  hydrateArthaleadsForm();
  const session = getAuthSession();
  if (!session.accessToken || !session.tenantId) {
    setArthaleadsStatus(false, '<span class="section-title">Connection Status</span><p style="margin:12px 0 0;line-height:1.6;">Sign in first, then connect Arthaleads to fetch live CRM leads.</p>');
    return;
  }
  try {
    const res = await fetch(API_BASE + '/tenants/' + encodeURIComponent(session.tenantId) + '/crm-connections', { headers: getAuthHeaders() });
    if (!res.ok) return;
    const data = await res.json();
    const list = Array.isArray(data) ? data : (data.connections || []);
    const connected = list.some(c => String(c.label || c.crm_type || '').toLowerCase().includes('arthaleads') || c.base_url?.includes('arthaleads'));
    if (connected) setArthaleadsStatus(true, '<span class="section-title">Connection Status</span><p style="margin:12px 0 0;line-height:1.6;">Arthaleads is saved for this workspace. Use refresh to load the latest leads.</p>');
  } catch (_) {}
}

async function testArthaleadsConnection() {
  const session = requireWorkspaceSession();
  if (!session) return;
  const cfg = buildArthaleadsConnectionConfig();
  if (!cfg.credentials.api_key) return showToast('Paste the Arthaleads API key first.', 'error');
  try {
    const res = await fetch(API_BASE + '/tenants/' + encodeURIComponent(session.tenantId) + '/crm-connections/test', { method: 'POST', headers: getAuthHeaders(), body: JSON.stringify(cfg) });
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();
    const sample = data.sample_leads || data.leads || data.preview || [];
    setArthaleadsStatus(true, '<span class="section-title">Connection Test</span><p style="margin:12px 0 0;line-height:1.6;">Connected to Arthaleads. Sample leads found: <strong style="color:#b1ec3e;">' + sample.length + '</strong>.</p>');
    renderCrmPreview(sample);
    showToast('Arthaleads test passed');
  } catch (err) {
    setArthaleadsStatus(false, '<span class="section-title">Connection Test</span><p style="margin:12px 0 0;line-height:1.6;color:var(--red);">Test failed: ' + escapeHtml(String(err.message || err).slice(0, 160)) + '</p>');
    showToast('Arthaleads test failed', 'error');
  }
}

async function connectArthaleadsAndFetchLeads() {
  const session = requireWorkspaceSession();
  if (!session) return;
  const cfg = buildArthaleadsConnectionConfig();
  if (!cfg.credentials.api_key) return showToast('Paste the Arthaleads API key first.', 'error');
  try {
    const res = await fetch(API_BASE + '/tenants/' + encodeURIComponent(session.tenantId) + '/crm-connections', { method: 'POST', headers: getAuthHeaders(), body: JSON.stringify(cfg) });
    if (!res.ok) throw new Error(await res.text());
    setArthaleadsStatus(true, '<span class="section-title">Connection Status</span><p style="margin:12px 0 0;line-height:1.6;">Arthaleads saved. Fetching the latest leads now...</p>');
    await loadLeadsFromArthaleads(true);
  } catch (err) {
    showToast('Could not save Arthaleads CRM: ' + String(err.message || err).slice(0, 120), 'error');
  }
}

function initDashboardPage() {
  loadDashboardStats();
  refreshLiveCalls();
  loadDashboardCharts();
}

// ═══════════════════════════════════════════════════════════
// CALL HISTORY PAGE
// ═══════════════════════════════════════════════════════════
const CH = {
  all: [],          // full unfiltered data
  filtered: [],     // after filter + search
  filter: 'all',
  page: 0,
  pageSize: 25,
};

async function initCallHistoryPage() {
  await loadCallHistory();
}

async function loadCallHistory() {
  const tbody = document.getElementById('ch-tbody');
  if (tbody) tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text-3);padding:40px;">Loading…</td></tr>';

  try {
    const res = await fetch(`${API_BASE}/internal/calls?limit=500`, {
      headers: { 'X-Internal-Token': INTERNAL_TOKEN }
    });
    if (!res.ok) throw new Error(res.status);
    const data = await res.json();
    CH.all = data.calls || [];
  } catch (e) {
    CH.all = [];
  }

  CH.page = 0;
  chApplyFilter();
  chUpdateStats();
}

function chSetFilter(f) {
  CH.filter = f;
  CH.page = 0;
  ['all','completed','failed','active'].forEach(k => {
    const btn = document.getElementById(`ch-tab-${k}`);
    if (!btn) return;
    btn.className = k === f ? 'btn-primary' : 'btn-ghost';
    btn.style.padding = '7px 16px';
    btn.style.fontSize = '11px';
  });
  chApplyFilter();
}

function chApplyFilter() {
  const q = (document.getElementById('ch-search')?.value || '').toLowerCase();
  CH.filtered = CH.all.filter(c => {
    if (CH.filter === 'completed' && !['completed','qualified','not_interested','callback'].includes(c.status || c.outcome)) return false;
    if (CH.filter === 'failed'    && !['failed','no_answer','dropped','timeout','busy'].includes(c.status || c.outcome)) return false;
    if (CH.filter === 'active'    && (c.status || c.outcome) !== 'active') return false;
    if (q) {
      const hay = `${c.phone||''} ${c.lead_name||''} ${c.outcome||''}`.toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });
  chRender();
}

function chRender() {
  const tbody = document.getElementById('ch-tbody');
  if (!tbody) return;

  const start = CH.page * CH.pageSize;
  const slice = CH.filtered.slice(start, start + CH.pageSize);

  if (!slice.length) {
    tbody.innerHTML = `<tr><td colspan="8" style="text-align:center;color:var(--text-3);padding:48px;">No calls found</td></tr>`;
    chUpdatePagination();
    return;
  }

  tbody.innerHTML = slice.map((c, i) => {
    const recUrl = (c.recording_url || '').replace(/^https?:\/\/orchestrator-[^/]+\.up\.railway\.app/, '');
    const sentimentColor = { positive:'#b1ec3e', negative:'#ffb4ab', neutral:'#c3c9b0' };
    const sentiment = c.duration > 60 ? 'positive' : c.duration > 20 ? 'neutral' : 'negative';
    const audioId = `ch-audio-${i}`;
    return `<tr>
      <td><span class="mono" style="color:${cyanColor()};">${c.phone || '—'}</span></td>
      <td style="font-weight:500;">${c.lead_name || '—'}</td>
      <td>${statusChip(c.outcome || c.status)}</td>
      <td><span class="chip chip-slate">Outbound</span></td>
      <td>${formatDuration(c.duration)}</td>
      <td><span style="color:${sentimentColor[sentiment]};font-size:12px;font-weight:600;">${sentiment.charAt(0).toUpperCase()+sentiment.slice(1)}</span></td>
      <td>${recUrl
        ? `<div style="display:flex;align-items:center;gap:6px;">
             <audio id="${audioId}" src="${recUrl}" preload="none" style="display:none;"></audio>
             <button class="btn-ghost" onclick="toggleAudio('${audioId}',this)" style="padding:4px 10px;font-size:11px;gap:4px;">
               <span class="material-symbols-outlined" style="font-size:14px;">play_circle</span><span>Play</span>
             </button>
           </div>`
        : '<span style="color:var(--text-3);font-size:12px;">—</span>'}</td>
      <td style="color:var(--text-3);font-size:12px;">${formatTime(c.created_at)}</td>
    </tr>`;
  }).join('');

  chUpdatePagination();
}

function chUpdateStats() {
  const all   = CH.all;
  const done  = all.filter(c => ['completed','qualified','callback','not_interested'].includes(c.status||c.outcome)).length;
  const fail  = all.filter(c => ['failed','no_answer','dropped','timeout','busy'].includes(c.status||c.outcome)).length;
  const mins  = Math.round(all.reduce((s,c) => s + (c.duration||0), 0) / 60);

  const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  set('ch-total',     all.length);
  set('ch-completed', done);
  set('ch-failed',    fail);
  set('ch-duration',  `${mins}m`);
}

function chUpdatePagination() {
  const total  = CH.filtered.length;
  const pages  = Math.max(1, Math.ceil(total / CH.pageSize));
  const cur    = CH.page + 1;
  const start  = CH.page * CH.pageSize + 1;
  const end    = Math.min(total, start + CH.pageSize - 1);

  const setEl = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  setEl('ch-count-label', total ? `${start}–${end} of ${total} calls` : '0 calls');
  setEl('ch-page-label',  `Page ${cur} / ${pages}`);

  const prev = document.getElementById('ch-prev-btn');
  const next = document.getElementById('ch-next-btn');
  if (prev) prev.disabled = CH.page === 0;
  if (next) next.disabled = CH.page >= pages - 1;
}

function chPrevPage() { if (CH.page > 0) { CH.page--; chRender(); } }
function chNextPage() {
  const pages = Math.ceil(CH.filtered.length / CH.pageSize);
  if (CH.page < pages - 1) { CH.page++; chRender(); }
}

function chExportCSV() {
  if (!CH.filtered.length) { showToast('No data to export', 'error'); return; }
  const header = ['Phone','Lead Name','Status','Channel','Duration (sec)','Time'];
  const rows = CH.filtered.map(c => [
    c.phone||'',
    c.lead_name||'',
    c.outcome||c.status||'',
    'Outbound',
    c.duration||0,
    c.created_at||''
  ]);
  const csv = [header, ...rows].map(r => r.map(v => `"${String(v).replace(/"/g,'""')}"`).join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `call-history-${new Date().toISOString().slice(0,10)}.csv`;
  a.click();
  showToast('Exported CSV');
}

// ═══════════════════════════════════════════════════════════
// KNOWLEDGE BASE PAGE  (localStorage-based)
// ═══════════════════════════════════════════════════════════
const KB = {
  list: [],
  activeKbId: null,
  sourceType: 'text',
  editingSourceId: null,
};

const KB_KEY = 'prophunt_knowledge_bases';
function kbLoad()       { try { return JSON.parse(localStorage.getItem(KB_KEY) || '[]'); } catch { return []; } }
function kbSave(data)   { localStorage.setItem(KB_KEY, JSON.stringify(data)); }
function kbGenId()      { return 'kb_' + Date.now().toString(36) + Math.random().toString(36).slice(2,6); }

// ── Seed Mahindra Citadel on first run ────────────────────
function kbSeedIfNeeded() {
  if (kbLoad().length > 0) return;
  const now = new Date().toISOString();
  const kbId = 'kb_mahindra_citadel';
  const src = (id, name, type, content) => ({ id: kbGenId(), kb_id: kbId, name, type, content, created_at: now });

  const sources = [
    src(1, 'Project Overview', 'text', `PROJECT: Mahindra Citadel — by Mahindra Lifespaces Developers Ltd

LOCATION: Pimpri, Pune, Maharashtra — near PCMC, adjacent to the Mumbai-Pune Expressway. Walking distance to Pimpri railway station.

DEVELOPER: Mahindra Lifespaces — 30+ years of experience. RERA registered.
RERA NUMBER: P52100019279 (Pune)

PROJECT TYPE: Premium integrated residential township — multi-tower high-rise.
TOTAL AREA: ~40 acres of gated township land.
TOWERS: Wings I, J, K (actively selling). Total 2,000+ homes across all wings.
CONFIGURATIONS: 1 BHK, 2 BHK, 2.5 BHK, 3 BHK, 4 BHK apartments.
STATUS: Under construction. Expected possession 2025–2026 (tower-wise).
GREEN CERTIFICATION: IGBC pre-certified. Part of 140-acre integrated township.`),

    src(2, 'Unit Configurations — Wings I, J & K', 'text', `UNIT TYPES AND CARPET AREAS

WING J:
• 1 BHK — Carpet: 401–428 sq ft | Built-up: ~570–610 sq ft
• 2 BHK Compact — Carpet: 592–617 sq ft | Built-up: ~840–875 sq ft
• 2 BHK Standard — Carpet: 667–695 sq ft | Built-up: ~945–985 sq ft
• 3 BHK — Carpet: 866–920 sq ft | Built-up: ~1,230–1,305 sq ft

WING K:
• 2 BHK — Carpet: 601–635 sq ft | Built-up: ~852–900 sq ft
• 2.5 BHK — Carpet: 710–745 sq ft | Built-up: ~1,005–1,055 sq ft
• 3 BHK Premium — Carpet: 892–948 sq ft | Built-up: ~1,265–1,345 sq ft

WING I:
• 2 BHK — Carpet: 582–620 sq ft | Built-up: ~825–880 sq ft
• 3 BHK — Carpet: 855–910 sq ft | Built-up: ~1,210–1,290 sq ft
• 4 BHK Penthouse — Carpet: 1,350–1,480 sq ft | Built-up: ~1,910–2,095 sq ft

FLOOR PLAN FEATURES:
- Living + Dining with natural light
- Modular kitchen with utility area
- Master bedroom with attached bathroom
- Balconies in all units (2+ in 3 & 4 BHK)
- Large windows (floor-to-ceiling in premium units)
- Vitrified tile flooring | Premium fittings in bathrooms`),

    src(3, 'Pricing & Payment Plans', 'text', `PRICING — Mahindra Citadel

BASE PRICE PER SQ FT (carpet area):
• 1 BHK: ₹8,500–₹9,200/sq ft
• 2 BHK: ₹8,800–₹9,500/sq ft
• 2.5 BHK: ₹9,200–₹9,800/sq ft
• 3 BHK: ₹9,500–₹10,200/sq ft
• 4 BHK Penthouse: ₹11,000–₹12,500/sq ft

ALL-INCLUSIVE INDICATIVE PRICES:
• 1 BHK: ₹38–42 Lakhs
• 2 BHK Compact: ₹54–60 Lakhs
• 2 BHK Standard: ₹62–70 Lakhs
• 2.5 BHK: ₹70–78 Lakhs
• 3 BHK: ₹85–98 Lakhs
• 4 BHK Penthouse: ₹1.55–1.85 Crores

ADDITIONAL CHARGES:
• Car Parking: ₹3–4 Lakhs/slot (covered)
• Club Membership: ₹1.5 Lakhs (one-time)
• GST: 5% (under-construction)
• Stamp Duty + Registration: As per Maharashtra govt

PAYMENT OPTIONS:
1. Construction Linked Plan (CLP) — pay at each milestone
2. Down Payment Plan — 95% upfront = 5–6% discount
3. Bank Subvention — bank pays builder; EMI starts post-possession
4. Home Loan: Approved by HDFC, SBI, ICICI, Axis, Kotak`),

    src(4, 'Amenities & Facilities', 'text', `CLUBHOUSE (25,000 sq ft):
• Gymnasium & fitness centre
• Olympic-length swimming pool + Kids pool
• Indoor badminton (2 courts) & squash court
• Billiards room, indoor games (TT, foosball)
• Banquet hall (300+ capacity)
• Mini theatre / screening room
• Library & co-working lounge
• Creche and kids play zone

OUTDOOR:
• Half-basketball court, multi-sport court
• 1.2 km jogging/cycling track
• Amphitheatre (open air)
• Yoga & meditation lawn
• Senior citizen's garden
• Children's play area (safety surface)
• Pet-friendly zone

INFRASTRUCTURE:
• 3-tier security + CCTV
• Video door-phone per apartment
• High-speed elevators (4 per tower)
• 100% power backup (common) + 1 KVA per flat
• Piped gas connection
• EV charging points in parking
• Rainwater harvesting & STP
• Wi-Fi in common areas`),

    src(5, 'Location Advantages', 'text', `ADDRESS: Mahindra Citadel, Old Mumbai-Pune Highway, Pimpri, Pune – 411018

CONNECTIVITY:
• 2 min — Pimpri railway station (Mumbai–Pune line)
• 5 min — Mumbai-Pune Expressway entry
• 12 min — Hinjewadi IT Park Phase 1
• 15 min — Pune Airport (NH 48)
• 20 min — Hinjewadi Phase 3 (Rajiv Gandhi IT Park)
• 10 min — Aundh/Baner business districts
• 30 min — Pune city centre

NEARBY:
• Schools: Orchid School (3 km), DPS Pimpri (4 km)
• Hospitals: Dr. D.Y. Patil (2 km), Aditya Birla Memorial (3 km)
• Shopping: Xion Mall (3 km), D-Mart (1 km), Phoenix Marketcity (8 km)
• Banks: HDFC, SBI, ICICI within 500m

MARKET: Pimpri-Chinchwad — fastest-growing Pune corridor. 10–12% YOY appreciation. Strong IT/ITES and manufacturing demand.`),

    src(6, 'FAQs — Customer Queries', 'qa', `Q: What is the minimum budget to buy at Mahindra Citadel?
A: Starting price is approximately ₹38 Lakhs for a 1 BHK (401 sq ft carpet) in Wing J. Additional charges — parking, club membership, GST, registration — apply on top.

Q: Is the project RERA approved?
A: Yes. RERA number P52100019279, verifiable at maharerait.mahaonline.gov.in.

Q: What is the possession date?
A: Wings J and K target 2025; Wing I targets 2026. Please confirm current status with our sales team as construction milestones may update.

Q: How many floors per tower?
A: Wings J and K are 20-storey towers. Wing I is a premium 24-storey tower with penthouses on the top floors.

Q: Is parking included in the price?
A: No. Covered car parking is ₹3–4 Lakhs per slot. One slot per apartment; additional on first-come basis.

Q: Which banks offer home loans here?
A: Approved by HDFC, SBI, ICICI, Axis, Bank of Baroda, Kotak. Also eligible for PMAY subsidy for eligible buyers.

Q: Is there a subvention scheme?
A: Yes. Bank pays builder during construction; you start EMI only after possession. Terms vary by bank — ask our relationship manager.

Q: What is the maintenance charge?
A: Approximately ₹2–2.5 per sq ft per month (built-up area). For a 2 BHK (~870 sq ft) that's roughly ₹1,750–₹2,200/month covering security, housekeeping, lift, utilities.

Q: Can NRIs buy here?
A: Yes. NRIs/PIOs can purchase under FEMA guidelines via NRE/NRO accounts. Dedicated NRI relationship managers available.

Q: What is the floor rise charge?
A: ₹50–₹100 per sq ft per floor above the 5th floor, depending on wing and unit type. View premiums apply for higher floors in Wing I.

Q: Is there a broker policy?
A: Yes. Channel partners earn 2% brokerage. Pre-register leads with Mahindra Citadel CRM to protect your deal.

Q: What documents are needed for booking?
A: PAN card, Aadhaar, income proof, bank statement (3 months), 2 passport photos. NRIs additionally need passport and visa copy.`),

    src(7, 'Sales Process', 'text', `BOOKING AMOUNTS:
• 1 BHK: ₹1 Lakh
• 2/2.5 BHK: ₹2 Lakhs
• 3 BHK: ₹3 Lakhs
• 4 BHK: ₹5 Lakhs

PROCESS:
1. Booking — cheque / RTGS / NEFT; unit blocked 7 days
2. Agreement — within 30 days; stamp duty paid at registration
3. Payment — as per CLP / DP / Subvention plan
4. Possession — pre-inspection → OC receipt → key handover

CONTACT:
• Sales Office: Old Mumbai-Pune Highway, Pimpri (Mon–Sun, 10AM–7PM)
• Helpline: 1800-102-9498 (toll-free)
• Email: citadel.pune@mahindralifespaces.com

KEY DOCS TO REVIEW:
1. RERA certificate
2. Allotment letter + AFS draft
3. Your specific unit floor plan
4. Master layout plan
5. Builder-buyer agreement penalty clauses`),
  ];

  kbSave([{
    id: kbId,
    name: 'Mahindra Citadel',
    description: 'Pimpri, Pune — floor plans, pricing, amenities, FAQs & sales process for AI agent',
    sources,
    created_at: now,
    source_count: sources.length,
  }]);
}

function initKnowledgeBasePage() {
  kbSeedIfNeeded();
  loadKnowledgeBases();
}

function loadKnowledgeBases() {
  KB.list = kbLoad();
  renderKBGrid();
}

function renderKBGrid() {
  const grid = document.getElementById('kb-grid');
  if (!grid) return;

  if (!KB.list.length) {
    grid.innerHTML = `
      <div style="grid-column:1/-1;text-align:center;padding:60px 20px;">
        <div style="width:60px;height:60px;border-radius:14px;background:var(--surface-2);border:1px dashed var(--border-md);display:flex;align-items:center;justify-content:center;margin:0 auto 16px;">
          <span class="material-symbols-outlined" style="font-size:26px;color:var(--text-3);">library_books</span>
        </div>
        <div style="font-size:16px;font-weight:600;color:var(--text-1);margin-bottom:6px;">No knowledge bases yet</div>
        <div style="font-size:13px;color:var(--text-2);margin-bottom:20px;">Create one and add your project documents, pricing sheets, and FAQs</div>
        <button class="btn-primary" onclick="showKBModal()">
          <span class="material-symbols-outlined" style="font-size:15px;">add</span>
          Create First Knowledge Base
        </button>
      </div>`;
    return;
  }

  grid.innerHTML = KB.list.map(kb => {
    const srcCount = (kb.sources || []).length;
    const desc = kb.description || 'No description';
    return `
    <div class="glass-panel" style="padding:18px;cursor:pointer;transition:border-color 0.15s;" onclick="openKBSources('${kb.id}')"
         onmouseover="this.style.borderColor='var(--accent-border)'" onmouseout="this.style.borderColor='var(--border)'">
      <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:12px;">
        <div style="width:40px;height:40px;border-radius:10px;background:var(--accent-dim);border:1px solid var(--accent-border);display:flex;align-items:center;justify-content:center;">
          <span class="material-symbols-outlined" style="font-size:20px;color:var(--accent);">database</span>
        </div>
        <span class="chip chip-green">${srcCount} source${srcCount !== 1 ? 's' : ''}</span>
      </div>
      <div style="font-size:15px;font-weight:700;color:var(--text-1);margin-bottom:3px;">${kb.name}</div>
      <div style="font-size:12px;color:var(--text-2);margin-bottom:12px;line-height:1.4;">${desc.slice(0,110)}${desc.length>110?'…':''}</div>
      <div style="display:flex;gap:6px;border-top:1px solid var(--border);padding-top:10px;" onclick="event.stopPropagation()">
        <button class="btn-ghost" onclick="openKBSources('${kb.id}')" style="padding:5px 12px;font-size:11px;flex:1;justify-content:center;">
          <span class="material-symbols-outlined" style="font-size:13px;">open_in_new</span> Open
        </button>
        <button class="btn-ghost" onclick="copyKBContext('${kb.id}')" style="padding:5px 10px;font-size:11px;" title="Copy context for agent">
          <span class="material-symbols-outlined" style="font-size:13px;">content_copy</span>
        </button>
        <button class="btn-ghost" onclick="deleteKB('${kb.id}')" style="padding:5px 10px;font-size:11px;border-color:rgba(248,113,113,0.2);color:var(--red);">
          <span class="material-symbols-outlined" style="font-size:13px;">delete</span>
        </button>
      </div>
    </div>`;
  }).join('');
}

function showKBModal() {
  const m = document.getElementById('kb-modal');
  if (m) { m.style.display = 'flex'; }
  setTimeout(() => document.getElementById('kb-name-input')?.focus(), 50);
}
function hideKBModal() {
  const m = document.getElementById('kb-modal');
  if (m) m.style.display = 'none';
  const ni = document.getElementById('kb-name-input');
  const di = document.getElementById('kb-desc-input');
  if (ni) ni.value = '';
  if (di) di.value = '';
}

function createKB() {
  const name = document.getElementById('kb-name-input')?.value?.trim();
  const desc = document.getElementById('kb-desc-input')?.value?.trim();
  if (!name) { showToast('Name is required', 'error'); return; }
  const kbs = kbLoad();
  kbs.push({ id: kbGenId(), name, description: desc || '', sources: [], source_count: 0, created_at: new Date().toISOString() });
  kbSave(kbs);
  showToast('Knowledge base created');
  hideKBModal();
  loadKnowledgeBases();
}

function deleteKB(id) {
  if (!confirm('Delete this knowledge base and all its sources?')) return;
  kbSave(kbLoad().filter(k => k.id !== id));
  if (KB.activeKbId === id) hideKBSources();
  showToast('Deleted');
  loadKnowledgeBases();
}

function openKBSources(kbId) {
  KB.activeKbId = kbId;
  const kb = kbLoad().find(k => k.id === kbId);
  const panel = document.getElementById('kb-sources-panel');
  const title = document.getElementById('kb-sources-title');
  if (panel) panel.style.display = 'block';
  if (title && kb) title.textContent = kb.name;
  panel?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  loadKBSources(kbId);
}

function hideKBSources() {
  const panel = document.getElementById('kb-sources-panel');
  if (panel) panel.style.display = 'none';
  KB.activeKbId = null;
}

function loadKBSources(kbId) {
  const kb = kbLoad().find(k => k.id === kbId);
  renderKBSources(kb?.sources || []);
}

function renderKBSources(sources) {
  const list = document.getElementById('kb-sources-list');
  if (!list) return;
  if (!sources.length) {
    list.innerHTML = `<div style="color:var(--text-2);font-size:13px;text-align:center;padding:32px;">
      No sources yet. Add project documents, pricing sheets, or FAQs.
    </div>`;
    return;
  }
  list.innerHTML = sources.map(s => `
    <div style="display:flex;align-items:flex-start;gap:12px;padding:14px;border-bottom:1px solid var(--border);">
      <div style="width:34px;height:34px;border-radius:8px;background:rgba(34,211,238,0.07);border:1px solid rgba(34,211,238,0.15);display:flex;align-items:center;justify-content:center;flex-shrink:0;">
        <span class="material-symbols-outlined" style="font-size:16px;color:var(--cyan);">${s.type === 'qa' ? 'quiz' : 'article'}</span>
      </div>
      <div style="flex:1;min-width:0;">
        <div style="font-weight:600;font-size:13px;color:var(--text-1);margin-bottom:3px;">${s.name}
          <span class="chip chip-slate" style="margin-left:6px;font-size:9px;">${(s.type||'text').toUpperCase()}</span>
        </div>
        <div style="font-size:11px;color:var(--text-2);line-height:1.55;max-height:52px;overflow:hidden;">${(s.content||'').slice(0,200)}${(s.content||'').length>200?'…':''}</div>
        <div style="font-size:10px;color:var(--text-3);margin-top:4px;">${(s.content||'').length.toLocaleString()} chars · ${s.created_at ? new Date(s.created_at).toLocaleDateString('en-IN') : ''}</div>
      </div>
      <button class="btn-ghost" onclick="editKBSource('${s.id}')" style="padding:4px 8px;flex-shrink:0;margin-right:4px;"><span class="material-symbols-outlined" style="font-size:14px;">edit</span></button>
      <button class="btn-ghost" onclick="deleteKBSource('${s.id}')" style="padding:4px 8px;border-color:rgba(248,113,113,0.2);color:var(--red);flex-shrink:0;">
        <span class="material-symbols-outlined" style="font-size:14px;">delete</span>
      </button>
    </div>`).join('');
}

// Copy full KB context to clipboard (paste into agent system prompt)
function copyKBContext(kbId) {
  const kb = kbLoad().find(k => k.id === kbId);
  if (!kb) return;
  const text = (kb.sources || []).map(s => `## ${s.name}\n${s.content}`).join('\n\n---\n\n');
  navigator.clipboard.writeText(text)
    .then(() => showToast('Context copied — paste into agent system prompt'))
    .catch(() => showToast('Copy failed', 'error'));
}

// Source Modal
function showAddSourceModal() {
  if (!KB.activeKbId) { showToast('Open a knowledge base first', 'error'); return; }
  KB.editingSourceId = null;
  const title = document.getElementById('source-modal-title');
  const saveBtn = document.getElementById('source-modal-save-btn');
  if (title) title.textContent = 'Add Source';
  if (saveBtn) saveBtn.textContent = 'Save Source';
  const m = document.getElementById('source-modal');
  if (m) m.style.display = 'flex';
  kbSourceType('text');
}
function hideSourceModal() {
  KB.editingSourceId = null;
  const title = document.getElementById('source-modal-title');
  const saveBtn = document.getElementById('source-modal-save-btn');
  if (title) title.textContent = 'Add Source';
  if (saveBtn) saveBtn.textContent = 'Save Source';
  const m = document.getElementById('source-modal');
  if (m) m.style.display = 'none';
  ['src-name-input','src-content-input','src-qa-name'].forEach(id => {
    const el = document.getElementById(id); if (el) el.value = '';
  });
  const qp = document.getElementById('qa-pairs');
  if (qp) qp.innerHTML = `<div class="qa-pair" style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;"><input class="input-field qa-q" placeholder="Question"><input class="input-field qa-a" placeholder="Answer"></div>`;
}

function kbSourceType(type) {
  KB.sourceType = type;
  document.getElementById('src-text-area').style.display = type === 'text' ? 'block' : 'none';
  document.getElementById('src-qa-area').style.display   = type === 'qa'   ? 'block' : 'none';
  document.getElementById('src-tab-text').className = type === 'text' ? 'btn-primary' : 'btn-ghost';
  document.getElementById('src-tab-qa').className   = type === 'qa'   ? 'btn-primary' : 'btn-ghost';
  ['src-tab-text','src-tab-qa'].forEach(id => {
    const el = document.getElementById(id);
    if (el) { el.style.padding = '6px 14px'; el.style.fontSize = '11px'; }
  });
}

function addQAPair() {
  const pairs = document.getElementById('qa-pairs');
  if (!pairs) return;
  const div = document.createElement('div');
  div.className = 'qa-pair';
  div.style.cssText = 'display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;';
  div.innerHTML = '<input class="input-field qa-q" placeholder="Question"><input class="input-field qa-a" placeholder="Answer">';
  pairs.appendChild(div);
}

function addKBSource() {
  if (!KB.activeKbId) return;
  let name, content, type;
  if (KB.sourceType === 'text') {
    name    = document.getElementById('src-name-input')?.value?.trim();
    content = document.getElementById('src-content-input')?.value?.trim();
    type    = 'text';
  } else {
    name = document.getElementById('src-qa-name')?.value?.trim();
    const qa = [];
    document.querySelectorAll('.qa-pair').forEach(p => {
      const q = p.querySelector('.qa-q')?.value?.trim();
      const a = p.querySelector('.qa-a')?.value?.trim();
      if (q && a) qa.push(`Q: ${q}\nA: ${a}`);
    });
    content = qa.join('\n\n');
    type    = 'qa';
  }
  if (!name)    { showToast('Source name is required', 'error'); return; }
  if (!content) { showToast('Content cannot be empty', 'error'); return; }

  const kbs = kbLoad();
  const kb = kbs.find(k => k.id === KB.activeKbId);
  if (!kb) return;
  if (!kb.sources) kb.sources = [];

  if (KB.editingSourceId) {
    // Update existing source
    const idx = kb.sources.findIndex(s => s.id === KB.editingSourceId);
    if (idx !== -1) {
      kb.sources[idx] = { ...kb.sources[idx], name, content, type, updated_at: new Date().toISOString() };
    }
    KB.editingSourceId = null;
    showToast('Source updated');
  } else {
    kb.sources.push({ id: kbGenId(), kb_id: KB.activeKbId, name, content, type, created_at: new Date().toISOString() });
    showToast('Source added');
  }
  kb.source_count = kb.sources.length;
  kbSave(kbs);
  hideSourceModal();
  loadKBSources(KB.activeKbId);
  loadKnowledgeBases();
}

// Edit Source — opens the source modal pre-filled with existing data
function editKBSource(sourceId) {
  if (!KB.activeKbId) return;
  const kb = kbLoad().find(k => k.id === KB.activeKbId);
  const src = (kb?.sources || []).find(s => s.id === sourceId);
  if (!src) return;
  KB.editingSourceId = sourceId;
  // Set modal title
  const title = document.getElementById('source-modal-title');
  const saveBtn = document.getElementById('source-modal-save-btn');
  if (title) title.textContent = 'Edit Source';
  if (saveBtn) saveBtn.textContent = 'Save Changes';
  // Pre-fill
  if (src.type === 'qa') {
    kbSourceType('qa');
    const nameEl = document.getElementById('src-qa-name');
    if (nameEl) nameEl.value = src.name;
    // Rebuild QA pairs
    const pairsEl = document.getElementById('qa-pairs');
    if (pairsEl) {
      const lines = (src.content || '').split('\n\n').filter(Boolean);
      pairsEl.innerHTML = lines.map(pair => {
        const q = (pair.match(/^Q:\s*(.+)$/m) || ['',''])[1];
        const a = (pair.match(/^A:\s*(.+)$/m) || ['',''])[1];
        return `<div class="qa-pair" style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;"><input class="input-field qa-q" placeholder="Question" value="${escapeHtml(q)}"><input class="input-field qa-a" placeholder="Answer" value="${escapeHtml(a)}"></div>`;
      }).join('') || `<div class="qa-pair" style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;"><input class="input-field qa-q" placeholder="Question"><input class="input-field qa-a" placeholder="Answer"></div>`;
    }
  } else {
    kbSourceType('text');
    const nameEl = document.getElementById('src-name-input');
    const contentEl = document.getElementById('src-content-input');
    if (nameEl) nameEl.value = src.name;
    if (contentEl) contentEl.value = src.content || '';
  }
  const m = document.getElementById('source-modal');
  if (m) m.style.display = 'flex';
}

function deleteKBSource(sourceId) {
  if (!KB.activeKbId || !confirm('Delete this source?')) return;
  const kbs = kbLoad();
  const kb = kbs.find(k => k.id === KB.activeKbId);
  if (!kb) return;
  kb.sources = (kb.sources || []).filter(s => s.id !== sourceId);
  kb.source_count = kb.sources.length;
  kbSave(kbs);
  showToast('Source deleted');
  loadKBSources(KB.activeKbId);
  loadKnowledgeBases();
}

// ─── Analytics Charts ────────────────────────────────────────
function renderAnalyticsCharts(calls = []) {
  renderSVGBarChart('an-usage-svg', calls, 14);
  renderSVGDonutChart('an-outcomes-svg', 'an-outcomes-legend', calls, 60);
}

// ─── Voice Agent ─────────────────────────────────────────────
function initVoiceAgentPage() {
  // Load settings into the AI Agent config bar
  const s = loadSettings();
  const al = document.getElementById('agent-language');
  const ao = document.getElementById('agent-opening');
  if (al && s.language)    al.value = s.language;
  if (ao && s.openingLine) ao.value = s.openingLine;
  // Populate ElevenLabs voices (replaces hardcoded Sarvam list)
  populateVoiceSelect('agent-voice', s.voice || '');

  // Opening line is optional — only restore if user previously saved one
  // Clear old hardcoded templates that contain legacy brand/agent names
  const co = document.getElementById('call-opening');
  if (co) {
    const saved = s.openingLine || '';
    const isLegacy = /prop.?hunt|priya|ritu|roopa/i.test(saved);
    const isValidOpening = !isLegacy && saved.length >= 20
      && !/^[\w.+%-]+@[\w.-]+\.\w+$/.test(saved.trim())
      && !/^https?:\/\//.test(saved.trim())
      && !/^\+?\d[\d\s().-]{7,}$/.test(saved.trim());
    co.value = isValidOpening ? saved : '';
    if (isLegacy) { s.openingLine = ''; localStorage.setItem('prophunt_settings', JSON.stringify(s)); }
    updateOpeningPreview();
  }

  populateKBSelector();
  initTestCallForm();
  loadRecentCalls();
  initLiveSessions();
}

// ─── Live Sessions Panel ──────────────────────────────────────
let _liveSessionsPollTimer = null;

function initLiveSessions() {
  // Don't double-start
  if (_liveSessionsPollTimer) return;
  refreshLiveSessions();
  _liveSessionsPollTimer = setInterval(refreshLiveSessions, 4000);
}

async function refreshLiveSessions() {
  const container = document.getElementById('live-sessions-list');
  if (!container) { clearInterval(_liveSessionsPollTimer); _liveSessionsPollTimer = null; return; }
  try {
    const res = await fetch(`${ORCH_BASE}/sessions`);
    if (!res.ok) throw new Error('fetch failed');
    const data = await res.json();
    const sessions = (data.sessions || []).filter(s => !s.closed);
    const badge = document.getElementById('live-sessions-badge');
    if (badge) badge.textContent = sessions.length ? `${sessions.length} live` : 'none';

    if (!sessions.length) {
      container.innerHTML = '<div style="color:var(--text-3);font-size:12px;text-align:center;padding:20px 0;">No active calls right now</div>';
      return;
    }
    container.innerHTML = sessions.map(s => {
      const dur = s.started_at ? Math.floor((Date.now() - new Date(s.started_at).getTime()) / 1000) : 0;
      const mm = String(Math.floor(dur / 60)).padStart(2, '0');
      const ss = String(dur % 60).padStart(2, '0');
      const lang = s.language || '—';
      const state = s.state || s.status || 'active';
      const name = s.lead_name || s.phone || '—';
      const sid = (s.call_sid || '').slice(-8);
      return `<div style="display:flex;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid var(--border);">
        <div style="width:8px;height:8px;border-radius:50%;background:#b1ec3e;flex-shrink:0;box-shadow:0 0 6px rgba(177,236,62,0.6);" class="pulse-dot"></div>
        <div style="flex:1;min-width:0;">
          <div style="font-size:12px;font-weight:600;color:var(--text-1);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${name}</div>
          <div style="font-size:10px;color:var(--text-3);">${lang} · ${state} · ${mm}:${ss}</div>
        </div>
        <div style="font-size:10px;color:var(--text-3);font-family:monospace;">${sid}</div>
        <button onclick="viewLiveSession('${s.call_sid}')" class="btn-ghost" style="padding:4px 10px;font-size:10px;">View</button>
      </div>`;
    }).join('');
  } catch {
    // silently ignore — don't thrash UI on network errors
  }
}

async function viewLiveSession(callSid) {
  const panel = document.getElementById('live-session-detail');
  const titleEl = document.getElementById('live-session-title');
  const bodyEl  = document.getElementById('live-session-body');
  if (!panel || !bodyEl) return;
  panel.style.display = 'block';
  if (titleEl) titleEl.textContent = `Session: ${callSid.slice(-10)}`;
  bodyEl.textContent = 'Loading…';
  try {
    const res = await fetch(`${ORCH_BASE}/sessions/${callSid}`);
    if (!res.ok) throw new Error('not found');
    const s = await res.json();
    const dur = s.started_at ? Math.floor((Date.now() - new Date(s.started_at).getTime()) / 1000) : 0;
    const mm = String(Math.floor(dur / 60)).padStart(2, '0');
    const ss2 = String(dur % 60).padStart(2, '0');
    bodyEl.innerHTML = `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px;">
        <div><span style="color:var(--text-3);font-size:10px;">PHONE</span><div style="font-size:12px;color:var(--text-1);">${s.phone || '—'}</div></div>
        <div><span style="color:var(--text-3);font-size:10px;">LEAD</span><div style="font-size:12px;color:var(--text-1);">${s.lead_name || '—'}</div></div>
        <div><span style="color:var(--text-3);font-size:10px;">LANGUAGE</span><div style="font-size:12px;color:var(--cyan);">${s.language || '—'}</div></div>
        <div><span style="color:var(--text-3);font-size:10px;">DURATION</span><div style="font-size:12px;color:var(--accent);">${mm}:${ss2}</div></div>
        <div><span style="color:var(--text-3);font-size:10px;">STATE</span><div style="font-size:12px;color:var(--text-1);">${s.state || s.status || '—'}</div></div>
        <div><span style="color:var(--text-3);font-size:10px;">TURNS</span><div style="font-size:12px;color:var(--text-1);">${s.turn_count ?? '—'}</div></div>
      </div>
      ${s.last_agent_reply ? `<div style="margin-top:6px;"><span style="color:var(--text-3);font-size:10px;">LAST AGENT REPLY</span><div style="margin-top:4px;padding:8px;background:rgba(0,0,0,0.3);border-radius:8px;font-size:11px;color:var(--text-2);font-style:italic;">"${s.last_agent_reply}"</div></div>` : ''}
    `;
  } catch {
    bodyEl.textContent = 'Could not load session detail.';
  }
}

// Populate the KB dropdown on the Voice Agent call form
function populateKBSelector() {
  const sel = document.getElementById('call-kb');
  if (!sel) return;
  const kbs = kbLoad();
  // Rebuild options (keep the "None" first option)
  while (sel.options.length > 1) sel.remove(1);
  kbs.forEach(kb => {
    const opt = document.createElement('option');
    opt.value = kb.id;
    opt.textContent = kb.name + (kb.source_count ? ` (${kb.source_count} sources)` : '');
    sel.appendChild(opt);
  });
  // Auto-select first KB if only one exists
  if (kbs.length === 1) sel.value = kbs[0].id;

  // Show a preview snippet when a KB is selected
  sel.onchange = () => {
    const preview = document.getElementById('call-kb-preview');
    if (!preview) return;
    const kb = kbLoad().find(k => k.id === sel.value);
    if (kb && kb.sources?.length) {
      preview.style.display = 'block';
      const firstSrc = kb.sources[0];
      preview.textContent = `"${firstSrc.name}": ${firstSrc.content.slice(0, 120)}…`;
    } else {
      preview.style.display = 'none';
    }
  };
  // Trigger once to show preview for auto-selected KB
  sel.dispatchEvent(new Event('change'));
}

// Build a flat context string from all sources of a KB
function getKBContext(kbId) {
  if (!kbId) return null;
  const kb = kbLoad().find(k => k.id === kbId);
  if (!kb || !kb.sources?.length) return null;
  const lines = [`# Knowledge Base: ${kb.name}`];
  if (kb.description) lines.push(kb.description + '\n');
  kb.sources.forEach(s => {
    lines.push(`## ${s.name}`);
    lines.push(s.content);
    lines.push('');
  });
  return lines.join('\n');
}

function initAnalyticsPage() {
  loadAnalytics();
}

// ═══════════════════════════════════════════════════════════
// SHARED SVG CHART HELPERS
// ═══════════════════════════════════════════════════════════

function renderSVGBarChart(svgId, calls, days) {
  const svg = document.getElementById(svgId);
  if (!svg) return;
  const now = new Date();
  const buckets = [];
  for (let i = days - 1; i >= 0; i--) {
    const d = new Date(now);
    d.setDate(d.getDate() - i);
    const key = d.toISOString().slice(0, 10);
    const label = days <= 7
      ? d.toLocaleDateString('en', { weekday: 'short' })
      : (i === 0 ? 'Today' : d.getDate() + '/' + (d.getMonth() + 1));
    buckets.push({ key, label: i === 0 ? 'Today' : label, count: 0 });
  }
  calls.forEach(c => {
    if (!c.created_at) return;
    const b = buckets.find(x => x.key === c.created_at.slice(0, 10));
    if (b) b.count++;
  });
  const maxCount = Math.max(...buckets.map(b => b.count), 1);

  // Wide viewBox so SVG scales horizontally; fixed height avoids vertical stretch
  const VW = days <= 7 ? 280 : 500;
  const VH = 90;
  const padB = 18;
  const chartH = VH - padB;
  const slotW = VW / buckets.length;
  const barW  = slotW * 0.52;
  const barX  = i => i * slotW + slotW * 0.24;

  const showEvery = days <= 10 ? 1 : Math.ceil(days / 10);

  const parts = buckets.map((b, i) => {
    // Max bar fills 60% of chart height — leaves headroom and looks proportional
    const barH = Math.max((b.count / maxCount) * chartH * 0.60, b.count > 0 ? 2 : 0);
    const y    = chartH - barH;
    const color = b.count > 0 ? '#b1ec3e' : 'rgba(255,255,255,0.05)';
    const showLbl = i % showEvery === 0 || i === buckets.length - 1;
    const lbl  = showLbl ? `<text x="${barX(i)+barW/2}" y="${VH-2}" text-anchor="middle" font-size="7" fill="#666" font-family="Inter,sans-serif">${b.label}</text>` : '';
    const cnt  = b.count > 0 ? `<text x="${barX(i)+barW/2}" y="${y-3}" text-anchor="middle" font-size="7" fill="#b1ec3e" font-family="Inter,sans-serif" font-weight="600">${b.count}</text>` : '';
    return `<rect x="${barX(i)}" y="${y}" width="${barW}" height="${barH}" rx="2" fill="${color}" opacity="0.9"/>` + cnt + lbl;
  });

  svg.setAttribute('viewBox', `0 0 ${VW} ${VH}`);
  svg.setAttribute('preserveAspectRatio', 'none');
  svg.innerHTML = parts.join('') +
    `<line x1="0" y1="${chartH}" x2="${VW}" y2="${chartH}" stroke="rgba(255,255,255,0.07)" stroke-width="0.5"/>`;
}

function renderSVGDonutChart(svgId, legendId, calls, svgSize) {
  const svgEl = document.getElementById(svgId);
  const legendEl = document.getElementById(legendId);
  if (!svgEl) return;
  const total = calls.length;
  if (!total) {
    svgEl.innerHTML = `<text x="${svgSize / 2}" y="${svgSize / 2 + 5}" text-anchor="middle" font-size="10" fill="#888" font-family="Inter,sans-serif">No data</text>`;
    if (legendEl) legendEl.innerHTML = '';
    return;
  }
  const segments = [
    { label: 'Completed', color: '#b1ec3e', count: calls.filter(c => ['completed','qualified','not_interested','callback'].includes(c.outcome || c.status)).length },
    { label: 'Failed', color: '#ffb4ab', count: calls.filter(c => ['failed','no_answer','dropped','timeout','busy'].includes(c.outcome || c.status)).length },
    { label: 'Active', color: 'var(--cyan)', count: calls.filter(c => (c.outcome || c.status) === 'active').length },
  ];
  const r = svgSize * 0.4, ri = svgSize * 0.26, cx = svgSize / 2, cy = svgSize / 2;
  let startA = -Math.PI / 2;
  const paths = segments.filter(s => s.count > 0).map(s => {
    const a = (s.count / (total || 1)) * Math.PI * 2;
    const ea = startA + a;
    const p = (angle) => [cx + r * Math.cos(angle), cy + r * Math.sin(angle)];
    const q = (angle) => [cx + ri * Math.cos(angle), cy + ri * Math.sin(angle)];
    const [x1, y1] = p(startA), [x2, y2] = p(ea);
    const [xi1, yi1] = q(startA), [xi2, yi2] = q(ea);
    const large = a > Math.PI ? 1 : 0;
    const d = `M ${x1} ${y1} A ${r} ${r} 0 ${large} 1 ${x2} ${y2} L ${xi2} ${yi2} A ${ri} ${ri} 0 ${large} 0 ${xi1} ${yi1} Z`;
    startA = ea;
    return `<path d="${d}" fill="${s.color}" opacity="0.9"/>`;
  });
  svgEl.innerHTML = paths.join('') +
    `<text x="${cx}" y="${cy - 5}" text-anchor="middle" font-size="${svgSize * 0.12}" font-weight="700" fill="#d3e4fe" font-family="Inter,sans-serif">${total}</text>` +
    `<text x="${cx}" y="${cy + svgSize * 0.12}" text-anchor="middle" font-size="${svgSize * 0.065}" fill="#888" font-family="Inter,sans-serif">TOTAL</text>`;
  if (legendEl) {
    legendEl.innerHTML = segments.map(s =>
      `<div style="display:flex;align-items:center;justify-content:space-between;">
        <div style="display:flex;align-items:center;gap:6px;">
          <div style="width:10px;height:10px;border-radius:2px;background:${s.color};flex-shrink:0;"></div>
          <span style="color:var(--text-2);">${s.label}</span>
        </div>
        <span style="color:var(--text-1);font-weight:600;">${s.count}</span>
      </div>`
    ).join('');
  }
}

// ═══════════════════════════════════════════════════════════
// DASHBOARD CHARTS
// ═══════════════════════════════════════════════════════════

const dashState = { range: '7d', calls: [] };

async function loadDashboardCharts() {
  try {
    const res = await fetch(`${API_BASE}/internal/calls?limit=500`, {
      headers: { 'X-Internal-Token': INTERNAL_TOKEN }
    });
    if (!res.ok) return;
    const data = await res.json();
    dashState.calls = data.calls || [];
  } catch (_) { dashState.calls = []; }
  renderUsageChart();
  renderOutcomesChart();
  updateExtraStats();
}

function updateExtraStats() {
  const calls = dashState.calls;
  const totalSec = calls.reduce((s, c) => s + (c.duration || 0), 0);
  const totalMins = Math.round(totalSec / 60);
  const connected = calls.filter(c => (c.duration || 0) > 10).length;
  const qualified = calls.filter(c => (c.outcome || '') === 'qualified').length;
  const successRate = calls.length ? Math.round((connected / calls.length) * 100) : null;
  const convRate    = calls.length ? Math.round((qualified / calls.length) * 100) : null;
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  set('stat-total-mins',   totalMins + 'm');
  set('stat-success-rate', successRate !== null ? successRate + '%' : '—');
  set('stat-conversion',   convRate    !== null ? convRate    + '%' : '—');
}

function setDashRange(range) {
  dashState.range = range;
  ['7d', '30d'].forEach(r => {
    const btn = document.getElementById('dr-' + r);
    if (!btn) return;
    btn.className = r === range ? 'chip chip-green' : 'chip chip-slate';
    btn.style.cssText = 'cursor:pointer;border:none;padding:4px 12px;font-size:10px;';
  });
  renderUsageChart();
}

function renderUsageChart() {
  const days = dashState.range === '30d' ? 30 : 7;
  renderSVGBarChart('usage-chart-svg', dashState.calls, days);
}

function renderOutcomesChart() {
  renderSVGDonutChart('outcomes-svg', 'outcomes-legend', dashState.calls, 180);
}

// ═══════════════════════════════════════════════════════════
// AGENTS PAGE
// ═══════════════════════════════════════════════════════════

const AGENTS_KEY = 'prophunt_agents';
let agentEditId = null;

function loadAgents() {
  try { return JSON.parse(localStorage.getItem(AGENTS_KEY) || '[]'); } catch { return []; }
}

function saveAgentsData(agents) {
  localStorage.setItem(AGENTS_KEY, JSON.stringify(agents));
}

function initAgentsPage() {
  // Remove the hardcoded default agent if it's the only one and has never been customised
  const agents = loadAgents();
  if (agents.length === 1 && agents[0].id === 'default' &&
      agents[0].systemPrompt?.includes('Key behaviors:')) {
    saveAgentsData([]);
  }
  renderAgentsList();
}

function renderAgentsList() {
  const container = document.getElementById('agents-list');
  if (!container) return;
  const agents = loadAgents();

  if (!agents.length) {
    container.innerHTML = `
      <div style="text-align:center;padding:64px 24px;color:var(--text-3);">
        <span class="material-symbols-outlined" style="font-size:48px;margin-bottom:16px;display:block;opacity:0.4;">smart_toy</span>
        <div style="font-size:16px;font-weight:600;color:var(--text-2);margin-bottom:8px;">No agents yet</div>
        <div style="font-size:13px;margin-bottom:20px;">Create your first AI agent to start making calls.</div>
        <button class="btn-primary" onclick="addNewAgent()">
          <span class="material-symbols-outlined" style="font-size:15px;">add</span>
          New Agent
        </button>
      </div>`;
    return;
  }

  container.innerHTML = agents.map(agent => {
    const isActive = agent.status === 'active';
    return `
    <div class="glass-panel" style="border-radius:24px;padding:28px;">
      <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:20px;">
        <div style="display:flex;align-items:center;gap:14px;">
          <div style="width:52px;height:52px;border-radius:16px;background:${isActive ? 'rgba(177,236,62,0.10)' : 'rgba(255,255,255,0.04)'};border:1px solid ${isActive ? 'rgba(177,236,62,0.25)' : 'rgba(255,255,255,0.08)'};display:flex;align-items:center;justify-content:center;">
            <span class="material-symbols-outlined" style="font-size:24px;color:${isActive ? '#b1ec3e' : '#8d947c'};">smart_toy</span>
          </div>
          <div>
            <div style="font-family:'Space Grotesk',sans-serif;font-size:18px;font-weight:700;color:var(--text-1);">${escapeHtml(agent.name)}</div>
            <div style="font-size:12px;color:var(--text-3);margin-top:2px;">Voice: ${escapeHtml(resolveVoiceName(agent.voice))} · Language: ${escapeHtml(agent.language)}</div>
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:8px;">
          <span class="chip ${isActive ? 'chip-green' : 'chip-slate'}">${isActive ? 'Active' : 'Inactive'}</span>
          <button class="btn-ghost" onclick="editAgent('${agent.id}')" style="padding:8px 14px;">
            <span class="material-symbols-outlined" style="font-size:16px;">edit</span>
            Configure
          </button>
        </div>
      </div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px;">
        <div style="background:rgba(255,255,255,0.03);border-radius:12px;padding:12px 14px;">
          <div class="section-title" style="margin-bottom:4px;">Voice</div>
          <div style="font-size:13px;color:var(--text-1);font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="${escapeHtml(agent.voice)}">${escapeHtml(resolveVoiceName(agent.voice))}</div>
        </div>
        <div style="background:rgba(255,255,255,0.03);border-radius:12px;padding:12px 14px;">
          <div class="section-title" style="margin-bottom:4px;">Language</div>
          <div style="font-size:13px;color:var(--text-1);font-weight:500;">${escapeHtml(agent.language)}
            <span style="font-size:11px;color:var(--text-3);display:block;margin-top:2px;">${agent.langStrictness === 'pure-hindi' ? 'Pure Hindi' : agent.langStrictness === 'hinglish' ? 'Hinglish' : 'Auto'}</span>
          </div>
        </div>
        <div style="background:rgba(255,255,255,0.03);border-radius:12px;padding:12px 14px;">
          <div class="section-title" style="margin-bottom:4px;">Pitch Tone</div>
          <div style="font-size:13px;color:${agent.pitchTone === 'aggressive' ? '#f59e0b' : agent.pitchTone === 'consultative' ? '#22d3ee' : '#b1ec3e'};font-weight:500;">
            ${agent.pitchTone === 'aggressive' ? '🔥 Aggressive' : agent.pitchTone === 'consultative' ? '🤝 Consultative' : '⚖️ Balanced'}
            <span style="font-size:11px;color:var(--text-3);display:block;margin-top:2px;">${agent.wordCap || 30} word cap</span>
          </div>
        </div>
        <div style="background:rgba(255,255,255,0.03);border-radius:12px;padding:12px 14px;">
          <div class="section-title" style="margin-bottom:4px;">Knowledge Base</div>
          <div style="font-size:13px;color:${agent.kbId ? '#b1ec3e' : '#8d947c'};font-weight:500;">${agent.kbId ? '✓ Assigned' : 'None'}
            <span style="font-size:11px;color:var(--text-3);display:block;margin-top:2px;">Calls: ${agent.callCount || 0}</span>
          </div>
        </div>
      </div>
      <div style="background:rgba(0,0,0,0.25);border-radius:14px;padding:14px;margin-bottom:16px;">
        <div class="section-title" style="margin-bottom:6px;">System Prompt Preview</div>
        <div style="font-size:12px;color:var(--text-2);line-height:1.65;max-height:70px;overflow:hidden;">${escapeHtml((agent.systemPrompt || 'No system prompt configured.').slice(0, 300))}</div>
      </div>
      <div style="display:flex;gap:10px;">
        <button class="btn-primary" style="padding:9px 18px;" onclick="editAgent('${agent.id}')">
          <span class="material-symbols-outlined" style="font-size:15px;">edit</span>
          Configure
        </button>
        <button class="btn-ghost" style="padding:9px 18px;" onclick="duplicateAgent('${agent.id}')">
          <span class="material-symbols-outlined" style="font-size:15px;">content_copy</span>
          Duplicate
        </button>
        ${agents.length > 1 ? `<button class="btn-ghost" style="padding:9px 18px;border-color:rgba(255,100,100,0.2);color:var(--red);margin-left:auto;" onclick="deleteAgent('${agent.id}')">
          <span class="material-symbols-outlined" style="font-size:15px;">delete</span>
        </button>` : ''}
      </div>
    </div>`;
  }).join('');
}

// ── System prompt generator (frontend is the source of truth) ────────────
function generateAgentSystemPrompt(cfg = {}) {
  const sess           = getCurrentSession();
  const companyName    = cfg.companyName || sess?.tenantName || localStorage.getItem('prophunt_tenant_name') || 'our company';
  const agentName      = cfg.agentName   || cfg.name?.split('—')[0]?.trim() || 'Maya';
  const language       = cfg.language       || 'Hindi';
  const pitchTone      = cfg.pitchTone      || 'balanced';
  const langStrictness = cfg.langStrictness || 'pure-hindi';
  const wordCap        = parseInt(cfg.wordCap || 30, 10);
  const escalationLine = cfg.escalationLine ||
    'Iske liye main aapko hamare sales expert se connect karti hoon jo bilkul sahi detail de sakenge.';

  const langMap = { Multilingual:'auto', Hindi:'hi', English:'en', Marathi:'mr', Tamil:'ta', Telugu:'te', Bengali:'bn', Kannada:'kn', Gujarati:'gu', Punjabi:'pa' };
  const lang = langMap[language] || 'hi';

  let langInstruction;
  if (lang === 'en') {
    langInstruction = "Mirror the lead's language exactly — if they speak Hindi, reply in Hindi; if English, reply in English.";
  } else {
    let extra = '';
    if (lang === 'hi') {
      if (langStrictness === 'pure-hindi')
        extra = ' Use pure conversational Hindi — avoid English words where Hindi exists (e.g. "kimat" not "price", "jagah" not "location", "kamre" not "rooms"). Spell out all abbreviations — no Rs, sq.ft, BHK.';
      else if (langStrictness === 'hinglish')
        extra = ' Speak natural Hinglish — Hindi sentences, but English brand names, BHK, EMI, price, site visit are fine.';
    }
    langInstruction = `CRITICAL LANGUAGE RULE — OVERRIDES EVERYTHING: The lead speaks ${language}. Reply ONLY in ${language} for every message — greetings, goodbyes, follow-ups. NEVER use even one English word. If you reply in English, that is a critical failure.${extra}`;
  }

  const pitchBlock = {
    aggressive:
`SALES PHILOSOPHY — AGGRESSIVE CLOSER:
Push confidently toward a site visit every single turn.
- After giving any info: "Main abhi 30-minute visit arrange kar sakti hoon — aaj ya kal theek rahega?"
- After FIRST soft refusal ("sochna hai" / "baad mein"): persist once — "Bina dekhe decision lena mushkil hota hai — ek 20-min visit mein sab clear ho jayega. Kaisa rahega?"
- After SECOND refusal: close warmly and end the call.
OBJECTION SCRIPTS:
• "Budget tight hai" → "EMI option bhi available hai — exact figure bata sakti hoon"
• "Sochna hai" → "Slots limited hain — tentative book kar lein, cancel karna free hai"
• "Abhi time nahi" → "Weekend mein bhi 20-minute visit ho sakti hai"`,
    balanced:
`SALES PITCH — 3-STEP FLOW:
STEP 1 — ANSWER & DISCOVER: Answer fully from KB. Ask ONE focused discovery question (BHK / budget / purpose).
STEP 2 — BUILD VALUE: Share BHK layout, price, key USPs. Use natural urgency: "Limited inventory" / "Launch price hai — baad mein daam badhenge."
STEP 3 — INVITE SITE VISIT: After covering BHK + price — "Ek baar personally dekhenge toh sab clear ho jayega — model flat, views, amenities live. Main 30-minute visit arrange kar sakti hoon, kya is weekend free hain?"
After ONE soft refusal: gently re-ask once. After second refusal: close warmly.`,
    consultative:
`SALES APPROACH — TRUSTED ADVISOR:
Understand the lead's needs first — purpose (investment/self-use), budget, BHK, timeline.
Answer all questions honestly and completely from the KB.
Invite site visit ONLY when the lead signals genuine interest (asks price, possession, or visiting).
NEVER mention site visit more than once if they hesitate.
If not interested: "Koi pressure nahi — kabhi bhi humse contact kar saktein hain."
Build trust; a good experience today leads to a referral tomorrow.`
  }[pitchTone] || '';

  const toneStyle = pitchTone === 'aggressive'
    ? 'Confident and urgent — every turn moves toward a booking.'
    : pitchTone === 'consultative'
    ? 'Warm and patient — build trust first, never pressure.'
    : 'Warm and natural — balance helpful information with gentle sales momentum.';

  return `You are ${agentName}, a friendly real estate consultant calling on behalf of ${companyName}.

{{KNOWLEDGE_BASE}}

LEAD INFO:
- Name: {{LEAD_NAME}}
- Project Interest: {{PROJECT_NAME}}
- Budget: {{LEAD_BUDGET}}

${langInstruction}

${pitchBlock}

RULES:
1. ONLY ANSWER THE LATEST MESSAGE — conversation history is context only. Never re-answer earlier questions.
2. LISTEN FIRST — answer the lead's question completely BEFORE asking your own.
3. Use the KB to answer ALL project questions: price, size, location, amenities, RERA, possession date, parking. Give real answers — never deflect.
4. If genuinely not in KB: "${escalationLine}" — do NOT use for simple affirmations like "haan", "ok", "theek hai".
5. NEVER pitch site visit mid-answer — complete the full answer FIRST, then invite as a separate sentence at the end.
6. STRICT LENGTH: 1-2 sentences maximum. Hard cap of ${wordCap} words. No long speeches, no lists.
7. ANTI-REPETITION: Never open with "Dhanyawaad / Shukriya / Aapka shukriya" mid-call. If lead says "theek hai / ok / accha" — ask a follow-up, don't thank them.
8. Never repeat your introduction after the first greeting.
9. If asked if AI: say you're calling from the developer's sales team.
10. QUALIFY before closing: note lead's BHK preference, budget range, purpose (investment/self-use), and timeline.

CONVERSATION STYLE: ${toneStyle}

Return this JSON silently when closing:
OUTCOME:{"status":"interested","site_visit":false,"callback_date":null,"qualification":{"bhk":"","budget_range":"","purpose":"","timeline":""},"notes":""}`;
}

function regenerateAgentPrompt() {
  const get = id => document.getElementById(id)?.value || '';
  const prompt = generateAgentSystemPrompt({
    name:           get('ag-name'),
    language:       get('ag-language'),
    pitchTone:      get('ag-pitch-tone'),
    langStrictness: get('ag-lang-strictness'),
    wordCap:        get('ag-word-cap'),
    escalationLine: get('ag-escalation-line'),
  });
  const ta = document.getElementById('ag-system-prompt');
  if (ta) { ta.value = prompt; ta.style.color = 'var(--accent)'; setTimeout(() => ta.style.color = '', 600); }
}

function resolveVoiceName(voiceId) {
  if (!voiceId) return 'Not set';
  if (_cachedELVoices) {
    const v = _cachedELVoices.find(v => v.voice_id === voiceId);
    if (v) return v.name;
  }
  // Fallback for old Sarvam names or short IDs
  if (voiceId.length < 30) return voiceId;
  return voiceId.slice(0, 10) + '…';
}

// ── ElevenLabs voice loader ────────────────────────────────────────────────
const ORCH_DIRECT = 'https://orchestrator-production-7c9d.up.railway.app';

async function fetchElevenLabsVoices() {
  if (_cachedELVoices) return _cachedELVoices;
  // Try Vercel rewrite first; fallback to Railway directly (CORS enabled on orchestrator)
  let res = await fetch(`${ORCH_BASE}/voices`);
  if (!res.ok) res = await fetch(`${ORCH_DIRECT}/voices`);
  const data = await res.json();
  _cachedELVoices = data.voices || [];
  return _cachedELVoices;
}

// Populate any <select> element with ElevenLabs voices
async function populateVoiceSelect(selectId, selectedVoiceId = '', labelId = null) {
  const sel = document.getElementById(selectId);
  const lbl = labelId ? document.getElementById(labelId) : null;
  if (!sel) return;
  if (lbl) lbl.textContent = '(loading…)';

  try {
    const voices = await fetchElevenLabsVoices();
    const female = voices.filter(v => v.gender === 'female');
    const male   = voices.filter(v => v.gender === 'male');
    const other  = voices.filter(v => v.gender !== 'female' && v.gender !== 'male');

    sel.innerHTML = '';
    const addGroup = (label, list) => {
      if (!list.length) return;
      const grp = document.createElement('optgroup');
      grp.label = label;
      list.forEach(v => {
        const opt = document.createElement('option');
        opt.value = v.voice_id;
        const lang = v.language ? ` · ${v.language}` : '';
        opt.textContent = `${v.name}${lang}`;
        if (v.voice_id === selectedVoiceId) opt.selected = true;
        grp.appendChild(opt);
      });
      sel.appendChild(grp);
    };
    addGroup('── Female Voices ──', female);
    addGroup('── Male Voices ──', male);
    if (other.length) addGroup('── Other ──', other);
    if (lbl) lbl.textContent = `(${voices.length} voices)`;
  } catch (e) {
    if (lbl) lbl.textContent = '(failed to load)';
    sel.innerHTML = `<option value="${selectedVoiceId}">${selectedVoiceId || 'Unknown voice'}</option>`;
  }
}

// Wrapper kept for backward-compat (Configure Agent modal uses this)
async function loadElevenLabsVoices(selectedVoiceId = '') {
  return populateVoiceSelect('ag-voice', selectedVoiceId, 'ag-voice-loading');
}

function addNewAgent() {
  agentEditId = null;
  ['ag-name','ag-first-message','ag-system-prompt','ag-escalation-line'].forEach(id => {
    const el = document.getElementById(id); if (el) el.value = '';
  });
  const set = (id, val) => { const el = document.getElementById(id); if (el) el.value = val; };
  set('ag-status', 'active');
  set('ag-language', 'Hindi');
  set('ag-pitch-tone', 'balanced');
  set('ag-lang-strictness', 'pure-hindi');
  set('ag-word-cap', '30');
  // Default escalation line
  const escEl = document.getElementById('ag-escalation-line');
  if (escEl && !escEl.value) escEl.value = 'Iske liye main aapko hamare sales expert se connect karti hoon jo bilkul sahi detail de sakenge.';
  populateAgentKBDropdown('');
  loadElevenLabsVoices('');
  const modal = document.getElementById('agent-modal');
  if (modal) modal.style.display = 'flex';
  // Auto-generate prompt after voices settle
  setTimeout(() => {
    document.getElementById('ag-name')?.focus();
    regenerateAgentPrompt();
  }, 80);
}

function editAgent(id) {
  const agents = loadAgents();
  const agent = agents.find(a => a.id === id);
  if (!agent) return;
  agentEditId = id;
  const set = (elId, val) => { const el = document.getElementById(elId); if (el) el.value = val || ''; };
  set('ag-name', agent.name);
  set('ag-status', agent.status || 'active');
  set('ag-language', agent.language);
  set('ag-first-message', agent.firstMessage);
  set('ag-system-prompt', agent.systemPrompt || '');
  set('ag-pitch-tone', agent.pitchTone || 'balanced');
  set('ag-lang-strictness', agent.langStrictness || 'pure-hindi');
  set('ag-word-cap', String(agent.wordCap || 30));
  set('ag-escalation-line', agent.escalationLine || 'Iske liye main aapko hamare sales expert se connect karti hoon jo bilkul sahi detail de sakenge.');
  populateAgentKBDropdown(agent.kbId || '');
  // Load ElevenLabs voices and pre-select saved voice
  loadElevenLabsVoices(agent.voice || '');
  const modal = document.getElementById('agent-modal');
  if (modal) modal.style.display = 'flex';
  // If saved prompt is the old default (pre-generator), replace with fresh generated one
  setTimeout(() => {
    const ta = document.getElementById('ag-system-prompt');
    const isOldDefault = ta?.value && (
      ta.value.includes('working for Prophunt') ||
      ta.value.includes('Key behaviors:') ||
      !ta.value.includes('{{KNOWLEDGE_BASE}}')
    );
    if (!ta?.value.trim() || isOldDefault) regenerateAgentPrompt();
  }, 80);
}

function populateAgentKBDropdown(selectedId) {
  const select = document.getElementById('ag-kb');
  if (!select) return;
  // Seed Mahindra Citadel KB if user hasn't visited KB page yet
  if (typeof kbSeedIfNeeded === 'function') kbSeedIfNeeded();
  // Always read fresh from storage (KB.list only set if KB page was visited)
  const kbs = (typeof kbLoad === 'function' ? kbLoad() : KB.list) || [];
  select.innerHTML = '<option value="">No knowledge base</option>' +
    kbs.map(kb => `<option value="${kb.id}"${kb.id === selectedId ? ' selected' : ''}>${escapeHtml(kb.name)}</option>`).join('');
}

function hideAgentModal() {
  const modal = document.getElementById('agent-modal');
  if (modal) modal.style.display = 'none';
  agentEditId = null;
}

function saveAgent() {
  const name = document.getElementById('ag-name')?.value.trim();
  if (!name) { showToast('Agent name is required', 'error'); return; }
  // Auto-generate system prompt if empty
  const ta = document.getElementById('ag-system-prompt');
  if (ta && !ta.value.trim()) regenerateAgentPrompt();
  const agentData = {
    id: agentEditId || ('agent-' + Date.now()),
    name,
    status:          document.getElementById('ag-status')?.value          || 'active',
    voice:           document.getElementById('ag-voice')?.value           || '',
    language:        document.getElementById('ag-language')?.value        || 'Hindi',
    pitchTone:       document.getElementById('ag-pitch-tone')?.value      || 'balanced',
    langStrictness:  document.getElementById('ag-lang-strictness')?.value || 'pure-hindi',
    wordCap:         parseInt(document.getElementById('ag-word-cap')?.value || '30', 10),
    escalationLine:  document.getElementById('ag-escalation-line')?.value.trim() ||
                     'Iske liye main aapko hamare sales expert se connect karti hoon jo bilkul sahi detail de sakenge.',
    kbId:            document.getElementById('ag-kb')?.value              || '',
    firstMessage:    document.getElementById('ag-first-message')?.value.trim() || '',
    systemPrompt:    document.getElementById('ag-system-prompt')?.value.trim() || '',
    callCount: 0,
  };
  let agents = loadAgents();
  if (agentEditId) {
    const idx = agents.findIndex(a => a.id === agentEditId);
    if (idx !== -1) { agentData.callCount = agents[idx].callCount || 0; agents[idx] = agentData; }
    else agents.push(agentData);
  } else {
    agents.push(agentData);
  }
  saveAgentsData(agents);
  hideAgentModal();
  renderAgentsList();
  showToast('Agent saved');
  // Sync active agent settings to global settings
  const activeAgent = agents.find(a => a.status === 'active') || agents[0];
  if (activeAgent) {
    const s = loadSettings();
    s.voice = activeAgent.voice;
    s.language = activeAgent.language;
    s.openingLine = activeAgent.firstMessage;
    localStorage.setItem('prophunt_settings', JSON.stringify(s));
  }
}

function duplicateAgent(id) {
  const agents = loadAgents();
  const agent = agents.find(a => a.id === id);
  if (!agent) return;
  const copy = { ...agent, id: 'agent-' + Date.now(), name: agent.name + ' (Copy)', status: 'inactive', callCount: 0 };
  agents.push(copy);
  saveAgentsData(agents);
  renderAgentsList();
  showToast('Agent duplicated');
}

function deleteAgent(id) {
  if (!confirm('Delete this agent?')) return;
  let agents = loadAgents();
  agents = agents.filter(a => a.id !== id);
  saveAgentsData(agents);
  renderAgentsList();
  showToast('Agent deleted');
}

// ═══════════════════════════════════════════════════════════
// CONTACTS PAGE
// ═══════════════════════════════════════════════════════════

const CONTACTS_KEY = 'prophunt_contacts';
let contactEditId = null;
let contactDetailId = null;
let allCallsForContacts = [];

function loadContactsData() {
  try { return JSON.parse(localStorage.getItem(CONTACTS_KEY) || '[]'); } catch { return []; }
}

function saveContactsData(contacts) {
  localStorage.setItem(CONTACTS_KEY, JSON.stringify(contacts));
}

async function initContactsPage() {
  await loadContacts();
}

async function loadContacts() {
  try {
    const res = await fetch(`${API_BASE}/internal/calls?limit=500`, {
      headers: { 'X-Internal-Token': INTERNAL_TOKEN }
    });
    if (res.ok) {
      const data = await res.json();
      allCallsForContacts = data.calls || [];
    }
  } catch (_) { allCallsForContacts = []; }
  renderContacts();
}

function renderContacts() {
  const contacts = loadContactsData();
  const tbody = document.getElementById('contacts-tbody');
  const search = (document.getElementById('contacts-search')?.value || '').toLowerCase();
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  set('contacts-total', contacts.length);
  const calledNums = new Set(allCallsForContacts.map(c => c.phone || ''));
  set('contacts-called', contacts.filter(c => calledNums.has(c.phone)).length);
  if (!tbody) return;
  const filtered = contacts.filter(c =>
    !search || (c.name + c.phone + (c.tags || '')).toLowerCase().includes(search)
  );
  if (!filtered.length) {
    tbody.innerHTML = `<tr><td colspan="6" style="text-align:center;color:var(--text-3);padding:40px;">${contacts.length ? 'No contacts match the search.' : 'No contacts yet. Add your first contact.'}</td></tr>`;
    return;
  }
  tbody.innerHTML = filtered.map(c => {
    const contactCalls = allCallsForContacts.filter(call => call.phone === c.phone);
    const lastCall = [...contactCalls].sort((a, b) => new Date(b.created_at) - new Date(a.created_at))[0];
    const tags = (c.tags || '').split(',').filter(Boolean)
      .map(t => `<span class="chip chip-slate" style="font-size:10px;padding:2px 8px;">${escapeHtml(t.trim())}</span>`).join(' ');
    return `<tr>
      <td>
        <div style="font-weight:600;color:var(--text-1);">${escapeHtml(c.name)}</div>
        ${c.email ? `<div style="font-size:11px;color:var(--text-3);">${escapeHtml(c.email)}</div>` : ''}
      </td>
      <td class="mono" style="color:${cyanColor()};">${escapeHtml(c.phone)}</td>
      <td>${tags || '<span style="color:var(--text-3);font-size:12px;">—</span>'}</td>
      <td style="color:var(--text-3);font-size:12px;">${lastCall ? formatTime(lastCall.created_at) : '—'}</td>
      <td style="font-weight:600;color:${contactCalls.length ? '#b1ec3e' : '#8d947c'};">${contactCalls.length}</td>
      <td>
        <div style="display:flex;gap:5px;flex-wrap:wrap;">
          <button class="btn-ghost" onclick="showContactDetail('${c.id}')" style="padding:5px 9px;" title="Call history">
            <span class="material-symbols-outlined" style="font-size:14px;">history</span>
          </button>
          <button class="btn-ghost" onclick="editContact('${c.id}')" style="padding:5px 9px;" title="Edit">
            <span class="material-symbols-outlined" style="font-size:14px;">edit</span>
          </button>
          <button class="btn-primary" onclick="callContact('${escapeHtml(c.phone)}','${escapeHtml(c.name)}')" style="padding:5px 10px;font-size:10px;" title="Call now">
            <span class="material-symbols-outlined" style="font-size:13px;">call</span>
          </button>
          <button class="btn-ghost" onclick="deleteContact('${c.id}')" style="padding:5px 9px;border-color:rgba(255,100,100,0.2);color:var(--red);" title="Delete">
            <span class="material-symbols-outlined" style="font-size:14px;">delete</span>
          </button>
        </div>
      </td>
    </tr>`;
  }).join('');
}

function showContactModal() {
  contactEditId = null;
  ['contact-name-input','contact-phone-input','contact-email-input','contact-tags-input','contact-notes-input'].forEach(id => {
    const el = document.getElementById(id); if (el) el.value = '';
  });
  const idEl = document.getElementById('contact-edit-id');
  if (idEl) idEl.value = '';
  const t = document.getElementById('contact-modal-title');
  if (t) t.textContent = 'Add Contact';
  const modal = document.getElementById('contact-modal');
  if (modal) modal.style.display = 'flex';
  setTimeout(() => document.getElementById('contact-name-input')?.focus(), 50);
}

function editContact(id) {
  const contacts = loadContactsData();
  const c = contacts.find(x => x.id === id);
  if (!c) return;
  contactEditId = id;
  const set = (elId, val) => { const el = document.getElementById(elId); if (el) el.value = val || ''; };
  set('contact-edit-id', id);
  set('contact-name-input', c.name);
  set('contact-phone-input', c.phone);
  set('contact-email-input', c.email);
  set('contact-tags-input', c.tags);
  set('contact-notes-input', c.notes);
  const t = document.getElementById('contact-modal-title');
  if (t) t.textContent = 'Edit Contact';
  const modal = document.getElementById('contact-modal');
  if (modal) modal.style.display = 'flex';
}

function hideContactModal() {
  const modal = document.getElementById('contact-modal');
  if (modal) modal.style.display = 'none';
  contactEditId = null;
}

function saveContact() {
  const name = document.getElementById('contact-name-input')?.value.trim();
  const phone = document.getElementById('contact-phone-input')?.value.trim();
  if (!name) { showToast('Name is required', 'error'); return; }
  if (!phone) { showToast('Phone is required', 'error'); return; }
  const contactData = {
    id: contactEditId || ('contact-' + Date.now()),
    name, phone,
    email: document.getElementById('contact-email-input')?.value.trim() || '',
    tags: document.getElementById('contact-tags-input')?.value.trim() || '',
    notes: document.getElementById('contact-notes-input')?.value.trim() || '',
    createdAt: new Date().toISOString(),
  };
  let contacts = loadContactsData();
  if (contactEditId) {
    const idx = contacts.findIndex(c => c.id === contactEditId);
    if (idx !== -1) { contactData.createdAt = contacts[idx].createdAt; contacts[idx] = contactData; }
    else contacts.push(contactData);
  } else {
    contacts.push(contactData);
  }
  saveContactsData(contacts);
  hideContactModal();
  renderContacts();
  showToast('Contact saved');
}

function deleteContact(id) {
  if (!confirm('Delete this contact?')) return;
  let contacts = loadContactsData();
  contacts = contacts.filter(c => c.id !== id);
  saveContactsData(contacts);
  renderContacts();
  if (contactDetailId === id) hideContactDetail();
  showToast('Contact deleted');
}

function callContact(phone, name) {
  navigate('voice-agent');
  setTimeout(() => {
    const pe = document.getElementById('call-phone');
    const ne = document.getElementById('call-name');
    if (pe) pe.value = phone;
    if (ne) ne.value = name;
  }, 150);
}

function showContactDetail(id) {
  contactDetailId = id;
  const contacts = loadContactsData();
  const c = contacts.find(x => x.id === id);
  if (!c) return;
  const panel = document.getElementById('contact-detail-panel');
  const nameEl = document.getElementById('contact-detail-name');
  const phoneEl = document.getElementById('contact-detail-phone');
  if (nameEl) nameEl.textContent = c.name;
  if (phoneEl) phoneEl.textContent = c.phone;
  // Wire up call button
  const callBtn = document.getElementById('contact-detail-call-btn');
  if (callBtn) callBtn.setAttribute('onclick', `callContact('${escapeHtml(c.phone)}','${escapeHtml(c.name)}')`);
  const histDiv = document.getElementById('contact-call-history');
  if (histDiv) {
    const calls = [...allCallsForContacts]
      .filter(x => x.phone === c.phone)
      .sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
    if (!calls.length) {
      histDiv.innerHTML = '<div style="color:var(--text-3);font-size:13px;text-align:center;padding:20px;">No calls recorded for this contact yet.</div>';
    } else {
      histDiv.innerHTML = calls.slice(0, 20).map(call =>
        `<div style="display:flex;align-items:center;gap:12px;padding:12px 0;border-bottom:1px solid var(--border);">
          ${statusChip(call.outcome || call.status)}
          <span style="font-size:13px;color:var(--text-2);">${formatDuration(call.duration)}</span>
          <span style="font-size:12px;color:var(--text-3);margin-left:auto;">${formatTime(call.created_at)}</span>
        </div>`
      ).join('');
    }
  }
  if (panel) {
    panel.style.display = 'block';
    panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
}

function hideContactDetail() {
  const panel = document.getElementById('contact-detail-panel');
  if (panel) panel.style.display = 'none';
  contactDetailId = null;
}

// ═══════════════════════════════════════════════════════════
// PHONE NUMBERS PAGE
// ═══════════════════════════════════════════════════════════

function initPhoneNumbersPage() {
  const orchUrl = document.getElementById('webhook-url-display')?.textContent
    || 'https://orchestrator-production-7c9d.up.railway.app';
  const pnWebhook = document.getElementById('pn-webhook-url');
  if (pnWebhook) pnWebhook.textContent = orchUrl + '/events';

  const storedPhone = localStorage.getItem('prophunt_phone_number') || '';
  const pnNum = document.getElementById('pn-number');
  const pnInput = document.getElementById('pn-number-input');
  if (pnNum) {
    pnNum.textContent = storedPhone || 'Not configured';
    pnNum.style.color = storedPhone ? '#b1ec3e' : '#8d947c';
    pnNum.style.fontSize = storedPhone ? '' : '14px';
  }
  if (pnInput && storedPhone) pnInput.value = storedPhone;

  const agniChip = document.getElementById('agni-status-chip');
  if (agniChip) {
    const hasAgni = !!(localStorage.getItem('prophunt_agni_key') || '');
    agniChip.className = hasAgni ? 'chip chip-green' : 'chip chip-slate';
    agniChip.textContent = hasAgni ? 'Configured' : 'Not Configured';
  }
}

function savePNNumber(val) {
  localStorage.setItem('prophunt_phone_number', val.trim());
  const pnNum = document.getElementById('pn-number');
  if (pnNum) {
    pnNum.textContent = val.trim() || 'Not configured';
    pnNum.style.color = val.trim() ? '#b1ec3e' : '#8d947c';
    pnNum.style.fontSize = val.trim() ? '' : '14px';
  }
}

// ═══════════════════════════════════════════════════════════
// INBOUND PAGE
// ═══════════════════════════════════════════════════════════

function initInboundPage() {
  const orchUrl = document.getElementById('webhook-url-display')?.textContent
    || 'https://orchestrator-production-7c9d.up.railway.app';
  const el = document.getElementById('inbound-webhook-url');
  if (el) el.textContent = orchUrl + '/events/inbound';

  const select = document.getElementById('inbound-agent-select');
  if (select) {
    const agents = loadAgents();
    select.innerHTML = '<option value="default">Default AI Agent</option>' +
      agents.filter(a => a.id !== 'default')
        .map(a => `<option value="${a.id}">${escapeHtml(a.name)}</option>`).join('');
  }

  const cfg = (() => { try { return JSON.parse(localStorage.getItem('prophunt_inbound_config') || '{}'); } catch { return {}; } })();
  if (cfg.agentId) { const s = document.getElementById('inbound-agent-select'); if (s) s.value = cfg.agentId; }
  if (cfg.language) { const l = document.getElementById('inbound-language'); if (l) l.value = cfg.language; }
  if (cfg.opening) { const o = document.getElementById('inbound-opening'); if (o) o.value = cfg.opening; }

  refreshInboundCalls();
}

function saveInboundConfig() {
  const cfg = {
    agentId: document.getElementById('inbound-agent-select')?.value || 'default',
    language: document.getElementById('inbound-language')?.value || 'Hindi',
    opening: document.getElementById('inbound-opening')?.value.trim() || '',
  };
  localStorage.setItem('prophunt_inbound_config', JSON.stringify(cfg));
  showToast('Inbound config saved');
}

async function refreshInboundCalls() {
  const container = document.getElementById('inbound-calls-list');
  if (!container) return;
  try {
    const res = await fetch(`${ORCH_BASE}/sessions`, { signal: AbortSignal.timeout(5000) });
    if (!res.ok) throw new Error();
    const data = await res.json();
    const sessions = data.sessions || (Array.isArray(data) ? data : []);
    const inbound = sessions.filter(s => s.direction === 'inbound' && !s.closed);
    if (!inbound.length) {
      container.innerHTML = '<div style="color:var(--text-3);font-size:13px;text-align:center;padding:32px;border:1px dashed rgba(255,255,255,0.12);border-radius:16px;">No active inbound calls. Incoming calls will appear here in real time.</div>';
      return;
    }
    container.innerHTML = inbound.map(s =>
      `<div style="display:flex;align-items:center;gap:12px;padding:14px;background:rgba(93,230,255,0.04);border:1px solid rgba(93,230,255,0.15);border-radius:16px;margin-bottom:8px;">
        <div class="pulse-dot" style="width:8px;height:8px;border-radius:50%;background:var(--cyan);flex-shrink:0;"></div>
        <div style="flex:1;">
          <div style="font-size:13px;font-weight:600;color:var(--cyan);">${escapeHtml(s.phone || 'Unknown')}</div>
          <div style="font-size:11px;color:var(--text-3);">SID: ${escapeHtml(s.callSid || s.id || '—')}</div>
        </div>
        <span class="chip chip-cyan">Live</span>
      </div>`
    ).join('');
  } catch (_) {
    container.innerHTML = '<div style="color:var(--text-3);font-size:13px;text-align:center;padding:20px;">Could not fetch live sessions.</div>';
  }
}

// ─── Integrations Page ──────────────────────────────────────
const INT_KEY = 'prophunt_integrations';
function intLoad() { try { return JSON.parse(localStorage.getItem(INT_KEY) || '{}'); } catch { return {}; } }
function intSave(d) { localStorage.setItem(INT_KEY, JSON.stringify(d)); }

function initIntegrationsPage() {
  const cfg = intLoad();
  // Google Calendar status
  if (cfg.gcal?.connected) {
    const chip = document.getElementById('gcal-status-chip');
    const val  = document.getElementById('gcal-status-val');
    const calId= document.getElementById('gcal-cal-id');
    const btn  = document.getElementById('gcal-connect-btn');
    if (chip) { chip.className = 'chip chip-green'; chip.textContent = 'Connected'; }
    if (val)  val.textContent  = 'Active';
    if (calId) calId.textContent = cfg.gcal.calendarId || '—';
    if (btn)  { btn.textContent = ''; btn.innerHTML = '<span class="material-symbols-outlined" style="font-size:15px;">link_off</span> Disconnect'; btn.onclick = disconnectGoogleCalendar; }
  }
  // Count connected
  let count = 0;
  if (cfg.gcal?.connected)  count++;
  if (cfg.mscal?.connected) count++;
  const el = document.getElementById('int-connected-count');
  if (el) el.textContent = count;
  // Last import time
  const lastImport = localStorage.getItem('prophunt_last_import');
  const li = document.getElementById('artha-last-import');
  if (li && lastImport) li.textContent = new Date(lastImport).toLocaleString('en-IN');
}

function connectGoogleCalendar() {
  const m = document.getElementById('gcal-modal');
  if (m) m.style.display = 'flex';
  const cfg = intLoad();
  if (cfg.gcal?.calendarId) {
    const el = document.getElementById('gcal-input-id');
    if (el) el.value = cfg.gcal.calendarId;
  }
}

function saveGoogleCalendar() {
  const calendarId = document.getElementById('gcal-input-id')?.value?.trim();
  const clientId   = document.getElementById('gcal-input-client')?.value?.trim();
  const duration   = document.getElementById('gcal-input-duration')?.value || '60';
  if (!calendarId) { showToast('Calendar ID is required', 'error'); return; }
  const cfg = intLoad();
  cfg.gcal = { connected: true, calendarId, clientId, duration, connectedAt: new Date().toISOString() };
  intSave(cfg);
  document.getElementById('gcal-modal').style.display = 'none';
  showToast('Google Calendar connected ✓');
  initIntegrationsPage();
}

function disconnectGoogleCalendar() {
  if (!confirm('Disconnect Google Calendar?')) return;
  const cfg = intLoad();
  delete cfg.gcal;
  intSave(cfg);
  showToast('Google Calendar disconnected');
  initIntegrationsPage();
}

function connectMsCalendar() {
  showToast('Microsoft Calendar integration coming soon', 'info');
}

// Book site visit to calendar
function bookSiteVisit({ leadName, phone, project, preferredDate, notes }) {
  const cfg = intLoad();
  if (!cfg.gcal?.connected) {
    showToast('Connect Google Calendar first (Integrations page)', 'error');
    return;
  }
  // Build Google Calendar event URL
  const title = encodeURIComponent(`Site Visit — ${project} — ${leadName}`);
  const details = encodeURIComponent(`Lead: ${leadName}\nPhone: ${phone}\nProject: ${project}\n${notes ? 'Notes: ' + notes : ''}`);
  const start = preferredDate ? new Date(preferredDate) : new Date(Date.now() + 24*60*60*1000);
  const end = new Date(start.getTime() + (cfg.gcal.duration || 60) * 60 * 1000);
  const fmt = d => d.toISOString().replace(/[-:]/g,'').split('.')[0] + 'Z';
  const url = `https://calendar.google.com/calendar/render?action=TEMPLATE&text=${title}&dates=${fmt(start)}/${fmt(end)}&details=${details}&add=${encodeURIComponent(cfg.gcal.calendarId || '')}`;
  window.open(url, '_blank');
  showToast('Opening Google Calendar to book site visit');
}

// ─── Boot ─────────────────────────────────────────────────────
(function boot() {
  restoreTheme();
  bindAuthForms();

  document.querySelectorAll('#sidebar-nav .nav-item[data-view]').forEach(el => {
    el.addEventListener('click', e => {
      e.preventDefault();
      navigate(el.getAttribute('data-view'));
    });
  });

  const session = getAuthSession();
  if (!hasWorkspaceGateSession()) {
    clearAuthSession();
    showAuth('login');
  } else if (session.accessToken && session.tenantId) {
    // Restore the page the user was on — hash takes priority, then localStorage, then dashboard
    state.activeView = getInitialView();
    openWorkspace();
  } else {
    clearAuthSession();
    showAuth('login');
  }
})();
