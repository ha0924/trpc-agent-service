// 设计依据：docs/技术设计方案.md §4.2 Admin API

package web

import "time"

func dayAgo() time.Time { return time.Now().Add(-24 * time.Hour) }

// consoleHTML is the whole console: one file, no build step, no CDN.
//
// A single template rather than a bundled front-end because the console's job
// is to demonstrate and operate the platform, not to be a product. A build
// pipeline would add a second toolchain to the repo and a network dependency
// to a page whose entire value is being available when things are broken.
const consoleHTML = `<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Agent 平台控制台</title>
<style>
  :root {
    --bg: #0f1117; --panel: #171a21; --line: #262b36;
    --fg: #e6e8ec; --dim: #8b929f; --accent: #4c8dff;
    --ok: #3fb950; --warn: #d29922; --err: #f85149;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--fg);
    font: 14px/1.6 -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif;
  }
  header {
    padding: 14px 20px; border-bottom: 1px solid var(--line);
    display: flex; align-items: center; gap: 16px;
  }
  header h1 { margin: 0; font-size: 15px; font-weight: 600; }
  header .tag {
    font-size: 12px; color: var(--dim); border: 1px solid var(--line);
    padding: 2px 8px; border-radius: 10px;
  }
  nav { margin-left: auto; display: flex; gap: 4px; }
  nav button {
    background: none; border: 1px solid transparent; color: var(--dim);
    padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: 13px;
  }
  nav button.on { color: var(--fg); background: var(--panel); border-color: var(--line); }
  main { padding: 20px; max-width: 1180px; margin: 0 auto; }
  .view { display: none; }
  .view.on { display: block; }

  .row { display: flex; gap: 16px; flex-wrap: wrap; }
  .card {
    background: var(--panel); border: 1px solid var(--line);
    border-radius: 10px; padding: 16px; flex: 1; min-width: 280px;
  }
  .card h2 {
    margin: 0 0 12px; font-size: 13px; font-weight: 600; color: var(--dim);
    text-transform: uppercase; letter-spacing: .06em;
  }

  label { display: block; font-size: 12px; color: var(--dim); margin: 10px 0 4px; }
  input, select, textarea {
    width: 100%; background: #0d0f14; color: var(--fg);
    border: 1px solid var(--line); border-radius: 6px; padding: 8px 10px;
    font: inherit;
  }
  input:focus, select:focus, textarea:focus { outline: none; border-color: var(--accent); }
  button.go {
    margin-top: 12px; background: var(--accent); color: #fff; border: none;
    border-radius: 6px; padding: 9px 18px; cursor: pointer; font: inherit;
  }
  button.go:disabled { opacity: .5; cursor: default; }

  #chat {
    height: 46vh; overflow-y: auto; background: #0d0f14;
    border: 1px solid var(--line); border-radius: 8px; padding: 14px;
  }
  .msg { margin-bottom: 14px; }
  .msg .who { font-size: 11px; color: var(--dim); margin-bottom: 3px; }
  .msg .body { white-space: pre-wrap; word-break: break-word; }
  .msg.user .body { color: #a5d6ff; }
  .msg.tool .body { color: var(--warn); font-size: 13px; }
  .msg.sys .body { color: var(--dim); font-style: italic; }

  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td {
    text-align: left; padding: 7px 10px; border-bottom: 1px solid var(--line);
    vertical-align: top;
  }
  th { color: var(--dim); font-weight: 500; font-size: 12px; }
  code {
    font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace;
    color: var(--dim);
  }
  .pill {
    display: inline-block; font-size: 11px; padding: 1px 7px;
    border-radius: 9px; border: 1px solid var(--line);
  }
  .pill.ok { color: var(--ok); border-color: #1f4526; }
  .pill.warn { color: var(--warn); border-color: #4a3a11; }
  .pill.err { color: var(--err); border-color: #5c1f1c; }
  .hint { color: var(--dim); font-size: 12px; margin-top: 10px; }
  .empty { color: var(--dim); padding: 12px 0; }
</style>
</head>
<body>

<header>
  <h1>多租户节点化 Agent 平台</h1>
  <span class="tag">控制台</span>
  <nav>
    <button class="on" data-view="chat">对话</button>
    <button data-view="admin">租户与配置</button>
    <button data-view="ops">运维</button>
  </nav>
</header>

<main>

<!-- ======================= 对话 ======================= -->
<section class="view on" id="view-chat">
  <div class="row">
    <div class="card" style="flex:0 0 300px">
      <h2>发送目标</h2>
      <label>Webhook 路径</label>
      <select id="hook"></select>
      <label>通道令牌</label>
      <input id="token" value="mock-token-abc">
      <label>用户标识</label>
      <input id="user" value="console-user">
      <label>群组标识（留空为单聊）</label>
      <input id="group" placeholder="team-1">
      <p class="hint">
        本页直接调用真实的 webhook 入口，不走任何私有捷径——
        因此走的是验签、幂等、ACK、队列、租约、装配、投递的完整生产路径。
        回复是异步推送的，页面通过轮询会话事件取回。
      </p>
    </div>

    <div class="card">
      <h2>会话</h2>
      <div id="chat"><div class="empty">发一条消息开始。</div></div>
      <label>消息</label>
      <textarea id="text" rows="3">帮我算 1234 乘以 5678</textarea>
      <button class="go" id="send">发送</button>
      <span class="hint" id="status"></span>
    </div>
  </div>
</section>

<!-- ======================= 租户与配置 ======================= -->
<section class="view" id="view-admin">
  <div id="tenants"><div class="empty">加载中…</div></div>
</section>

<!-- ======================= 运维 ======================= -->
<section class="view" id="view-ops">
  <div class="row">
    <div class="card" style="flex:0 0 320px">
      <h2>灰度与回滚</h2>
      <label>租户</label><input id="dTenant" value="tenant-demo">
      <label>Agent</label><input id="dAgent" value="assistant">
      <label>权重（版本=权重，逗号分隔）</label>
      <input id="dRoutes" value="v1=90,v2=10">
      <button class="go" id="applyRoutes">应用</button>
      <p class="hint">
        灰度与回滚是同一个接口：往新版本挪权重是灰度，挪回去是回滚，
        都是一次单行原子更新。进行中的会话不受影响——每个会话在创建时
        就固化了版本。
      </p>
      <div id="dResult"></div>
    </div>

    <div class="card">
      <h2>会话与审计</h2>
      <label>租户</label><input id="oTenant" value="tenant-demo">
      <button class="go" id="loadOps">查询</button>
      <div id="opsResult"></div>
    </div>
  </div>
</section>

</main>

<script>
const $ = s => document.querySelector(s);
const esc = s => String(s ?? '').replace(/[&<>"]/g, c =>
  ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

// ---- 视图切换 ----
document.querySelectorAll('nav button').forEach(b => b.onclick = () => {
  document.querySelectorAll('nav button').forEach(x => x.classList.remove('on'));
  document.querySelectorAll('.view').forEach(x => x.classList.remove('on'));
  b.classList.add('on');
  $('#view-' + b.dataset.view).classList.add('on');
  if (b.dataset.view === 'admin') loadTenants();
});

// ---- 对话 ----
let sessionID = null, tenantID = null;

async function loadHooks() {
  const sel = $('#hook');
  try {
    const { tenants } = await (await fetch('/console/api/overview')).json();
    for (const t of tenants || []) {
      for (const b of t.bindings || []) {
        if (b.status !== 'active' || !b.webhook_path) continue;
        const o = document.createElement('option');
        o.value = b.webhook_path;
        o.dataset.tenant = t.tenant_id;
        o.textContent = b.webhook_path + '  (' + t.tenant_id + ' / ' + b.channel + ')';
        sel.appendChild(o);
      }
    }
  } catch (e) { /* 页面在后端不可用时仍应可见 */ }
  if (!sel.options.length) {
    const o = document.createElement('option');
    o.value = '/webhook/mock/demo'; o.dataset.tenant = 'tenant-demo';
    o.textContent = '/webhook/mock/demo';
    sel.appendChild(o);
  }
}

function render(events) {
  const box = $('#chat');
  if (!events.length) { box.innerHTML = '<div class="empty">暂无消息。</div>'; return; }
  box.innerHTML = events.map(e => {
    if (e.event_type === 'tool_call') {
      return '<div class="msg tool"><div class="who">调用工具</div>' +
             '<div class="body">' + esc(e.tool) + '</div></div>';
    }
    const isUser = e.event_type === 'user_message';
    return '<div class="msg ' + (isUser ? 'user' : '') + '">' +
           '<div class="who">' + (isUser ? '用户' : 'Agent') + ' · #' + e.sequence + '</div>' +
           '<div class="body">' + esc(e.text) + '</div></div>';
  }).join('');
  box.scrollTop = box.scrollHeight;
}

async function poll(requestID, beforeCount) {
  // Poll for at most two minutes. A real model call can take tens of
  // seconds; the inbound state is checked alongside so a request that
  // already failed stops the loop instead of spinning to the deadline.
  const deadline = Date.now() + 120000;
  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 1500));

    try {
      const { events } = await (await fetch(
        '/console/api/sessions/' + sessionID + '/events?tenant=' + tenantID)).json();
      render(events || []);
      const replies = (events || []).filter(e => e.event_type === 'agent_message');
      if (replies.length > beforeCount) { $('#status').textContent = ''; return; }
    } catch (e) { /* keep polling */ }

    try {
      const st = await (await fetch('/console/api/requests/' + requestID + '/state')).json();
      if (st.state === 'failed') {
        $('#status').innerHTML = '<span class="pill err">执行失败</span> ' + esc(st.error || '');
        return;
      }
      if (st.state === 'delivery_failed') {
        // Not terminal: the sweeper retries the delivery, so the reply may
        // still land. Say so rather than declaring failure.
        $('#status').innerHTML = '<span class="pill warn">投递失败，等待重投</span>';
      }
    } catch (e) { /* state unknown, keep polling */ }
  }
  $('#status').innerHTML = '<span class="pill warn">等待超时</span>';
}

$('#send').onclick = async () => {
  const text = $('#text').value.trim();
  if (!text) return;

  const opt = $('#hook').selectedOptions[0];
  tenantID = opt.dataset.tenant;

  const before = (await currentReplies()) ;
  $('#send').disabled = true;
  $('#status').innerHTML = '<span class="pill">已受理，Agent 执行中…</span>';

  const body = { event_id: 'console-' + Date.now(), user_id: $('#user').value, text };
  const group = $('#group').value.trim();
  if (group) body.group_id = group;

  try {
    const resp = await fetch(opt.value, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Mock-Token': $('#token').value },
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!resp.ok) {
      $('#status').innerHTML = '<span class="pill err">HTTP ' + resp.status + '</span> ' +
                               esc(data.error || '');
      return;
    }
    sessionID = data.session_id;
    $('#text').value = '';
    await poll(data.request_id, before);
  } catch (e) {
    $('#status').innerHTML = '<span class="pill err">' + esc(e.message) + '</span>';
  } finally {
    $('#send').disabled = false;
  }
};

async function currentReplies() {
  if (!sessionID || !tenantID) return 0;
  try {
    const { events } = await (await fetch(
      '/console/api/sessions/' + sessionID + '/events?tenant=' + tenantID)).json();
    return (events || []).filter(e => e.event_type === 'agent_message').length;
  } catch (e) { return 0; }
}

// ---- 租户与配置 ----
async function loadTenants() {
  const box = $('#tenants');
  try {
    const { tenants } = await (await fetch('/console/api/overview')).json();
    box.innerHTML = (tenants || []).map(t => {
      const agents = (t.agents || []).map(a =>
        '<code>' + esc(a.agent_app_id) + '</code> ' + esc(a.name)).join('<br>') || '—';
      const binds = (t.bindings || []).map(b =>
        '<tr><td><code>' + esc(b.channel_binding_id) + '</code></td>' +
        '<td>' + esc(b.channel) + '</td>' +
        '<td><code>' + esc(b.webhook_path) + '</code></td>' +
        '<td>' + (b.capabilities?.max_text_length ?? '—') + '</td>' +
        '<td><span class="pill ' + (b.status === 'active' ? 'ok' : '') + '">' +
        esc(b.status) + '</span></td></tr>').join('') ||
        '<tr><td colspan="5" class="empty">无</td></tr>';
      const u = t.usage || {};
      return '<div class="card" style="margin-bottom:16px">' +
        '<h2>' + esc(t.tenant_id) + ' · ' + esc(t.name) +
        ' <span class="pill ' + (t.status === 'active' ? 'ok' : 'err') + '">' +
        esc(t.status) + '</span></h2>' +
        '<div class="row">' +
          '<div style="flex:0 0 220px"><label>Agent</label>' + agents + '</div>' +
          '<div style="flex:0 0 200px"><label>近 24h 用量</label>' +
            (u.requests || 0) + ' 次 · ' + (u.total_tokens || 0) + ' tokens · $' +
            (u.cost_usd || 0).toFixed(6) + '</div>' +
          '<div style="flex:1"><label>通道绑定</label>' +
            '<table><tr><th>绑定</th><th>通道</th><th>Webhook</th><th>长度上限</th><th>状态</th></tr>' +
            binds + '</table></div>' +
        '</div></div>';
    }).join('') || '<div class="empty">无租户。</div>';
  } catch (e) {
    box.innerHTML = '<div class="empty">加载失败：' + esc(e.message) + '</div>';
  }
}

// ---- 运维 ----
$('#applyRoutes').onclick = async () => {
  const routes = $('#dRoutes').value.split(',').map(s => {
    const [version, weight] = s.split('=').map(x => x.trim());
    return { version, weight: parseInt(weight, 10) };
  }).filter(r => r.version && !isNaN(r.weight));

  const url = '/admin/tenants/' + $('#dTenant').value +
              '/agents/' + $('#dAgent').value + '/deployment';
  try {
    const resp = await fetch(url, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ routes, updated_by: 'console' }),
    });
    const data = await resp.json();
    $('#dResult').innerHTML = resp.ok
      ? '<p class="hint"><span class="pill ok">已生效</span> ' +
        esc(JSON.stringify(data.deployment.routes)) + '<br>' + esc(data.note) + '</p>'
      : '<p class="hint"><span class="pill err">被拒绝</span> ' + esc(data.error) + '</p>';
  } catch (e) {
    $('#dResult').innerHTML = '<p class="hint"><span class="pill err">' + esc(e.message) + '</span></p>';
  }
};

$('#loadOps').onclick = async () => {
  const t = $('#oTenant').value;
  const box = $('#opsResult');
  try {
    const [s, a] = await Promise.all([
      (await fetch('/admin/tenants/' + t + '/sessions?limit=10')).json(),
      (await fetch('/admin/tenants/' + t + '/audit?limit=10')).json(),
    ]);
    const sessions = (s.sessions || []).map(x =>
      '<tr><td><code>' + esc(x.session_id.slice(0, 18)) + '…</code></td>' +
      '<td>' + esc(x.agent_version) + '</td><td>' + esc(x.scope) + '</td>' +
      '<td>' + esc(x.scope_key) + '</td><td>' + x.last_sequence + '</td>' +
      '<td><a href="#" onclick="showDead(\'' + esc(x.session_id) +
      '\');return false">死信</a></td></tr>').join('') ||
      '<tr><td colspan="6" class="empty">无</td></tr>';
    const audit = (a.records || []).map(x =>
      '<tr><td>' + esc(x.event_type) + '</td>' +
      '<td><span class="pill ' + (x.decision === 'allow' ? 'ok' :
        x.decision === 'deny' ? 'err' : 'warn') + '">' + esc(x.decision) + '</span></td>' +
      '<td>' + esc(x.tool_name || '—') + '</td><td>' + (x.latency_ms || 0) + 'ms</td>' +
      '<td><code>' + esc((x.trace_id || '').slice(0, 12)) + '</code></td></tr>').join('') ||
      '<tr><td colspan="5" class="empty">无</td></tr>';

    box.innerHTML =
      '<label>最近会话</label><table><tr><th>会话</th><th>版本</th><th>范围</th>' +
      '<th>标识</th><th>事件数</th><th></th></tr>' + sessions + '</table>' +
      '<label style="margin-top:14px">最近审计</label><table><tr><th>类型</th>' +
      '<th>判定</th><th>工具</th><th>耗时</th><th>trace</th></tr>' + audit + '</table>' +
      '<div id="dead"></div>';
  } catch (e) {
    box.innerHTML = '<div class="empty">加载失败：' + esc(e.message) + '</div>';
  }
};

async function showDead(session) {
  const box = document.getElementById('dead');
  try {
    const d = await (await fetch('/admin/sessions/' + session + '/deadletters?limit=5')).json();
    const rows = (d.dead_letters || []).map(x =>
      '<tr><td>' + x.attempts + '</td><td>' + esc((x.last_error || '').slice(0, 90)) +
      '</td><td>' + esc(x.message?.text || '') + '</td></tr>').join('') ||
      '<tr><td colspan="3" class="empty">该会话无死信</td></tr>';
    box.innerHTML = '<label style="margin-top:14px">死信（' + (d.total || 0) + '）' +
      ' <button class="go" style="margin:0;padding:3px 10px;font-size:12px"' +
      ' onclick="replayDead(\'' + esc(session) + '\')">重放一条</button></label>' +
      '<table><tr><th>尝试</th><th>失败原因</th><th>消息</th></tr>' + rows + '</table>';
  } catch (e) {
    box.innerHTML = '<div class="empty">' + esc(e.message) + '</div>';
  }
}

async function replayDead(session) {
  const r = await (await fetch('/admin/sessions/' + session + '/deadletters/replay',
    { method: 'POST' })).json();
  alert(r.replayed ? '已重放：' + r.request_id + '\n' + r.note : (r.reason || '无死信'));
  showDead(session);
}

loadHooks();
</script>
</body>
</html>`
