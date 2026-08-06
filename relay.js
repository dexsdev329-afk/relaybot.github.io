#!/usr/bin/env node
/**
 * ByeBoss sniper signal relay
 * ───────────────────────────
 * Mirrors the sniper's signals into a SECOND Telegram bot/channel.
 *
 * It subscribes to the live event stream the main bot already exposes
 * (GET /api/sniper/stream, Server-Sent Events, public, no auth) and re-posts
 * each event with the same formatting. No scanner, no API keys, no database —
 * the detection engine stays in one place and this is a pure consumer.
 *
 * Zero npm dependencies: Node 18+ only (https + fetch are built in).
 *
 *   TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=... node relay.js
 *
 * See README.md for the full event contract.
 */

'use strict';

const https = require('https');
const fs = require('fs');
const path = require('path');

// ── CONFIG ──────────────────────────────────────────────────────────────────
const SOURCE = (process.env.SNIPER_SOURCE || 'https://dexscreener-telegram-bot-production.up.railway.app').replace(/\/+$/, '');
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const STATE_FILE = process.env.RELAY_STATE_FILE || path.join(__dirname, 'relay-state.json');

// Which events to forward. Default = entry signals only, which is what a
// second channel usually wants. Add MILESTONE/ADVISORY/CLOSED to mirror everything.
const EVENTS = new Set(
  (process.env.RELAY_EVENTS || 'SIGNAL')
    .split(',').map(s => s.trim().toUpperCase()).filter(Boolean)
);
// Advisory sub-types to MUTE even when ADVISORY is enabled. Default mutes ALL
// advisories — the "🩸 CUT LOSS?" (LOSS), "⚠️ HEADS UP" (RISK) and
// "💡 PROFIT WATCH" (PROFIT) posts — so a channel only ever gets clean entry
// signals. Set ADVISORY_MUTE="" to re-enable them all, or a subset like
// "LOSS,RISK" to keep only PROFIT.
const ADVISORY_MUTE = new Set(
  (process.env.ADVISORY_MUTE === undefined ? 'LOSS,RISK,PROFIT' : process.env.ADVISORY_MUTE)
    .split(',').map(s => s.trim().toUpperCase()).filter(Boolean)
);

// On reconnect the relay compares the stream's state dump against what it has
// already posted, so signals emitted while it was disconnected are not lost.
// Anything older than this is treated as backlog and stays silent.
const CATCHUP_MAX_AGE_MS = Number(process.env.RELAY_CATCHUP_MAX_AGE_MIN || 180) * 60 * 1000;

const SEEN_CAP = 2000;
const TERMINAL_URL = process.env.SWOGE_SITE_URL || 'https://swoleeswoge.dog';

if (!BOT_TOKEN || !CHAT_ID) {
  console.error('✖ TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required.');
  process.exit(1);
}

// ── PERSISTED DEDUPE STATE ──────────────────────────────────────────────────
// Survives restarts so a redeploy never re-posts the whole open book.
let seen = new Set();
let lastEntryTime = 0;
let seeded = false;

function loadState() {
  try {
    const d = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    seen = new Set(d.seen || []);
    lastEntryTime = d.lastEntryTime || 0;
    seeded = !!d.seeded;
    console.log(`↺ state restored — ${seen.size} signals already posted`);
  } catch (e) { /* first run */ }
}
function saveState() {
  try {
    const arr = [...seen].slice(-SEEN_CAP);
    seen = new Set(arr);
    fs.writeFileSync(STATE_FILE, JSON.stringify({ seen: arr, lastEntryTime, seeded }));
  } catch (e) { console.warn('state save failed:', e.message); }
}

// ── TELEGRAM ────────────────────────────────────────────────────────────────
// Serial queue: Telegram allows ~20 messages/minute to a group and 429s past
// that. One message at a time, spaced, with retry_after honoured.
const queue = [];
let sending = false;

function tgCall(method, body) {
  return new Promise((resolve) => {
    const payload = JSON.stringify(body);
    const req = https.request({
      hostname: 'api.telegram.org',
      path: `/bot${BOT_TOKEN}/${method}`,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) },
      timeout: 20000,
    }, (res) => {
      let raw = '';
      res.on('data', c => { raw += c; });
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); } catch (e) { resolve({ ok: false, description: raw.slice(0, 200) }); }
      });
    });
    req.on('error', e => resolve({ ok: false, description: e.message }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, description: 'timeout' }); });
    req.write(payload);
    req.end();
  });
}

async function drain() {
  if (sending) return;
  sending = true;
  while (queue.length) {
    const job = queue.shift();
    let res = await tgCall(job.method, job.body);

    // Rate limited — wait exactly as long as Telegram asks, then retry once.
    if (!res.ok && res.parameters && res.parameters.retry_after) {
      await sleep((res.parameters.retry_after + 1) * 1000);
      res = await tgCall(job.method, job.body);
    }
    // A photo URL can 400 (dead IPFS gateway, oversized image). Never lose a
    // signal over its logo — fall back to the text-only message.
    if (!res.ok && job.method === 'sendPhoto') {
      res = await tgCall('sendMessage', {
        chat_id: job.body.chat_id, text: job.body.caption,
        parse_mode: 'Markdown', disable_web_page_preview: true,
      });
    }
    // Markdown parse errors: a token symbol containing _ * ` [ breaks the
    // entity parser. Resend as plain text rather than dropping the alert.
    if (!res.ok && /can't parse entities|parse entities/i.test(res.description || '')) {
      const text = job.body.caption || job.body.text || '';
      res = await tgCall('sendMessage', {
        chat_id: job.body.chat_id, text: stripMd(text), disable_web_page_preview: true,
      });
    }
    if (!res.ok) console.warn('✖ telegram:', (res.description || 'unknown').slice(0, 140));
    await sleep(1200);
  }
  sending = false;
}

function send(text, opts = {}) {
  const body = { chat_id: CHAT_ID, text, parse_mode: 'Markdown', disable_web_page_preview: true };
  if (opts.reply_markup) body.reply_markup = opts.reply_markup;
  queue.push({ method: 'sendMessage', body });
  drain();
}
function sendPhoto(photo, caption, opts = {}) {
  if (!photo) return send(caption, opts);
  const body = { chat_id: CHAT_ID, photo, caption, parse_mode: 'Markdown' };
  if (opts.reply_markup) body.reply_markup = opts.reply_markup;
  queue.push({ method: 'sendPhoto', body });
  drain();
}

// ── FORMATTING ──────────────────────────────────────────────────────────────
// Mirrors sniperBroadcast() in bot.js so both channels read identically.
const sleep = ms => new Promise(r => setTimeout(r, ms));
const stripMd = s => String(s || '').replace(/[*_`[\]]/g, '');
// Only used inside *bold* spans, where an odd _ or * would break the message.
const safeSym = s => String(s || '?').replace(/[*_`[\]]/g, '');

function formatNumber(n) {
  n = Number(n) || 0;
  if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return String(Math.round(n));
}
const fmtX = x => (x >= 10 ? x.toFixed(0) : x.toFixed(2)) + 'x';
const chartUrl = d => `https://dexscreener.com/${d.chain || 'solana'}/${d.pairAddress || d.address || ''}`;

function postSignal(p) {
  const ca = p.address || '';
  const reasons = ((p.analysis && p.analysis.reasons) || []).filter(r => !String(r).startsWith('⏭'));
  let whyBlock = '';
  if (reasons.length) {
    // Photo captions cap at 1024 chars — trim to fit, keeping at least 3 reasons.
    let lines = reasons.slice(0, 8).map(r => '· ' + stripMd(r));
    while (lines.join('\n').length > 440 && lines.length > 3) lines.pop();
    whyBlock = '\n\n📋 *Why this call:*\n' + lines.join('\n');
  }
  const caption = `🎯 *NEW SIGNAL* · *$${safeSym(p.symbol)}*

⛓ ${(p.chain || 'solana').toUpperCase()}
📊 MCap $${formatNumber(p.entryMarketCap)} · Liq $${formatNumber(p.entryLiquidity)}${whyBlock}
${ca ? '\n`' + ca + '`' : ''}

📈 [Chart](${chartUrl(p)}) · 🐶 [SWOGE](${TERMINAL_URL})`;
  sendPhoto(p.logo || '', caption);
}

function postMilestone(d) {
  const m = d.milestone;
  const x = d.currentX || m;
  const emoji = m >= 50 ? '🌕' : m >= 10 ? '🚀' : m >= 5 ? '🔥' : m >= 3 ? '💥' : '📈';
  const ca = d.address || '';
  const caption =
    `${emoji} *${m}X HIT* · *$${safeSym(d.symbol)}*\n\n` +
    `Signal called at $${formatNumber(d.entryMarketCap)} MC.\n` +
    `Now trading at *${(x >= 10 ? x.toFixed(0) : x.toFixed(2))}X* from entry.\n` +
    (ca ? `\n\`${ca}\`\n` : '') +
    `\n📈 [Chart](${chartUrl(d)}) · 🐶 [SWOGE](${TERMINAL_URL})`;
  sendPhoto(d.logo || '', caption);
}

function postAdvisory(action, d) {
  const x = d.x || 1;
  const ca = d.address || '';
  const tail = (ca ? `\n\n\`${ca}\`` : '') + `\n📈 [Chart](${chartUrl(d)})`;
  let caption;
  if (action === 'PROFIT') {
    caption =
      `💡 *PROFIT WATCH* · *$${safeSym(d.symbol)}*\n\n` +
      `Up *+${fmtX(x)}* since our call ($${formatNumber(d.entryMc)} → $${formatNumber(d.curMc)} MC).\n` +
      `Momentum's cooling off — could be a smart spot to secure some. 🫡\n\n` +
      `_Your bag, your call. NFA._` + tail;
  } else if (action === 'LOSS') {
    caption =
      `🩸 *CUT LOSS?* · *$${safeSym(d.symbol)}*\n\n` +
      `Down *-${Math.round((1 - x) * 100)}%* from our call ($${formatNumber(d.entryMc)} → $${formatNumber(d.curMc)} MC).\n` +
      `Still bleeding — volume dried up, sells in control. Doesn't look like it's coming back.\n` +
      `If you're still holding, might be the spot to cut it and move on.\n\n` +
      `_Your bag, your call. NFA._` + tail;
  } else { // RISK
    caption =
      `⚠️ *HEADS UP* · *$${safeSym(d.symbol)}*\n\n` +
      `Pulling back from its peak (was +${fmtX(d.peakX || x)}, now +${fmtX(x)}), sells picking up.\n` +
      `If you're in profit, might be worth locking some in.\n\n` +
      `_NFA._` + tail;
  }
  send(caption);
}

function postClosed(d) {
  const ca = d.address || '';
  const hrs = d.holdMin ? Math.max(1, Math.round(d.holdMin / 60)) : null;
  send(
    `💤 *SIGNAL CLOSED* · *$${safeSym(d.symbol)}*\n\n` +
    `No momentum — volume dried up and it never developed${hrs ? ' after ~' + hrs + 'h' : ''} ` +
    `(peak ${fmtX(d.peakX || 1)}). Calling it dead and clearing it. 🪦\n\n` +
    `_We post our duds too, not just the runners. NFA._` +
    (ca ? `\n\n\`${ca}\`` : '') +
    `\n📈 [Chart](${chartUrl(d)})`
  );
}

// ── EVENT ROUTING ───────────────────────────────────────────────────────────
function markSeen(key) {
  if (!key || seen.has(key)) return false;
  seen.add(key);
  saveState();
  return true;
}

function onEvent(evt) {
  const { type, data } = evt;
  if (!data) return;
  const action = data.action || type;

  if (type === 'STATE') return onStateSnapshot(data);

  if (type === 'TRADE' && action === 'OPEN') {
    const p = data.position || {};
    // A high-score call also opens a "MOON ∞" twin position on the same
    // contract. It is the same signal — announce it once.
    if (p.isMoonBag) return;
    if (!markSeen('sig:' + (p.address || p.id))) return;
    if (p.entryTime > lastEntryTime) { lastEntryTime = p.entryTime; saveState(); }
    console.log(`🎯 SIGNAL $${p.symbol} @ $${formatNumber(p.entryMarketCap)} MC`);
    if (EVENTS.has('SIGNAL')) postSignal(p);
    return;
  }

  if (type === 'TRADE' && action === 'MILESTONE') {
    if (!markSeen(`ms:${data.address}:${data.milestone}`)) return;
    console.log(`🚀 ${data.milestone}X $${data.symbol}`);
    if (EVENTS.has('MILESTONE')) postMilestone(data);
    return;
  }

  if (type === 'ADVISORY') {
    if (ADVISORY_MUTE.has(action)) { console.log(`🔇 muted ${action} $${data.symbol}`); return; }
    if (!markSeen(`adv:${data.address}:${action}`)) return;
    console.log(`💡 ${action} $${data.symbol}`);
    if (EVENTS.has('ADVISORY')) postAdvisory(action, data);
    return;
  }

  // Profitable closes stay silent by design — the engine gives entries, never
  // "sell now". INACTIVE is the one close it announces: the call went dead.
  if (type === 'TRADE' && action === 'CLOSE' && data.reason === 'INACTIVE') {
    if (!markSeen('close:' + data.address)) return;
    console.log(`💤 CLOSED $${data.symbol}`);
    if (EVENTS.has('CLOSED')) postClosed(data);
  }
}

// The stream sends a full STATE dump on connect and after every scan. First one
// seeds the dedupe set (so a fresh relay never spams the whole open book);
// later ones are the safety net that recovers signals emitted while the relay
// was disconnected.
function onStateSnapshot(st) {
  const positions = st.positions || [];
  if (!seeded) {
    for (const p of positions) {
      seen.add('sig:' + (p.address || p.id));
      if (p.entryTime > lastEntryTime) lastEntryTime = p.entryTime;
    }
    seeded = true;
    saveState();
    console.log(`✓ seeded from ${positions.length} open calls — only new signals from here`);
    return;
  }
  const now = Date.now();
  const missed = positions
    .filter(p => !p.isMoonBag && p.entryTime > lastEntryTime && (now - p.entryTime) < CATCHUP_MAX_AGE_MS)
    .filter(p => !seen.has('sig:' + (p.address || p.id)))
    .sort((a, b) => a.entryTime - b.entryTime);
  for (const p of missed) {
    console.log(`↺ catch-up signal $${p.symbol} (missed while disconnected)`);
    onEvent({ type: 'TRADE', data: { action: 'OPEN', position: p } });
  }
}

// ── SSE CLIENT ──────────────────────────────────────────────────────────────
let backoff = 1000;

function connect() {
  const url = `${SOURCE}/api/sniper/stream`;
  console.log(`… connecting to ${url}`);

  const req = https.get(url, { headers: { Accept: 'text/event-stream' }, timeout: 0 }, (res) => {
    if (res.statusCode !== 200) {
      console.warn(`✖ stream returned HTTP ${res.statusCode}`);
      res.resume();
      return reconnect();
    }
    console.log('✓ connected — listening for signals');
    backoff = 1000;
    res.setEncoding('utf8');

    let buf = '';
    res.on('data', (chunk) => {
      buf += chunk;
      // Frames are separated by a blank line. STATE frames are ~500KB, so the
      // buffer legitimately holds a partial frame between chunks.
      let sep;
      while ((sep = buf.indexOf('\n\n')) !== -1) {
        const frame = buf.slice(0, sep);
        buf = buf.slice(sep + 2);
        const payload = frame.split('\n')
          .filter(l => l.startsWith('data:'))
          .map(l => l.slice(5).trim())
          .join('');
        if (!payload) continue;   // heartbeat comment line
        try { onEvent(JSON.parse(payload)); }
        catch (e) { console.warn('✖ bad frame:', e.message.slice(0, 80)); }
      }
      // Guard against a truncated stream growing the buffer forever.
      if (buf.length > 8e6) buf = '';
    });

    res.on('end', () => { console.warn('… stream ended'); reconnect(); });
    res.on('error', (e) => { console.warn('… stream error:', e.message); reconnect(); });
  });

  req.on('error', (e) => { console.warn('✖ connect failed:', e.message); reconnect(); });
}

let reconnecting = false;
function reconnect() {
  if (reconnecting) return;
  reconnecting = true;
  const wait = backoff;
  backoff = Math.min(backoff * 2, 60000);
  console.log(`… reconnecting in ${Math.round(wait / 1000)}s`);
  setTimeout(() => { reconnecting = false; connect(); }, wait);
}

// ── BOOT ────────────────────────────────────────────────────────────────────
loadState();
console.log(`🎯 sniper relay → chat ${CHAT_ID} | forwarding: ${[...EVENTS].join(', ')}`);
connect();

process.on('SIGINT', () => { saveState(); process.exit(0); });
process.on('SIGTERM', () => { saveState(); process.exit(0); });
