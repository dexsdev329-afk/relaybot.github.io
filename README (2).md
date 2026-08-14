# Sniper signal relay — mirror the signals into a second Telegram bot

The detection engine (scanner → safety checks → scoring → entry) stays in the main
bot. This relay is a **pure consumer**: it subscribes to the event stream the main
bot already exposes and re-posts each signal through a second Telegram bot token.

No API keys, no scanner, no database. Node 18+, zero npm dependencies.

```bash
cd signal-relay
TELEGRAM_BOT_TOKEN=<second bot token> TELEGRAM_CHAT_ID=<-100...> node relay.js
```

Both channels stay in sync automatically: one engine, one source of truth.

---

## Why not re-implement the algorithm

The entry logic is ~4 500 lines in `sniper-engine.js` and depends on Helius,
GeckoTerminal, DexScreener (through a proxy, because the origin blocks datacenter
IPs), GoPlus, RugCheck, CoinGecko and an LLM call. Running a second copy would mean
a second set of paid API keys, double the rate-limit pressure on shared quotas, and
two engines that drift apart the moment either is tuned — the two channels would
start disagreeing about what a signal is.

Consuming the stream costs one HTTP connection and always matches.

---

## The endpoints

Base URL: `https://dexscreener-telegram-bot-production.up.railway.app`

### `GET /api/sniper/stream` — live events (SSE, public, no auth)

The one you want. Standard Server-Sent Events; every frame is
`data: {"type":..., "data":..., "timestamp":...}`.

| `type` | `data.action` | Meaning |
|---|---|---|
| `STATE` | — | Full engine snapshot. Sent on connect and after each scan. |
| `TRADE` | `OPEN` | **New signal.** `data.position` holds the full call. |
| `TRADE` | `MILESTONE` | A live call crossed an integer X (2X, 3X, 5X…). |
| `TRADE` | `CLOSE` | Call archived. Only `reason: "INACTIVE"` is worth posting. |
| `ADVISORY` | `PROFIT` / `RISK` / `LOSS` | Opt-in heads-up. Never a sell order. |
| `SCAN` | `RESULT` | Scan summary. Noise for a channel — ignore. |

`TRADE/OPEN` payload (`data.position`), the fields that matter:

```jsonc
{
  "symbol": "PONKS",
  "address": "0x…",              // the contract — this is the dedupe key
  "pairAddress": "0x…",
  "chain": "robinhood",          // or "solana"
  "logo": "https://…",
  "entryMarketCap": 42443,
  "entryLiquidity": 18200,
  "score": 133,
  "isMoonBag": false,            // see the gotcha below
  "entryTime": 1785576914326,
  "analysis": { "reasons": ["✅ Sells OK (51 sells 1h)", "…"], "holders": { … } }
}
```

### `GET /api/sniper/status` — current state (JSON, polling fallback)

Same snapshot as the `STATE` frame: `positions`, `history`, `settings`, `logs`.
Use it if you would rather poll every 30 s than hold a connection.

### `GET /api/sniper/attribution` — which entry features actually pay

Win rate / 2X / 5X broken down by score, entry liquidity, entry MC, socials and
top-10 concentration. `?text=1` for a plain-text table.

---

## Two gotchas the relay already handles

**A high-score call creates two positions.** The trading bag plus a `"MOON ∞"`
twin on the *same contract*, flagged `isMoonBag: true`. It is one signal, not two
— skip `isMoonBag` or your channel double-posts every good call. (This is exactly
what was corrupting the published win rate: the twin was counted as a second,
separate, never-updated call.)

**Disconnects lose events.** SSE has no replay. The relay seeds its dedupe set
from the first `STATE` frame — so a fresh start never spams the whole open book —
and on every later `STATE` frame it re-checks for calls newer than the last one it
posted, recovering anything emitted while it was down. Only calls younger than
`RELAY_CATCHUP_MAX_AGE_MIN` are recovered; older ones are backlog and stay silent.

---

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | — | **Required.** Second bot's token from @BotFather. |
| `TELEGRAM_CHAT_ID` | — | **Required.** Target channel/group (`-100…`). |
| `SNIPER_SOURCE` | Railway URL | Where to stream from. |
| `RELAY_EVENTS` | `SIGNAL` | Comma list: `SIGNAL,MILESTONE,ADVISORY,CLOSED`. |
| `RELAY_CATCHUP_MAX_AGE_MIN` | `180` | How far back a reconnect may recover signals. |
| `RELAY_STATE_FILE` | `./relay-state.json` | Dedupe state. Put it on a persistent volume. |
| `RELAY_MIN_VOLUME` | `30000` | Skip signals whose entry 24h volume is below this. `0` disables the filter. |
| `RELAY_REQUIRE_SOCIALS` | off | `1` also drops signals with no socials. Mostly redundant with the volume filter — see below. |

The bot must be an **admin** of the target channel to post.

### The volume filter

Measured on 442 calls (17 Jul → 14 Aug), peak multiple since entry:

| Entry 24h volume | Calls | Reached 2x | Reached 5x | Never moved 10% |
|---|---|---|---|---|
| ≥ $30k | 229 | **38.0 %** | 13.5 % | 16 % |
| < $30k | 213 | **15.5 %** | 2.3 % | 31 % |

The gap is 22.5 pt ± 7.9 (z = 5.55). Checked again on the most recent half of
the period — which chose none of the thresholds — it holds: 42.6 % vs 20.8 %
(z = 3.60).

Cost: 48 % of calls stop being posted, and 14 % of the period's 5x runners go
with them. What it buys: the half that is dropped has a median peak of 1.25x
and a third of it never moves at all.

Requiring socials is **the same signal, not a second one** — 89 % of calls
agree on both criteria, and stacking them adds ~1.5 pt, which is inside the
noise. The flag exists; it is off.

Mirror everything, including exits and heads-ups:

```bash
RELAY_EVENTS=SIGNAL,MILESTONE,ADVISORY,CLOSED node relay.js
```

## Deploying

Any always-on Node host works (Railway, Fly, a VPS with systemd/pm2). Point
`RELAY_STATE_FILE` at a persistent path — on an ephemeral filesystem the relay
re-seeds after every deploy, which is safe (it stays quiet) but it also forgets
which signals it already posted.

## Rate limits

Telegram allows roughly 20 messages/minute to a group. The relay sends serially
with 1.2 s spacing, honours `retry_after` on 429, falls back to a text message
when a photo URL fails, and retries without Markdown if a token symbol contains
characters that break the entity parser.
