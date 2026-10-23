// Symbol sync — real streaming pipeline.
//
// Runs inside this worker process (no edge-function limits). For each of
// the 6 Fyers symbol CSVs it: streams the file, parses line-by-line,
// batches 5,000 rows into instruments_staging via a direct Postgres
// connection, then calls sync_finalize() which atomically swaps staging
// into the live instruments catalog and rebuilds all derived caches.
//
// Peak RAM: ~10 MB. Wall-clock: ~30–60 s for the full universe.
//
// Exposed as POST /sync (Bearer <SYNC_SHARED_SECRET>). Called by the
// admin UI via the sync-dispatcher edge function, or by cron.

import postgres from 'postgres';
import { randomUUID } from 'node:crypto';
import { runEodSync } from './eodSync.js';

const {
  DATABASE_URL,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  SYNC_SHARED_SECRET,
} = process.env;

const UA = process.env.SYNC_HTTP_UA || 'Mozilla/5.0 (compatible; sync-worker/1.0)';
const FYERS_SYMBOLS_BASE = (process.env.FYERS_SYMBOL_SOURCES_URL
  || 'https://public.fyers.in/sym_details').replace(/\/$/, '');

// ─── Postgres session tunables ───────────────────────────────────────────────
// Supabase pooled connections inherit a low default statement_timeout (~8s on
// the pooler role). Bulk sync operations (500k-row upsert in sync_finalize,
// large ON CONFLICT batches) legitimately need minutes. We raise the ceiling
// on every new connection via postgres.js `connection` options, and re-assert
// it inside long transactions with SET LOCAL so it's obvious in the code path
// and survives pooler modes that reset session GUCs.
const SYNC_STATEMENT_TIMEOUT_MS = Number(process.env.SYNC_STATEMENT_TIMEOUT_MS) > 0
  ? Number(process.env.SYNC_STATEMENT_TIMEOUT_MS) : 600_000;
const SYNC_FINALIZE_TIMEOUT_MS = Number(process.env.SYNC_FINALIZE_TIMEOUT_MS) > 0
  ? Number(process.env.SYNC_FINALIZE_TIMEOUT_MS) : 600_000;

function makeSql(max = 4, appName = 'sync-worker') {
  return postgres(DATABASE_URL, {
    max,
    idle_timeout: 20,
    prepare: false,
    connection: {
      application_name: appName,
      statement_timeout: String(SYNC_STATEMENT_TIMEOUT_MS),
      idle_in_transaction_session_timeout: '60000',
    },
  });
}

// Retry helper: bounded exponential backoff for transient Postgres failures
// (statement timeout, admin shutdown, connection reset). Non-retryable errors
// bubble up immediately so real bugs aren't masked.
const RETRYABLE_PG_CODES = new Set(['57014', '57P01', '08006', '08003', '08000']);
async function withPgRetry(label, fn, attempts = 3) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try { return await fn(i); } catch (e) {
      lastErr = e;
      const code = e?.code;
      if (!RETRYABLE_PG_CODES.has(code) || i === attempts - 1) throw e;
      const backoff = [2000, 8000, 30000][i] ?? 30000;
      console.warn(`[${label}] retryable pg error ${code}, backoff ${backoff}ms:`, String(e?.message || e).slice(0, 200));
      await new Promise((r) => setTimeout(r, backoff));
    }
  }
  throw lastErr;
}

// Fyers public symbol masters.
const SOURCES = [
  { key: 'NSE_CM',  file: 'NSE_CM.csv',  exchange: 'NSE', segment: 'CM'  },
  { key: 'NSE_FO',  file: 'NSE_FO.csv',  exchange: 'NSE', segment: 'FO'  },
  { key: 'NSE_CD',  file: 'NSE_CD.csv',  exchange: 'NSE', segment: 'CD'  },
  { key: 'BSE_CM',  file: 'BSE_CM.csv',  exchange: 'BSE', segment: 'CM'  },
  { key: 'BSE_FO',  file: 'BSE_FO.csv',  exchange: 'BSE', segment: 'FO'  },
  { key: 'MCX_COM', file: 'MCX_COM.csv', exchange: 'MCX', segment: 'COM' },
].map((s) => ({ ...s, url: `${FYERS_SYMBOLS_BASE}/${s.file}` }));

// ─── Row classifier (identical to supabase/functions/_shared/syncParser.ts) ──
function n(v) {
  if (v == null || v === '' || v === 'None') return null;
  const x = Number(v);
  return Number.isFinite(x) ? x : null;
}
function parseIsoDate(v) {
  return v && /^\d{4}-\d{2}-\d{2}$/.test(v) ? v : null;
}
function epochToDate(v) {
  const x = n(v);
  if (x == null || x <= 0) return null;
  const ms = x > 1e12 ? x : x * 1000;
  const d = new Date(ms);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}
const CURRENCY_TOKENS = new Set([
  'USD','EUR','GBP','JPY','INR','AUD','CAD','CHF','CNY','SGD','HKD','NZD','ZAR','AED',
  'USDINR','EURINR','GBPINR','JPYINR','EURUSD','GBPUSD','USDJPY',
]);
function looksLikeIRF(u, nm) {
  u = (u || '').toUpperCase(); nm = (nm || '').toUpperCase();
  if (/GS\d{2,}/.test(u) || /^\d+GS\d+/.test(u)) return true;
  if (u.includes('DTB') || u.includes('TBILL')) return true;
  if (nm.includes('GOVT') || nm.includes('G-SEC') || nm.includes('GSEC') ||
      nm.includes('T-BILL') || nm.includes('TREASURY')) return true;
  return false;
}
function looksLikeCurrencyPair(u) {
  u = (u || '').toUpperCase();
  if (!u) return false;
  if (CURRENCY_TOKENS.has(u)) return true;
  if (/^[A-Z]{6}$/.test(u)) {
    const a = u.slice(0, 3), b = u.slice(3);
    if (CURRENCY_TOKENS.has(a) && CURRENCY_TOKENS.has(b)) return true;
  }
  return false;
}
function classify(rawCode, optType, segment, underlying, name) {
  if (segment === 'COM') {
    if (rawCode === 11 || rawCode === 30) return { asset_class: 'FUT', instrument_type: 'FUT', product_group: 'COMMODITY_DERIV' };
    if (rawCode === 14 || rawCode === 31) return { asset_class: 'OPT', instrument_type: optType ?? 'OPT', product_group: 'COMMODITY_DERIV' };
    return { asset_class: 'COMMODITY', instrument_type: 'OTHER', product_group: 'COMMODITY_DERIV' };
  }
  if (segment === 'CD') {
    const isIrf = looksLikeIRF(underlying, name);
    const pg = isIrf ? 'IRF' : (looksLikeCurrencyPair(underlying) ? 'CURRENCY_DERIV' : 'CURRENCY_DERIV');
    if (rawCode === 16 || rawCode === 17 || rawCode === 18) return { asset_class: 'FUT', instrument_type: 'FUT', product_group: pg };
    if (rawCode === 19 || rawCode === 14 || rawCode === 15) return { asset_class: 'OPT', instrument_type: optType ?? 'OPT', product_group: pg };
    return { asset_class: 'CURRENCY', instrument_type: 'OTHER', product_group: pg };
  }
  if (segment === 'FO') {
    if (rawCode === 11) return { asset_class: 'FUT', instrument_type: 'FUT', product_group: 'INDEX_DERIV' };
    if (rawCode === 13) return { asset_class: 'FUT', instrument_type: 'FUT', product_group: 'EQUITY_DERIV' };
    if (rawCode === 14) return { asset_class: 'OPT', instrument_type: optType ?? 'OPT', product_group: 'INDEX_DERIV' };
    if (rawCode === 15) return { asset_class: 'OPT', instrument_type: optType ?? 'OPT', product_group: 'EQUITY_DERIV' };
    return { asset_class: 'OPT', instrument_type: optType ?? 'OTHER', product_group: 'EQUITY_DERIV' };
  }
  switch (rawCode) {
    case 0: case 1: return { asset_class: 'EQUITY', instrument_type: 'EQ', product_group: 'EQUITY' };
    case 3: return { asset_class: 'EQUITY', instrument_type: 'WARRANT', product_group: 'EQUITY' };
    case 9: return { asset_class: 'ETF', instrument_type: 'ETF', product_group: 'ETF' };
    case 10: return { asset_class: 'INDEX', instrument_type: 'INDEX', product_group: 'INDEX' };
    case 2: case 5: case 6: case 7: return { asset_class: 'BOND', instrument_type: 'GSEC', product_group: 'GSEC' };
    case 50: return { asset_class: 'BOND', instrument_type: 'BOND', product_group: 'BOND' };
    case 8: return { asset_class: 'OTHER', instrument_type: 'MF', product_group: 'MF' };
    case 4: return { asset_class: 'OTHER', instrument_type: 'SF', product_group: 'OTHER' };
    default: return { asset_class: 'OTHER', instrument_type: 'UNKNOWN', product_group: 'OTHER' };
  }
}
function parseLine(line, exchange, segment) {
  if (!line || line.length < 20) return null;
  const c = line.split(',');
  if (c.length < 17) return null;
  const fy_token = (c[0] || '').trim();
  const fyers_symbol = (c[9] || '').trim();
  if (!fy_token || !fyers_symbol) return null;
  const rawCode = n(c[2]);
  const optRaw = (c[16] || '').trim().toUpperCase();
  const optType = optRaw === 'CE' || optRaw === 'PE' ? optRaw : null;
  const underlying = (c[13] || '').trim() || null;
  const name = (c[1] || '').trim() || fyers_symbol;
  const { asset_class, instrument_type, product_group } = classify(rawCode, optType, segment, underlying, name);
  const strikeRaw = n(c[15]);
  const strike = strikeRaw != null && strikeRaw >= 0 ? strikeRaw : null;
  const expiry = epochToDate(c[8]);
  if (asset_class === 'OPT' && (strike == null || expiry == null || optType == null)) return null;
  if (asset_class === 'FUT' && (segment === 'FO' || segment === 'CD' || segment === 'COM') && expiry == null) return null;
  return {
    fy_token, fyers_symbol, exchange, segment, asset_class, instrument_type, product_group,
    underlying, name, short_symbol: underlying, isin: (c[5] || '').trim() || null,
    lot_size: n(c[3]), tick_size: n(c[4]), strike, option_type: optType, expiry,
    trading_session: (c[6] || '').trim() || null,
    raw_instr_code: rawCode == null ? null : Math.trunc(rawCode),
    data_as_of: parseIsoDate((c[7] || '').trim()),
  };
}

// ─── Streaming line iterator ─────────────────────────────────────────────────
async function* streamLines(body) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, idx).replace(/\r$/, '');
        buf = buf.slice(idx + 1);
        if (line) yield line;
      }
    }
    if (buf) yield buf.replace(/\r$/, '');
  } finally {
    reader.releaseLock();
  }
}

// ─── Worker main ─────────────────────────────────────────────────────────────
// Insert rows in smaller chunks so a single INSERT doesn't blow Postgres'
// bind-parameter limit (~65k). 2,000 rows × 21 cols = 42,000 params — safe.
const BATCH_SIZE = Number(process.env.SYNC_BATCH_SIZE) > 0
  ? Number(process.env.SYNC_BATCH_SIZE) : 2000;

async function processSource(sql, runId, source, jobId) {
  const t0 = Date.now();
  let attempt = 0, lastErr = '';
  for (attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(source.url, {
        headers: { 'User-Agent': UA, 'Accept': 'text/csv,*/*' },
      });
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);

      let batch = [];
      let parsed = 0, written = 0;
      let headerSkipped = false;

      const flush = async () => {
        if (!batch.length) return;
        // Bulk insert via postgres.js — parameterised, no manual CSV escaping.
        const rows = batch.map((r) => ({ ...r, run_id: runId, source: source.key }));
        await sql`INSERT INTO instruments_staging ${sql(rows,
          'run_id','source','fy_token','fyers_symbol','exchange','segment','asset_class','instrument_type',
          'product_group','underlying','name','short_symbol','isin','lot_size','tick_size','strike',
          'option_type','expiry','trading_session','raw_instr_code','data_as_of',
        )}`;
        written += batch.length;
        batch = [];
        // Heartbeat every batch so the watchdog sees progress.
        await sql`UPDATE sync_jobs SET rows_written = ${written}, rows_parsed = ${parsed}, heartbeat_at = now() WHERE id = ${jobId}`;
      };

      for await (const line of streamLines(res.body)) {
        if (!headerSkipped) { headerSkipped = true; if (/^[A-Za-z]/.test(line) && line.includes(',')) continue; }
        parsed++;
        const row = parseLine(line, source.exchange, source.segment);
        if (!row) continue;
        batch.push(row);
        if (batch.length >= BATCH_SIZE) await flush();
      }
      await flush();

      await sql`UPDATE sync_jobs SET status = 'ok', rows_written = ${written}, rows_parsed = ${parsed},
        finished_at = now(), duration_ms = ${Date.now() - t0}, heartbeat_at = now() WHERE id = ${jobId}`;
      return { rows: written };
    } catch (e) {
      lastErr = String(e?.message || e).slice(0, 400);
      // wipe any partial rows for this source before retry
      try { await sql`DELETE FROM instruments_staging WHERE run_id = ${runId} AND source = ${source.key}`; } catch {}
      await new Promise((r) => setTimeout(r, 800 * (attempt + 1)));
    }
  }
  await sql`UPDATE sync_jobs SET status = 'error', error = ${lastErr}, finished_at = now(),
    duration_ms = ${Date.now() - t0} WHERE id = ${jobId}`;
  throw new Error(`${source.key}: ${lastErr}`);
}

export async function runSymbolsSync() {
  if (!DATABASE_URL) throw new Error('DATABASE_URL not set');
  const sql = makeSql(4, 'sync-worker:symbols');
  const runId = randomUUID();
  const runTs = new Date().toISOString();
  try {
    // Wipe any stale staging rows from prior failed runs before we start.
    // sync_finalize() also truncates on success, but if a run crashed before
    // finalize the rows sit forever (that's how staging accumulated 244 MB).
    try { await sql`TRUNCATE TABLE public.instruments_staging`; } catch (e) {
      console.warn('[sync] pre-run truncate failed:', e?.message || e);
    }
    // Create job rows up front so admin can watch progress.
    const jobs = [];
    for (const s of SOURCES) {
      const [j] = await sql`INSERT INTO sync_jobs (run_id, source, stage, status, started_at, heartbeat_at)
        VALUES (${runId}, ${s.key}, 'load', 'running', now(), now()) RETURNING id`;
      jobs.push({ source: s, jobId: j.id });
    }

    // Process sources SEQUENTIALLY. Running all 6 in parallel (esp. NSE_FO,
    // which is ~100 MB and produces >200k rows) OOMs small Railway/Fly
    // instances mid-stream, leaving partial staging behind. Sequential adds
    // ~20s wall-clock but keeps peak RSS under 100 MB and — crucially —
    // actually finishes. If one source fails we still run the rest so a
    // single upstream hiccup doesn't wipe the whole catalog.
    const failures = [];
    for (const { source, jobId } of jobs) {
      try {
        await processSource(sql, runId, source, jobId);
      } catch (e) {
        failures.push(`${source.key}: ${String(e?.message || e).slice(0, 200)}`);
      }
    }
    if (failures.length === jobs.length) {
      throw new Error(`all sources failed: ${failures.join(' | ')}`);
    }

    // Finalize: swap + rebuild caches in one transaction.
    const [mat] = await sql`INSERT INTO sync_jobs (run_id, source, stage, status, started_at, heartbeat_at)
      VALUES (${runId}, 'FINALIZE', 'materialize', 'running', now(), now()) RETURNING id`;
    const t0 = Date.now();
    try {
      // sync_finalize does the atomic staging→live swap plus cache rebuild
      // and legitimately runs for minutes. Wrap in an explicit transaction
      // and re-assert statement_timeout via SET LOCAL so it's safe under
      // both session and transaction pooler modes. Retry on transient
      // timeout / connection loss.
      const r = await withPgRetry('sync_finalize', () => sql.begin(async (tx) => {
        await tx.unsafe(`SET LOCAL statement_timeout = ${SYNC_FINALIZE_TIMEOUT_MS}`);
        await tx.unsafe(`SET LOCAL idle_in_transaction_session_timeout = ${SYNC_FINALIZE_TIMEOUT_MS}`);
        const [row] = await tx`SELECT * FROM sync_finalize(${runId}::uuid, ${runTs}::timestamptz)`;
        return row;
      }));
      await sql`UPDATE sync_jobs SET status = 'ok', rows_written = ${r.upserted}, finished_at = now(),
        duration_ms = ${Date.now() - t0}, heartbeat_at = now() WHERE id = ${mat.id}`;
      await sql`INSERT INTO symbols_sync_runs (started_at, finished_at, ok, files, retired_count)
        VALUES (${runTs}, now(), true, ${sql.json({ run_id: runId, ...r })}, ${r.retired})`;
      return { ok: true, run_id: runId, ...r };
    } catch (e) {
      const msg = String(e?.message || e).slice(0, 500);
      await sql`UPDATE sync_jobs SET status = 'error', error = ${msg}, finished_at = now() WHERE id = ${mat.id}`;
      await sql`INSERT INTO symbols_sync_runs (started_at, finished_at, ok, files, retired_count)
        VALUES (${runTs}, now(), false, ${sql.json({ run_id: runId, error: msg })}, 0)`;
      throw e;
    }
  } finally {
    await sql.end({ timeout: 5 });
  }
}

// Back-compat alias — external callers may still import runSync.
export const runSync = runSymbolsSync;

// ─── IPO sync (iponotify.me → public.ipo_issues) ─────────────────────────────
const IPONOTIFY_BASE = (process.env.IPONOTIFY_BASE || 'https://iponotify.me/api/ipo').replace(/\/$/, '');
// The iponotify free plan only serves the `open` bucket; `upcoming` and
// `closed` return HTTP 403. Only request what the plan allows. To enable more
// buckets after a plan upgrade, set IPONOTIFY_BUCKETS=open,upcoming,closed on
// the worker (no code change needed).
const IPO_ALLOWED = new Set(['open', 'upcoming', 'closed']);
const IPO_BUCKETS = (process.env.IPONOTIFY_BUCKETS || 'open')
  .split(',').map((b) => b.trim().toLowerCase()).filter((b) => IPO_ALLOWED.has(b));
if (IPO_BUCKETS.length === 0) IPO_BUCKETS.push('open');

function ipoIsoDate(v) {
  if (!v) return null;
  const t = Date.parse(v);
  if (!Number.isFinite(t)) return null;
  return new Date(t).toISOString().slice(0, 10);
}
function ipoToSymbol(i) {
  const s = (i.symbol ?? i.searchId ?? i.companyShortName ?? i.companyName ?? '')
    .toString().toUpperCase().replace(/[^A-Z0-9]/g, '');
  return s.slice(0, 20) || 'UNKNOWN';
}
function ipoFmtIssueSize(n) {
  if (n == null || !Number.isFinite(n)) return null;
  if (n >= 1e7) return `₹${(n / 1e7).toFixed(2)} Cr`;
  if (n >= 1e5) return `₹${(n / 1e5).toFixed(2)} L`;
  return `₹${n.toLocaleString('en-IN')}`;
}
async function fetchIpoBucket(apiKey, bucket) {
  const res = await fetch(`${IPONOTIFY_BASE}/${bucket}?limit=100`, {
    headers: { 'X-API-KEY': apiKey, Accept: 'application/json' },
  });
  // Surface upstream failures instead of silently syncing an empty calendar —
  // an empty ipo_issues table must mean "no IPOs", never "the API said 401".
  if (!res.ok) throw new Error(`iponotify ${bucket} HTTP ${res.status}`);
  const body = await res.json().catch(() => null);
  return body?.ipos ?? [];
}

export async function runIpoSync() {
  const apiKey = process.env.IPONOTIFY_API_KEY || '';
  if (!DATABASE_URL) throw new Error('DATABASE_URL not set');
  if (!apiKey) {
    // Record the misconfiguration so it shows up in sync_jobs instead of
    // vanishing as a thrown error with no job row at all.
    const s = makeSql(1, 'sync-worker:ipo');
    try {
      await s`INSERT INTO sync_jobs (run_id, source, stage, status, started_at, finished_at, error)
        VALUES (${randomUUID()}, 'IPO', 'load', 'error', now(), now(),
                'IPONOTIFY_API_KEY not set on worker')`;
    } catch { /* logging must never mask the real error */ }
    finally { await s.end({ timeout: 5 }); }
    throw new Error('IPONOTIFY_API_KEY not set on worker');
  }
  const sql = makeSql(2, 'sync-worker:ipo');
  const runId = randomUUID();
  const nowIso = new Date().toISOString();
  const counts = { open: 0, upcoming: 0, closed: 0 };
  const bucketErrors = {};
  try {
    for (const status of IPO_BUCKETS) {
      const source = `IPO_${status.toUpperCase()}`;
      const [job] = await sql`INSERT INTO sync_jobs (run_id, source, stage, status, started_at, heartbeat_at)
        VALUES (${runId}, ${source}, 'load', 'running', now(), now()) RETURNING id`;
      const t0 = Date.now();
      try {
        const rows = await fetchIpoBucket(apiKey, status);
        let parsed = 0, written = 0;
        for (const i of rows) {
          parsed++;
          try {
            const symbol = ipoToSymbol(i);
            const priceHigh = i.maxPrice ?? i.issuePrice ?? i.minPrice ?? null;
            const row = {
              symbol,
              name: i.companyName ?? i.companyShortName ?? symbol,
              issue_start: ipoIsoDate(i.startDate),
              issue_end: ipoIsoDate(i.endDate),
              lot_size: i.lotSize ?? null,
              price_low: i.minPrice ?? null,
              price_high: priceHigh,
              is_sme: !!i.isSme,
              source: 'iponotify',
              slug: i.searchId ?? null,
              logo_url: i.logoUrl ?? null,
              listing_date: ipoIsoDate(i.allotmentDate),
              min_qty: i.minBidQuantity ?? i.lotSize ?? null,
              min_amount: i.minBidQuantity != null && priceHigh != null
                ? i.minBidQuantity * priceHigh : null,
              issue_size: ipoFmtIssueSize(i.issueSize),
              prospectus_url: i.documentUrl ?? null,
              about: i.aboutCompany?.aboutCompany ?? null,
              strengths: i.pros ? sql.json(i.pros) : null,
              risks: i.cons ? sql.json(i.cons) : null,

              nse_info_url: i.nseInfoUrl ?? null,
              info_url: i.infoUrl ?? null,
              status,
              ipo_type: i.issueType ?? null,
              external_id: i.searchId ?? null,
              fetched_at: nowIso,
            };
            const cols = Object.keys(row);
            const setCols = cols.filter((c) => c !== 'symbol');
            // ipo_issues has UNIQUE constraints on BOTH (symbol) and (slug).
            // Upstream (iponotify.me) sometimes returns the same slug with a
            // different symbol across buckets, which crashed the previous
            // upsert with 23505 on ipo_issues_slug_key. Strategy:
            //   1. Prefer slug as the natural key when present.
            //   2. Wrap in a savepoint so any 23505 doesn't abort the loop.
            //   3. On residual 23505 (other unique key collided), reconcile
            //      by updating the existing row that matches either key.
            const buildExcludedSet = (client) => setCols.map((c, idx) =>
              idx === 0
                ? client`${client(c)} = EXCLUDED.${client(c)}`
                : client`, ${client(c)} = EXCLUDED.${client(c)}`
            );
            // Each INSERT below is its own implicit transaction (autocommit),
            // so a 23505 does not poison subsequent statements — no savepoint
            // required. On residual collision on the other unique key, fall
            // back to a targeted UPDATE that reconciles by either key.
            try {
              if (row.slug) {
                await sql`
                  INSERT INTO ipo_issues ${sql(row, ...cols)}
                  ON CONFLICT (slug) WHERE (slug IS NOT NULL)
                  DO UPDATE SET ${buildExcludedSet(sql)}
                `;
              } else {
                await sql`
                  INSERT INTO ipo_issues ${sql(row, ...cols)}
                  ON CONFLICT (symbol) DO UPDATE SET ${buildExcludedSet(sql)}
                `;
              }
            } catch (e) {
              if (e?.code !== '23505') throw e;
              const updates = setCols.map((c, idx) =>
                idx === 0
                  ? sql`${sql(c)} = ${row[c] ?? null}`
                  : sql`, ${sql(c)} = ${row[c] ?? null}`
              );
              await sql`
                UPDATE ipo_issues SET ${updates}
                WHERE slug = ${row.slug ?? null} OR symbol = ${row.symbol}
              `;
            }
            written++;
          } catch (rowErr) {
            console.error(`[ipo/${status}] row skipped:`, String(rowErr?.message || rowErr).slice(0, 200));
          }
        }


        counts[status] = written;
        // Without the `closed` bucket, IPOs that dropped off the open list
        // would stay "open" forever. Once their issue window has passed,
        // mark them closed locally (only after a successful open fetch).
        if (status === 'open' && !IPO_BUCKETS.includes('closed')) {
          await sql`UPDATE ipo_issues SET status = 'closed'
            WHERE status = 'open' AND issue_end IS NOT NULL
              AND issue_end < (now() AT TIME ZONE 'Asia/Kolkata')::date`;
        }
        await sql`UPDATE sync_jobs SET status='ok', rows_parsed=${parsed}, rows_written=${written},
          finished_at=now(), duration_ms=${Date.now() - t0}, heartbeat_at=now() WHERE id=${job.id}`;
      } catch (e) {
        // A single bucket failing (e.g. iponotify returns 403 for a
        // plan-gated bucket) must not abort the other buckets or the whole
        // /sync/all run. The bucket's job row records the error; rows already
        // in ipo_issues for that bucket are left untouched.
        const msg = String(e?.message || e).slice(0, 400);
        bucketErrors[status] = msg;
        console.error(`[ipo/${status}] bucket failed:`, msg);
        await sql`UPDATE sync_jobs SET status='error', error=${msg}, finished_at=now(),
          duration_ms=${Date.now() - t0} WHERE id=${job.id}`;
      }
    }
    const failed = Object.keys(bucketErrors);
    if (failed.length === IPO_BUCKETS.length) {
      throw new Error(`all ipo buckets failed: ${failed.map((b) => `${b}: ${bucketErrors[b]}`).join(' | ')}`);
    }
    return {
      ok: true,
      partial: failed.length > 0,
      ...counts,
      total: counts.open + counts.upcoming + counts.closed,
      errors: failed.length ? bucketErrors : undefined,
      at: nowIso,
    };
  } finally {
    await sql.end({ timeout: 5 });
  }
}


// ─── Orchestrator: symbols → IPO → EOD, always runs all three ────────────────
export async function runAllSync() {
  const started = Date.now();
  const out = { ok: true, symbols: null, ipo: null, eod: null, durations_ms: {} };

  const runStage = async (key, fn) => {
    const t = Date.now();
    try {
      out[key] = await fn();
    } catch (e) {
      out.ok = false;
      out[key] = { ok: false, error: String(e?.message || e).slice(0, 400) };
      console.error(`[sync/all] ${key} failed:`, e);
    } finally {
      out.durations_ms[key] = Date.now() - t;
    }
  };

  await runStage('symbols', runSymbolsSync);
  await runStage('ipo',     runIpoSync);
  await runStage('eod',     runEodSync);
  out.durations_ms.total = Date.now() - started;
  return out;
}

export { runEodSync };

// Express route registrar — call from server.js
export function mountSyncRoute(app) {
  const auth = (req) => {
    const h = req.header('authorization') || '';
    const token = h.startsWith('Bearer ') ? h.slice(7) : '';
    return SYNC_SHARED_SECRET && token === SYNC_SHARED_SECRET;
  };
  const handler = (fn, label) => async (req, res) => {
    if (!auth(req)) return res.status(401).json({ ok: false, error: 'unauthorized' });
    const wait = req.query.wait === '1';
    const body = req.body && typeof req.body === 'object' ? req.body : {};
    if (wait) {
      try { return res.json(await fn(body)); }
      catch (e) { return res.status(500).json({ ok: false, error: String(e?.message || e) }); }
    }
    fn(body).catch((e) => console.error(`[${label}] failed:`, e));
    res.json({ ok: true, started: true });
  };

  // Back-compat: /sync still runs the symbols master only.
  app.post('/sync',         handler(runSymbolsSync, 'sync'));
  // New unified route: symbols + IPO + EOD in one call.
  app.post('/sync/all',     handler(runAllSync,     'sync/all'));
  // Targeted routes.
  app.post('/sync/symbols', handler(runSymbolsSync, 'sync/symbols'));
  app.post('/sync/ipo',     handler(runIpoSync,     'sync/ipo'));
  app.post('/sync/eod',     handler(runEodSync,     'sync/eod'));
}

