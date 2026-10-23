// EOD (end-of-day) market sync — bhavcopy ingest.
//
// Ported from supabase/functions/market-eod-sync so the whole bulk-ingest
// pipeline lives in one place (the Railway worker) with no edge-function
// memory/CPU limits and no round-trip through the Supabase REST layer.
//
// Data flow:
//   NSE / BSE bhavcopy CSVs  →  parse in-memory  →  UPSERT public.daily_bars
//                                                 →  SELECT refresh_market_quote_snapshots()
//                                                 →  INSERT into public.eod_sync_runs
//
// Exposed as POST /sync/eod on the worker; called via sync-dispatcher.

import postgres from 'postgres';
import { unzipSync } from 'fflate';

const { DATABASE_URL } = process.env;
const UA = process.env.SYNC_HTTP_UA
  || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const FYERS_APP_ID = process.env.FYERS_APP_ID || '';
const SOURCE_TIMEOUT_MS = 10_000;
// Bulk EOD ops (1000-row UPSERTs, refresh_market_quote_snapshots) can exceed
// the pooler default (~8s). Raise the ceiling explicitly.
const EOD_STATEMENT_TIMEOUT_MS = Number(process.env.EOD_STATEMENT_TIMEOUT_MS) > 0
  ? Number(process.env.EOD_STATEMENT_TIMEOUT_MS) : 600_000;

// Match the market-hours check used by server.js (Intl-based, DST-safe).
const MARKET_TZ = process.env.MARKET_TZ || 'Asia/Kolkata';
const MARKET_OPEN_MIN = Number(process.env.MARKET_OPEN_MIN) || 555;
const MARKET_CLOSE_MIN = Number(process.env.MARKET_CLOSE_MIN) || 932;
const IST_PARTS = new Intl.DateTimeFormat('en-US', {
  timeZone: MARKET_TZ, hour12: false,
  weekday: 'short', hour: 'numeric', minute: 'numeric', second: 'numeric',
});
function isMarketOpenIst() {
  const parts = Object.fromEntries(IST_PARTS.formatToParts(new Date()).map((p) => [p.type, p.value]));
  const wd = parts.weekday;
  if (wd === 'Sat' || wd === 'Sun') return false;
  const h = Number(parts.hour === '24' ? '0' : parts.hour);
  const m = Number(parts.minute);
  const s = Number(parts.second);
  const t = (h * 60 + m) * 60 + s;
  return t >= MARKET_OPEN_MIN * 60 && t < (MARKET_CLOSE_MIN + 1) * 60;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function istDate(offsetDays = 0) {
  const d = new Date(Date.now() + 5.5 * 60 * 60 * 1000);
  d.setUTCDate(d.getUTCDate() + offsetDays);
  return d;
}
const isWeekday = (d) => { const x = d.getUTCDay(); return x >= 1 && x <= 5; };
const pad2 = (n) => String(n).padStart(2, '0');
const iso = (d) => `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
const yyyymmdd = (d) => `${d.getUTCFullYear()}${pad2(d.getUTCMonth() + 1)}${pad2(d.getUTCDate())}`;
const ddmmyyyy = (d) => `${pad2(d.getUTCDate())}${pad2(d.getUTCMonth() + 1)}${d.getUTCFullYear()}`;
const ddmmyy   = (d) => `${pad2(d.getUTCDate())}${pad2(d.getUTCMonth() + 1)}${String(d.getUTCFullYear()).slice(2)}`;
const MONTHS = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
const ddMMMYYYY = (d) => `${pad2(d.getUTCDate())}${MONTHS[d.getUTCMonth()]}${d.getUTCFullYear()}`;

function num(v) {
  const s = String(v ?? '').replace(/,/g, '').trim();
  if (!s || s === '-' || s.toLowerCase() === 'null') return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function parseCsv(text) {
  const rows = [];
  let row = [], cell = '', quoted = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (quoted) {
      if (ch === '"' && text[i + 1] === '"') { cell += '"'; i++; }
      else if (ch === '"') quoted = false;
      else cell += ch;
    } else {
      if (ch === '"') quoted = true;
      else if (ch === ',') { row.push(cell.trim()); cell = ''; }
      else if (ch === '\n') { row.push(cell.trim()); rows.push(row); row = []; cell = ''; }
      else if (ch !== '\r') cell += ch;
    }
  }
  if (cell || row.length) { row.push(cell.trim()); rows.push(row); }
  return rows.filter((r) => r.some((c) => c.length));
}

function headerMap(header) {
  const m = new Map();
  header.forEach((h, i) => m.set(h.trim().toLowerCase().replace(/[^a-z0-9]/g, ''), i));
  return m;
}
function pick(row, h, names) {
  for (const name of names) {
    const idx = h.get(name.toLowerCase().replace(/[^a-z0-9]/g, ''));
    if (idx != null && idx >= 0) return row[idx];
  }
  return undefined;
}

function unzipFirstCsv(bytes) {
  const files = unzipSync(bytes);
  const names = Object.keys(files);
  const csvName = names.find((n) => n.toLowerCase().endsWith('.csv')) ?? names[0];
  if (!csvName) throw new Error('zip_empty');
  return new TextDecoder().decode(files[csvName]);
}

async function fetchText(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), SOURCE_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      headers: {
        'User-Agent': UA,
        Accept: 'text/csv,application/zip,application/octet-stream,*/*',
        Referer: 'https://www.nseindia.com/',
      },
      signal: controller.signal,
    });
    const buf = new Uint8Array(await res.arrayBuffer());
    if (!res.ok) {
      return { ok: false, url, status: res.status, snippet: new TextDecoder().decode(buf.slice(0, 300)) };
    }
    const text = url.toLowerCase().includes('.zip') || (buf[0] === 0x50 && buf[1] === 0x4b)
      ? unzipFirstCsv(buf) : new TextDecoder().decode(buf);
    return { ok: true, url, text };
  } catch (e) {
    return { ok: false, url, status: 0, err: String(e).slice(0, 160) };
  } finally { clearTimeout(timer); }
}

async function fetchFirst(urls) {
  const attempts = [];
  for (const url of urls) {
    const r = await fetchText(url);
    if (r.ok && r.text.length > 100) {
      const head = r.text.slice(0, 200).trimStart().toLowerCase();
      if (head.startsWith('<!doctype') || head.startsWith('<html') || head.startsWith('<?xml')) {
        attempts.push({ ok: false, url, status: 200, err: 'html_error_page' });
        continue;
      }
      return { hit: r, attempts };
    }
    if (!r.ok) attempts.push(r);
  }
  return { hit: null, attempts };
}

// ─── DB helpers ──────────────────────────────────────────────────────────────

async function upsertBars(sql, rows, size = 1000) {
  const filtered = rows.filter((r) =>
    r.symbol && r.trade_date && r.close != null && r.open != null && r.high != null && r.low != null);
  let upserted = 0;
  for (let i = 0; i < filtered.length; i += size) {
    const slice = filtered.slice(i, i + size);
    // The WHERE clause makes re-runs of the same bhavcopy a no-op instead of
    // rewriting every row (and its indexes) again — the EOD job runs 3x/day.
    await sql`
      INSERT INTO daily_bars ${sql(slice, 'symbol','trade_date','open','high','low','close','volume')}
      ON CONFLICT (symbol, trade_date) DO UPDATE SET
        open   = EXCLUDED.open,
        high   = EXCLUDED.high,
        low    = EXCLUDED.low,
        close  = EXCLUDED.close,
        volume = EXCLUDED.volume
      WHERE daily_bars.open   IS DISTINCT FROM EXCLUDED.open
         OR daily_bars.high   IS DISTINCT FROM EXCLUDED.high
         OR daily_bars.low    IS DISTINCT FROM EXCLUDED.low
         OR daily_bars.close  IS DISTINCT FROM EXCLUDED.close
         OR daily_bars.volume IS DISTINCT FROM EXCLUDED.volume
    `;
    upserted += slice.length;
  }
  return upserted;
}

async function upsertIndexDirectory(sql, entries) {
  const rows = [...entries.values()];
  if (!rows.length) return 0;
  await sql`
    INSERT INTO index_directory ${sql(rows, 'symbol','exchange','display_name','source_name','is_mapped','last_seen_on')}
    ON CONFLICT (symbol) DO UPDATE SET
      exchange = EXCLUDED.exchange,
      display_name = EXCLUDED.display_name,
      source_name = EXCLUDED.source_name,
      is_mapped = EXCLUDED.is_mapped,
      last_seen_on = EXCLUDED.last_seen_on,
      updated_at = now()
  `;
  // Derive underlying -> spot symbol from the instruments master, insert any
  // tradable index missing from the bhavcopy-driven directory, and set the
  // has_options / lot_size flags. Self-healing: never hardcode symbols here,
  // extend `instruments` / `fno_underlyings` instead.
  const [rb] = await sql`SELECT public.rebuild_underlying_spot_map() AS n`;
  if (!rb || Number(rb.n) === 0) {
    throw new Error('underlying_spot_map is empty after rebuild — check public.fno_underlyings (is_index) and public.instruments for -INDEX rows');
  }
  // Flag which directory rows are real Fyers scrips (present in the active
  // instruments master). Bhavcopy-derived names are not streamable, and the
  // UI/worker must not subscribe to them.
  const [mapped] = await sql`SELECT public.rebuild_index_directory_mapping() AS n`;
  if (!mapped || Number(mapped.n) === 0) {
    throw new Error('index_directory has no mapped rows after rebuild — check public.instruments for active -INDEX rows');
  }
  return rows.length;
}


async function loadEquityMap(sql) {
  const out = new Map();
  const rows = await sql`
    SELECT symbol, fyers_symbol FROM tradable_instruments
    WHERE segment = 'EQ' AND is_active = true
  `;
  for (const r of rows) {
    const sym = String(r.symbol ?? '').toUpperCase();
    if (sym && r.fyers_symbol) out.set(sym, r.fyers_symbol);
  }
  return out;
}

async function loadDerivativeMap(sql) {
  const out = new Map();
  const rows = await sql`SELECT symbol, fyers_symbol FROM derivatives_master`;
  for (const r of rows) {
    const sym = String(r.symbol ?? '').toUpperCase();
    if (sym && r.fyers_symbol) out.set(sym, r.fyers_symbol);
  }
  return out;
}

// Exchange index bhavcopies spell index names in human form ("Nifty 50",
// "Nifty Bank"), while `index_symbol_map` keys are Fyers-derived and unspaced
// ("NIFTY50", "NIFTYBANK"). Normalise both sides to alphanumerics so the two
// vocabularies meet. Names that differ beyond punctuation/spacing (e.g.
// "Nifty Financial Services" → FINNIFTY) live in `public.index_aliases`, which
// `rebuild_index_symbol_map()` folds into the map — never hardcode them here.
function normIndexName(s) {
  return String(s ?? '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function idxLookup(map, ...candidates) {
  for (const c of candidates) {
    if (!c) continue;
    const hit = map.get(String(c).trim().toUpperCase()) ?? map.get(normIndexName(c));
    if (hit) return hit;
  }
  return undefined;
}

async function loadIndexMap(sql) {
  const nse = new Map(), bse = new Map();
  const rows = await sql`SELECT exchange, source_name, fyers_symbol FROM index_symbol_map`;
  for (const r of rows) {
    const raw = String(r.source_name).trim().toUpperCase();
    const norm = normIndexName(raw);
    const target = r.exchange === 'NSE' ? nse : r.exchange === 'BSE' ? bse : null;
    if (!target) continue;
    target.set(raw, r.fyers_symbol);
    if (norm && !target.has(norm)) target.set(norm, r.fyers_symbol);
  }
  return { nse, bse };
}

async function loadFyersAuth(sql) {
  if (!FYERS_APP_ID) return null;
  const [row] = await sql`SELECT access_token, expires_at FROM fyers_session WHERE id = true`;
  if (!row?.access_token) return null;
  if (new Date(row.expires_at).getTime() < Date.now()) return null;
  return `${FYERS_APP_ID}:${row.access_token}`;
}
// NOTE(architecture): the `market_chart_snapshots` prewarm was removed —
// `/fyers-history` now fetches live from Fyers on every request, so there
// is no cache to warm. The `loadFyersAuth` helper is retained because
// other paths in this module may reuse it in the future.


// ─── Parsers (identical to old edge function) ────────────────────────────────

function parseNseCash(text, tradeDate, eqMap) {
  const rows = parseCsv(text); if (rows.length < 2) return [];
  const h = headerMap(rows[0]);
  const out = [];
  for (const row of rows.slice(1)) {
    const series = String(pick(row, h, ['SctySrs','Series','SERIES']) ?? '').toUpperCase();
    if (series && !['EQ','BE','BZ','ST','SM','IT'].includes(series)) continue;
    const ticker = String(pick(row, h, ['TckrSymb','SYMBOL','Symbol']) ?? '').toUpperCase();
    const symbol = eqMap.get(ticker) ?? (ticker ? `NSE:${ticker}-EQ` : '');
    const open = num(pick(row, h, ['OpnPric','OPEN','Open Price']));
    const high = num(pick(row, h, ['HghPric','HIGH','High Price']));
    const low  = num(pick(row, h, ['LwPric','LOW','Low Price']));
    const close = num(pick(row, h, ['ClsPric','CLOSE','Close Price','Last']));
    const volume = num(pick(row, h, ['TtlTradgVol','TOTTRDQTY','Total Traded Quantity','Volume']));
    if (symbol && open != null && high != null && low != null && close != null) {
      out.push({ symbol, trade_date: tradeDate, open, high, low, close, volume });
    }
  }
  return out;
}

function parseBseCash(text, tradeDate, eqMap) {
  const rows = parseCsv(text); if (rows.length < 2) return [];
  const h = headerMap(rows[0]);
  const out = [];
  for (const row of rows.slice(1)) {
    const ticker = String(pick(row, h, ['TckrSymb','SC_NAME','SCNAME','Symbol']) ?? '').trim().toUpperCase().replace(/\s+/g, '');
    const code = String(pick(row, h, ['SC_CODE','SCRIP_CD']) ?? '').trim();
    const series = String(pick(row, h, ['SctySrs','SERIES','Series']) ?? '').trim().toUpperCase();
    const suffix = series || 'A';
    const symbol = eqMap.get(ticker) ?? (ticker ? `BSE:${ticker}-${suffix}` : code ? `BSE:${code}-${suffix}` : '');
    const open = num(pick(row, h, ['OPEN','OpnPric']));
    const high = num(pick(row, h, ['HIGH','HghPric']));
    const low  = num(pick(row, h, ['LOW','LwPric']));
    const close = num(pick(row, h, ['CLOSE','ClsPric']));
    const volume = num(pick(row, h, ['NO_OF_SHRS','TtlTradgVol','Volume']));
    if (symbol && open != null && high != null && low != null && close != null) {
      out.push({ symbol, trade_date: tradeDate, open, high, low, close, volume });
    }
  }
  return out;
}

// Derive a stable Fyers-style scrip for an index the symbol map doesn't know.
// Keeps the ingest exhaustive: every index row in the exchange CSV lands in
// `daily_bars` + `index_directory`, mapped or not.
function derivedIndexSymbol(exchange, name) {
  const norm = normIndexName(name);
  return norm ? `${exchange}:${norm}-INDEX` : '';
}

function parseBseIndices(text, tradeDate, bseMap, dir) {
  const rows = parseCsv(text); if (rows.length < 2) return [];
  const h = headerMap(rows[0]);
  const out = [];
  for (const row of rows.slice(1)) {
    // BSE's INDEXSummary_DDMMYYYY.csv exposes IndexID (short code, e.g. SENSEX,
    // BANKEX) and IndexName (long, e.g. "BSE SENSEX"). Older schemas used
    // TckrSymb / FinInstrmNm. Match all of them against index_symbol_map.
    const idxId   = String(pick(row, h, ['IndexID','INDEXID']) ?? '').trim().toUpperCase();
    const idxName = String(pick(row, h, ['IndexName','INDEXNAME','FinInstrmNm','Index Name','INDEX_NAME','Index_Name','Instrument Name']) ?? '').trim().toUpperCase();
    const rawTicker = String(pick(row, h, ['TckrSymb','Ticker','TICKER']) ?? '').trim().toUpperCase();
    const mapped = idxLookup(
      bseMap,
      idxId,
      idxName,
      rawTicker,
      // Some BSE feeds prefix with "S&P " on the long name.
      idxName.replace(/^S&P\s+/, ''),
    );
    const display = idxName || idxId || rawTicker;
    const symbol = mapped || derivedIndexSymbol('BSE', idxId || display);
    if (!symbol) continue;
    const open  = num(pick(row, h, ['OpenPrice','OpnPric','Open Index Value','OPEN','Open']));
    const high  = num(pick(row, h, ['HighPrice','HghPric','High Index Value','HIGH','High']));
    const low   = num(pick(row, h, ['LowPrice','LwPric', 'Low Index Value',  'LOW', 'Low']));
    const close = num(pick(row, h, ['ClosePrice','ClsPric','Closing Index Value','CLOSE','Close']));
    if (open != null && high != null && low != null && close != null) {
      out.push({ symbol, trade_date: tradeDate, open, high, low, close, volume: null });
      if (dir) dir.set(symbol, {
        symbol, exchange: 'BSE',
        display_name: display || symbol,
        source_name: display || symbol,
        is_mapped: Boolean(mapped),
        last_seen_on: tradeDate,
      });
    }
  }
  return out;
}


function parseNseFo(text, tradeDate, derMap) {
  const rows = parseCsv(text); if (rows.length < 2) return [];
  const h = headerMap(rows[0]);
  const out = [];
  for (const row of rows.slice(1)) {
    const inst = String(pick(row, h, ['FinInstrmTp','INSTRUMENT','Instrument']) ?? '').toUpperCase();
    if (inst && !inst.includes('FUT') && !['STF','IDF'].includes(inst)) continue;
    const ticker = String(pick(row, h, ['TckrSymb','SYMBOL']) ?? '').toUpperCase();
    const expiryRaw = String(pick(row, h, ['XpryDt','EXPIRY_DT']) ?? '').slice(0, 10);
    const expiry = /^\d{4}-\d{2}-\d{2}$/.test(expiryRaw) ? expiryRaw : null;
    const appSymbol = expiry && ticker ? `FUT:${ticker}:${expiry}` : '';
    const symbol = derMap.get(appSymbol) ?? appSymbol;
    const open = num(pick(row, h, ['OpnPric','OPEN']));
    const high = num(pick(row, h, ['HghPric','HIGH']));
    const low  = num(pick(row, h, ['LwPric','LOW']));
    const close = num(pick(row, h, ['ClsPric','CLOSE','SttlmPric','SETTLE_PR']));
    const volume = num(pick(row, h, ['TtlTradgVol','CONTRACTS','TOTTRDQTY']));
    if (symbol && open != null && high != null && low != null && close != null) {
      out.push({ symbol, trade_date: tradeDate, open, high, low, close, volume });
    }
  }
  return out;
}

function parseNseIndices(text, tradeDate, nseMap, dir) {
  const rows = parseCsv(text); if (rows.length < 2) return [];
  const h = headerMap(rows[0]);
  const out = [];
  for (const row of rows.slice(1)) {
    const name = String(pick(row, h, ['Index Name','IndexName']) ?? '').trim();
    const mapped = idxLookup(nseMap, name);
    const symbol = mapped || derivedIndexSymbol('NSE', name);
    if (!symbol) continue;
    const open = num(pick(row, h, ['Open Index Value','Open']));
    const high = num(pick(row, h, ['High Index Value','High']));
    const low  = num(pick(row, h, ['Low Index Value','Low']));
    const close = num(pick(row, h, ['Closing Index Value','Close']));
    const volume = num(pick(row, h, ['Volume']));
    if (open != null && high != null && low != null && close != null) {
      out.push({ symbol, trade_date: tradeDate, open, high, low, close, volume });
      if (dir) dir.set(symbol, {
        symbol, exchange: 'NSE', display_name: name || symbol,
        source_name: name || symbol, is_mapped: Boolean(mapped),
        last_seen_on: tradeDate,
      });
    }
  }
  return out;
}

// ─── Source orchestration ────────────────────────────────────────────────────

async function syncSource(sql, urls, parse) {
  const { hit, attempts } = await fetchFirst(urls);
  if (!hit) return { ok: false, attempts, rowsParsed: 0, rowsUpserted: 0, error: 'no_file_reachable' };
  try {
    const rows = parse(hit.text);
    const rowsUpserted = await upsertBars(sql, rows);
    return { ok: true, url: hit.url, attempts, rowsParsed: rows.length, rowsUpserted };
  } catch (e) {
    return { ok: false, url: hit.url, attempts, rowsParsed: 0, rowsUpserted: 0, error: String(e).slice(0, 240) };
  }
}

async function syncBseIndexQuoteRepair(sql, tradeDate, bseMap) {
  const auth = await loadFyersAuth(sql);
  if (!auth) return { ok: false, attempts: [], rowsParsed: 0, rowsUpserted: 0, error: 'no_valid_fyers_rest_session' };
  const symbols = Array.from(new Set(bseMap.values()));
  if (!symbols.length) return { ok: false, attempts: [], rowsParsed: 0, rowsUpserted: 0, error: 'no_bse_index_symbols' };
  const url = `https://api-t1.fyers.in/data/quotes/?symbols=${encodeURIComponent(symbols.join(','))}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), SOURCE_TIMEOUT_MS);
  try {
    const res = await fetch(url, { headers: { Authorization: auth }, signal: controller.signal });
    if (!res.ok) {
      const snippet = await res.text().catch(() => '');
      return { ok: false, attempts: [{ ok: false, url, status: res.status, snippet: snippet.slice(0, 300) }], rowsParsed: 0, rowsUpserted: 0, error: 'quote_repair_http_error' };
    }
    const body = await res.json().catch(() => null);
    const allowed = new Set(symbols);
    const rows = [];
    for (const row of body?.d ?? []) {
      const symbol = String(row?.n ?? '');
      if (!allowed.has(symbol)) continue;
      const v = row.v ?? {};
      const close = num(v.lp);
      const open = num(v.open_price ?? v.op);
      const high = num(v.high_price ?? v.hp);
      const low  = num(v.low_price ?? v.low);
      const volume = num(v.volume ?? v.vol_traded_today ?? v.v);
      if (open != null && high != null && low != null && close != null) {
        rows.push({ symbol, trade_date: tradeDate, open, high, low, close, volume });
      }
    }
    const rowsUpserted = await upsertBars(sql, rows);
    return { ok: rowsUpserted > 0, url, attempts: [], rowsParsed: rows.length, rowsUpserted, error: rowsUpserted > 0 ? undefined : 'quote_repair_empty' };
  } catch (e) {
    return { ok: false, attempts: [{ ok: false, url, status: 0, err: String(e).slice(0, 160) }], rowsParsed: 0, rowsUpserted: 0, error: 'quote_repair_failed' };
  } finally { clearTimeout(timer); }
}

// Some real Fyers index scrips never appear in the exchange index bhavcopy
// (e.g. NSE:NIFTYFPI150-INDEX), so they'd end up with no daily bar and no
// snapshot at all — every cell renders as a dash in the UI. Fill those gaps
// from the Fyers REST quote endpoint. Fully derived from `index_directory`,
// no hardcoded symbols.
async function syncMappedIndexQuoteGapFill(sql, tradeDate) {
  const auth = await loadFyersAuth(sql);
  if (!auth) return { ok: false, attempts: [], rowsParsed: 0, rowsUpserted: 0, error: 'no_valid_fyers_rest_session' };
  const missing = await sql`
    SELECT d.symbol
      FROM index_directory d
     WHERE d.is_mapped
       AND NOT EXISTS (
         SELECT 1 FROM daily_bars b
          WHERE b.symbol = d.symbol AND b.trade_date = ${tradeDate}
       )
     LIMIT 400
  `;
  const symbols = missing.map((r) => r.symbol).filter(Boolean);
  if (!symbols.length) return { ok: true, attempts: [], rowsParsed: 0, rowsUpserted: 0 };

  let parsed = 0, upserted = 0;
  const attempts = [];
  for (let i = 0; i < symbols.length; i += 40) {
    const chunk = symbols.slice(i, i + 40);
    const url = `https://api-t1.fyers.in/data/quotes/?symbols=${encodeURIComponent(chunk.join(','))}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), SOURCE_TIMEOUT_MS);
    try {
      const res = await fetch(url, { headers: { Authorization: auth }, signal: controller.signal });
      if (!res.ok) { attempts.push({ ok: false, url, status: res.status }); continue; }
      const body = await res.json().catch(() => null);
      const allowed = new Set(chunk);
      const rows = [];
      for (const row of body?.d ?? []) {
        const symbol = String(row?.n ?? '');
        if (!allowed.has(symbol)) continue;
        const v = row.v ?? {};
        const close = num(v.lp);
        const open = num(v.open_price ?? v.op);
        const high = num(v.high_price ?? v.hp);
        const low  = num(v.low_price ?? v.low);
        const volume = num(v.volume ?? v.vol_traded_today ?? v.v);
        if (open != null && high != null && low != null && close != null) {
          rows.push({ symbol, trade_date: tradeDate, open, high, low, close, volume });
        }
      }
      parsed += rows.length;
      upserted += await upsertBars(sql, rows);
    } catch (e) {
      attempts.push({ ok: false, url, status: 0, err: String(e).slice(0, 160) });
    } finally { clearTimeout(timer); }
  }
  return { ok: upserted > 0, attempts, rowsParsed: parsed, rowsUpserted: upserted, error: upserted > 0 ? undefined : 'index_gap_fill_empty' };
}

function isRequiredSource(name, includeFo = true) {
  const required = ['nse_cash','nse_indices','bse_cash','nse_cash_prior','nse_indices_prior','bse_cash_prior','market_quote_snapshots'];
  if (includeFo) required.push('nse_fo');
  if (includeFo && name === 'nse_fo_prior') return true;
  return required.includes(name);
}

// ─── URL builders ────────────────────────────────────────────────────────────

const nseCashUrls = (d) => [
  `https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_${yyyymmdd(d)}_F_0000.csv.zip`,
  `https://archives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_${yyyymmdd(d)}_F_0000.csv.zip`,
  `https://nsearchives.nseindia.com/content/historical/EQUITIES/${d.getUTCFullYear()}/${ddMMMYYYY(d).slice(2,5)}/cm${ddMMMYYYY(d)}bhav.csv.zip`,
  `https://archives.nseindia.com/content/historical/EQUITIES/${d.getUTCFullYear()}/${ddMMMYYYY(d).slice(2,5)}/cm${ddMMMYYYY(d)}bhav.csv.zip`,
];
const nseIdxUrls = (d) => [
  `https://nsearchives.nseindia.com/content/indices/ind_close_all_${ddmmyyyy(d)}.csv`,
  `https://archives.nseindia.com/content/indices/ind_close_all_${ddmmyyyy(d)}.csv`,
];
const bseCashUrls = (d) => [
  `https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_${yyyymmdd(d)}_F_0000.CSV`,
  `https://www.bseindia.com/download/BhavCopy/Equity/EQ${ddmmyy(d)}_CSV.ZIP`,
  `https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_${ddmmyy(d)}.zip`,
];
const bseIdxUrls = (d) => [
  // Primary: BSE's public index bhavcopy CSV (confirmed working format).
  //   https://www.bseindia.com/bsedata/Index_Bhavcopy/INDEXSummary_DDMMYYYY.csv
  `https://www.bseindia.com/bsedata/Index_Bhavcopy/INDEXSummary_${ddmmyyyy(d)}.csv`,
  // Legacy fallbacks — kept in case BSE re-enables the old paths.
  `https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_IX_0_0_0_${yyyymmdd(d)}_F_0000.CSV`,
  `https://www.bseindia.com/download/BhavCopy/Equity/BSEINDEX${ddmmyy(d)}.zip`,
];
const nseFoUrls = (d) => [
  `https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_${yyyymmdd(d)}_F_0000.csv.zip`,
  `https://archives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_${yyyymmdd(d)}_F_0000.csv.zip`,
  `https://nsearchives.nseindia.com/content/historical/DERIVATIVES/${d.getUTCFullYear()}/${ddMMMYYYY(d).slice(2,5)}/fo${ddMMMYYYY(d)}bhav.csv.zip`,
  `https://archives.nseindia.com/content/historical/DERIVATIVES/${d.getUTCFullYear()}/${ddMMMYYYY(d).slice(2,5)}/fo${ddMMMYYYY(d)}bhav.csv.zip`,
];

// ─── Main entry point ────────────────────────────────────────────────────────

export async function runEodSync(opts = {}) {
  if (!DATABASE_URL) throw new Error('DATABASE_URL not set');
  const sql = postgres(DATABASE_URL, {
    max: 4,
    idle_timeout: 20,
    prepare: false,
    connection: {
      application_name: 'sync-worker:eod',
      statement_timeout: String(EOD_STATEMENT_TIMEOUT_MS),
      idle_in_transaction_session_timeout: '60000',
    },
  });
  const started = Date.now();
  const includeFo = opts.includeFo !== false;

  const dates = [];
  if (opts.date && /^\d{4}-\d{2}-\d{2}$/.test(opts.date)) {
    const [y, m, d] = opts.date.split('-').map(Number);
    const explicit = new Date(Date.UTC(y, m - 1, d));
    dates.push({ d: explicit, iso: opts.date });
    for (let i = 1; dates.length < 4 && i < 10; i++) {
      const prior = new Date(explicit);
      prior.setUTCDate(explicit.getUTCDate() - i);
      if (!isWeekday(prior)) continue;
      dates.push({ d: prior, iso: iso(prior) });
    }
  } else {
    for (let i = 0; dates.length < 4 && i < 10; i++) {
      const d = istDate(-i);
      if (!isWeekday(d)) continue;
      dates.push({ d, iso: iso(d) });
    }
  }

  let runId = null;
  try {
    const [rr] = await sql`
      INSERT INTO eod_sync_runs (status, trade_date) VALUES ('running', ${dates[0].iso}) RETURNING id`;
    runId = rr?.id ?? null;
  } catch (e) {
    console.warn('[eod] eod_sync_runs insert failed:', e.message);
  }

  const sources = {};
  let chosen = dates[0];
  let chosenIndex = 0;

  try {
    // Always refresh the index symbol map from `instruments` + `index_aliases`
    // before an EOD run so the map is guaranteed consistent with the master.
    // If it comes back empty, fail loudly instead of silently ingesting 0
    // index rows. `rebuild_index_symbol_map()` is a no-op when nothing
    // changed, cheap to call every run.
    try {
      const [rb] = await sql`SELECT public.rebuild_index_symbol_map() AS n`;
      if (!rb || Number(rb.n) === 0) {
        throw new Error('index_symbol_map is empty after rebuild — check public.instruments for -INDEX rows and public.index_aliases seed data');
      }
    } catch (e) {
      throw new Error(`rebuild_index_symbol_map failed: ${String(e?.message || e).slice(0, 200)}`);
    }
    const [eqMap, idxMap] = await Promise.all([loadEquityMap(sql), loadIndexMap(sql)]);

    // Pick the most recent date that actually has an NSE cash bhavcopy.
    for (const [idx, cand] of dates.entries()) {
      const cm = await syncSource(sql, nseCashUrls(cand.d), (t) => parseNseCash(t, cand.iso, eqMap));
      sources.nse_cash = cm;
      if (cm.ok && cm.rowsUpserted > 100) { chosen = cand; chosenIndex = idx; break; }
    }

    const indexDirectory = new Map();
    sources.nse_indices = await syncSource(sql, nseIdxUrls(chosen.d), (t) => parseNseIndices(t, chosen.iso, idxMap.nse, indexDirectory));
    sources.bse_cash    = await syncSource(sql, bseCashUrls(chosen.d), (t) => parseBseCash(t, chosen.iso, eqMap));
    sources.bse_indices = await syncSource(sql, bseIdxUrls(chosen.d),  (t) => parseBseIndices(t, chosen.iso, idxMap.bse, indexDirectory));
    const directoryRows = await upsertIndexDirectory(sql, indexDirectory);
    sources.index_directory = {
      ok: directoryRows > 0, attempts: [], rowsParsed: directoryRows, rowsUpserted: directoryRows,
      error: directoryRows > 0 ? undefined : 'no_index_directory_rows',
    };
    sources.index_quote_gap_fill = await syncMappedIndexQuoteGapFill(sql, chosen.iso);
    if (!sources.bse_indices.ok || sources.bse_indices.rowsUpserted === 0) {
      sources.bse_indices_quote_repair = await syncBseIndexQuoteRepair(sql, chosen.iso, idxMap.bse);
    }
    // NOTE(architecture): the dedicated NSE SME bhavcopy was retired upstream
    // (all known paths 404). SME rows (series SM/ST) already arrive in the main
    // NSE cash bhavcopy parsed above, so no separate source is needed.

    const prior = dates[chosenIndex + 1];
    if (prior) {
      sources.nse_cash_prior     = await syncSource(sql, nseCashUrls(prior.d), (t) => parseNseCash(t, prior.iso, eqMap));
      sources.nse_indices_prior  = await syncSource(sql, nseIdxUrls(prior.d),  (t) => parseNseIndices(t, prior.iso, idxMap.nse));
      sources.bse_cash_prior     = await syncSource(sql, bseCashUrls(prior.d).slice(0, 2), (t) => parseBseCash(t, prior.iso, eqMap));
      sources.bse_indices_prior  = await syncSource(sql, bseIdxUrls(prior.d),  (t) => parseBseIndices(t, prior.iso, idxMap.bse));
    }

    if (includeFo) {
      const derMap = await loadDerivativeMap(sql);
      sources.nse_fo = await syncSource(sql, nseFoUrls(chosen.d), (t) => parseNseFo(t, chosen.iso, derMap));
      if (prior) sources.nse_fo_prior = await syncSource(sql, nseFoUrls(prior.d), (t) => parseNseFo(t, prior.iso, derMap));
    }

    // Refresh the snapshot table read by the UI on mount. Skip during market
    // hours — otherwise this would overwrite the live-worker's in-progress
    // intraday snapshots with EOD bhavcopy values, and every browser mounted
    // after the refresh would see yesterday's close as LTP until the next
    // tick arrives. Admin can force via opts.forceSnapshotRefresh if needed.
    let snapshotResult = { ok: true, rows: 0, error: null };
    if (isMarketOpenIst() && !opts.forceSnapshotRefresh) {
      snapshotResult.ok = false;
      snapshotResult.error = 'skipped: market is open (would overwrite live snapshots)';
      console.warn('[eod] skipping refresh_market_quote_snapshots — market is open');
    } else {
      try {
        const [r] = await sql`SELECT refresh_market_quote_snapshots() AS n`;
        snapshotResult.rows = Number(r?.n ?? 0);
      } catch (e) {
        snapshotResult.ok = false;
        snapshotResult.error = String(e?.message || e).slice(0, 240);
      }
    }
    sources.market_quote_snapshots = {
      ok: snapshotResult.ok, attempts: [],
      rowsParsed: snapshotResult.rows, rowsUpserted: snapshotResult.rows,
      error: snapshotResult.error ?? undefined,
    };

    // Auto-allot any IPO applications whose issue is now published (bhavcopy
    // has just landed for the listing symbol) or whose listing date has
    // passed. Failures here must not fail the EOD run.
    try {
      const [r] = await sql`SELECT public.auto_finalize_ipo_allotments() AS result`;
      const res = r?.result || {};
      console.log('[eod] ipo auto-allot', res);
      sources.ipo_auto_allot = {
        ok: true, attempts: [],
        rowsParsed: Number(res.finalized || 0) + Number(res.refunded || 0),
        rowsUpserted: Number(res.finalized || 0) + Number(res.refunded || 0),
      };
    } catch (e) {
      console.warn('[eod] ipo auto-allot failed:', e?.message || e);
      sources.ipo_auto_allot = {
        ok: false, attempts: [], rowsParsed: 0, rowsUpserted: 0,
        error: String(e?.message || e).slice(0, 240),
      };
    }


    // Chart-snapshot prewarm removed — /fyers-history is now live-only
    // (no in-memory TTL, no persisted fallback), so there's nothing to warm.


    // Floor check: the NSE index bhavcopy always carries 100+ indices. A
    // handful of matched rows means the name↔symbol mapping regressed, which
    // is exactly how "NIFTY prices missing, SENSEX fine" happened before.
    const INDEX_MATCH_FLOOR = 20;
    for (const key of ['nse_indices', 'nse_indices_prior']) {
      const s = sources[key];
      if (s?.ok && s.rowsUpserted > 0 && s.rowsUpserted < INDEX_MATCH_FLOOR) {
        s.ok = false;
        s.error = `only ${s.rowsUpserted} of ${s.rowsParsed} index rows matched index_symbol_map (floor ${INDEX_MATCH_FLOOR}) — check public.index_aliases`;
        console.warn(`[eod] ${key}: ${s.error}`);
      }
    }

    const parsed = Object.values(sources).reduce((n, s) => n + s.rowsParsed, 0);
    const upserted = Object.values(sources).reduce((n, s) => n + s.rowsUpserted, 0);
    const failedSources = Object.entries(sources)
      .filter(([name, s]) => isRequiredSource(name, includeFo) && (!s.ok || s.rowsUpserted === 0))
      .map(([n]) => n);
    const warnings = Object.entries(sources)
      .filter(([name, s]) => !isRequiredSource(name, includeFo) && (!s.ok || s.rowsUpserted === 0))
      .map(([n]) => n);

    const bseIndexHealthy =
      (sources.bse_indices?.ok && sources.bse_indices.rowsUpserted > 0) ||
      (sources.bse_indices_quote_repair?.ok && sources.bse_indices_quote_repair.rowsUpserted > 0);
    const qualitySymbols = ['NSE:NIFTY50-INDEX','NSE:NIFTYBANK-INDEX','NSE:RELIANCE-EQ','NSE:TCS-EQ','NSE:SBIN-EQ'];
    if (bseIndexHealthy) qualitySymbols.push('BSE:SENSEX-INDEX');
    let missingSymbols = qualitySymbols.length;
    try {
      const q = await sql`
        SELECT symbol, last_open, last_high, last_low, last_close, prior_close
        FROM daily_bars_latest WHERE symbol = ANY(${qualitySymbols})`;
      const by = new Map(q.map((r) => [String(r.symbol), r]));
      missingSymbols = qualitySymbols.filter((sym) => {
        const r = by.get(sym);
        return !r || r.last_open == null || r.last_high == null || r.last_low == null || r.last_close == null || r.prior_close == null;
      }).length;
    } catch (e) {
      console.warn('[eod] quality check failed:', e.message);
    }

    const status = failedSources.length === 0 && upserted > 0 && missingSymbols < 3 ? 'success' : 'partial';
    if (runId) {
      await sql`UPDATE eod_sync_runs SET
        status       = ${status},
        trade_date   = ${chosen.iso},
        finished_at  = now(),
        duration_ms  = ${Date.now() - started},
        sources      = ${sql.json(sources)},
        rows_parsed  = ${parsed},
        rows_upserted = ${upserted},
        missing_symbols = ${missingSymbols}
        WHERE id = ${runId}`;
    }
    return {
      ok: status === 'success', status, tradeDate: chosen.iso,
      rowsParsed: parsed, rowsUpserted: upserted, missingSymbols,
      failedSources, warnings, sources, tookMs: Date.now() - started,
    };
  } catch (e) {
    const message = String(e?.message || e).slice(0, 400);
    if (runId) {
      try {
        await sql`UPDATE eod_sync_runs SET
          status = 'failed', finished_at = now(),
          duration_ms = ${Date.now() - started},
          sources = ${sql.json(sources)}, error = ${message}
          WHERE id = ${runId}`;
      } catch {}
    }
    throw e;
  } finally {
    await sql.end({ timeout: 5 });
  }
}
