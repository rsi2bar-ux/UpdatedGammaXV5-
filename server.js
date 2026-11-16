import express from "express";
import cors from "cors";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";
import { authenticator } from "otplib";
import webpush from "web-push";
import fs from "node:fs";
import path from "node:path";
import fsp from "node:fs/promises";
import readline from "node:readline";
import { createGunzip, createGzip } from "node:zlib";
import { pipeline } from "node:stream/promises";

const PORT = Number(process.env.PORT || 10000);
const HOST = process.env.HOST || "0.0.0.0";
const API_BASE_URL = (process.env.DHAN_API_BASE_URL || "https://api.dhan.co/v2").replace(/\/$/, "");
const ENABLE_LIVE_FEED = String(process.env.ENABLE_LIVE_FEED ?? "true").toLowerCase() === "true";
const FAILOVER_STANDBY = String(process.env.FAILOVER_STANDBY ?? "false").toLowerCase() === "true";
let liveFeedEnabled = ENABLE_LIVE_FEED && !FAILOVER_STANDBY;
const ENABLE_OPTION_CHAIN = String(process.env.ENABLE_OPTION_CHAIN ?? "true").toLowerCase() === "true";
const AUTO_SUBSCRIBE_INDICES = String(process.env.AUTO_SUBSCRIBE_INDICES ?? "true").toLowerCase() === "true";
const DHAN_LIVE_FEED_URL = process.env.DHAN_LIVE_FEED_URL || "wss://api-feed.dhan.co";
const DHAN_20_DEPTH_URL = process.env.DHAN_20_DEPTH_URL || "wss://depth-api-feed.dhan.co/twentydepth";
const DHAN_200_DEPTH_URL = process.env.DHAN_200_DEPTH_URL || "wss://full-depth-api.dhan.co/twohundreddepth";
const MAX_INSTRUMENTS_PER_WS = Number(process.env.MAX_INSTRUMENTS_PER_WS || 5000);
const ENABLE_20_DEPTH = String(process.env.ENABLE_20_DEPTH ?? "true").toLowerCase() === "true";
const ENABLE_200_DEPTH = String(process.env.ENABLE_200_DEPTH ?? "true").toLowerCase() === "true";
const ENABLE_MARKET_QUOTE = String(process.env.ENABLE_MARKET_QUOTE ?? "true").toLowerCase() === "true";
const ENABLE_TECHNICAL_DATA_API = String(process.env.ENABLE_TECHNICAL_DATA_API ?? "true").toLowerCase() === "true";
const ENABLE_MARKET_MOVERS_API = String(process.env.ENABLE_MARKET_MOVERS_API ?? "true").toLowerCase() === "true";
const ENABLE_NEWS_HEADLINES_API = String(process.env.ENABLE_NEWS_HEADLINES_API ?? "true").toLowerCase() === "true";
const ENABLE_COMPANY_INFO_API = String(process.env.ENABLE_COMPANY_INFO_API ?? "false").toLowerCase() === "true";
const OPTION_CHAIN_REFRESH_MS = Number(process.env.OPTION_CHAIN_REFRESH_MS || 3200);
const MAX_INSTRUMENTS_PER_MESSAGE = Number(process.env.MAX_INSTRUMENTS_PER_MESSAGE || 100);
const MAX_SUBSCRIPTIONS = Number(process.env.MAX_SUBSCRIPTIONS || 5000);
const MAX_CLIENTS = Number(process.env.MAX_CLIENTS || 100);
const MAX_TICK_CACHE_ITEMS = Number(process.env.TICK_BUFFER_SIZE || 10000);
const CANDLE_TIMEFRAME_MS = Number(process.env.CANDLE_TIMEFRAME_MS || 30000);
const CANDLE_TIMEFRAMES_MS = (process.env.SUPPORTED_TIMEFRAMES || "30000,60000,180000,300000,900000").split(",").map(Number).filter(x=>[30000,60000,180000,300000,900000,1800000,3600000].includes(x));
if(!CANDLE_TIMEFRAMES_MS.includes(30000))CANDLE_TIMEFRAMES_MS.unshift(30000);
const AUTO_SUBSCRIBE_OPTIONS = String(process.env.AUTO_SUBSCRIBE_OPTIONS ?? "true").toLowerCase() === "true";
const MAX_CANDLES_PER_INSTRUMENT = Number(process.env.MAX_CANDLES_PER_INSTRUMENT || 2000);
const ENABLE_MARKET_SNAPSHOTS = String(process.env.ENABLE_MARKET_SNAPSHOTS ?? "true").toLowerCase() === "true";
const SNAPSHOT_INTERVAL_MS = Math.max(1000, Number(process.env.SNAPSHOT_INTERVAL_MS || 1000));
const SNAPSHOT_DIR = path.resolve(process.env.SNAPSHOT_DIR || (process.env.RENDER ? "/var/data/gammax-snapshots" : "./data/snapshots"));
const SNAPSHOT_RETENTION_DAYS = Math.max(1, Number(process.env.SNAPSHOT_RETENTION_DAYS || 30));
const SNAPSHOT_OPTION_STRIKES_EACH_SIDE = Math.max(1, Math.min(25, Number(process.env.SNAPSHOT_OPTION_STRIKES_EACH_SIDE || 10)));
const SNAPSHOT_MARKET_HOURS_ONLY = String(process.env.SNAPSHOT_MARKET_HOURS_ONLY ?? "true").toLowerCase() === "true";
const TOKEN_REFRESH_BEFORE_MIN = Number(process.env.DHAN_TOKEN_REFRESH_BEFORE_MIN || 30);
const AUTO_RENEW_DHAN_TOKEN = String(process.env.DHAN_AUTO_RENEW_TOKEN ?? "true").toLowerCase() === "true";
const AUTH_REFRESH_INTERVAL_MS = Number(process.env.AUTH_REFRESH_INTERVAL_MS || 60000);
const corsSetting = (process.env.CORS_ORIGIN || "*").split(",").map(x => x.trim()).filter(Boolean);
const CLIENT_ID = (process.env.DHAN_CLIENT_ID || "").trim();
const PIN = (process.env.DHAN_PIN || "").trim();
const TOTP_SECRET = (process.env.DHAN_TOTP_SECRET || "").replace(/\s+/g, "").toUpperCase();

const VAPID_PUBLIC_KEY = (process.env.VAPID_PUBLIC_KEY || "").trim();
const VAPID_PRIVATE_KEY = (process.env.VAPID_PRIVATE_KEY || "").trim();
const VAPID_SUBJECT = (process.env.VAPID_SUBJECT || "mailto:alerts@example.com").trim();
if (VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY) webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

const app = express();
app.use(cors({ origin: corsSetting.includes("*") ? true : corsSetting }));
app.use(express.json({ limit: "2mb" }));
app.use((req,res,next)=>{res.setHeader("Cache-Control","no-store, no-cache, must-revalidate, proxy-revalidate");res.setHeader("Pragma","no-cache");res.setHeader("Expires","0");next();});

const INDEXES = {
  NIFTY: { securityId: process.env.NIFTY_SECURITY_ID || "13", segment: process.env.NIFTY_SEGMENT || "IDX_I", feedSegment: process.env.NIFTY_SEGMENT || "IDX_I", name: "NIFTY", step: 50, optionSegment: process.env.NIFTY_OPTION_SEGMENT || "NSE_FNO" },
  BANKNIFTY: { securityId: process.env.BANKNIFTY_SECURITY_ID || "25", segment: process.env.BANKNIFTY_SEGMENT || "IDX_I", feedSegment: process.env.BANKNIFTY_SEGMENT || "IDX_I", name: "BANKNIFTY", step: 100, optionSegment: process.env.BANKNIFTY_OPTION_SEGMENT || "NSE_FNO" },
  FINNIFTY: { securityId: process.env.FINNIFTY_SECURITY_ID || "27", segment: process.env.FINNIFTY_SEGMENT || "IDX_I", feedSegment: process.env.FINNIFTY_SEGMENT || "IDX_I", name: "FINNIFTY", step: 50, optionSegment: process.env.FINNIFTY_OPTION_SEGMENT || "NSE_FNO" },
  MIDCPNIFTY: { securityId: process.env.MIDCPNIFTY_SECURITY_ID || "442", segment: process.env.MIDCPNIFTY_SEGMENT || "IDX_I", feedSegment: process.env.MIDCPNIFTY_SEGMENT || "IDX_I", name: "MIDCPNIFTY", step: 25, optionSegment: process.env.MIDCPNIFTY_OPTION_SEGMENT || "NSE_FNO" },
  SENSEX: { securityId: process.env.SENSEX_SECURITY_ID || "51", segment: process.env.SENSEX_SEGMENT || "IDX_I", feedSegment: process.env.SENSEX_SEGMENT || "IDX_I", name: "SENSEX", step: 100, optionSegment: process.env.SENSEX_OPTION_SEGMENT || "BSE_FNO" },
};

const state = {
  version: "5.2-PRO-FAILOVER",
  indexKey: process.env.DEFAULT_INDEX || "NIFTY",
  expiry: null,
  spot: null,
  dhanConnected: false,
  depthConnected: false,
  lastTick: null,
  marketStatus: null,
  option: null,
  analytics: null,
  chain: { updatedAt: null, expiry: null, rows: [], atm: null, maxPain: null },
  subscriptions: [],
  server: { startedAt: Date.now(), ticks: 0, packets: 0, reconnects: 0 },
};

const clients = new Set();
const pushSubscriptions = new Map();
const ticks = new Map();
const candles = new Map();
const candleSeries = new Map();
const candleVolumeState = new Map();
const snapshotCandleCache = new Map();
const prevSession = new Map();
const instruments = new Map();
const optionMeta = new Map();
const lastChainRows = new Map();

let dhanWs = null;
let depthWs = null;
let depth200Ws = null;
let depth200ReconnectTimer = null;
let depth200Target = null;
let depth200Connected = false;
const depthSideCache = new Map();
const pinnedDepthInstruments = new Map();
const snapshotStats = { enabled: ENABLE_MARKET_SNAPSHOTS, ready: false, writes: 0, skipped: 0, busySkipped: 0, lastSnapshotAt: null, lastError: null, lastBytes: 0, activeDate: null };
let snapshotBusy = false;
let snapshotTimer = null;
let reconnectTimer = null;
let depthReconnectTimer = null;
let chainTimer = null;
let instrumentRefreshTimer = null;
let accessToken = "";
let tokenExpiry = 0;
let authStatus = "WAITING";
let authLastError = null;
let authLastSuccessAt = null;
let dhanConnectedAt = null;
let depthConnectedAt = null;
let lastPacketAt = null;
let lastAnyTickAt = null;
let lastIndexTickAt = null;
let lastOptionTickAt = null;
let lastFutureTickAt = null;
let lastSubscriptionAt = null;
let lastDepthPacketAt = null;
let decodedPacketCount = 0;
let decodeErrorCount = 0;
let unknownPacketCount = 0;
const packetCounts = Object.create(null);

let chainBusy = false;
let instrumentCacheLoaded = false;

function now() { return Date.now(); }
function log(...a) { console.log(new Date().toISOString(), ...a); }
function clone(x) { return JSON.parse(JSON.stringify(x)); }
function broadcast(message) {
  const s = JSON.stringify(message);
  for (const ws of clients) if (ws.readyState === WebSocket.OPEN) ws.send(s);
}
function safeNum(v, fallback = null) { const n = Number(v); return Number.isFinite(n) ? n : fallback; }
function key(seg, sec) { return `${seg}:${sec}`; }
function round(v, d = 4) { return v == null || !Number.isFinite(v) ? null : Number(v.toFixed(d)); }

let authPromise = null;
async function getAccessToken(force = false) {
  if (authPromise) return authPromise;
  if (!CLIENT_ID || !PIN || !TOTP_SECRET) { authStatus = "CONFIG_ERROR"; throw new Error("Set DHAN_CLIENT_ID, DHAN_PIN and DHAN_TOTP_SECRET for automatic TOTP authentication"); }
  if (!force && accessToken && now() < tokenExpiry - TOKEN_REFRESH_BEFORE_MIN * 60000) return accessToken;
  authPromise = (async () => {
    try {
      authStatus = "AUTHENTICATING";
      const totp = authenticator.generate(TOTP_SECRET);
      const url = `https://auth.dhan.co/app/generateAccessToken?dhanClientId=${encodeURIComponent(CLIENT_ID)}&pin=${encodeURIComponent(PIN)}&totp=${encodeURIComponent(totp)}`;
      const r = await fetch(url, { method: "POST", headers: { Accept: "application/json", "Content-Type": "application/json" } });
      const text = await r.text(); let j; try { j = JSON.parse(text); } catch { throw new Error(`Dhan auth returned non-JSON: ${text.slice(0, 200)}`); }
      if (!r.ok || !j.accessToken) { authStatus = `FAILED_${r.status}`; authLastError = j.errorMessage || j.message || text.slice(0, 200); throw new Error(`Dhan auth failed ${r.status}: ${authLastError}`); }
      accessToken = j.accessToken; authStatus = "AUTHENTICATED_TOTP"; authLastError = null; authLastSuccessAt = now();
      tokenExpiry = j.expiryTime ? new Date(j.expiryTime).getTime() : now() + 23 * 3600000;
      log("Dhan access token generated via TOTP; expires", j.expiryTime || new Date(tokenExpiry).toISOString());
      return accessToken;
    } catch (e) { authStatus = "AUTH_ERROR"; authLastError = e.message; throw e; }
    finally { authPromise = null; }
  })();
  return authPromise;
}

async function dhanPost(path, body) {
  const token = await getAccessToken();
  const headers = { "Content-Type": "application/json", Accept: "application/json", "access-token": token };
  if (CLIENT_ID) headers["client-id"] = CLIENT_ID;
  const r = await fetch(`${API_BASE_URL}${path}`, { method: "POST", headers, body: JSON.stringify(body) });
  const text = await r.text();
  let j; try { j = JSON.parse(text); } catch { j = { raw: text }; }
  if (!r.ok) throw new Error(`Dhan ${r.status}: ${text.slice(0, 400)}`);
  return j;
}

async function dhanGet(path) {
  const token = await getAccessToken();
  const headers = { Accept: "application/json", "access-token": token };
  if (CLIENT_ID) headers["client-id"] = CLIENT_ID;
  const r = await fetch(`${API_BASE_URL}${path}`, { headers });
  const text = await r.text();
  let j; try { j = JSON.parse(text); } catch { j = { raw: text }; }
  if (!r.ok) throw new Error(`Dhan ${r.status}: ${text.slice(0, 400)}`);
  return j;
}

function parseCsvLine(line) {
  const out = []; let cur = ""; let q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') { if (q && line[i + 1] === '"') { cur += '"'; i++; } else q = !q; }
    else if (ch === ',' && !q) { out.push(cur); cur = ""; }
    else cur += ch;
  }
  out.push(cur); return out;
}

async function loadInstrumentMaster() {
  const url = "https://images.dhan.co/api-data/api-scrip-master.csv";
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Instrument master ${r.status}`);
  const text = await r.text();
  const lines = text.split(/\r?\n/).filter(Boolean);
  const headers = parseCsvLine(lines[0]).map(x => x.trim());
  const idx = n => headers.indexOf(n);
  const ix = {
    exch: idx("SEM_EXM_EXCH_ID"), seg: idx("SEM_SEGMENT"), id: idx("SEM_SMST_SECURITY_ID"),
    inst: idx("SEM_INSTRUMENT_NAME"), expiry: idx("SEM_EXPIRY_DATE"), strike: idx("SEM_STRIKE_PRICE"),
    opt: idx("SEM_OPTION_TYPE"), trading: idx("SEM_TRADING_SYMBOL"), custom: idx("SEM_CUSTOM_SYMBOL"),
    lot: idx("SEM_LOT_UNITS"), symbol: idx("SM_SYMBOL_NAME"), tick: idx("SEM_TICK_SIZE")
  };
  instruments.clear();
  const segmentMap = { "NSE:I": "IDX_I", "NSE:D": "NSE_FNO", "NSE:E": "NSE_EQ", "BSE:I": "BSE_IDX", "BSE:D": "BSE_FNO", "BSE:E": "BSE_EQ", "MCX:M": "MCX_COMM" };
  for (let i = 1; i < lines.length; i++) {
    const c = parseCsvLine(lines[i]); if (c.length < 8) continue;
    const ex = c[ix.exch], sg = c[ix.seg], es = segmentMap[`${ex}:${sg}`]; if (!es) continue;
    const id = c[ix.id]; if (!id) continue;
    const row = {
      securityId: String(id), exchangeSegment: es, exchange: ex, segmentCode: sg,
      instrument: c[ix.inst] || null, expiry: c[ix.expiry] || null, strike: safeNum(c[ix.strike], 0),
      optionType: c[ix.opt] || null, tradingSymbol: c[ix.trading] || null, customSymbol: c[ix.custom] || null,
      symbol: c[ix.symbol] || null, lotSize: safeNum(c[ix.lot], 1), tickSize: safeNum(c[ix.tick], null)
    };
    instruments.set(key(es, id), row);
  }
  instrumentCacheLoaded = true;
  log(`Loaded ${instruments.size} Dhan instruments`);
}

function underlyingInfo() { return INDEXES[state.indexKey] || INDEXES.NIFTY; }

async function getExpiryList() {
  const x = underlyingInfo();
  const j = await dhanPost("/optionchain/expirylist", { UnderlyingScrip: Number(x.securityId), UnderlyingSeg: x.segment });
  const dates = Array.isArray(j.data) ? j.data : [];
  if (dates.length && (!state.expiry || !dates.includes(state.expiry))) state.expiry = dates[0];
  return dates;
}

async function getOptionChain() {
  const x = underlyingInfo();
  return dhanPost("/optionchain", { UnderlyingScrip: Number(x.securityId), UnderlyingSeg: x.segment, Expiry: state.expiry });
}

function bsTime(expiry) {
  const t = new Date(expiry).getTime() - now();
  return Math.max(t / 86400000 / 365, 1 / (365 * 24 * 60));
}
function normPdf(x) { return Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI); }
function normCdf(x) { return 0.5 * (1 + erf(x / Math.sqrt(2))); }
function erf(x) {
  const sign = x < 0 ? -1 : 1; x = Math.abs(x);
  const a1=0.254829592,a2=-0.284496736,a3=1.421413741,a4=-1.453152027,a5=1.061405429,p=0.3275911;
  const t=1/(1+p*x); const y=1-(((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*Math.exp(-x*x); return sign*y;
}
function hiddenGreeks({ spot, strike, iv, expiry, optionType, rate = 0.06 }) {
  const S = Number(spot), K = Number(strike), sigma = Math.max(Number(iv) / 100, 0.0001), T = bsTime(expiry);
  if (!(S > 0 && K > 0)) return {};
  const isCall = optionType === "CE" || optionType === "CALL";
  const q = 0;
  const d1 = (Math.log(S / K) + (rate - q + sigma*sigma/2)*T) / (sigma*Math.sqrt(T));
  const d2 = d1 - sigma*Math.sqrt(T);
  const pdf = normPdf(d1);
  const gamma = Math.exp(-q*T)*pdf/(S*sigma*Math.sqrt(T));
  const vega = S*Math.exp(-q*T)*pdf*Math.sqrt(T) / 100;
  const theta = (-S*pdf*sigma*Math.exp(-q*T)/(2*Math.sqrt(T)) - (isCall ? rate*K*Math.exp(-rate*T)*normCdf(d2) : -rate*K*Math.exp(-rate*T)*normCdf(-d2))) / 365;
  const delta = isCall ? Math.exp(-q*T)*normCdf(d1) : Math.exp(-q*T)*(normCdf(d1)-1);
  const vanna = -Math.exp(-q*T) * pdf * d2 / sigma / 100;
  const vomma = (vega * d1 * d2) / sigma;
  const charm = isCall
    ? q*Math.exp(-q*T)*normCdf(d1) - Math.exp(-q*T)*pdf*(2*(rate-q)*T-d2*sigma*Math.sqrt(T))/(2*T*sigma*Math.sqrt(T))
    : q*Math.exp(-q*T)*normCdf(-d1) + Math.exp(-q*T)*pdf*(2*(rate-q)*T-d2*sigma*Math.sqrt(T))/(2*T*sigma*Math.sqrt(T));
  const color = -Math.exp(-q*T)*pdf/(2*S*T*sigma*Math.sqrt(T)) * (2*q*T + 1 + ((2*(rate-q)*T-d2*sigma*Math.sqrt(T))*d1)/(sigma*Math.sqrt(T)));
  const speed = -gamma/S * (1 + d1/(sigma*Math.sqrt(T)));
  const zomma = gamma * ((d1*d2 - 1) / sigma);
  return { delta, gamma, vega, theta, vanna, vomma, charm, color, speed, zomma };
}

function optionRow(strike, leg, type, expiry, spot) {
  if (!leg) return null;
  const ltp = safeNum(leg.last_price, 0);
  const oi = safeNum(leg.oi, 0);
  const volume = safeNum(leg.volume, 0);
  const prevOI = safeNum(leg.previous_oi, null);
  const prevVol = safeNum(leg.previous_volume, null);
  const iv = safeNum(leg.implied_volatility, 0);
  const g = hiddenGreeks({ spot, strike, iv, expiry, optionType: type });
  const lot = safeNum(instruments.get(key(underlyingInfo().optionSegment, leg.security_id))?.lotSize, 1);
  const gex = (g.gamma || safeNum(leg.greeks?.gamma, 0)) * oi * lot * spot * spot / 1e7;
  return {
    strike, type, securityId: String(leg.security_id), ltp, oi, previousOI: prevOI, changeOI: prevOI==null?null:oi-prevOI,
    volume, previousVolume: prevVol, changeVolume: prevVol==null?null:volume-prevVol, iv,
    bid: safeNum(leg.top_bid_price, 0), ask: safeNum(leg.top_ask_price, 0),
    bidQty: safeNum(leg.top_bid_quantity, 0), askQty: safeNum(leg.top_ask_quantity, 0),
    averagePrice: safeNum(leg.average_price, 0),
    dhanGreeks: leg.greeks || {}, hiddenGreeks: g, gexProxy: round(gex, 3),
    notionalOI: round(oi*lot*strike, 2)
  };
}

function buildAnalytics(rows, spot) {
  const ce = rows.filter(x => x.type === "CE");
  const pe = rows.filter(x => x.type === "PE");
  const sum = (a,k) => a.reduce((s,x)=>s+(Number(x[k])||0),0);
  const callOI=sum(ce,"oi"), putOI=sum(pe,"oi"), callVol=sum(ce,"volume"), putVol=sum(pe,"volume");
  const callChOI=sum(ce,"changeOI"), putChOI=sum(pe,"changeOI");
  const pcr = callOI ? putOI/callOI : null;
  const pcrVol = callVol ? putVol/callVol : null;
  const maxCallWall = ce.reduce((a,b)=>!a||b.oi>a.oi?b:a,null);
  const maxPutWall = pe.reduce((a,b)=>!a||b.oi>a.oi?b:a,null);
  const maxCallChWall = ce.reduce((a,b)=>!a||b.changeOI>a.changeOI?b:a,null);
  const maxPutChWall = pe.reduce((a,b)=>!a||b.changeOI>a.changeOI?b:a,null);
  const atm = rows.reduce((a,b)=>!a||Math.abs(b.strike-spot)<Math.abs(a.strike-spot)?b:a,null)?.strike || null;
  const strikes=[...new Set(rows.map(x=>x.strike))].sort((a,b)=>a-b);
  let minPain=null, minPainVal=Infinity;
  for(const k of strikes){ let pain=0; for(const x of ce) pain += Math.max(0,k-x.strike)*x.oi; for(const x of pe) pain += Math.max(0,x.strike-k)*x.oi; if(pain<minPainVal){minPainVal=pain;minPain=k;} }
  const gexCE=sum(ce,"gexProxy"), gexPE=sum(pe,"gexProxy");
  const dealerProxy = gexCE - gexPE;
  const ivAtm = rows.filter(x=>x.strike===atm).reduce((s,x)=>s+(x.iv||0),0)/(rows.filter(x=>x.strike===atm).length||1);
  const smile = strikes.map(k=>{const r=rows.filter(x=>x.strike===k);return {strike:k,ceIV:r.find(x=>x.type==='CE')?.iv??null,peIV:r.find(x=>x.type==='PE')?.iv??null};});
  return {
    atm, pcr:round(pcr,4), pcrVolume:round(pcrVol,4), callOI, putOI, callChangeOI:callChOI, putChangeOI:putChOI,
    callVolume:callVol, putVolume:putVol, callChangeVolume:sum(ce,"changeVolume"), putChangeVolume:sum(pe,"changeVolume"),
    callOIWall:maxCallWall?.strike??null, putOIWall:maxPutWall?.strike??null,
    callChangeOIWall:maxCallChWall?.strike??null, putChangeOIWall:maxPutChWall?.strike??null,
    maxPain:minPain, maxPainValue:minPainVal===Infinity?null:minPainVal, atmIV:round(ivAtm,3),
    totalGEXProxy:round(dealerProxy,3), callGEXProxy:round(gexCE,3), putGEXProxy:round(gexPE,3),
    dealerHedgePressureProxy: round(-dealerProxy,3), smile,
    interpretation: dealerProxy>0?"positive-gamma proxy / stabilizing hedge tendency":dealerProxy<0?"negative-gamma proxy / amplifying hedge tendency":"neutral gamma proxy"
  };
}

async function refreshChain() {
  if (!ENABLE_OPTION_CHAIN || chainBusy) return; chainBusy = true;
  try {
    if (!state.expiry) await getExpiryList();
    const j = await getOptionChain();
    const spot = safeNum(j?.data?.last_price, state.spot); if (spot) state.spot=spot;
    const oc=j?.data?.oc||{}; const rows=[];
    for(const [ks,row] of Object.entries(oc)){
      const strike=Number(ks); if(!Number.isFinite(strike)) continue;
      const ce=optionRow(strike,row.ce,"CE",state.expiry,state.spot); const pe=optionRow(strike,row.pe,"PE",state.expiry,state.spot);
      if(ce) rows.push(ce); if(pe) rows.push(pe);
      if(ce) optionMeta.set(key(underlyingInfo().optionSegment,ce.securityId),ce); if(pe) optionMeta.set(key(underlyingInfo().optionSegment,pe.securityId),pe);
    }
    state.chain={updatedAt:now(),expiry:state.expiry,rows,atm:buildAnalytics(rows,state.spot).atm,maxPain:buildAnalytics(rows,state.spot).maxPain};
    state.analytics=buildAnalytics(rows,state.spot);
    state.option=selectPrimaryOption(rows);
    await syncMarketSubscriptions(rows);
    broadcast({type:"optionChain",chain:clone(state.chain),analytics:clone(state.analytics)});
    broadcast({type:"state",state:publicState()});
  } catch(e){ log("Option chain:",e.message); broadcast({type:"error",message:e.message}); }
  finally { chainBusy=false; }
}

function selectPrimaryOption(rows){
  const side = state.analytics?.pcr != null && state.analytics.pcr < 0.9 ? "CE" : "PE";
  const pool=rows.filter(x=>x.type===side&&x.ltp>0&&x.ltp>=5&&x.ltp<=30);
  const sorted=(pool.length?pool:rows.filter(x=>x.type===side&&x.ltp>0)).sort((a,b)=>Math.abs(a.strike-state.spot)-Math.abs(b.strike-state.spot));
  return sorted[0]||null;
}

async function syncMarketSubscriptions(rows){
  const base=AUTO_SUBSCRIBE_INDICES?[{ExchangeSegment:underlyingInfo().feedSegment,SecurityId:underlyingInfo().securityId}]:[];
  const near=AUTO_SUBSCRIBE_OPTIONS?[...new Map(rows.map(x=>[x.securityId,x])).values()].sort((a,b)=>Math.abs(a.strike-state.spot)-Math.abs(b.strike-state.spot)).slice(0,Math.max(0,Math.min(600,MAX_INSTRUMENTS_PER_WS,MAX_SUBSCRIPTIONS-base.length))):[];
  const inst=[...base,...near.map(x=>({ExchangeSegment:underlyingInfo().optionSegment,SecurityId:x.securityId}))].slice(0,MAX_SUBSCRIPTIONS);
  state.subscriptions=inst; subscribe(dhanWs,inst,21);
  syncDepthSubscriptions(near.slice(0,20));
}

function subscribe(ws, insts, requestCode=21){
  if(!ws||ws.readyState!==WebSocket.OPEN||!insts?.length)return {sent:0,requested:0};
  const valid=insts.filter(x=>x&&x.ExchangeSegment&&String(x.SecurityId||"").trim());
  const unique=[...new Map(valid.map(x=>[key(x.ExchangeSegment,String(x.SecurityId)),{ExchangeSegment:String(x.ExchangeSegment),SecurityId:String(x.SecurityId)}])).values()];
  let sent=0;
  for(let i=0;i<unique.length;i+=Math.min(100,MAX_INSTRUMENTS_PER_MESSAGE)){
    const a=unique.slice(i,i+Math.min(100,MAX_INSTRUMENTS_PER_MESSAGE));
    try{ws.send(JSON.stringify({RequestCode:requestCode,InstrumentCount:a.length,InstrumentList:a}));sent+=a.length;}
    catch(e){log("Subscription send failed",e.message);}
  }
  if(sent){lastSubscriptionAt=now();state.subscriptionDiagnostics={requestCode,requested:unique.length,sent,at:lastSubscriptionAt};}
  return {sent,requested:unique.length};
}

function decodeOne(buf){
  if(buf.length<8)return null;
  const dv=new DataView(buf.buffer,buf.byteOffset,buf.byteLength);
  const code=dv.getUint8(0), seg=dv.getUint8(3), sec=dv.getUint32(4,true), len=dv.getUint16(1,true)||buf.length;
  const f=o=>dv.getFloat32(o,true), i=o=>dv.getInt32(o,true), u=o=>dv.getUint32(o,true), i16=o=>dv.getInt16(o,true), u16=o=>dv.getUint16(o,true);
  const base={code,seg,sec,len};
  if(code===1&&buf.length>=16)return {...base,type:"index",ltp:f(8),ltt:u(12)};
  if(code===2&&buf.length>=16)return {...base,type:"ticker",ltp:f(8),ltt:u(12)};
  if(code===4&&buf.length>=51)return {...base,type:"quote",ltp:f(8),ltq:u16(12),ltt:u(14),avgPrice:f(18),volume:u(22),sellQty:u(26),buyQty:u(30),open:f(34),close:f(38),high:f(42),low:f(46)};
  if(code===5&&buf.length>=12)return {...base,type:"oi",oi:u(8)};
  if(code===6&&buf.length>=16)return {...base,type:"prev",prevClose:f(8),prevOI:u(12)};
  if(code===7)return {...base,type:"marketStatus",statusCode:buf.length>=10?u16(8):null};
  if(code===8&&buf.length>=163){
    const depth=[]; for(let n=0;n<5;n++){const o=63+n*20;depth.push({bidQty:i(o),askQty:i(o+4),bidOrders:i16(o+8),askOrders:i16(o+10),bid:f(o+12),ask:f(o+16)});}
    return {...base,type:"full",ltp:f(8),ltq:u16(12),ltt:u(14),avgPrice:f(18),volume:u(22),sellQty:u(26),buyQty:u(30),oi:u(34),oiDayHigh:u(38),oiDayLow:u(42),open:f(46),close:f(50),high:f(54),low:f(58),depth};
  }
  if(code===50)return {...base,type:"disconnect",disconnectCode:buf.length>=10?u16(8):null};
  return base;
}

function decodeMessage(data){
  const b=Buffer.from(data); const out=[]; let off=0;
  while(off+8<=b.length){
    const declared=b.readUInt16LE(off+1);
    const len=declared|| (b.length-off);
    if(len<8||off+len>b.length){decodeErrorCount++;log("Invalid/truncated Dhan packet frame",{offset:off,declared,total:b.length});break;}
    const packet=b.subarray(off,off+len);
    try{const x=decodeOne(packet);if(x){out.push(x);decodedPacketCount++;}else decodeErrorCount++;}
    catch(e){decodeErrorCount++;log("Dhan packet decode error",e.message);}
    off+=len;
  }
  if(off!==b.length&&b.length-off>0&&b.length-off<8)decodeErrorCount++;
  return out;
}

// Dhan depth packet: 12-byte header; 41=bid side, 51=ask side; each row is 16 bytes.
function decodeDepthMessage(data, maxLevels) {
  const b = Buffer.from(data), out = []; let off = 0;
  while (off + 12 <= b.length) {
    const len = b.readUInt16LE(off) || (b.length - off); if (len < 12 || off + len > b.length) break;
    const code=b.readUInt8(off+2), seg=b.readUInt8(off+3), sec=b.readUInt32LE(off+4), rowCount=b.readUInt32LE(off+8);
    if (code===41 || code===51) {
      const available=Math.floor((len-12)/16), count=Math.min(maxLevels,available,maxLevels===200&&rowCount>0?rowCount:maxLevels), levels=[];
      for(let i=0;i<count;i++){const p=off+12+i*16,price=b.readDoubleLE(p),quantity=b.readUInt32LE(p+8),orders=b.readUInt32LE(p+12);if(price>0)levels.push({price,quantity,orders});}
      out.push({side:code===41?"bid":"ask",seg,sec,levels});
    }
    off+=len;
  }
  return out;
}
function updateDepthSide(packet, levelKey) {
  const segName=({0:"IDX_I",1:"NSE_EQ",2:"NSE_FNO",3:"NSE_CURRENCY",4:"BSE_EQ",5:"MCX_COMM",7:"BSE_CURRENCY",8:"BSE_FNO"})[packet.seg]||packet.seg;
  const k=key(segName,packet.sec), cache=depthSideCache.get(k)||{bid20:[],ask20:[],bid200:[],ask200:[]};
  const sideStamp=now();
  cache[`${packet.side}${levelKey}`]=packet.levels;
  cache[`${packet.side}${levelKey}UpdatedAt`]=sideStamp;
  depthSideCache.set(k,cache);
  const bids=cache[`bid${levelKey}`]||[], asks=cache[`ask${levelKey}`]||[], count=Math.max(bids.length,asks.length), merged=[];
  for(let i=0;i<count;i++)merged.push({bid:bids[i]?.price||0,bidQty:bids[i]?.quantity||0,bidOrders:bids[i]?.orders||0,ask:asks[i]?.price||0,askQty:asks[i]?.quantity||0,askOrders:asks[i]?.orders||0});
  const t=ticks.get(k)||{securityId:String(packet.sec),exchangeSegment:segName};
  if(levelKey==="20")t.depth20=merged; else t.depth200=merged;
  if(levelKey==="20"){
    t.depthBidUpdatedAt=cache.bid20UpdatedAt||null;
    t.depthAskUpdatedAt=cache.ask20UpdatedAt||null;
    t.depthUpdatedAt=Math.max(t.depthBidUpdatedAt||0,t.depthAskUpdatedAt||0)||null;
  } else t.depth200UpdatedAt=sideStamp;
  t.depth=merged.slice(0,20);ticks.set(k,t);
  broadcast({type:"depth",segment:segName,securityId:String(packet.sec),levels:levelKey,data:merged,updatedAt:sideStamp,bidUpdatedAt:cache[`bid${levelKey}UpdatedAt`]||null,askUpdatedAt:cache[`ask${levelKey}UpdatedAt`]||null});
}

function updateTick(p){
  if(!p||p.sec==null)return;
  const segName=Object.entries({0:"IDX_I",1:"NSE_EQ",2:"NSE_FNO",3:"NSE_CURRENCY",4:"BSE_EQ",5:"MCX_COMM",7:"BSE_CURRENCY",8:"BSE_FNO"}).find(([,n])=>n===p.seg)?.[1]||p.seg;
  const receivedAt=now();
  if(p.type!=="prev")lastAnyTickAt=receivedAt;
  if(p.type!=="prev"&&(p.type==="index"||segName==="IDX_I"))lastIndexTickAt=receivedAt;
  else if(p.type!=="prev"&&(segName==="NSE_FNO"||segName==="BSE_FNO")){
    lastOptionTickAt=receivedAt;
    const metaForTick=instruments.get(key(segName,p.sec));
    if(String(metaForTick?.instrument||"").toUpperCase().includes("FUT"))lastFutureTickAt=receivedAt;
  }
  const k=key(segName,p.sec); let t=ticks.get(k)||{securityId:String(p.sec),exchangeSegment:segName,securityIdNum:p.sec};
  Object.assign(t,{updatedAt:receivedAt,receivedAt,packetType:p.type||"unknown"});
  if(p.ltp!=null){t.ltp=p.ltp;t.ltpUpdatedAt=now();} if(p.ltt!=null)t.ltt=p.ltt*1000; if(p.ltq!=null)t.lastTradeQty=p.ltq;
  if(p.avgPrice!=null)t.avgPrice=p.avgPrice; if(p.volume!=null){if(t.sessionStartVolume==null)t.sessionStartVolume=p.volume;t.volume=p.volume;t.volumeUpdatedAt=now();t.changeVolume=p.volume-t.sessionStartVolume;}
  if(p.sellQty!=null)t.sellQuantity=p.sellQty; if(p.buyQty!=null)t.buyQuantity=p.buyQty;
  for(const k0 of ["open","close","high","low","oi","oiDayHigh","oiDayLow","prevClose","prevOI"])if(p[k0]!=null){t[k0]=p[k0];if(k0==="oi")t.oiUpdatedAt=now();if(k0==="prevOI")t.prevOIUpdatedAt=now();}
  if(t.oi!=null){if(t.sessionStartOI==null)t.sessionStartOI=t.oi;t.changeOI=t.oi-t.sessionStartOI;}
  if(p.depth)t.depth=p.depth;
  if(p.type==="prev"){t.prevClose=p.prevClose;t.prevOI=p.prevOI;t.prevOIUpdatedAt=now();if(t.oi!=null)t.changeOI=t.oi-p.prevOI;}
  ticks.set(k,t);
  if(ticks.size>MAX_TICK_CACHE_ITEMS){const oldest=ticks.keys().next().value;if(oldest!==k)ticks.delete(oldest);}
  state.server.ticks++;
  if(segName==="IDX_I"&&String(p.sec)===underlyingInfo().securityId){state.spot=p.ltp??state.spot;state.lastTick=now();}
  const meta=optionMeta.get(k); if(meta){Object.assign(meta,{ltp:t.ltp,volume:t.volume,changeVolume:t.changeVolume,oi:t.oi,changeOI:t.changeOI,bid:t.depth?.[0]?.bid??meta.bid,ask:t.depth?.[0]?.ask??meta.ask});}
  if(p.ltp!=null)updateCandle(k,t);
}

function updateCandle(k,t){
  if(!(Number(t.ltp)>0))return;
  const eventTime=Number(t.ltt)||now();
  const cumulative=Number(t.volume);
  const previous=candleVolumeState.get(k);
  let delta=Number(t.lastTradeQty??t.ltq)||0;
  if(!(delta>0)&&Number.isFinite(cumulative)&&previous!=null)delta=Math.max(0,cumulative-previous);
  if(Number.isFinite(cumulative))candleVolumeState.set(k,cumulative);
  for(const tf of CANDLE_TIMEFRAMES_MS){
    const seriesKey=`${k}|${tf}`,tm=Math.floor(eventTime/tf)*tf;
    let arr=candleSeries.get(seriesKey)||[],c=arr[arr.length-1];
    if(!c||c.time!==tm){c={time:tm,open:Number(t.ltp),high:Number(t.ltp),low:Number(t.ltp),close:Number(t.ltp),volume:Math.max(0,delta),timeframeMs:tf,source:"DHAN_LIVE_TICKS"};arr=[...arr,c];}
    else{c.high=Math.max(c.high,Number(t.ltp));c.low=Math.min(c.low,Number(t.ltp));c.close=Number(t.ltp);c.volume=(Number(c.volume)||0)+Math.max(0,delta);}
    if(arr.length>MAX_CANDLES_PER_INSTRUMENT)arr=arr.slice(-MAX_CANDLES_PER_INSTRUMENT);
    candleSeries.set(seriesKey,arr);
    if(tf===CANDLE_TIMEFRAME_MS)candles.set(k,arr);
  }
}

function publicState(){return {...state,ticks:undefined};}
function feedOpen(){return dhanWs&&dhanWs.readyState===WebSocket.OPEN;}

function connectFeed(){
  clearTimeout(reconnectTimer); try{dhanWs?.close();}catch{}
  if (!liveFeedEnabled) { state.dhanConnected=false; return; }
  getAccessToken().then(token=>{
    if(!liveFeedEnabled)return;
    const url=`${DHAN_LIVE_FEED_URL}?version=2&token=${encodeURIComponent(token)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`;
    dhanWs=new WebSocket(url);
    dhanWs.on("open",()=>{state.dhanConnected=true;dhanConnectedAt=now();state.server.reconnects++;log("Dhan live feed connected (authenticated)");subscribe(dhanWs,state.subscriptions.length?state.subscriptions:[{ExchangeSegment:underlyingInfo().feedSegment,SecurityId:underlyingInfo().securityId}],21);broadcast({type:"state",state:publicState()});});
    dhanWs.on("message",data=>{
      lastPacketAt=now();
      for(const p of decodeMessage(data)){
        state.server.packets++;
        packetCounts[p.type||("code_"+p.code)]=(packetCounts[p.type||("code_"+p.code)]||0)+1;
        if(p.type==="disconnect"){state.dhanConnected=false;state.lastDisconnectCode=p.disconnectCode??null;log("Dhan feed disconnect packet",p.disconnectCode);try{dhanWs?.close();}catch{}continue;}
        if(p.type==="marketStatus"){state.marketStatus=p.statusCode;continue;}
        if(p.type==="index"||p.type==="ticker"||p.type==="quote"||p.type==="oi"||p.type==="prev"||p.type==="full")updateTick(p);
        else unknownPacketCount++;
      }
    });
    dhanWs.on("close",(code,reason)=>{state.dhanConnected=false;state.lastDisconnectCode=code||null;log(`Dhan feed closed code=${code} reason=${reason?.toString()||""}`);broadcast({type:"state",state:publicState()});if(liveFeedEnabled)reconnectTimer=setTimeout(connectFeed,5000);});
    dhanWs.on("error",e=>log("Dhan feed error",e.message));
  }).catch(e=>{log("Dhan auth failed",e.message);state.dhanConnected=false;broadcast({type:"error",message:e.message});reconnectTimer=setTimeout(connectFeed,10000);});
}

function connectDepth(){
  if (!ENABLE_20_DEPTH || !liveFeedEnabled) return;
  clearTimeout(depthReconnectTimer); try{depthWs?.close();}catch{}
  getAccessToken().then(token=>{
    if(!liveFeedEnabled)return;
    depthWs=new WebSocket(`${DHAN_20_DEPTH_URL}?token=${encodeURIComponent(token)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`);
    depthWs.on("open",()=>{state.depthConnected=true;depthConnectedAt=now();syncDepthSubscriptions([...state.chain.rows].sort((a,b)=>Math.abs(a.strike-state.spot)-Math.abs(b.strike-state.spot)).slice(0,20));});
    depthWs.on("message",data=>{lastDepthPacketAt=now();for(const p of decodeDepthMessage(data,20))updateDepthSide(p,"20");});
    depthWs.on("close",()=>{state.depthConnected=false;if(liveFeedEnabled)depthReconnectTimer=setTimeout(connectDepth,7000);});
    depthWs.on("error",e=>log("Depth feed error",e.message));
  }).catch(e=>log("Depth auth",e.message));
}
function autoPinSnapshotFutures(){
  const futureSubscriptions=[];
  for(const name of ["NIFTY","SENSEX"]){
    const cfg=INDEXES[name];if(!cfg)continue;
    const candidates=[...instruments.values()].filter(x=>{
      const text=[x.symbol,x.tradingSymbol,x.customSymbol].filter(Boolean).join(" ").toUpperCase();
      const instrument=String(x.instrument||"").toUpperCase();
      const expiry=x.expiry?new Date(x.expiry).getTime():Infinity;
      return x.exchangeSegment===cfg.optionSegment&&instrument.includes("FUT")&&text.includes(name)&&(!Number.isFinite(expiry)||expiry>=now()-86400000);
    }).sort((a,b)=>{
      const ax=a.expiry?new Date(a.expiry).getTime():Infinity,bx=b.expiry?new Date(b.expiry).getTime():Infinity;
      return ax-bx;
    });
    const chosen=candidates[0];if(!chosen)continue;
    for(const [oldKey,oldSub] of pinnedDepthInstruments){
      const oldMeta=instruments.get(oldKey);
      const oldText=[oldMeta?.symbol,oldMeta?.tradingSymbol,oldMeta?.customSymbol].filter(Boolean).join(" ").toUpperCase();
      if(oldSub.ExchangeSegment===cfg.optionSegment&&String(oldMeta?.instrument||"").toUpperCase().includes("FUT")&&oldText.includes(name))pinnedDepthInstruments.delete(oldKey);
    }
    const sub={ExchangeSegment:chosen.exchangeSegment,SecurityId:String(chosen.securityId)};
    pinnedDepthInstruments.set(key(sub.ExchangeSegment,sub.SecurityId),sub);futureSubscriptions.push(sub);
    log("Snapshot future pinned",name,chosen.tradingSymbol||chosen.customSymbol||chosen.securityId,chosen.expiry||"expiry unavailable");
  }
  if(futureSubscriptions.length){
    state.subscriptions=[...new Map([...state.subscriptions,...futureSubscriptions].map(x=>[key(x.ExchangeSegment,x.SecurityId),x])).values()].slice(0,MAX_SUBSCRIPTIONS);
    subscribe(dhanWs,futureSubscriptions,21);
    syncDepthSubscriptions(state.chain.rows||[]);
  }else log("Snapshot future pinning: no matching NIFTY/SENSEX futures in instrument master");
}

function syncDepthSubscriptions(rows){
  if(!depthWs||depthWs.readyState!==WebSocket.OPEN)return;
  const optionInst=rows.slice(0,20).map(x=>({ExchangeSegment:underlyingInfo().optionSegment,SecurityId:String(x.securityId)}));
  const inst=[...new Map([...pinnedDepthInstruments.values(),...optionInst].map(x=>[key(x.ExchangeSegment,x.SecurityId),x])).values()].slice(0,50);
  for(let i=0;i<inst.length;i+=50){const a=inst.slice(i,i+50);try{depthWs.send(JSON.stringify({RequestCode:23,InstrumentCount:a.length,InstrumentList:a}));}catch{}}
}

function connect200Depth(target) {
  if (!ENABLE_200_DEPTH) throw new Error("200-level depth disabled by ENABLE_200_DEPTH");
  if (!liveFeedEnabled) throw new Error("Backend is in failover standby mode");
  const clean={ExchangeSegment:String(target.ExchangeSegment||target.exchangeSegment||""),SecurityId:String(target.SecurityId||target.securityId||"")};
  if(!clean.ExchangeSegment||!clean.SecurityId)throw new Error("ExchangeSegment and SecurityId are required");
  if(!["NSE_EQ","NSE_FNO"].includes(clean.ExchangeSegment))throw new Error("Dhan 200-level depth is currently supported only for NSE_EQ and NSE_FNO segments");
  if(depth200Target&&key(depth200Target.ExchangeSegment,depth200Target.SecurityId)===key(clean.ExchangeSegment,clean.SecurityId)&&depth200Ws?.readyState===WebSocket.OPEN)return;
  clearTimeout(depth200ReconnectTimer);try{depth200Ws?.close();}catch{}depth200Target=clean;
  getAccessToken().then(token=>{
    depth200Ws=new WebSocket(`${DHAN_200_DEPTH_URL}?token=${encodeURIComponent(token)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`);
    depth200Ws.on("open",()=>{depth200Connected=true;depth200Ws.send(JSON.stringify({RequestCode:23,ExchangeSegment:clean.ExchangeSegment,SecurityId:clean.SecurityId}));broadcast({type:"depth200Status",connected:true,target:clean});log("200-level depth connected",clean);});
    depth200Ws.on("message",data=>{lastDepthPacketAt=now();for(const p of decodeDepthMessage(data,200))updateDepthSide(p,"200");});
    depth200Ws.on("close",()=>{depth200Connected=false;broadcast({type:"depth200Status",connected:false,target:clean});if(liveFeedEnabled)depth200ReconnectTimer=setTimeout(()=>{if(depth200Target&&liveFeedEnabled)connect200Depth(depth200Target);},Number(process.env.RECONNECT_DELAY_MS||3000));});
    depth200Ws.on("error",e=>log("200-depth feed error",e.message));
  }).catch(e=>{depth200Connected=false;log("200-depth auth",e.message);});
}


function snapshotIndiaDate(ts=now()) {
  const parts=new Intl.DateTimeFormat("en-CA",{timeZone:"Asia/Kolkata",year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(new Date(ts));
  const p=Object.fromEntries(parts.map(x=>[x.type,x.value]));
  return `${p.year}-${p.month}-${p.day}`;
}
function snapshotIndiaClock(ts=now()) {
  const p=Object.fromEntries(new Intl.DateTimeFormat("en-GB",{timeZone:"Asia/Kolkata",hour:"2-digit",minute:"2-digit",weekday:"short",hour12:false}).formatToParts(new Date(ts)).map(x=>[x.type,x.value]));
  return {weekday:p.weekday,minute:Number(p.hour)*60+Number(p.minute)};
}
function snapshotSessionOpen(ts=now()) {
  if(!SNAPSHOT_MARKET_HOURS_ONLY)return true;
  const c=snapshotIndiaClock(ts);
  return !["Sat","Sun"].includes(c.weekday)&&c.minute>=555&&c.minute<=930;
}
function snapshotFile(date,compressed=false){return path.join(SNAPSHOT_DIR,date+(compressed?".jsonl.gz":".jsonl"));}
async function listSnapshotFiles(){
  try{return (await fsp.readdir(SNAPSHOT_DIR)).filter(n=>/^\d{4}-\d{2}-\d{2}\.jsonl(?:\.gz)?$/.test(n)).sort();}
  catch(e){if(e.code==="ENOENT")return [];throw e;}
}
async function compressSnapshotFile(date){
  const src=snapshotFile(date,false),dst=snapshotFile(date,true);
  try{await fsp.access(src);try{await fsp.access(dst);await fsp.unlink(src);return;}catch{}}
  catch(e){if(e.code==="ENOENT")return;throw e;}
  const temp=dst+".tmp";
  try{await pipeline(fs.createReadStream(src),createGzip({level:6}),fs.createWriteStream(temp,{flags:"w"}));await fsp.rename(temp,dst);await fsp.unlink(src);}
  catch(e){try{await fsp.unlink(temp);}catch{}throw e;}
}
async function maintainSnapshotFiles(){
  const today=snapshotIndiaDate(),files=await listSnapshotFiles();
  const cutoff=Date.parse(today+"T00:00:00+05:30")-SNAPSHOT_RETENTION_DAYS*86400000;
  for(const name of files){
    const date=name.slice(0,10),stamp=Date.parse(date+"T00:00:00+05:30");
    if(Number.isFinite(stamp)&&stamp<cutoff){try{await fsp.unlink(path.join(SNAPSHOT_DIR,name));}catch(e){log("Snapshot retention delete failed:",e.message);}continue;}
    if(name.endsWith(".jsonl")&&date!==today){try{await compressSnapshotFile(date);}catch(e){log("Snapshot compression failed:",e.message);}}
  }
}
function pickSnapshotField(t,row,field,timeField,chainTime){
  const tickValue=t?.[field],tickTime=t?.[timeField]||0;
  if(tickValue!=null&&tickTime>=(chainTime||0))return {value:tickValue,updatedAt:tickTime,source:"tick"};
  return {value:row?.[field]??null,updatedAt:chainTime||null,source:"option-chain"};
}
function buildSnapshotRecord(ts){
  const index=state.indexKey,under=underlyingInfo(),chainTime=state.chain.updatedAt||null;
  const allRows=state.chain.rows||[];
  const strikes=[...new Set(allRows.map(r=>Number(r.strike)).filter(Number.isFinite))].sort((a,b)=>a-b);
  const spot=Number(state.spot)||null;
  const atm=state.chain.atm||state.analytics?.atm||(spot&&strikes.length?strikes.reduce((a,b)=>Math.abs(b-spot)<Math.abs(a-spot)?b:a):null);
  let atmIndex=atm==null?-1:strikes.indexOf(Number(atm));
  if(atmIndex<0&&atm!=null)atmIndex=0;
  const selectedStrikes=atmIndex<0?new Set():new Set(strikes.slice(Math.max(0,atmIndex-SNAPSHOT_OPTION_STRIKES_EACH_SIDE),atmIndex+SNAPSHOT_OPTION_STRIKES_EACH_SIDE+1));
  const options=allRows.filter(r=>selectedStrikes.has(Number(r.strike))).map(r=>{
    const t=ticks.get(key(under.optionSegment,r.securityId));
    const ltp=pickSnapshotField(t,r,"ltp","ltpUpdatedAt",chainTime);
    const oi=pickSnapshotField(t,r,"oi","oiUpdatedAt",chainTime);
    const volume=pickSnapshotField(t,r,"volume","volumeUpdatedAt",chainTime);
    const prevOI=r.previousOI==null?null:Number(r.previousOI),prevVolume=r.previousVolume==null?null:Number(r.previousVolume);
    return {strike:r.strike,type:r.type,securityId:String(r.securityId),ltp:ltp.value,ltpUpdatedAt:ltp.updatedAt,ltpExchangeTime:t?.ltt??null,ltpSource:ltp.source,oi:oi.value,oiUpdatedAt:oi.updatedAt,oiSource:oi.source,previousOI:prevOI,changeOI:oi.value==null||prevOI==null?null:Number(oi.value)-prevOI,volume:volume.value,volumeUpdatedAt:volume.updatedAt,volumeSource:volume.source,previousVolume:prevVolume,changeVolume:volume.value==null||prevVolume==null?null:Number(volume.value)-prevVolume,iv:r.iv??null,bid:r.bid??null,ask:r.ask??null,bidQty:r.bidQty??null,askQty:r.askQty??null};
  });
  const sum=(type,field)=>{const values=options.filter(r=>r.type===type&&r[field]!=null&&Number.isFinite(Number(r[field]))).map(r=>Number(r[field]));return values.length?values.reduce((a,v)=>a+v,0):null;};
  const coverage=(type,field)=>options.filter(r=>r.type===type&&r[field]!=null).length;
  const futures=[...pinnedDepthInstruments.values()].map(x=>{
    const k=key(x.ExchangeSegment,x.SecurityId),t=ticks.get(k)||{},meta=instruments.get(k)||{};
    const bidAt=t.depthBidUpdatedAt||null,askAt=t.depthAskUpdatedAt||null,depthAt=t.depthUpdatedAt||null;
    return {exchangeSegment:x.ExchangeSegment,securityId:String(x.SecurityId),symbol:meta.tradingSymbol||meta.customSymbol||meta.symbol||null,expiry:meta.expiry||null,lotSize:meta.lotSize||null,ltp:t.ltp??null,ltpUpdatedAt:t.ltpUpdatedAt||null,ltpExchangeTime:t.ltt??null,volume:t.volume??null,volumeUpdatedAt:t.volumeUpdatedAt||null,oi:t.oi??null,oiUpdatedAt:t.oiUpdatedAt||null,changeOI:t.changeOI??null,changeVolume:t.changeVolume??null,depthUpdatedAt:depthAt,depthAgeMs:depthAt?ts-depthAt:null,bidUpdatedAt:bidAt,askUpdatedAt:askAt,bidAgeMs:bidAt?ts-bidAt:null,askAgeMs:askAt?ts-askAt:null,tickAgeMs:t.ltpUpdatedAt?ts-t.ltpUpdatedAt:null,depth20:t.depth20||t.depth||[]};
  });
  const activeFuture=futures.find(f=>f.symbol&&f.symbol.toUpperCase().includes(index))||futures.find(f=>f.exchangeSegment===under.optionSegment)||futures[0]||null;
  const chainAgeMs=chainTime?ts-chainTime:null;
  return {schemaVersion:1,ts,iso:new Date(ts).toISOString(),indiaDate:snapshotIndiaDate(ts),index,expiry:state.expiry||null,spot,atm,market:{dhanConnected:!!state.dhanConnected,depthConnected:!!state.depthConnected,marketStatus:state.marketStatus??null,lastTick:state.lastTick||null,tickAgeMs:state.lastTick?ts-state.lastTick:null},chain:{updatedAt:chainTime,ageMs:chainAgeMs,stale:chainAgeMs==null||chainAgeMs>10000,rowsCaptured:options.length,coverage:{callOI:coverage("CE","oi"),putOI:coverage("PE","oi"),callChangeOI:coverage("CE","changeOI"),putChangeOI:coverage("PE","changeOI"),callVolume:coverage("CE","volume"),putVolume:coverage("PE","volume"),callChangeVolume:coverage("CE","changeVolume"),putChangeVolume:coverage("PE","changeVolume")},callOI:sum("CE","oi"),putOI:sum("PE","oi"),callChangeOI:sum("CE","changeOI"),putChangeOI:sum("PE","changeOI"),callVolume:sum("CE","volume"),putVolume:sum("PE","volume"),callChangeVolume:sum("CE","changeVolume"),putChangeVolume:sum("PE","changeVolume")},future:activeFuture,futures,options};
}
async function captureMarketSnapshot(){
  if(!ENABLE_MARKET_SNAPSHOTS||!snapshotStats.ready)return;
  if(snapshotBusy){snapshotStats.busySkipped++;return;}
  const ts=now();
  if(!snapshotSessionOpen(ts)){snapshotStats.skipped++;return;}
  snapshotBusy=true;
  try{
    const date=snapshotIndiaDate(ts);
    if(snapshotStats.activeDate&&snapshotStats.activeDate!==date){try{await compressSnapshotFile(snapshotStats.activeDate);}catch(e){log("Previous snapshot compression:",e.message);}}
    const record=buildSnapshotRecord(ts),line=JSON.stringify(record)+"\n",target=snapshotFile(date,false);
    await fsp.appendFile(target,line,{encoding:"utf8"});
    snapshotStats.writes++;snapshotStats.lastSnapshotAt=ts;snapshotStats.lastBytes=Buffer.byteLength(line);snapshotStats.activeDate=date;snapshotStats.lastError=null;
  }catch(e){snapshotStats.lastError=e.message;log("Market snapshot write failed:",e.message);}
  finally{snapshotBusy=false;}
}
async function initializeSnapshotStore(){
  if(!ENABLE_MARKET_SNAPSHOTS){snapshotStats.ready=false;return;}
  try{await fsp.mkdir(SNAPSHOT_DIR,{recursive:true});const probe=path.join(SNAPSHOT_DIR,".snapshot-write-test");await fsp.writeFile(probe,String(now()),{flag:"w"});await fsp.unlink(probe);await maintainSnapshotFiles();snapshotStats.ready=true;snapshotStats.activeDate=snapshotIndiaDate();log("Market snapshot recorder ready",{dir:SNAPSHOT_DIR,interval:SNAPSHOT_INTERVAL_MS,retentionDays:SNAPSHOT_RETENTION_DAYS});}
  catch(e){snapshotStats.ready=false;snapshotStats.lastError=e.message;log("Market snapshot recorder disabled:",e.message);}
}
async function readSnapshotDate(date,{from=null,to=null,index=null,limit=1000}={}){
  const plain=snapshotFile(date,false),compressed=snapshotFile(date,true);let input;
  try{await fsp.access(plain);input=fs.createReadStream(plain);}catch(e){if(e.code!=="ENOENT")throw e;try{await fsp.access(compressed);input=fs.createReadStream(compressed).pipe(createGunzip());}catch(e2){if(e2.code==="ENOENT")return [];throw e2;}}
  const rl=readline.createInterface({input,crlfDelay:Infinity}),out=[];
  for await(const line of rl){if(!line)continue;let row;try{row=JSON.parse(line);}catch{continue;}if(from!=null&&row.ts<from)continue;if(to!=null&&row.ts>to)continue;if(index&&row.index!==index)continue;out.push(row);if(out.length>limit)out.shift();}
  return out;
}
async function getSnapshotDates(){
  const files=await listSnapshotFiles();return files.map(name=>({date:name.slice(0,10),compressed:name.endsWith(".gz"),bytes:fs.statSync(path.join(SNAPSHOT_DIR,name)).size}));
}

function rowsWithLive(){return state.chain.rows.map(r=>{const t=ticks.get(key(underlyingInfo().optionSegment,r.securityId));return t?{...r,ltp:t.ltp,volume:t.volume,changeVolume:t.changeVolume,oi:t.oi,changeOI:t.changeOI,bid:t.depth?.[0]?.bid??r.bid,ask:t.depth?.[0]?.ask??r.ask,bidQty:t.depth?.[0]?.bidQty??r.bidQty,askQty:t.depth?.[0]?.askQty??r.askQty}:r;});}

app.get("/",(_q,res)=>{
  res.status(200).type("html").send(`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="cache-control" content="no-store"><title>GAMMAX V5 PRO</title><style>body{font-family:system-ui,sans-serif;background:#0b1220;color:#eef2ff;padding:24px;line-height:1.5}h1{margin:0 0 8px}a{color:#7dd3fc}.ok{font-weight:700}.card{background:#111b2e;padding:18px;border-radius:14px;max-width:720px}code{background:#1e293b;padding:2px 6px;border-radius:6px}.row{margin:10px 0}.muted{color:#94a3b8}.good{color:#4ade80}.warn{color:#fbbf24}.bad{color:#fb7185}</style></head><body><div class="card"><h1>GAMMAX V5 PRO</h1><div class="good">● ONLINE</div><p>Ultimate frontend-agnostic market data gateway.</p><p>Version: <code>${state.version}</code></p><div class="row">Dhan feed: <strong id="dhan" class="warn">CHECKING...</strong></div><div class="row">Depth feed: <strong id="depth" class="warn">CHECKING...</strong></div><div class="row muted" id="stats">Checking live status...</div><p><a href="/health">Health</a> · <a href="/api/health">API Health JSON</a> · <a href="/api/state">State</a> · <a href="/api/analytics">Analytics</a> · <a href="/api/option-chain">Option Chain</a></p></div><script>async function refresh(){try{const r=await fetch('/api/health',{cache:'no-store'});const j=await r.json();const d=document.getElementById('dhan');const dep=document.getElementById('depth');d.textContent=j.dhanConnected?'CONNECTED':'CONNECTING';d.className=j.dhanConnected?'good':'warn';dep.textContent=j.depthConnected?'CONNECTED':'CONNECTING';dep.className=j.depthConnected?'good':'warn';document.getElementById('stats').textContent='Ticks: '+j.ticks+' · Packets: '+j.packets+' · Subscriptions: '+j.subscriptions+' · Chain rows: '+j.chainRows;}catch(e){document.getElementById('dhan').textContent='UNAVAILABLE';document.getElementById('dhan').className='bad';}}refresh();setInterval(refresh,3000);</script></body></html>`);
});
app.get("/health",(_q,res)=>res.status(200).type("text").send(`OK\nGAMMAX V5 PRO\nDhan feed: ${state.dhanConnected ? "CONNECTED" : "CONNECTING"}\nVersion: ${state.version}\n`));
app.get("/api/snapshots/status",async(_q,res)=>{
  try{const files=await getSnapshotDates();res.json({ok:true,enabled:ENABLE_MARKET_SNAPSHOTS,ready:snapshotStats.ready,intervalMs:SNAPSHOT_INTERVAL_MS,retentionDays:SNAPSHOT_RETENTION_DAYS,optionStrikesEachSide:SNAPSHOT_OPTION_STRIKES_EACH_SIDE,marketHoursOnly:SNAPSHOT_MARKET_HOURS_ONLY,storagePath:SNAPSHOT_DIR,persistentDiskExpected:SNAPSHOT_DIR.startsWith("/var/data/"),persistentDiskVerified:false,writes:snapshotStats.writes,skipped:snapshotStats.skipped,busySkipped:snapshotStats.busySkipped,lastSnapshotAt:snapshotStats.lastSnapshotAt,lastError:snapshotStats.lastError,files:files.length,futuresPinned:pinnedDepthInstruments.size,dates:files});}catch(e){res.status(500).json({ok:false,error:e.message});}
});
app.get("/api/snapshots/dates",async(_q,res)=>{try{res.json({ok:true,dates:await getSnapshotDates()});}catch(e){res.status(500).json({ok:false,error:e.message});}});
app.get("/api/snapshots",async(q,res)=>{
  try{
    const date=String(q.query.date||"");if(!/^\d{4}-\d{2}-\d{2}$/.test(date))return res.status(400).json({ok:false,error:"date must be YYYY-MM-DD (India market date)"});
    const parseTime=v=>v==null||v===""?null:(/^\d+$/.test(String(v))?Number(v):Date.parse(String(v)));
    const from=parseTime(q.query.from),to=parseTime(q.query.to),limit=Math.max(1,Math.min(5000,Number(q.query.limit)||1000)),index=q.query.index?String(q.query.index).toUpperCase():null;
    if(from!=null&&!Number.isFinite(from)||to!=null&&!Number.isFinite(to))return res.status(400).json({ok:false,error:"Invalid from/to timestamp"});
    if(from!=null&&to!=null&&from>to)return res.status(400).json({ok:false,error:"from must be before to"});
    const data=await readSnapshotDate(date,{from,to,index,limit});
    res.json({ok:true,date,index,from,to,count:data.length,limit,data});
  }catch(e){res.status(500).json({ok:false,error:e.message});}
});
app.get("/api/health",(_q,res)=>res.status(200).json({ok:true,version:state.version,dhanConnected:state.dhanConnected,depthConnected:state.depthConnected,depth200Connected,depth200Target,authStatus,authLastSuccessAt,authLastError:authLastError?String(authLastError).slice(0,200):null,ticks:state.server.ticks,packets:state.server.packets,subscriptions:state.subscriptions.length,chainRows:state.chain.rows.length,lastTick:state.lastTick,
  lastPacketAt,lastAnyTickAt,lastIndexTickAt,lastOptionTickAt,lastFutureTickAt,lastSubscriptionAt,lastDepthPacketAt,dhanConnectedAt,depthConnectedAt,serverStartedAt:state.server.startedAt,
  packetCounts,decodedPacketCount,decodeErrorCount,unknownPacketCount,
  websocketReadyState:dhanWs?.readyState??null,depthWebsocketReadyState:depthWs?.readyState??null,
  liveFeedEnabled,failoverStandby:!liveFeedEnabled,subscriptionDiagnostics:state.subscriptionDiagnostics||null,
  feedStatus:!liveFeedEnabled?"STANDBY":!state.dhanConnected?"DISCONNECTED":(lastAnyTickAt&&now()-lastAnyTickAt<15000)?"LIVE":"CONNECTED_NO_RECENT_TICK",
  serverTime:now(),snapshots:{enabled:ENABLE_MARKET_SNAPSHOTS,ready:snapshotStats.ready,intervalMs:SNAPSHOT_INTERVAL_MS,writes:snapshotStats.writes,busySkipped:snapshotStats.busySkipped,lastSnapshotAt:snapshotStats.lastSnapshotAt,lastError:snapshotStats.lastError,storagePath:SNAPSHOT_DIR,persistentDiskExpected:SNAPSHOT_DIR.startsWith("/var/data/"),persistentDiskVerified:false,futuresPinned:pinnedDepthInstruments.size},time:now()}));
app.get("/api/diagnostics",(_q,res)=>res.redirect(307,"/api/health"));
app.post("/api/failover/activate",(_q,res)=>{
  liveFeedEnabled=ENABLE_LIVE_FEED;
  if(!liveFeedEnabled)return res.status(503).json({ok:false,error:"ENABLE_LIVE_FEED=false; enable live feed on this backend before failover"});
  connectFeed();
  if(ENABLE_20_DEPTH)connectDepth();
  if(depth200Target&&ENABLE_200_DEPTH)connect200Depth(depth200Target);
  log("Failover role activated by PWA");
  res.json({ok:true,role:"ACTIVE",liveFeedEnabled});
});
app.post("/api/failover/standby",(_q,res)=>{
  liveFeedEnabled=false;
  clearTimeout(reconnectTimer);clearTimeout(depthReconnectTimer);clearTimeout(depth200ReconnectTimer);
  try{dhanWs?.close();}catch{} try{depthWs?.close();}catch{} try{depth200Ws?.close();}catch{}
  state.dhanConnected=false;state.depthConnected=false;depth200Connected=false;
  log("Backend placed in standby by PWA");
  res.json({ok:true,role:"STANDBY",liveFeedEnabled});
});
app.get("/api/config",(_q,res)=>res.json({ok:true,version:state.version,indexes:INDEXES,vapidPublicKey:VAPID_PUBLIC_KEY,features:["tick","market-quote","oi","volume","depth20","depth200","option-chain","greeks","hidden-greeks","oi-walls","pcr","max-pain","iv-smile","historical-intraday","historical-daily","expired-options","technical-data","market-movers","news-headlines","websocket","one-second-snapshots","snapshot-history","live-30s-ohlc"],timeframes:["30s","1m","3m","5m","15m","30m","60m"]}));
app.get("/api/state",(_q,res)=>res.json(publicState()));
app.post("/api/auth/refresh",async(_q,res)=>{try{await getAccessToken(true);connectFeed();if(ENABLE_20_DEPTH)connectDepth();if(depth200Target&&ENABLE_200_DEPTH)connect200Depth(depth200Target);res.json({ok:true,authStatus,authLastSuccessAt,tokenExpiry});}catch(e){res.status(502).json({ok:false,error:e.message});}});
app.get("/api/tick",(q,res)=>{const seg=q.query.segment||"NSE_FNO";const sec=String(q.query.securityId||"");const t=ticks.get(key(seg,sec));res.json({ok:!!t,data:t||null});});
app.get("/api/ticks",(_q,res)=>res.json({ok:true,data:[...ticks.values()]}));
app.get("/api/option-chain",(_q,res)=>res.json({ok:true,expiry:state.expiry,spot:state.spot,rows:rowsWithLive(),analytics:state.analytics,updatedAt:state.chain.updatedAt}));
app.get("/api/analytics",(_q,res)=>res.json({ok:true,spot:state.spot,analytics:state.analytics}));
async function candlesFromPersistentSnapshots(index,tf,limit){
  const cacheKey=`${index}|${tf}`;
  const cached=snapshotCandleCache.get(cacheKey);
  if(cached&&now()-cached.at<60000)return cached.data.slice(-limit);
  const dates=(await getSnapshotDates()).slice(-5);
  const points=[];
  for(const item of dates){
    const rows=await readSnapshotDate(item.date,{index,limit:50000});
    for(const row of rows){const px=Number(row.future?.ltp);if(px>0&&Number.isFinite(Number(row.ts)))points.push({time:Number(row.ts),price:px,volume:Number(row.future?.volume)||0});}
  }
  points.sort((a,b)=>a.time-b.time);
  const out=[],prevVol=new Map();
  for(const p of points){
    const tm=Math.floor(p.time/tf)*tf,last=out[out.length-1];
    const prior=prevVol.get(index);const dv=prior==null?0:Math.max(0,p.volume-prior);prevVol.set(index,p.volume);
    if(!last||last.time!==tm)out.push({time:tm,open:p.price,high:p.price,low:p.price,close:p.price,volume:dv,source:"PERSISTENT_1S_SNAPSHOTS"});
    else{last.high=Math.max(last.high,p.price);last.low=Math.min(last.low,p.price);last.close=p.price;last.volume+=dv;}
  }
  snapshotCandleCache.set(cacheKey,{at:now(),data:out});
  return out.slice(-limit);
}
app.get("/api/history",async(q,res)=>{
  try{
    const seg=q.query.segment||"IDX_I",sec=String(q.query.securityId||underlyingInfo().securityId);
    const tfText=String(q.query.tf||q.query.timeframe||"30s").toLowerCase();
    const tfMap={"30s":30000,"1m":60000,"3m":180000,"5m":300000,"15m":900000,"30m":1800000,"60m":3600000};
    const tf=tfMap[tfText]||Number(tfText),limit=Math.max(1,Math.min(5000,Number(q.query.limit)||2000));
    if(!CANDLE_TIMEFRAMES_MS.includes(tf))return res.status(400).json({ok:false,error:"Unsupported timeframe",supported:["30s","1m","3m","5m","15m","30m","60m"]});
    let data=candleSeries.get(`${key(seg,sec)}|${tf}`)||[],source="LIVE_TICK_AGGREGATION";
    if(tf===30000&&data.length<50){const historical=await candlesFromPersistentSnapshots(state.indexKey,tf,limit);const merged=new Map(historical.map(c=>[Number(c.time),c]));for(const c of data)merged.set(Number(c.time),c);data=[...merged.values()].sort((a,b)=>a.time-b.time);source="LIVE_TICKS+PERSISTENT_1S_SNAPSHOTS";}
    res.json({ok:true,segment:seg,securityId:sec,timeframe:tfText,timeframeMs:tf,source,data:data.slice(-limit)});
  }catch(e){res.status(500).json({ok:false,error:e.message});}
});
app.get("/api/candles",(q,res)=>{
  const seg=q.query.segment||"IDX_I",sec=String(q.query.securityId||underlyingInfo().securityId);
  const tfText=String(q.query.tf||q.query.timeframe||"30s").toLowerCase();
  const tfMap={"30s":30000,"1m":60000,"3m":180000,"5m":300000,"15m":900000,"30m":1800000,"60m":3600000};
  const tf=tfMap[tfText]||Number(tfText),limit=Math.max(1,Math.min(5000,Number(q.query.limit)||2000));
  if(!CANDLE_TIMEFRAMES_MS.includes(tf))return res.status(400).json({ok:false,error:"Unsupported timeframe"});
  const data=(candleSeries.get(`${key(seg,sec)}|${tf}`)||[]).slice(-limit);
  res.json({ok:true,timeframe:tfText,timeframeMs:tf,source:"LIVE_TICK_AGGREGATION",count:data.length,data});
});
app.get("/api/instruments",(q,res)=>{const search=String(q.query.search||"").toUpperCase();const limit=Math.min(Number(q.query.limit||100),1000);const arr=[...instruments.values()].filter(x=>!search||JSON.stringify(x).toUpperCase().includes(search)).slice(0,limit);res.json({ok:true,count:arr.length,data:arr});});
app.get("/api/depth",(q,res)=>{const seg=q.query.segment||underlyingInfo().optionSegment,sec=String(q.query.securityId||""),levels=String(q.query.levels||"20"),t=ticks.get(key(seg,sec));res.json({ok:true,levels:levels==="200"?200:20,connected:levels==="200"?depth200Connected:state.depthConnected,data:levels==="200"?(t?.depth200||[]):(t?.depth20||t?.depth||[])});});
app.post("/api/depth/20/subscribe",(req,res)=>{
  const body=req.body||{};
  const list=Array.isArray(body.instruments)?body.instruments:[body];
  const clean=list.map(x=>({ExchangeSegment:String(x.ExchangeSegment||x.exchangeSegment||""),SecurityId:String(x.SecurityId||x.securityId||"")})).filter(x=>x.ExchangeSegment&&x.SecurityId);
  if(!clean.length)return res.status(400).json({ok:false,error:"ExchangeSegment and SecurityId are required"});
  for(const x of clean)pinnedDepthInstruments.set(key(x.ExchangeSegment,x.SecurityId),x);
  if(pinnedDepthInstruments.size>50){const first=pinnedDepthInstruments.keys().next().value;pinnedDepthInstruments.delete(first);}
  syncDepthSubscriptions(state.chain.rows||[]);
  res.json({ok:true,count:pinnedDepthInstruments.size,subscribed:clean});
});
app.post("/api/depth/200/subscribe",(req,res)=>{try{connect200Depth(req.body||{});res.json({ok:true,connected:depth200Connected,target:depth200Target,levels:200});}catch(e){res.status(400).json({ok:false,error:e.message});}});
app.get("/api/market-quote",async(q,res)=>{try{if(!ENABLE_MARKET_QUOTE)return res.status(503).json({ok:false,error:"Market Quote API disabled"});const seg=q.query.segment||underlyingInfo().optionSegment,sec=String(q.query.securityId||"");if(!sec)return res.status(400).json({ok:false,error:"securityId required"});const data=await dhanPost("/marketfeed/quote",{[seg]:[Number(sec)]});res.json({ok:true,data});}catch(e){res.status(502).json({ok:false,error:e.message});}});
app.post("/api/market-quote",async(req,res)=>{try{if(!ENABLE_MARKET_QUOTE)return res.status(503).json({ok:false,error:"Market Quote API disabled"});const instruments=Array.isArray(req.body?.instruments)?req.body.instruments:[];if(!instruments.length||instruments.length>1000)return res.status(400).json({ok:false,error:"Provide 1–1000 instruments"});const grouped={};for(const x of instruments){const seg=String(x.ExchangeSegment||x.exchangeSegment||""),id=Number(x.SecurityId||x.securityId);if(seg&&Number.isFinite(id))(grouped[seg]||=[]).push(id);}const data=await dhanPost("/marketfeed/quote",grouped);res.json({ok:true,data});}catch(e){res.status(502).json({ok:false,error:e.message});}});
app.post("/api/history/intraday",async(req,res)=>{try{res.json({ok:true,data:await dhanPost("/charts/intraday",req.body||{})});}catch(e){res.status(502).json({ok:false,error:e.message});}});
app.post("/api/history/daily",async(req,res)=>{try{res.json({ok:true,data:await dhanPost("/charts/historical",req.body||{})});}catch(e){res.status(502).json({ok:false,error:e.message});}});
app.post("/api/history/expired-options",async(req,res)=>{try{res.json({ok:true,data:await dhanPost("/charts/rollingoption",req.body||{})});}catch(e){res.status(502).json({ok:false,error:e.message});}});
app.post("/api/technical",async(req,res)=>{try{if(!ENABLE_TECHNICAL_DATA_API)return res.status(503).json({ok:false,error:"Technical Data API disabled"});res.json({ok:true,data:await dhanPost("/data/technical",req.body||{})});}catch(e){res.status(502).json({ok:false,error:e.message});}});
app.post("/api/market-movers",async(req,res)=>{try{if(!ENABLE_MARKET_MOVERS_API)return res.status(503).json({ok:false,error:"Market Movers API disabled"});res.json({ok:true,data:await dhanPost("/data/marketmovers",req.body||{})});}catch(e){res.status(502).json({ok:false,error:e.message});}});
app.post("/api/news",async(req,res)=>{try{if(!ENABLE_NEWS_HEADLINES_API)return res.status(503).json({ok:false,error:"News Headlines API disabled"});res.json({ok:true,data:await dhanPost("/data/newsheadline",{...(req.body||{}),dhanClientId:CLIENT_ID})});}catch(e){res.status(502).json({ok:false,error:e.message});}});
app.post("/api/company-info",async(req,res)=>{try{if(!ENABLE_COMPANY_INFO_API)return res.status(503).json({ok:false,error:"Company Info API disabled pending account entitlement confirmation"});res.json({ok:true,data:await dhanPost("/data/companyinfo",req.body||{})});}catch(e){res.status(502).json({ok:false,error:e.message});}});
app.post("/api/index",async(req,res)=>{const k=String(req.body?.index||"NIFTY").toUpperCase();if(!INDEXES[k])return res.status(400).json({ok:false,error:`Unknown index. Use ${Object.keys(INDEXES).join(", ")}`});state.indexKey=k;state.expiry=null;state.chain={updatedAt:null,expiry:null,rows:[],atm:null,maxPain:null};state.analytics=null;await refreshChain();broadcast({type:"state",state:publicState()});res.json({ok:true,state:publicState()});});
app.post("/api/expiry",async(req,res)=>{try{const dates=await getExpiryList();res.json({ok:true,dates,selected:state.expiry});}catch(e){res.status(500).json({ok:false,error:e.message});}});
app.post("/api/expiry/select",async(req,res)=>{const x=String(req.body?.expiry||"");if(!x)return res.status(400).json({ok:false,error:"expiry required"});state.expiry=x;await refreshChain();res.json({ok:true,state:publicState()});});
app.post("/api/subscribe",(req,res)=>{const list=Array.isArray(req.body?.instruments)?req.body.instruments:[];const clean=list.map(x=>({ExchangeSegment:String(x.ExchangeSegment||x.exchangeSegment),SecurityId:String(x.SecurityId||x.securityId)})).filter(x=>x.ExchangeSegment&&x.SecurityId);subscribe(dhanWs,clean,21);state.subscriptions=[...new Map([...state.subscriptions,...clean].map(x=>[key(x.ExchangeSegment,x.SecurityId),x])).values()];res.json({ok:true,count:state.subscriptions.length});});
app.post("/api/push/subscribe",(req,res)=>{const s=req.body;if(!s?.endpoint)return res.status(400).json({ok:false,error:"Invalid subscription"});pushSubscriptions.set(s.endpoint,s);res.json({ok:true});});
app.post("/api/push/signal",async(req,res)=>{
  const body=req.body||{},title=String(body.title||"OI Sniper Entry").slice(0,120),message=String(body.body||"A new entry condition has been confirmed.").slice(0,300),tag=String(body.tag||"oi-sniper-entry").slice(0,100);
  if(!pushSubscriptions.size)return res.json({ok:true,sent:0,reason:"No registered push subscriptions"});
  let sent=0,failed=0;
  for(const [k,sub] of pushSubscriptions){
    try{await webpush.sendNotification(sub,JSON.stringify({title,body:message,tag,data:{url:"/"}}),{TTL:60});sent++;}
    catch(e){failed++;if([404,410].includes(e.statusCode))pushSubscriptions.delete(k);}
  }
  res.json({ok:true,sent,failed,devices:pushSubscriptions.size});
});
app.post("/api/push/test",async(_q,res)=>{let sent=0;for(const [k,s] of pushSubscriptions){try{await webpush.sendNotification(s,JSON.stringify({title:"GAMMAX V5 PRO Test",body:"Live backend push is working."}));sent++;}catch(e){if([404,410].includes(e.statusCode))pushSubscriptions.delete(k);}}res.json({ok:true,devices:pushSubscriptions.size,sent});});

const server=http.createServer(app);const wss=new WebSocketServer({server,path:"/ws"});
wss.on("connection",ws=>{if(clients.size>=MAX_CLIENTS){ws.close(1013,"Maximum clients reached");return;}clients.add(ws);ws.send(JSON.stringify({type:"hello",version:state.version,features:["tick","quote","oi","volume","depth","chain","analytics"]}));ws.send(JSON.stringify({type:"state",state:publicState()}));ws.on("close",()=>clients.delete(ws));});

async function boot(){
  try{await loadInstrumentMaster();}catch(e){log("Instrument master:",e.message);}
  if(ENABLE_OPTION_CHAIN){try{await getExpiryList();await refreshChain();}catch(e){log("Chain startup:",e.message);}}
  autoPinSnapshotFutures();
  await initializeSnapshotStore();
  if(ENABLE_MARKET_SNAPSHOTS&&snapshotStats.ready){snapshotTimer=setInterval(captureMarketSnapshot,SNAPSHOT_INTERVAL_MS);snapshotTimer.unref();}
  connectFeed();connectDepth();
  setInterval(async()=>{if(AUTO_RENEW_DHAN_TOKEN&&accessToken&&tokenExpiry&&now()>=tokenExpiry-TOKEN_REFRESH_BEFORE_MIN*60000){try{await getAccessToken(true);connectFeed();if(ENABLE_20_DEPTH)connectDepth();if(depth200Target&&ENABLE_200_DEPTH)connect200Depth(depth200Target);}catch(e){log("Scheduled token refresh failed:",e.message);}}},AUTH_REFRESH_INTERVAL_MS);
  if(ENABLE_OPTION_CHAIN)chainTimer=setInterval(refreshChain,OPTION_CHAIN_REFRESH_MS);
  instrumentRefreshTimer=setInterval(async()=>{try{await loadInstrumentMaster();autoPinSnapshotFutures();}catch(e){log("Instrument refresh:",e.message);}},6*3600_000);
  setInterval(()=>maintainSnapshotFiles().catch(e=>{snapshotStats.lastError=e.message;log("Snapshot maintenance failed:",e.message);}),6*3600_000).unref();
}
server.listen(PORT,HOST,()=>{log(`GAMMAX V5.2 PRO listening on 0.0.0.0:${PORT}`);boot();});
