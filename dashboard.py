#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard Macro — servidor local de cotações.

Fontes:
  - Yahoo Finance (yfinance)            -> símbolos normais: ^BVSP, GC=F, EUR=X...
  - FRED (St. Louis Fed, dado diário)   -> prefixo fred:   ex. fred:T10Y2Y
  - TradingView scanner (não-oficial)   -> prefixo tv:     ex. tv:TVC:US10Y

Uso:
    python3 dashboard.py            # inicia o servidor e abre o navegador
    python3 dashboard.py --check    # testa todos os símbolos do config
    python3 dashboard.py --snapshot # (nuvem) busca 1x e grava quotes.json

Requisitos: pip install yfinance
"""
import csv
import io
import json
import os
import sys
import threading
import time
import urllib.request
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from yfinance.data import YfData
except ImportError:
    print("ERRO: yfinance não instalado. Rode:  pip3 install yfinance")
    sys.exit(1)

BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE, "config.json")
INDEX_PATH = os.path.join(BASE, "index.html")

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

_lock = threading.Lock()
_quotes = {}
_meta = {"last_update": None, "failed": [], "mode": "-"}
_config_changed = threading.Event()
_fred_cache = {}   # series -> (quote_dict, fetched_epoch)
FRED_TTL = 1800    # 30 min


# ----------------------------- config -----------------------------

def load_config():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def save_config(cfg):
    tmp = CONFIG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CONFIG_PATH)


def all_symbols(cfg):
    syms = []
    for b in cfg.get("blocks", []):
        t = b.get("type")
        if t == "quotes":
            for it in b.get("items", []):
                s = (it.get("symbol") or "").strip()
                if s and s not in syms:
                    syms.append(s)
        elif t == "matrix":
            for ct in b.get("countries", []):
                for tn in b.get("tenors", []):
                    s = f"tv:TVC:{ct['code']}{tn}"
                    if s not in syms:
                        syms.append(s)
    return syms


def split_sources(symbols):
    ya, fred, tv, td = [], [], [], []
    for s in symbols:
        if s.startswith("fred:"):
            fred.append(s[5:])
        elif s.startswith("tv:"):
            tv.append(s[3:])
        elif s.startswith("td:"):
            td.append(s)
        else:
            ya.append(s)
    return ya, fred, tv, td


# ----------------------------- normalização -----------------------------

def _norm(sym, price, prev, ts, currency="", state=""):
    chg = chg_pct = None
    if price is not None and prev not in (None, 0):
        chg = price - prev
        chg_pct = (price / prev - 1.0) * 100.0
    return {"symbol": sym, "price": price, "chg": chg, "chg_pct": chg_pct,
            "time": ts, "currency": currency or "", "state": state or ""}


# ----------------------------- Yahoo -----------------------------

def fetch_v7(data, symbols):
    out = {}
    for i in range(0, len(symbols), 80):
        chunk = symbols[i:i + 80]
        r = data.get_raw_json(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={"symbols": ",".join(chunk)})
        for q in r.get("quoteResponse", {}).get("result", []):
            out[q["symbol"]] = _norm(
                q["symbol"], q.get("regularMarketPrice"),
                q.get("regularMarketPreviousClose"), q.get("regularMarketTime"),
                q.get("currency"), q.get("marketState"))
    return out


def fetch_spark(data, symbols):
    out = {}
    for i in range(0, len(symbols), 80):
        chunk = symbols[i:i + 80]
        r = data.get_raw_json(
            "https://query1.finance.yahoo.com/v7/finance/spark",
            params={"symbols": ",".join(chunk), "range": "1d", "interval": "5m"})
        results = r.get("spark", {}).get("result") or r.get("result") or []
        for item in results:
            sym = item.get("symbol")
            meta = (item.get("response") or [{}])[0].get("meta", {})
            if sym and meta:
                out[sym] = _norm(
                    sym, meta.get("regularMarketPrice"),
                    meta.get("chartPreviousClose") or meta.get("previousClose"),
                    meta.get("regularMarketTime"), meta.get("currency"))
    return out


def fetch_chart_one(data, sym):
    r = data.get_raw_json(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
        params={"range": "1d", "interval": "5m"})
    meta = r["chart"]["result"][0]["meta"]
    return _norm(sym, meta.get("regularMarketPrice"),
                 meta.get("chartPreviousClose") or meta.get("previousClose"),
                 meta.get("regularMarketTime"), meta.get("currency"))


def fetch_chart(data, symbols):
    out = {}
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(fetch_chart_one, data, s): s for s in symbols}
        for fut in as_completed(futs):
            try:
                out[futs[fut]] = fut.result()
            except Exception:
                pass
    return out


def fetch_yahoo(symbols):
    if not symbols:
        return {}, "-"
    data = YfData()
    for mode, fn in (("v7", fetch_v7), ("spark", fetch_spark), ("chart", fetch_chart)):
        try:
            out = fn(data, symbols)
            ok = [s for s in symbols if out.get(s, {}).get("price") is not None]
            if len(ok) >= max(1, int(len(symbols) * 0.5)):
                missing = [s for s in symbols if s not in ok]
                if missing and mode != "chart":
                    try:
                        out.update(fetch_chart(data, missing))
                    except Exception:
                        pass
                return out, mode
        except Exception as e:
            sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] yahoo {mode}: "
                             f"{type(e).__name__}: {str(e)[:120]}\n")
    return {}, "erro"


# ----------------------------- FRED -----------------------------

def _http_get(url, headers=None):
    """GET com fingerprint de navegador (curl_cffi); fallback urllib."""
    h = {"User-Agent": UA}
    h.update(headers or {})
    try:
        from curl_cffi import requests as creq
        r = creq.get(url, impersonate="chrome", timeout=20, headers=h)
        r.raise_for_status()
        return r.text
    except ImportError:
        req = urllib.request.Request(url, headers=h)
        return urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "replace")


def fetch_fred_one(series):
    start = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")
    url = (f"https://fred.stlouisfed.org/graph/fredgraph.csv"
           f"?id={series}&cosd={start}")
    raw = _http_get(url)
    rows = [r for r in csv.reader(io.StringIO(raw))][1:]
    vals = [(d, float(v)) for d, v in rows if v not in (".", "")]
    if not vals:
        return None
    d, v = vals[-1]
    prev = vals[-2][1] if len(vals) > 1 else None
    ts = int(datetime.strptime(d, "%Y-%m-%d").timestamp()) + 12 * 3600
    q = _norm("fred:" + series, v, prev, ts, "", "CLOSED")
    # p/ séries de taxa, variação em pontos faz mais sentido que %
    return q


def fetch_fred(series_list):
    out, now = {}, time.time()
    todo = []
    for s in series_list:
        cached = _fred_cache.get(s)
        if cached and now - cached[1] < FRED_TTL:
            out["fred:" + s] = cached[0]
        else:
            todo.append(s)
    if todo:
        with ThreadPoolExecutor(max_workers=6) as ex:
            futs = {ex.submit(fetch_fred_one, s): s for s in todo}
            for fut in as_completed(futs):
                s = futs[fut]
                try:
                    q = fut.result()
                    if q:
                        _fred_cache[s] = (q, now)
                        out["fred:" + s] = q
                except Exception as e:
                    sys.stderr.write(f"[fred {s}] {type(e).__name__}: {str(e)[:80]}\n")
    return out


# ----------------------------- TradingView scanner -----------------------------

def _tv_post(tickers):
    body = json.dumps({
        "symbols": {"tickers": tickers, "query": {"types": []}},
        "columns": ["close", "change", "change_abs"]
    }).encode()
    req = urllib.request.Request(
        "https://scanner.tradingview.com/global/scan", data=body,
        headers={"User-Agent": UA, "Content-Type": "application/json",
                 "Origin": "https://www.tradingview.com",
                 "Referer": "https://www.tradingview.com/"})
    r = json.loads(urllib.request.urlopen(req, timeout=20).read().decode())
    return r.get("data") or []


def _tv_get_one(ticker):
    from urllib.parse import quote
    url = (f"https://scanner.tradingview.com/symbol?symbol={quote(ticker)}"
           f"&fields=close,change,change_abs")
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    d = json.loads(urllib.request.urlopen(req, timeout=15).read().decode())
    return [{"s": ticker, "d": [d.get("close"), d.get("change"), d.get("change_abs")]}]


def fetch_tv(tickers):
    if not tickers:
        return {}
    rows = []
    try:
        rows = _tv_post(tickers)
    except Exception as e:
        sys.stderr.write(f"[tv scan lote] {type(e).__name__}: {str(e)[:100]}\n")
        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = {ex.submit(_tv_get_one, t): t for t in tickers}
            for fut in as_completed(futs):
                try:
                    rows.extend(fut.result())
                except Exception:
                    pass
    out, now = {}, int(time.time())
    for row in rows:
        t = row.get("s")
        d = row.get("d") or []
        close = d[0] if len(d) > 0 else None
        chg_pct = d[1] if len(d) > 1 else None
        chg = d[2] if len(d) > 2 else None
        if t and close is not None:
            out["tv:" + t] = {"symbol": "tv:" + t, "price": close, "chg": chg,
                              "chg_pct": chg_pct, "time": now, "currency": "",
                              "state": "REGULAR"}
    return out


# ----------------------------- Tesouro Direto -----------------------------

TD_URLS = [
    # API oficial do site do Tesouro Direto (capturada da página de
    # rendimentos em 06/2026)
    "https://www.tesourodireto.com.br/o/c/rentabilidades/?pageSize=200",
]
_td_cache = {"data": None, "ts": 0}
TD_TTL = 600  # 10 min


def _td_slug(nm, year):
    if "IPCA" in nm:
        base = "ipca"
    elif "Prefixado" in nm:
        base = "pre"
    elif "Selic" in nm:
        base = "selic"
    elif "Renda+" in nm:
        base = "renda"
    elif "Educa+" in nm:
        base = "educa"
    else:
        base = "titulo"
    js = "-js" if "Juros Semestrais" in nm else ""
    return f"td:{base}{js}-{year}"


def fetch_td(slugs):
    """Taxas do Tesouro Direto. Slugs: td:ipca-2032, td:ipca-js-2037, td:pre-2029…"""
    if not slugs:
        return {}
    now = time.time()
    if not _td_cache["data"] or now - _td_cache["ts"] > TD_TTL:
        data = None
        for url in TD_URLS:
            try:
                data = json.loads(_http_get(url, headers={
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "pt-BR,pt;q=0.9",
                }))
                break
            except Exception as e:
                sys.stderr.write(f"[tesouro] {url.split('/')[2]}: "
                                 f"{type(e).__name__}: {str(e)[:120]}\n")
        if data is not None:
            _td_cache["data"] = data
            _td_cache["ts"] = now
        elif not _td_cache["data"]:
            return {}
    data = _td_cache["data"] or {}
    out = {}
    if "items" in data:  # formato novo (Liferay /o/c/rentabilidades)
        for bd in data.get("items", []):
            nm = bd.get("treasuryBondName", "")
            year = str(bd.get("targetYear") or "")[:4]
            rate = bd.get("investmentProfitabilityFee")
            state = "REGULAR"
            if not rate:  # só disponível p/ resgate
                rate = bd.get("redemptionProfitabilityFee")
                state = "CLOSED"
            if not nm or not year or rate in (None, 0):
                continue
            slug = _td_slug(nm, year)
            out[slug] = {"symbol": slug, "price": rate, "chg": None,
                         "chg_pct": None, "time": int(now), "currency": "%",
                         "state": state}
    else:  # formato legado (treasurybondsinfo.json)
        resp = data.get("response", {})
        sts = str((resp.get("TrsrBdMkt") or {}).get("sts", ""))
        state = "REGULAR" if sts.lower().startswith("aberto") else "CLOSED"
        for entry in resp.get("TrsrBdTradgList", []):
            bd = entry.get("TrsrBd") or {}
            nm = bd.get("nm", "")
            year = (bd.get("mtrtyDt") or "")[:4]
            rate = bd.get("anulInvstmtRate") or bd.get("anulRedRate")
            if not nm or not year or rate in (None, 0):
                continue
            slug = _td_slug(nm, year)
            out[slug] = {"symbol": slug, "price": rate, "chg": None,
                         "chg_pct": None, "time": int(now), "currency": "%",
                         "state": state}
    missing = [s for s in slugs if s not in out]
    if missing and not _td_cache.get("logged"):
        _td_cache["logged"] = True
        sys.stderr.write(f"[tesouro] pedidos sem match: {missing}\n")
        sys.stderr.write(f"[tesouro] slugs disponíveis: {sorted(out.keys())}\n")
    return out


# ----------------------------- agregador -----------------------------

def fetch_all(symbols):
    ya, fred, tv, td = split_sources(symbols)
    quotes = {}
    mode = "-"
    yq, mode = fetch_yahoo(ya)
    quotes.update(yq)
    try:
        quotes.update(fetch_fred(fred))
    except Exception as e:
        sys.stderr.write(f"[fred] {type(e).__name__}: {e}\n")
    try:
        quotes.update(fetch_tv(tv))
    except Exception as e:
        sys.stderr.write(f"[tv] {type(e).__name__}: {e}\n")
    try:
        quotes.update(fetch_td(td))
    except Exception as e:
        sys.stderr.write(f"[tesouro] {type(e).__name__}: {e}\n")
    failed = [s for s in symbols if quotes.get(s, {}).get("price") is None]
    return quotes, failed, mode


# ------------------------- refresh thread -------------------------

def refresh_loop():
    while True:
        try:
            cfg = load_config()
            syms = all_symbols(cfg)
            quotes, failed, mode = fetch_all(syms)
            if quotes:
                with _lock:
                    _quotes.update(quotes)
                    _meta["last_update"] = int(time.time())
                    _meta["failed"] = failed
                    _meta["mode"] = mode
            interval = max(5, int(cfg.get("refresh_seconds", 15)))
            if mode == "chart":
                interval = max(interval, 60)
        except Exception as e:
            sys.stderr.write(f"[refresh] {type(e).__name__}: {e}\n")
            interval = 30
        _config_changed.wait(timeout=interval)
        _config_changed.clear()


# ----------------------------- server -----------------------------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False).encode("utf-8")
        elif isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            with open(INDEX_PATH, encoding="utf-8") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
        elif path == "/api/quotes":
            with _lock:
                self._send(200, {"quotes": _quotes, "meta": _meta})
        elif path == "/api/config":
            self._send(200, load_config())
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path.split("?")[0] == "/api/config":
            try:
                n = int(self.headers.get("Content-Length", 0))
                cfg = json.loads(self.rfile.read(n).decode("utf-8"))
                assert isinstance(cfg.get("blocks"), list)
                save_config(cfg)
                _config_changed.set()
                self._send(200, {"ok": True})
            except Exception as e:
                self._send(400, {"ok": False, "error": str(e)})
        else:
            self._send(404, {"error": "not found"})


# ------------------------------ main ------------------------------

def check_mode():
    cfg = load_config()
    syms = all_symbols(cfg)
    print(f"Testando {len(syms)} símbolos...")
    quotes, failed, mode = fetch_all(syms)
    print(f"Método Yahoo: {mode}\n")
    for s in syms:
        q = quotes.get(s)
        if q and q.get("price") is not None:
            pct = q.get("chg_pct")
            pct_s = f"{pct:+.2f}%" if pct is not None else "  -  "
            print(f"  OK    {s:<22} {q['price']:>14.4f}  {pct_s}")
        else:
            print(f"  FALHOU {s}")
    if failed:
        print(f"\n{len(failed)} símbolo(s) falharam: {', '.join(failed)}")
    else:
        print("\nTodos os símbolos OK.")


def snapshot_mode():
    """Modo nuvem (GitHub Actions): busca tudo uma vez e grava quotes.json."""
    cfg = load_config()
    syms = all_symbols(cfg)
    quotes, failed, mode = fetch_all(syms)
    out = {"quotes": quotes,
           "meta": {"last_update": int(time.time()), "failed": failed,
                    "mode": mode, "n": len(syms)}}
    path = os.path.join(BASE, "quotes.json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)
    ok = len(syms) - len(failed)
    print(f"snapshot: {ok}/{len(syms)} símbolos OK · yahoo/{mode}")
    if failed:
        print("sem dado: " + ", ".join(failed))
    if ok == 0:
        sys.exit(1)


def main():
    if "--snapshot" in sys.argv:
        snapshot_mode()
        return
    if "--td" in sys.argv:
        qs = fetch_td(["td:_debug"])
        print(f"{len(qs)} títulos retornados:")
        for k in sorted(qs):
            print(f"  {k:<22} {qs[k]['price']}")
        return
    if "--check" in sys.argv:
        check_mode()
        return
    cfg = load_config()
    port = int(cfg.get("port", 8787))
    threading.Thread(target=refresh_loop, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    url = f"http://localhost:{port}"
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        lan_ip = s.getsockname()[0]
        s.close()
        print(f"Dashboard rodando em {url}")
        print(f"No celular/tablet (mesma rede Wi-Fi): http://{lan_ip}:{port}")
        print("(Ctrl+C para parar)")
    except Exception:
        print(f"Dashboard rodando em {url}  (Ctrl+C para parar)")
    threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nEncerrado.")


if __name__ == "__main__":
    main()
