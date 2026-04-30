#!/usr/bin/env python3
"""
Portfolio Dashboard — local web app.

Run:
    python app.py

Then open the URL it prints (default: http://127.0.0.1:5000)

Features:
- Live data from Yahoo Finance (yfinance) — no API key needed
- Full TASE support (.TA tickers, agorot→ILS conversion handled)
- Holdings managed in browser, saved to portfolio.json next to this file
- 1W / 1M / 1Y portfolio charts
- Top 3 gainers & losers
- Dual-currency: native per-position, USD or ILS for totals

Dependencies:
    pip install flask yfinance pandas
"""

from __future__ import annotations

import hmac
import json
import os
import secrets
import sys
import threading
import time
import webbrowser
from datetime import datetime, timedelta
from pathlib import Path

try:
    from flask import Flask, request, jsonify, Response, session, redirect, url_for
    import yfinance as yf
    import pandas as pd
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Run: pip install flask yfinance pandas")
    sys.exit(1)


# ============================================================================
# CONFIG
# ============================================================================
PORTFOLIO_FILE = Path(__file__).parent / "portfolio.json"
HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "5000"))

# Authentication
# To enable password protection, set the PORTFOLIO_PASSWORD environment variable
# before running the app. Examples:
#   Windows:  set PORTFOLIO_PASSWORD=mySecretPass123 && python app.py
#   macOS:    PORTFOLIO_PASSWORD=mySecretPass123 python3 app.py
# If left empty, the app runs WITHOUT a login wall (localhost only — fine).
APP_PASSWORD = os.environ.get("PORTFOLIO_PASSWORD", "").strip()

# A new random secret each run — invalidates old sessions automatically.
# In production (Render), set FLASK_SECRET_KEY to keep sessions across restarts.
SESSION_SECRET = os.environ.get("FLASK_SECRET_KEY", "").strip() or secrets.token_hex(32)

# Email (Gmail SMTP) — set these env vars to enable daily reports
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com").strip()
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "").strip()       # your gmail address
SMTP_PASS = os.environ.get("SMTP_PASS", "").strip()       # gmail app password
EMAIL_FROM = os.environ.get("EMAIL_FROM", SMTP_USER).strip()
EMAIL_TO = os.environ.get("EMAIL_TO", "").strip()         # comma-separated
DEFAULT_DISPLAY_CCY = os.environ.get("DEFAULT_DISPLAY_CCY", "ILS").strip().upper()

# A simple shared secret for the cron endpoint, so a public scheduler can call
# /tasks/send-report?key=... without exposing the email-sending power to anyone.
CRON_SECRET = os.environ.get("CRON_SECRET", "").strip()

# Cache prices in memory between refreshes (per-ticker, 60-second TTL)
# Keeps clicking "Refresh" repeatedly from hammering Yahoo.
PRICE_CACHE: dict[str, tuple[float, dict]] = {}
CACHE_TTL_SECONDS = 60


# ============================================================================
# PORTFOLIO STORAGE
# ============================================================================
# File schema (v2):
#   {
#     "portfolios": {
#         "Main":      [ {ticker, quantity, avg_price, currency}, ... ],
#         "Long-term": [ ... ],
#     },
#     "active": "Main"
#   }
#
# Auto-migrates from v1 (which was a flat list of holdings).
#
# Storage backend is auto-selected:
#   - DATABASE_URL env var set → Postgres (production / Render)
#   - Otherwise                → JSON file at PORTFOLIO_FILE (local dev)

DEFAULT_PORTFOLIO_NAME = "Main"
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()


def _store_default() -> dict:
    return {"portfolios": {DEFAULT_PORTFOLIO_NAME: []}, "active": DEFAULT_PORTFOLIO_NAME}


def _normalize_store(raw) -> dict:
    """Accept v1 (list) or v2 (dict) and return a clean v2 dict."""
    # v1 → v2 migration
    if isinstance(raw, list):
        return {"portfolios": {DEFAULT_PORTFOLIO_NAME: raw}, "active": DEFAULT_PORTFOLIO_NAME}
    if not isinstance(raw, dict) or "portfolios" not in raw:
        return _store_default()
    if not raw["portfolios"]:
        raw["portfolios"] = {DEFAULT_PORTFOLIO_NAME: []}
    if raw.get("active") not in raw["portfolios"]:
        raw["active"] = next(iter(raw["portfolios"]))
    return raw


# ---- JSON-file backend -----------------------------------------------------
def _load_json() -> dict:
    if not PORTFOLIO_FILE.exists():
        return _store_default()
    try:
        raw = json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Warning: could not read {PORTFOLIO_FILE}: {e}")
        return _store_default()
    store = _normalize_store(raw)
    # If we migrated from v1, save v2 immediately
    if isinstance(raw, list):
        _save_json(store)
    return store


def _save_json(store: dict) -> None:
    PORTFOLIO_FILE.write_text(
        json.dumps(store, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ---- Postgres backend ------------------------------------------------------
def _pg_conn():
    import psycopg
    # Render provides URLs starting with "postgres://"; psycopg wants "postgresql://"
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    return psycopg.connect(url, autocommit=True)


def _pg_init():
    """Create the kv table if missing."""
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_store (
                id INT PRIMARY KEY DEFAULT 1,
                data JSONB NOT NULL,
                CONSTRAINT single_row CHECK (id = 1)
            )
        """)


def _load_pg() -> dict:
    _pg_init()
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT data FROM portfolio_store WHERE id = 1")
        row = cur.fetchone()
        if not row:
            return _store_default()
        return _normalize_store(row[0])


def _save_pg(store: dict) -> None:
    _pg_init()
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO portfolio_store (id, data) VALUES (1, %s::jsonb)
            ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data
        """, (json.dumps(store),))


# ---- Public storage API ----------------------------------------------------
def load_store() -> dict:
    if DATABASE_URL:
        return _load_pg()
    return _load_json()


def save_store(store: dict) -> None:
    if DATABASE_URL:
        _save_pg(store)
    else:
        _save_json(store)


def load_portfolio(name: str | None = None) -> list[dict]:
    """Load holdings for a specific portfolio (or the active one)."""
    store = load_store()
    name = name or store["active"]
    return store["portfolios"].get(name, [])


# ============================================================================
# DATA FETCHING
# ============================================================================
def fetch_ticker_data(ticker: str, force: bool = False) -> dict:
    """Fetch 1y daily history + currency for a ticker. Cached briefly."""
    now = time.time()
    if not force and ticker in PRICE_CACHE:
        ts, data = PRICE_CACHE[ticker]
        if now - ts < CACHE_TTL_SECONDS:
            return data

    result: dict = {"ticker": ticker, "error": None}
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="1y", auto_adjust=False)
        if hist.empty or len(hist) < 2:
            result["error"] = "No price history returned"
            PRICE_CACHE[ticker] = (now, result)
            return result

        # Currency detection
        try:
            info = t.fast_info
            currency = info.get("currency") if hasattr(info, "get") else getattr(info, "currency", None)
        except Exception:
            currency = None
        if not currency:
            try:
                currency = t.info.get("currency", "USD")
            except Exception:
                currency = "USD"

        close = hist["Close"]

        # TASE quirk: Yahoo returns prices in AGOROT for .TA tickers.
        if ticker.upper().endswith(".TA"):
            close = close / 100.0
            currency = "ILS"

        # Build history list (date string + close)
        history = [
            {"date": d.strftime("%Y-%m-%d"), "close": float(c)}
            for d, c in close.items()
        ]

        result.update({
            "currency": currency,
            "current": float(close.iloc[-1]),
            "prev":    float(close.iloc[-2]),
            "history": history,
        })
    except Exception as e:
        result["error"] = str(e)

    PRICE_CACHE[ticker] = (now, result)
    return result


def fetch_fx(base: str, quote: str) -> float:
    """Fetch FX rate, e.g., USD->ILS. Returns 1.0 on failure."""
    if base == quote:
        return 1.0
    cache_key = f"FX_{base}_{quote}"
    now = time.time()
    if cache_key in PRICE_CACHE:
        ts, val = PRICE_CACHE[cache_key]
        if now - ts < CACHE_TTL_SECONDS:
            return val["rate"]

    pair = f"{base}{quote}=X"
    try:
        data = yf.Ticker(pair).history(period="2d")
        if not data.empty:
            rate = float(data["Close"].iloc[-1])
            PRICE_CACHE[cache_key] = (now, {"rate": rate})
            return rate
    except Exception as e:
        print(f"FX fetch failed for {pair}: {e}")

    # Try inverse
    try:
        inv = yf.Ticker(f"{quote}{base}=X").history(period="2d")
        if not inv.empty:
            rate = 1.0 / float(inv["Close"].iloc[-1])
            PRICE_CACHE[cache_key] = (now, {"rate": rate})
            return rate
    except Exception:
        pass

    print(f"WARNING: could not fetch {base}->{quote}, using 1.0")
    return 1.0


# ============================================================================
# REPORT GENERATION (PDF + EMAIL)
# ============================================================================
def _portfolio_summary(portfolio_name: str, display_ccy: str) -> dict:
    """Compute totals + holdings rows for one portfolio. Pure data, no HTML."""
    store = load_store()
    holdings_raw = store["portfolios"].get(portfolio_name, [])
    if not holdings_raw:
        return {"name": portfolio_name, "empty": True}

    # Fetch prices
    tickers = list({h["ticker"] for h in holdings_raw})
    ticker_data = {t: fetch_ticker_data(t, force=False) for t in tickers}

    # FX
    currencies = {h["currency"] for h in holdings_raw}
    fx = {ccy: fetch_fx(ccy, display_ccy) for ccy in currencies}

    rows, total_cost, total_value, today_v, yesterday_v = [], 0.0, 0.0, 0.0, 0.0
    for h in holdings_raw:
        td = ticker_data.get(h["ticker"], {})
        ccy = h["currency"]
        fx_rate = fx.get(ccy, 1.0)
        cost = h["quantity"] * h["avg_price"]
        total_cost += cost * fx_rate
        if td and not td.get("error"):
            value = h["quantity"] * td["current"]
            total_value += value * fx_rate
            today_v += value * fx_rate
            yesterday_v += h["quantity"] * td["prev"] * fx_rate
            daily_pct = (td["current"] - td["prev"]) / td["prev"] * 100 if td["prev"] else 0
            pl = value - cost
            pl_pct = (pl / cost * 100) if cost else 0
            rows.append({
                "ticker": h["ticker"],
                "qty": h["quantity"],
                "avg_price": h["avg_price"],
                "current": td["current"],
                "currency": ccy,
                "daily_pct": daily_pct,
                "value": value,
                "pl": pl,
                "pl_pct": pl_pct,
                "ok": True,
            })
        else:
            rows.append({
                "ticker": h["ticker"],
                "qty": h["quantity"],
                "avg_price": h["avg_price"],
                "currency": ccy,
                "error": td.get("error", "unavailable"),
                "ok": False,
            })

    return {
        "name": portfolio_name,
        "empty": False,
        "rows": rows,
        "total_cost": total_cost,
        "total_value": total_value,
        "daily_change": today_v - yesterday_v,
        "daily_pct": ((today_v - yesterday_v) / yesterday_v * 100) if yesterday_v else 0,
        "total_pl": total_value - total_cost,
        "total_pl_pct": ((total_value - total_cost) / total_cost * 100) if total_cost else 0,
        "display_ccy": display_ccy,
    }


def _fmt_money(v: float, ccy: str) -> str:
    sym = {"USD": "$", "ILS": "₪", "EUR": "€"}.get(ccy, "")
    if v is None:
        return "—"
    return f"{sym}{v:,.2f}" if v >= 0 else f"-{sym}{abs(v):,.2f}"


def _fmt_pct(v: float) -> str:
    if v is None:
        return "—"
    return f"{v:+.2f}%"


def build_report_html(display_ccy: str = None) -> str:
    """Build a self-contained HTML report covering all portfolios."""
    display_ccy = (display_ccy or DEFAULT_DISPLAY_CCY).upper()
    store = load_store()
    summaries = [_portfolio_summary(name, display_ccy) for name in store["portfolios"]]
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Combined totals across all portfolios
    grand_cost = sum(s.get("total_cost", 0) for s in summaries if not s.get("empty"))
    grand_value = sum(s.get("total_value", 0) for s in summaries if not s.get("empty"))
    grand_pl = grand_value - grand_cost
    grand_pl_pct = (grand_pl / grand_cost * 100) if grand_cost else 0

    sections = []
    for s in summaries:
        if s.get("empty"):
            sections.append(f"""
              <h2>{s['name']}</h2>
              <p style="color:#888; font-style:italic;">No holdings.</p>
            """)
            continue
        rows_html = ""
        for r in s["rows"]:
            if not r["ok"]:
                rows_html += f"""<tr><td><b>{r['ticker']}</b></td>
                  <td colspan="6" style="color:#c1121f; font-style:italic;">{r['error']}</td></tr>"""
                continue
            color_d = "#2d6a4f" if r["daily_pct"] >= 0 else "#c1121f"
            color_p = "#2d6a4f" if r["pl"] >= 0 else "#c1121f"
            rows_html += f"""<tr>
              <td><b>{r['ticker']}</b></td>
              <td style="text-align:right;">{r['qty']:g}</td>
              <td style="text-align:right;">{_fmt_money(r['avg_price'], r['currency'])}</td>
              <td style="text-align:right;">{_fmt_money(r['current'], r['currency'])}</td>
              <td style="text-align:right; color:{color_d};">{_fmt_pct(r['daily_pct'])}</td>
              <td style="text-align:right;">{_fmt_money(r['value'], r['currency'])}</td>
              <td style="text-align:right; color:{color_p};">{_fmt_money(r['pl'], r['currency'])} ({_fmt_pct(r['pl_pct'])})</td>
            </tr>"""

        c_daily = "#2d6a4f" if s["daily_change"] >= 0 else "#c1121f"
        c_pl = "#2d6a4f" if s["total_pl"] >= 0 else "#c1121f"

        sections.append(f"""
          <h2>{s['name']}</h2>
          <table class="summary">
            <tr>
              <td><div class="lbl">Paid</div><div class="big">{_fmt_money(s['total_cost'], display_ccy)}</div></td>
              <td><div class="lbl">Worth Now</div><div class="big">{_fmt_money(s['total_value'], display_ccy)}</div>
                  <div style="color:{c_daily}; font-size:11px;">Today: {_fmt_money(s['daily_change'], display_ccy)} ({_fmt_pct(s['daily_pct'])})</div></td>
              <td><div class="lbl">P / L</div><div class="big" style="color:{c_pl};">{_fmt_money(s['total_pl'], display_ccy)}</div>
                  <div style="color:{c_pl}; font-size:11px;">{_fmt_pct(s['total_pl_pct'])} since purchase</div></td>
            </tr>
          </table>
          <table class="holdings">
            <thead><tr>
              <th>Ticker</th><th>Qty</th><th>Avg Cost</th><th>Current</th><th>Day %</th><th>Value</th><th>P / L</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        """)

    grand_color = "#2d6a4f" if grand_pl >= 0 else "#c1121f"
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Portfolio Report</title>
<style>
  body {{ font-family: Georgia, 'Iowan Old Style', serif; color: #222; padding: 24px; max-width: 900px; margin: 0 auto; }}
  h1 {{ font-family: 'Bodoni 72', Didot, Georgia, serif; font-weight: 400; margin: 0; font-size: 32px; }}
  h2 {{ font-family: 'Bodoni 72', Didot, Georgia, serif; font-weight: 400; margin: 28px 0 10px; font-size: 22px;
        border-bottom: 1px solid #ddd; padding-bottom: 6px; }}
  .meta {{ color: #888; font-style: italic; margin-bottom: 28px; padding-bottom: 12px; border-bottom: 2px solid #222; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 12px; }}
  table.summary td {{ padding: 12px; border: 1px solid #ddd; vertical-align: top; width: 33.3%; }}
  table.summary .lbl {{ font-size: 9px; text-transform: uppercase; letter-spacing: 1.5px; color: #888; }}
  table.summary .big {{ font-family: 'Bodoni 72', Didot, Georgia, serif; font-size: 22px; margin-top: 4px; }}
  table.holdings {{ font-size: 11px; }}
  table.holdings th, table.holdings td {{ padding: 6px 8px; border-bottom: 1px solid #eee; text-align: left; }}
  table.holdings th {{ background: #f5f5f0; font-size: 9px; text-transform: uppercase; letter-spacing: 1px; color: #666; }}
  .grand {{ background: #f5f5f0; padding: 16px; border-radius: 4px; margin-bottom: 24px; }}
  .grand .lbl {{ font-size: 10px; text-transform: uppercase; letter-spacing: 1.5px; color: #888; }}
  footer {{ margin-top: 32px; padding-top: 16px; border-top: 1px solid #ddd; color: #888; font-size: 11px; font-style: italic; }}
</style></head>
<body>
  <h1>Portfolio Report</h1>
  <div class="meta">Generated {now} · Display: {display_ccy}</div>

  <div class="grand">
    <div class="lbl">All portfolios combined</div>
    <table class="summary" style="margin-top: 6px;">
      <tr>
        <td><div class="lbl">Total Paid</div><div class="big">{_fmt_money(grand_cost, display_ccy)}</div></td>
        <td><div class="lbl">Worth Now</div><div class="big">{_fmt_money(grand_value, display_ccy)}</div></td>
        <td><div class="lbl">P / L</div><div class="big" style="color:{grand_color};">{_fmt_money(grand_pl, display_ccy)} ({_fmt_pct(grand_pl_pct)})</div></td>
      </tr>
    </table>
  </div>

  {''.join(sections)}

  <footer>Data: Yahoo Finance · Snapshot only, not investment advice.</footer>
</body></html>"""


def html_to_pdf_bytes(html: str) -> bytes:
    """Convert HTML to PDF using weasyprint."""
    from weasyprint import HTML
    return HTML(string=html).write_pdf()


def send_report_email(to: str = None, display_ccy: str = None) -> dict:
    """Generate report PDF and email it. Returns dict with status info."""
    import smtplib
    from email.message import EmailMessage

    if not (SMTP_USER and SMTP_PASS):
        return {"ok": False, "error": "SMTP_USER and SMTP_PASS env vars not set"}

    recipient = to or EMAIL_TO
    if not recipient:
        return {"ok": False, "error": "No EMAIL_TO configured"}

    display_ccy = (display_ccy or DEFAULT_DISPLAY_CCY).upper()
    html = build_report_html(display_ccy)

    try:
        pdf_bytes = html_to_pdf_bytes(html)
    except ImportError:
        return {"ok": False, "error": "weasyprint not installed; run: pip install weasyprint"}
    except Exception as e:
        return {"ok": False, "error": f"PDF generation failed: {e}"}

    today = datetime.now().strftime("%Y-%m-%d")
    msg = EmailMessage()
    msg["Subject"] = f"Portfolio report — {today}"
    msg["From"] = EMAIL_FROM or SMTP_USER
    msg["To"] = recipient
    msg.set_content(
        f"Daily portfolio snapshot generated {datetime.now().strftime('%Y-%m-%d %H:%M')}.\n\n"
        f"See attached PDF.\n\n"
        f"— Sent automatically by your portfolio dashboard."
    )
    msg.add_attachment(
        pdf_bytes,
        maintype="application",
        subtype="pdf",
        filename=f"portfolio-{today}.pdf",
    )

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
            smtp.starttls()
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.send_message(msg)
    except Exception as e:
        return {"ok": False, "error": f"SMTP send failed: {e}"}

    return {"ok": True, "to": recipient, "size_kb": round(len(pdf_bytes) / 1024, 1)}


# ============================================================================
# FLASK APP
# ============================================================================
app = Flask(__name__)
app.config["SECRET_KEY"] = SESSION_SECRET
# Sessions last 7 days unless the server restarts (then secret rotates → re-login)
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)


# Simple rate-limiter for the login endpoint (per-IP, in memory)
LOGIN_ATTEMPTS: dict[str, list[float]] = {}
MAX_LOGIN_ATTEMPTS = 5
LOGIN_WINDOW_SECONDS = 300  # 5 minutes


def is_authed() -> bool:
    """Returns True if no password is set (open access) or session is valid."""
    if not APP_PASSWORD:
        return True
    return session.get("authed") is True


@app.before_request
def require_auth():
    """Block all non-public routes if password is set and user isn't authed."""
    if not APP_PASSWORD:
        return None
    public_endpoints = {"login", "static"}
    public_paths = {"/login", "/api/health"}
    # /tasks/* uses its own CRON_SECRET auth
    if request.path.startswith("/tasks/"):
        return None
    if request.endpoint in public_endpoints or request.path in public_paths:
        return None
    if not is_authed():
        if request.path.startswith("/api/"):
            return jsonify({"error": "Authentication required"}), 401
        return redirect(url_for("login"))
    return None


@app.route("/login", methods=["GET", "POST"])
def login():
    if not APP_PASSWORD:
        return redirect("/")

    error = ""
    if request.method == "POST":
        # Rate limit
        ip = request.remote_addr or "unknown"
        now = time.time()
        attempts = [t for t in LOGIN_ATTEMPTS.get(ip, []) if now - t < LOGIN_WINDOW_SECONDS]
        if len(attempts) >= MAX_LOGIN_ATTEMPTS:
            error = "Too many failed attempts. Try again in a few minutes."
        else:
            submitted = request.form.get("password", "")
            # Constant-time compare to avoid timing attacks
            if hmac.compare_digest(submitted, APP_PASSWORD):
                session.permanent = True
                session["authed"] = True
                LOGIN_ATTEMPTS.pop(ip, None)
                return redirect("/")
            attempts.append(now)
            LOGIN_ATTEMPTS[ip] = attempts
            error = "Wrong password."

    return Response(LOGIN_HTML.replace("{{ERROR}}", error), mimetype="text/html; charset=utf-8")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login" if APP_PASSWORD else "/")


@app.route("/")
def index() -> Response:
    return Response(INDEX_HTML, mimetype="text/html; charset=utf-8")


@app.route("/api/portfolios", methods=["GET"])
def api_list_portfolios():
    """Return the list of portfolio names + which is active."""
    store = load_store()
    return jsonify({
        "names": list(store["portfolios"].keys()),
        "active": store["active"],
    })


@app.route("/api/portfolios/active", methods=["POST"])
def api_set_active():
    """Set the active portfolio."""
    data = request.get_json() or {}
    name = data.get("name")
    store = load_store()
    if name not in store["portfolios"]:
        return jsonify({"error": f"Portfolio '{name}' does not exist"}), 404
    store["active"] = name
    save_store(store)
    return jsonify({"ok": True, "active": name})


@app.route("/api/portfolios/create", methods=["POST"])
def api_create_portfolio():
    """Create a new (empty) portfolio."""
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Name required"}), 400
    if len(name) > 50:
        return jsonify({"error": "Name too long"}), 400
    store = load_store()
    if name in store["portfolios"]:
        return jsonify({"error": f"Portfolio '{name}' already exists"}), 409
    store["portfolios"][name] = []
    store["active"] = name
    save_store(store)
    return jsonify({"ok": True, "active": name})


@app.route("/api/portfolios/delete", methods=["POST"])
def api_delete_portfolio():
    """Delete a portfolio. Refuses to delete the last one."""
    data = request.get_json() or {}
    name = data.get("name")
    store = load_store()
    if name not in store["portfolios"]:
        return jsonify({"error": "Not found"}), 404
    if len(store["portfolios"]) == 1:
        return jsonify({"error": "Cannot delete the last portfolio"}), 400
    del store["portfolios"][name]
    if store["active"] == name:
        store["active"] = next(iter(store["portfolios"]))
    save_store(store)
    return jsonify({"ok": True, "active": store["active"]})


@app.route("/api/portfolio", methods=["GET", "POST"])
def api_portfolio():
    """Read or write the currently-active portfolio's holdings."""
    store = load_store()
    active = store["active"]
    if request.method == "POST":
        holdings = request.get_json()
        if not isinstance(holdings, list):
            return jsonify({"error": "Expected a list"}), 400
        for h in holdings:
            if not all(k in h for k in ("ticker", "quantity", "avg_price", "currency")):
                return jsonify({"error": "Missing required fields"}), 400
        store["portfolios"][active] = holdings
        save_store(store)
        return jsonify({"ok": True})
    return jsonify(store["portfolios"][active])


@app.route("/api/refresh")
def api_refresh():
    """Fetch live data for the active portfolio's holdings + FX."""
    display_ccy = request.args.get("display_ccy", "ILS").upper()
    force = request.args.get("force", "0") == "1"

    holdings = load_portfolio()
    if not holdings:
        return jsonify({"holdings": [], "fx": {}, "display_ccy": display_ccy})

    # Fetch each unique ticker
    tickers = list({h["ticker"] for h in holdings})
    ticker_data = {t: fetch_ticker_data(t, force=force) for t in tickers}

    # Determine FX rates needed
    currencies = {h["currency"] for h in holdings}
    fx = {}
    for ccy in currencies:
        fx[ccy] = fetch_fx(ccy, display_ccy)

    # Attach holding-specific data
    enriched = []
    for h in holdings:
        td = ticker_data.get(h["ticker"], {})
        enriched.append({
            **h,
            "ticker_data": td,
        })

    return jsonify({
        "holdings": enriched,
        "fx": fx,
        "display_ccy": display_ccy,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
    })


@app.route("/api/health")
def api_health():
    return jsonify({"ok": True})


@app.route("/api/send-report", methods=["POST"])
def api_send_report():
    """Manual trigger from the UI. Requires login (handled by before_request)."""
    data = request.get_json() or {}
    to = (data.get("to") or "").strip() or None
    display_ccy = (data.get("display_ccy") or "").strip() or None
    result = send_report_email(to=to, display_ccy=display_ccy)
    return jsonify(result), (200 if result.get("ok") else 500)


@app.route("/tasks/send-report", methods=["GET", "POST"])
def task_send_report():
    """Public endpoint for cron schedulers. Auth via CRON_SECRET query param."""
    if not CRON_SECRET:
        return jsonify({"error": "CRON_SECRET not configured"}), 503
    provided = request.args.get("key", "")
    if not hmac.compare_digest(provided, CRON_SECRET):
        return jsonify({"error": "Invalid key"}), 403
    result = send_report_email()
    return jsonify(result), (200 if result.get("ok") else 500)


# ============================================================================
# HTML (single-page app)
# ============================================================================
LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Portfolio · Sign in</title>
<style>
  :root {
    --bg: #0e0e0c; --paper: #161613; --paper-2: #1d1d19;
    --ink: #ebe6d8; --muted: #7a7569; --line: #2a2a25;
    --accent: #c9a96e; --neg: #d96459;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Iowan Old Style', 'Palatino', 'Georgia', serif;
    background: var(--bg); color: var(--ink);
    min-height: 100vh; display: flex; align-items: center; justify-content: center;
    padding: 20px;
  }
  .login-card {
    background: var(--paper); border: 1px solid var(--line);
    padding: 48px 40px; border-radius: 4px;
    max-width: 380px; width: 100%;
  }
  h1 {
    font-family: 'Bodoni 72', 'Didot', 'Playfair Display', 'Georgia', serif;
    font-size: 36px; font-weight: 400; line-height: 1;
    margin-bottom: 6px; letter-spacing: -0.5px;
  }
  h1 em { font-style: italic; color: var(--accent); font-weight: 300; }
  .tagline {
    color: var(--muted); font-style: italic; font-size: 14px;
    margin-bottom: 36px; padding-bottom: 24px; border-bottom: 1px solid var(--line);
  }
  label {
    display: block; font-family: 'Helvetica Neue', sans-serif;
    font-size: 10px; text-transform: uppercase; letter-spacing: 2px;
    color: var(--muted); margin-bottom: 8px; font-weight: 600;
  }
  input[type="password"] {
    width: 100%;
    background: var(--paper-2); color: var(--ink);
    border: 1px solid var(--line); padding: 12px 14px;
    border-radius: 2px; font-family: 'Iowan Old Style', 'Georgia', serif;
    font-size: 16px; margin-bottom: 24px;
  }
  input:focus { outline: 1px solid var(--accent); }
  button {
    width: 100%;
    background: var(--accent); color: var(--bg); border: 0;
    padding: 12px; border-radius: 2px;
    font-family: 'Helvetica Neue', sans-serif; font-size: 12px;
    text-transform: uppercase; letter-spacing: 2px; font-weight: 700;
    cursor: pointer;
  }
  button:hover { opacity: 0.85; }
  .error {
    color: var(--neg); font-size: 13px; font-style: italic;
    margin-bottom: 16px; padding: 10px 12px;
    background: rgba(217, 100, 89, 0.08); border-left: 3px solid var(--neg);
    border-radius: 2px;
  }
  .error:empty { display: none; }
</style>
</head>
<body>
  <div class="login-card">
    <h1>Portfolio <em>Live</em></h1>
    <div class="tagline">Sign in to continue</div>
    <div class="error">{{ERROR}}</div>
    <form method="post">
      <label for="password">Password</label>
      <input type="password" id="password" name="password" autofocus required>
      <button type="submit">Sign in</button>
    </form>
  </div>
</body>
</html>
"""


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Portfolio · Live</title>
<style>
  :root {
    --bg: #0e0e0c;
    --paper: #161613;
    --paper-2: #1d1d19;
    --ink: #ebe6d8;
    --muted: #7a7569;
    --line: #2a2a25;
    --pos: #7fb069;
    --neg: #d96459;
    --accent: #c9a96e;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Iowan Old Style', 'Palatino', 'Georgia', serif;
    background: var(--bg);
    color: var(--ink);
    line-height: 1.55;
    padding: 32px 20px 80px;
    min-height: 100vh;
  }
  .wrap { max-width: 1100px; margin: 0 auto; }

  header {
    border-bottom: 1px solid var(--line);
    padding-bottom: 24px;
    margin-bottom: 36px;
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
    flex-wrap: wrap;
    gap: 16px;
  }
  h1 {
    font-family: 'Bodoni 72', 'Didot', 'Playfair Display', 'Georgia', serif;
    font-size: 44px;
    font-weight: 400;
    letter-spacing: -0.5px;
    line-height: 1;
  }
  h1 em {
    font-style: italic;
    color: var(--accent);
    font-weight: 300;
  }
  .meta {
    color: var(--muted);
    font-size: 13px;
    font-style: italic;
    text-align: right;
  }

  section { margin-bottom: 44px; }
  .label {
    font-family: 'Helvetica Neue', 'Arial', sans-serif;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 2.5px;
    color: var(--muted);
    margin-bottom: 14px;
    font-weight: 600;
  }
  h2 {
    font-family: 'Bodoni 72', 'Didot', 'Playfair Display', 'Georgia', serif;
    font-size: 26px;
    font-weight: 400;
    margin-bottom: 16px;
    border-bottom: 1px solid var(--line);
    padding-bottom: 8px;
  }

  .toolbar {
    background: var(--paper);
    border: 1px solid var(--line);
    padding: 16px 20px;
    border-radius: 2px;
    margin-bottom: 28px;
    display: flex;
    align-items: center;
    gap: 14px;
    flex-wrap: wrap;
  }
  .toolbar label {
    font-family: 'Helvetica Neue', sans-serif;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: var(--muted);
  }

  input[type="text"], input[type="number"], select {
    background: var(--paper-2);
    color: var(--ink);
    border: 1px solid var(--line);
    padding: 8px 10px;
    border-radius: 2px;
    font-family: 'Iowan Old Style', 'Georgia', serif;
    font-size: 14px;
  }
  input:focus, select:focus { outline: 1px solid var(--accent); }

  button {
    background: var(--accent);
    color: var(--bg);
    border: 0;
    padding: 9px 18px;
    border-radius: 2px;
    font-family: 'Helvetica Neue', sans-serif;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    font-weight: 700;
    cursor: pointer;
    transition: opacity 0.15s;
  }
  button:hover { opacity: 0.85; }
  button:disabled { opacity: 0.4; cursor: wait; }
  button.ghost {
    background: transparent;
    color: var(--ink);
    border: 1px solid var(--line);
  }
  button.danger {
    background: transparent;
    color: var(--neg);
    border: 1px solid var(--neg);
    padding: 4px 10px;
    font-size: 10px;
  }

  .summary { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
  .card {
    background: var(--paper);
    border: 1px solid var(--line);
    padding: 20px;
    border-radius: 2px;
  }
  .card .big {
    font-family: 'Bodoni 72', 'Didot', 'Playfair Display', 'Georgia', serif;
    font-size: 34px;
    font-weight: 400;
    line-height: 1.1;
    margin: 4px 0;
  }
  .card .sub { color: var(--muted); font-size: 13px; }

  .pos { color: var(--pos); }
  .neg { color: var(--neg); }

  .add-form {
    display: grid;
    grid-template-columns: 1.2fr 0.8fr 1fr 0.8fr auto;
    gap: 8px;
    align-items: end;
    margin-bottom: 12px;
  }
  .add-form .field { display: flex; flex-direction: column; gap: 4px; }
  .add-form .field label {
    font-family: 'Helvetica Neue', sans-serif;
    font-size: 9px;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: var(--muted);
  }

  table {
    width: 100%;
    border-collapse: collapse;
    background: var(--paper);
    font-size: 14px;
  }
  th, td {
    padding: 10px 12px;
    text-align: right;
    border-bottom: 1px solid var(--line);
  }
  th:first-child, td:first-child { text-align: left; }
  th {
    font-family: 'Helvetica Neue', sans-serif;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: var(--muted);
    font-weight: 600;
    background: var(--paper-2);
  }
  td .ticker-name { font-weight: 700; }
  td .ticker-meta { color: var(--muted); font-size: 11px; }
  .row-error td { color: var(--neg); font-style: italic; }

  .movers-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
  }
  .mover-list { display: flex; flex-direction: column; gap: 8px; }
  .mover {
    background: var(--paper);
    border: 1px solid var(--line);
    padding: 12px 16px;
    border-radius: 2px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .mover-left .t { font-weight: 700; font-size: 14px; }
  .mover-left .p { color: var(--muted); font-size: 12px; }
  .mover-pct {
    font-family: 'Bodoni 72', 'Didot', 'Georgia', serif;
    font-size: 22px;
  }

  .chart-tabs { display: flex; gap: 2px; margin-bottom: 12px; }
  .tab {
    background: var(--paper);
    color: var(--muted);
    border: 1px solid var(--line);
    padding: 6px 14px;
    cursor: pointer;
    font-family: 'Helvetica Neue', sans-serif;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 1.5px;
  }
  .tab.active {
    background: var(--accent);
    color: var(--bg);
    border-color: var(--accent);
  }
  #chart-wrap {
    background: var(--paper);
    border: 1px solid var(--line);
    padding: 16px;
    border-radius: 2px;
  }
  #chart-svg { width: 100%; height: 280px; display: block; }

  .empty {
    background: var(--paper);
    border: 1px dashed var(--line);
    padding: 40px 20px;
    text-align: center;
    color: var(--muted);
    font-style: italic;
    border-radius: 2px;
  }

  .notice {
    padding: 12px 16px;
    color: var(--ink);
    font-size: 13px;
    margin-bottom: 20px;
    border-radius: 2px;
    border-left: 3px solid var(--neg);
    background: rgba(217, 100, 89, 0.08);
  }
  .notice b { color: var(--neg); }
  .notice.ok { border-left-color: var(--pos); background: rgba(127, 176, 105, 0.08); }
  .notice.ok b { color: var(--pos); }

  footer {
    margin-top: 60px;
    padding-top: 20px;
    border-top: 1px solid var(--line);
    color: var(--muted);
    font-size: 12px;
    font-style: italic;
    text-align: center;
  }

  @media (max-width: 760px) {
    h1 { font-size: 32px; }
    .summary { grid-template-columns: 1fr; }
    .add-form { grid-template-columns: 1fr 1fr; }
    .movers-grid { grid-template-columns: 1fr; }
    th, td { padding: 8px 6px; font-size: 12px; }
  }

  .spin {
    display: inline-block;
    width: 12px; height: 12px;
    border: 2px solid var(--muted);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
    vertical-align: middle;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<div class="wrap">

<header>
  <h1>Portfolio <em>Live</em></h1>
  <div class="meta" id="meta">—</div>
</header>

<div class="toolbar">
  <label>Portfolio</label>
  <select id="portfolio-select" style="min-width: 140px;"></select>
  <button id="new-portfolio" class="ghost" title="Create a new portfolio">+ New</button>
  <button id="delete-portfolio" class="ghost" title="Delete the active portfolio" style="color: var(--neg); border-color: var(--neg);">Delete</button>
  <span style="border-left: 1px solid var(--line); height: 24px; margin: 0 4px;"></span>
  <label>Display</label>
  <select id="display-ccy">
    <option value="USD">USD ($)</option>
    <option value="ILS" selected>ILS (₪)</option>
  </select>
  <button id="refresh-btn">↻ Refresh prices</button>
  <button id="force-refresh-btn" class="ghost" title="Bypass cache">Force fresh</button>
  <button id="email-btn" class="ghost" title="Email PDF report now">📧 Email report</button>
  <span id="status" style="color: var(--muted); font-size: 12px; margin-left: auto;"></span>
</div>

<div id="banner" style="display:none;"></div>

<section>
  <div class="label">Snapshot · <span id="active-portfolio-name" style="color: var(--accent);">—</span></div>
  <div class="summary">
    <div class="card">
      <div class="label" style="margin:0 0 6px;">Total Paid</div>
      <div class="big" id="total-paid">—</div>
      <div class="sub">Cost at purchase</div>
    </div>
    <div class="card">
      <div class="label" style="margin:0 0 6px;">Worth Now</div>
      <div class="big" id="total-worth">—</div>
      <div class="sub" id="daily-line">—</div>
    </div>
    <div class="card">
      <div class="label" style="margin:0 0 6px;">Profit / Loss</div>
      <div class="big" id="total-pl">—</div>
      <div class="sub" id="total-pl-pct">—</div>
    </div>
  </div>
</section>

<section>
  <h2>Performance</h2>
  <div class="chart-tabs">
    <div class="tab active" data-period="1w">1 Week</div>
    <div class="tab" data-period="1m">1 Month</div>
    <div class="tab" data-period="1y">1 Year</div>
  </div>
  <div id="chart-wrap">
    <svg id="chart-svg" viewBox="0 0 800 280" preserveAspectRatio="none"></svg>
    <div id="chart-status" style="text-align:center; color:var(--muted); font-size:12px; margin-top:8px;"></div>
  </div>
</section>

<section>
  <h2>Top Movers Today</h2>
  <div class="movers-grid">
    <div>
      <div class="label">↑ Top Gainers</div>
      <div class="mover-list" id="gainers"></div>
    </div>
    <div>
      <div class="label">↓ Top Losers</div>
      <div class="mover-list" id="losers"></div>
    </div>
  </div>
</section>

<section>
  <h2>Holdings</h2>
  <div class="add-form">
    <div class="field">
      <label>Ticker</label>
      <input type="text" id="f-ticker" placeholder="AAPL or TEVA.TA">
    </div>
    <div class="field">
      <label>Quantity</label>
      <input type="number" id="f-qty" step="any" placeholder="10">
    </div>
    <div class="field">
      <label>Avg Price (native ccy)</label>
      <input type="number" id="f-price" step="any" placeholder="150.00">
    </div>
    <div class="field">
      <label>Currency</label>
      <select id="f-ccy">
        <option value="USD">USD</option>
        <option value="ILS">ILS</option>
      </select>
    </div>
    <button id="add-holding">Add</button>
  </div>
  <div id="holdings-area"></div>
</section>

<footer>
  Data: Yahoo Finance (yfinance) · Holdings stored in <code>portfolio.json</code> on your machine.<br>
  Snapshot only. Not investment advice. · <a href="/logout" style="color: var(--accent);">Sign out</a>
</footer>

</div>

<script>
// ============================================================================
// STATE
// ============================================================================
const state = {
  portfolios: [],      // list of names
  activePortfolio: '',
  holdings: [],        // raw holdings (with ticker_data after refresh)
  fx: { USD: 1, ILS: 1 },
  displayCcy: 'ILS',
  chartPeriod: '1w',
};

// ============================================================================
// UTILS
// ============================================================================
const SYMBOLS = { USD: '$', ILS: '₪', EUR: '€' };
function fmtMoney(v, ccy) {
  if (v === null || v === undefined || isNaN(v)) return '—';
  const sym = SYMBOLS[ccy] || '';
  const abs = Math.abs(v);
  const sign = v < 0 ? '-' : '';
  return `${sign}${sym}${abs.toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2})}`;
}
function fmtPct(v) {
  if (v === null || v === undefined || isNaN(v)) return '—';
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
}
function colorClass(v) {
  if (v === null || v === undefined || isNaN(v)) return '';
  return v > 0 ? 'pos' : v < 0 ? 'neg' : '';
}

function showBanner(html, kind = '') {
  const b = document.getElementById('banner');
  b.className = 'notice' + (kind ? ' ' + kind : '');
  b.innerHTML = html;
  b.style.display = 'block';
}
function hideBanner() {
  document.getElementById('banner').style.display = 'none';
}

// ============================================================================
// API
// ============================================================================
async function apiGetPortfolio() {
  const r = await fetch('/api/portfolio');
  return r.json();
}
async function apiSavePortfolio(holdings) {
  const r = await fetch('/api/portfolio', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(holdings),
  });
  return r.json();
}
async function apiRefresh(displayCcy, force = false) {
  const params = new URLSearchParams({ display_ccy: displayCcy });
  if (force) params.set('force', '1');
  const r = await fetch('/api/refresh?' + params.toString());
  return r.json();
}
async function apiListPortfolios() {
  const r = await fetch('/api/portfolios');
  return r.json();
}
async function apiSetActive(name) {
  const r = await fetch('/api/portfolios/active', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name }),
  });
  return r.json();
}
async function apiCreatePortfolio(name) {
  const r = await fetch('/api/portfolios/create', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name }),
  });
  return { ok: r.ok, ...(await r.json()) };
}
async function apiDeletePortfolio(name) {
  const r = await fetch('/api/portfolios/delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name }),
  });
  return { ok: r.ok, ...(await r.json()) };
}

// ============================================================================
// LOAD & REFRESH
// ============================================================================
async function loadInitial() {
  await reloadPortfolioList();
  const list = await apiGetPortfolio();
  state.holdings = list.map(h => ({ ...h, ticker_data: null }));
  render();
  if (state.holdings.length > 0) {
    await refresh(false);
  }
}

async function reloadPortfolioList() {
  const data = await apiListPortfolios();
  state.portfolios = data.names || [];
  state.activePortfolio = data.active || '';
  // Update selector
  const sel = document.getElementById('portfolio-select');
  sel.innerHTML = state.portfolios.map(n =>
    `<option value="${n}" ${n === state.activePortfolio ? 'selected' : ''}>${n}</option>`
  ).join('');
  // Disable delete if it's the only one
  document.getElementById('delete-portfolio').disabled = state.portfolios.length <= 1;
  // Update header label
  document.getElementById('active-portfolio-name').textContent = state.activePortfolio || '—';
}

async function switchPortfolio(name) {
  await apiSetActive(name);
  state.activePortfolio = name;
  document.getElementById('active-portfolio-name').textContent = name;
  // Clear stale data and reload
  state.holdings = [];
  render();
  const list = await apiGetPortfolio();
  state.holdings = list.map(h => ({ ...h, ticker_data: null }));
  render();
  if (state.holdings.length > 0) {
    refresh(false);
  }
}

let refreshing = false;
async function refresh(force) {
  if (refreshing) return;
  refreshing = true;
  hideBanner();

  const refreshBtn = document.getElementById('refresh-btn');
  const forceBtn = document.getElementById('force-refresh-btn');
  refreshBtn.disabled = true;
  forceBtn.disabled = true;
  document.getElementById('status').innerHTML = '<span class="spin"></span> fetching...';

  try {
    const data = await apiRefresh(state.displayCcy, force);
    state.holdings = data.holdings || [];
    state.fx = data.fx || {};
    state.displayCcy = data.display_ccy || state.displayCcy;

    const errors = state.holdings
      .map(h => h.ticker_data)
      .filter(td => td && td.error);
    if (errors.length > 0) {
      showBanner(
        `<b>Could not fetch:</b> ` +
        errors.map(e => `<code>${e.ticker}</code> — ${e.error}`).join(' · ')
      );
    }

    document.getElementById('meta').textContent =
      `Updated ${new Date(data.fetched_at || Date.now()).toLocaleString()} · Display: ${state.displayCcy}`;
    document.getElementById('status').textContent = '';
  } catch (e) {
    showBanner(`<b>Refresh failed:</b> ${e.message}`);
    document.getElementById('status').textContent = '';
  } finally {
    refreshing = false;
    refreshBtn.disabled = false;
    forceBtn.disabled = false;
    render();
  }
}

// ============================================================================
// RENDER
// ============================================================================
function render() {
  renderSummary();
  renderHoldings();
  renderMovers();
  renderChart();
}

function computeTotals() {
  let totalCost = 0, totalValue = 0, todayValue = 0, yesterdayValue = 0;
  for (const h of state.holdings) {
    const td = h.ticker_data;
    const fx = state.fx[h.currency] || 1;
    totalCost += h.quantity * h.avg_price * fx;
    if (td && !td.error) {
      totalValue     += h.quantity * td.current * fx;
      todayValue     += h.quantity * td.current * fx;
      yesterdayValue += h.quantity * td.prev * fx;
    }
  }
  const dailyChange = todayValue - yesterdayValue;
  const dailyPct = yesterdayValue ? (dailyChange / yesterdayValue) * 100 : 0;
  const totalPl = totalValue - totalCost;
  const totalPlPct = totalCost ? (totalPl / totalCost) * 100 : 0;
  return { totalCost, totalValue, dailyChange, dailyPct, totalPl, totalPlPct };
}

function renderSummary() {
  const t = computeTotals();
  const ccy = state.displayCcy;

  document.getElementById('total-paid').textContent = fmtMoney(t.totalCost, ccy);

  const worth = document.getElementById('total-worth');
  worth.textContent = fmtMoney(t.totalValue, ccy);

  const dailyLine = document.getElementById('daily-line');
  if (t.dailyChange === 0 && t.totalValue === 0) {
    dailyLine.textContent = '—';
    dailyLine.className = 'sub';
  } else {
    dailyLine.textContent = `Today: ${fmtMoney(t.dailyChange, ccy)} (${fmtPct(t.dailyPct)})`;
    dailyLine.className = 'sub ' + colorClass(t.dailyChange);
  }

  const pl = document.getElementById('total-pl');
  pl.textContent = fmtMoney(t.totalPl, ccy);
  pl.className = 'big ' + colorClass(t.totalPl);

  const plp = document.getElementById('total-pl-pct');
  plp.textContent = `${fmtPct(t.totalPlPct)} since purchase`;
  plp.className = 'sub ' + colorClass(t.totalPlPct);
}

function renderHoldings() {
  const area = document.getElementById('holdings-area');
  if (state.holdings.length === 0) {
    area.innerHTML = '<div class="empty">No holdings yet. Add your first position above.</div>';
    return;
  }

  const rowsHtml = state.holdings.map((h, idx) => {
    const td = h.ticker_data;
    if (!td) {
      return `<tr class="row-error">
        <td><span class="ticker-name">${h.ticker}</span><br><span class="ticker-meta">${h.quantity} @ ${fmtMoney(h.avg_price, h.currency)}</span></td>
        <td colspan="5">Click Refresh to load price</td>
        <td><button class="danger" data-remove="${idx}">Remove</button></td>
      </tr>`;
    }
    if (td.error) {
      return `<tr class="row-error">
        <td><span class="ticker-name">${h.ticker}</span><br><span class="ticker-meta">${h.quantity} @ ${fmtMoney(h.avg_price, h.currency)}</span></td>
        <td colspan="5">${td.error}</td>
        <td><button class="danger" data-remove="${idx}">Remove</button></td>
      </tr>`;
    }
    const dailyPct = ((td.current - td.prev) / td.prev) * 100;
    const value = h.quantity * td.current;
    const cost  = h.quantity * h.avg_price;
    const pl    = value - cost;
    const plPct = cost ? (pl / cost) * 100 : 0;
    return `<tr>
      <td><span class="ticker-name">${h.ticker}</span></td>
      <td>${h.quantity}</td>
      <td>${fmtMoney(h.avg_price, h.currency)}</td>
      <td>${fmtMoney(td.current, h.currency)}</td>
      <td class="${colorClass(dailyPct)}">${fmtPct(dailyPct)}</td>
      <td>${fmtMoney(value, h.currency)}</td>
      <td class="${colorClass(pl)}">${fmtMoney(pl, h.currency)} <span class="${colorClass(plPct)}">(${fmtPct(plPct)})</span></td>
      <td><button class="danger" data-remove="${idx}">Remove</button></td>
    </tr>`;
  }).join('');

  area.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Ticker</th>
          <th>Qty</th>
          <th>Avg Cost</th>
          <th>Current</th>
          <th>Day %</th>
          <th>Value</th>
          <th>P / L</th>
          <th></th>
        </tr>
      </thead>
      <tbody>${rowsHtml}</tbody>
    </table>
    <div style="color:var(--muted); font-size:12px; margin-top:8px;">
      Holdings shown in their native currency. Top totals converted to ${state.displayCcy}.
    </div>
  `;

  area.querySelectorAll('[data-remove]').forEach(btn => {
    btn.addEventListener('click', async (e) => {
      const i = parseInt(e.target.getAttribute('data-remove'), 10);
      if (confirm(`Remove ${state.holdings[i].ticker}?`)) {
        state.holdings.splice(i, 1);
        // Save just the raw fields (not ticker_data)
        await apiSavePortfolio(state.holdings.map(h => ({
          ticker: h.ticker, quantity: h.quantity, avg_price: h.avg_price, currency: h.currency
        })));
        render();
      }
    });
  });
}

function renderMovers() {
  const valid = state.holdings
    .map(h => {
      const td = h.ticker_data;
      if (!td || td.error) return null;
      return {
        ticker: h.ticker,
        currency: h.currency,
        current: td.current,
        dailyPct: ((td.current - td.prev) / td.prev) * 100,
      };
    })
    .filter(Boolean);

  const gainers = [...valid].sort((a,b) => b.dailyPct - a.dailyPct).slice(0, 3);
  const losers  = [...valid].sort((a,b) => a.dailyPct - b.dailyPct).slice(0, 3);

  function moverHtml(m) {
    return `<div class="mover">
      <div class="mover-left">
        <div class="t">${m.ticker}</div>
        <div class="p">${fmtMoney(m.current, m.currency)}</div>
      </div>
      <div class="mover-pct ${colorClass(m.dailyPct)}">${fmtPct(m.dailyPct)}</div>
    </div>`;
  }

  document.getElementById('gainers').innerHTML =
    gainers.length ? gainers.map(moverHtml).join('') : '<div class="empty">No data</div>';
  document.getElementById('losers').innerHTML =
    losers.length ? losers.map(moverHtml).join('') : '<div class="empty">No data</div>';
}

function renderChart() {
  const periodDays = { '1w': 7, '1m': 30, '1y': 365 }[state.chartPeriod];
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - periodDays);

  const dateMap = new Map();
  for (const h of state.holdings) {
    const td = h.ticker_data;
    if (!td || td.error || !td.history) continue;
    for (const pt of td.history) {
      if (new Date(pt.date) < cutoff) continue;
      if (!dateMap.has(pt.date)) dateMap.set(pt.date, {});
      dateMap.get(pt.date)[h.ticker] = pt.close;
    }
  }

  if (dateMap.size === 0) {
    drawChartEmpty('No price history available. Click Refresh.');
    return;
  }

  const dates = [...dateMap.keys()].sort();
  const lastPrice = {};
  const points = dates.map(d => {
    const prices = dateMap.get(d);
    let total = 0;
    for (const h of state.holdings) {
      if (prices[h.ticker] !== undefined) lastPrice[h.ticker] = prices[h.ticker];
      const px = lastPrice[h.ticker];
      if (px === undefined) continue;
      const fx = state.fx[h.currency] || 1;
      total += h.quantity * px * fx;
    }
    return { date: d, value: total };
  }).filter(p => p.value > 0);

  if (points.length < 2) {
    drawChartEmpty('Not enough data points yet.');
    return;
  }
  drawChart(points);
}

function drawChartEmpty(msg) {
  document.getElementById('chart-svg').innerHTML = '';
  document.getElementById('chart-status').textContent = msg;
}

function drawChart(points) {
  const svg = document.getElementById('chart-svg');
  const W = 800, H = 280;
  const PAD_L = 60, PAD_R = 12, PAD_T = 16, PAD_B = 28;

  const values = points.map(p => p.value);
  let min = Math.min(...values), max = Math.max(...values);
  if (min === max) { min -= 1; max += 1; }
  const pad = (max - min) * 0.1;
  min -= pad; max += pad;

  const xStep = (W - PAD_L - PAD_R) / (points.length - 1);
  const yScale = v => PAD_T + (1 - (v - min) / (max - min)) * (H - PAD_T - PAD_B);

  const linePath = points.map((p, i) =>
    `${i === 0 ? 'M' : 'L'} ${PAD_L + i * xStep} ${yScale(p.value)}`).join(' ');
  const areaPath = `${linePath} L ${PAD_L + (points.length - 1) * xStep} ${H - PAD_B} L ${PAD_L} ${H - PAD_B} Z`;

  const startV = points[0].value;
  const endV = points[points.length - 1].value;
  const change = endV - startV;
  const pct = (change / startV) * 100;
  const color = change >= 0 ? 'var(--pos)' : 'var(--neg)';
  const gradColor = change >= 0 ? '#7fb069' : '#d96459';

  const yTicks = [];
  for (let i = 0; i <= 4; i++) {
    const v = min + ((max - min) * i / 4);
    const y = yScale(v);
    yTicks.push(`<line x1="${PAD_L}" y1="${y}" x2="${W - PAD_R}" y2="${y}" stroke="var(--line)" stroke-dasharray="2,4"/>
                 <text x="${PAD_L - 6}" y="${y + 4}" text-anchor="end" fill="var(--muted)" font-size="10" font-family="Helvetica Neue">${Math.round(v).toLocaleString()}</text>`);
  }

  const xLabels = [0, Math.floor(points.length / 2), points.length - 1].map(i => {
    const x = PAD_L + i * xStep;
    return `<text x="${x}" y="${H - 8}" text-anchor="middle" fill="var(--muted)" font-size="10" font-family="Helvetica Neue">${points[i].date}</text>`;
  }).join('');

  svg.innerHTML = `
    <defs>
      <linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="${gradColor}" stop-opacity="0.3"/>
        <stop offset="100%" stop-color="${gradColor}" stop-opacity="0"/>
      </linearGradient>
    </defs>
    ${yTicks.join('')}
    <path d="${areaPath}" fill="url(#areaGrad)"/>
    <path d="${linePath}" fill="none" stroke="${color}" stroke-width="2"/>
    ${xLabels}
  `;

  document.getElementById('chart-status').textContent =
    `${points.length} data points · ${fmtMoney(change, state.displayCcy)} (${fmtPct(pct)})`;
}

// ============================================================================
// EVENTS
// ============================================================================
document.getElementById('refresh-btn').addEventListener('click', () => refresh(false));
document.getElementById('force-refresh-btn').addEventListener('click', () => refresh(true));

document.getElementById('portfolio-select').addEventListener('change', (e) => {
  switchPortfolio(e.target.value);
});

document.getElementById('new-portfolio').addEventListener('click', async () => {
  const name = prompt('Name for the new portfolio (e.g., "Long-term", "Speculation"):');
  if (!name || !name.trim()) return;
  const result = await apiCreatePortfolio(name.trim());
  if (!result.ok) {
    alert(result.error || 'Could not create portfolio');
    return;
  }
  await reloadPortfolioList();
  await switchPortfolio(name.trim());
});

document.getElementById('delete-portfolio').addEventListener('click', async () => {
  if (!confirm(`Delete portfolio "${state.activePortfolio}"? This cannot be undone.`)) return;
  const result = await apiDeletePortfolio(state.activePortfolio);
  if (!result.ok) {
    alert(result.error || 'Could not delete portfolio');
    return;
  }
  await reloadPortfolioList();
  await switchPortfolio(result.active);
});

document.getElementById('display-ccy').addEventListener('change', (e) => {
  state.displayCcy = e.target.value;
  if (state.holdings.length > 0) refresh(false);
});

document.getElementById('email-btn').addEventListener('click', async () => {
  const btn = document.getElementById('email-btn');
  btn.disabled = true;
  btn.textContent = '📧 Sending...';
  try {
    const r = await fetch('/api/send-report', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ display_ccy: state.displayCcy })
    });
    const data = await r.json();
    if (data.ok) {
      showBanner(`<b style="color:var(--pos);">Report sent</b> to ${data.to} (${data.size_kb} KB).`, 'ok');
    } else {
      showBanner(`<b>Could not send report:</b> ${data.error}`);
    }
  } catch (e) {
    showBanner(`<b>Email failed:</b> ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = '📧 Email report';
  }
});

document.getElementById('add-holding').addEventListener('click', async () => {
  const t  = document.getElementById('f-ticker').value.trim().toUpperCase();
  const q  = parseFloat(document.getElementById('f-qty').value);
  const ap = parseFloat(document.getElementById('f-price').value);
  const c  = document.getElementById('f-ccy').value;
  if (!t || !q || !ap) {
    alert('Please fill in ticker, quantity and avg price.');
    return;
  }
  const newHolding = { ticker: t, quantity: q, avg_price: ap, currency: c };
  const raw = state.holdings.map(h => ({
    ticker: h.ticker, quantity: h.quantity, avg_price: h.avg_price, currency: h.currency
  }));
  raw.push(newHolding);
  await apiSavePortfolio(raw);
  ['f-ticker', 'f-qty', 'f-price'].forEach(id => document.getElementById(id).value = '');
  state.holdings = raw.map(h => ({ ...h, ticker_data: null }));
  render();
  refresh(false);
});

document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    state.chartPeriod = tab.dataset.period;
    renderChart();
  });
});

// ============================================================================
// BOOT
// ============================================================================
loadInitial();
</script>
</body>
</html>
"""


# ============================================================================
# RUN
# ============================================================================
def open_browser():
    """Open the app in the default browser after a short delay."""
    time.sleep(1.0)
    try:
        webbrowser.open(f"http://{HOST}:{PORT}/")
    except Exception:
        pass


def main():
    print()
    print("=" * 60)
    print("  Portfolio Dashboard")
    print("=" * 60)
    print(f"  Open in browser:  http://{HOST}:{PORT}/")
    print(f"  Storage:          {'Postgres (DATABASE_URL)' if DATABASE_URL else f'JSON file ({PORTFOLIO_FILE})'}")
    if APP_PASSWORD:
        print(f"  Authentication:   ON (password protected)")
    else:
        print(f"  Authentication:   off (set PORTFOLIO_PASSWORD to enable)")
    if SMTP_USER and SMTP_PASS:
        print(f"  Email reports:    ON (sending as {SMTP_USER})")
    else:
        print(f"  Email reports:    off (set SMTP_USER/SMTP_PASS to enable)")
    if CRON_SECRET:
        print(f"  Cron endpoint:    /tasks/send-report?key=<secret>")
    if HOST == "0.0.0.0":
        print(f"  ⚠ Listening on ALL interfaces — use only behind a proxy/firewall")
    print(f"  Stop the server:  Ctrl+C")
    print("=" * 60)
    print()

    threading.Thread(target=open_browser, daemon=True).start()
    # Disable the noisy default logger lines for cleaner output
    import logging
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    app.run(host=HOST, port=PORT, debug=False)


if __name__ == "__main__":
    main()
