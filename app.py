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
    # PWA assets must be public — iOS fetches them before auth
    pwa_paths = {"/manifest.json", "/icon-180.png", "/icon-192.png", "/icon-512.png"}
    # /tasks/* uses its own CRON_SECRET auth
    if request.path.startswith("/tasks/"):
        return None
    if request.endpoint in public_endpoints or request.path in public_paths:
        return None
    if request.path in pwa_paths:
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



# ============================================================================
# PWA — icons + manifest
# ============================================================================
import base64 as _b64

_ICON_180 = "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"
_ICON_192 = "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"
_ICON_512 = "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"

@app.route("/icon-180.png")
def icon_180():
    return Response(_b64.b64decode(_ICON_180), mimetype="image/png",
                    headers={"Cache-Control": "public, max-age=86400"})

@app.route("/icon-192.png")
def icon_192():
    return Response(_b64.b64decode(_ICON_192), mimetype="image/png",
                    headers={"Cache-Control": "public, max-age=86400"})

@app.route("/icon-512.png")
def icon_512():
    return Response(_b64.b64decode(_ICON_512), mimetype="image/png",
                    headers={"Cache-Control": "public, max-age=86400"})

@app.route("/manifest.json")
def manifest():
    data = {
        "name": "ISAK Stocks",
        "short_name": "ISAK",
        "description": "Personal stock portfolio tracker",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#0e0e0c",
        "theme_color": "#0e0e0c",
        "orientation": "portrait",
        "icons": [
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"},
            {"src": "/icon-180.png", "sizes": "180x180", "type": "image/png", "purpose": "any maskable"},
        ]
    }
    import json as _json
    return Response(_json.dumps(data), mimetype="application/manifest+json")

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
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="ISAK Stocks">
<meta name="theme-color" content="#0e0e0c">
<link rel="manifest" href="/manifest.json">
<link rel="apple-touch-icon" href="/icon-180.png">
<title>ISAK Stocks · Sign in</title>
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
    <h1>ISAK <em>Stocks</em></h1>
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
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="ISAK Stocks">
<meta name="theme-color" content="#0e0e0c">
<link rel="manifest" href="/manifest.json">
<link rel="apple-touch-icon" href="/icon-180.png">
<title>ISAK Stocks</title>
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
  <h1>ISAK <em>Stocks</em></h1>
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
