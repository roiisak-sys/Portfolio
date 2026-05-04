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

# Anthropic API — for the analyst research widget
# Set ANTHROPIC_API_KEY in your environment / Render env vars.
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()

# Cache for analyst reports (expensive to generate — cache for 6 hours)
ANALYSIS_CACHE: dict[str, tuple[float, str]] = {}
ANALYSIS_CACHE_TTL = 6 * 3600

# Cache prices in memory between refreshes (per-ticker, 60-second TTL)
# Keeps clicking "Refresh" repeatedly from hammering Yahoo.
PRICE_CACHE: dict[str, tuple[float, dict]] = {}
CACHE_TTL_SECONDS = 60


# ============================================================================
# PORTFOLIO STORAGE
# ============================================================================
# ============================================================================
# STORAGE — v3 Transaction Ledger
# ============================================================================
#
# Schema v3:
#   {
#     "version": 3,
#     "active": "Main",
#     "portfolios": {
#       "Main": {
#         "transactions": [
#           {"id": "uuid", "action": "BUY"|"SELL", "ticker": "NVDA",
#            "quantity": 10, "price": 13.43, "currency": "USD", "note": ""},
#           ...
#         ]
#       }
#     }
#   }
#
# Migration path:
#   v1 (flat list of holdings)  → v3  (each holding becomes one BUY tx)
#   v2 (dict of holding lists)  → v3  (each holding becomes one BUY tx)
#
# Storage backend auto-selected:
#   DATABASE_URL set  → Postgres
#   Otherwise         → JSON file (PORTFOLIO_FILE)

DEFAULT_PORTFOLIO_NAME = "Main"
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()


def _new_id() -> str:
    return secrets.token_hex(8)


def _store_default() -> dict:
    return {
        "version": 3,
        "active": DEFAULT_PORTFOLIO_NAME,
        "portfolios": {DEFAULT_PORTFOLIO_NAME: {"transactions": []}},
    }


def _migrate_to_v3(raw) -> dict:
    """Migrate v1 (list) or v2 (dict of lists) to v3 (transaction ledger)."""
    store = _store_default()

    # v1: flat list of holdings
    if isinstance(raw, list):
        txs = []
        for h in raw:
            if h.get("quantity", 0) > 0:
                txs.append({
                    "id": _new_id(),
                    "action": "BUY",
                    "ticker": h["ticker"].upper(),
                    "quantity": float(h["quantity"]),
                    "price": float(h["avg_price"]),
                    "currency": h.get("currency", "USD"),
                    "note": "Imported from legacy data",
                })
        store["portfolios"][DEFAULT_PORTFOLIO_NAME]["transactions"] = txs
        return store

    # v2: dict with "portfolios" key containing lists of holdings
    if isinstance(raw, dict) and raw.get("version", 2) < 3:
        portfolios = raw.get("portfolios", {})
        active = raw.get("active", DEFAULT_PORTFOLIO_NAME)
        for pname, holdings in portfolios.items():
            txs = []
            for h in (holdings if isinstance(holdings, list) else []):
                if h.get("quantity", 0) > 0:
                    txs.append({
                        "id": _new_id(),
                        "action": "BUY",
                        "ticker": h["ticker"].upper(),
                        "quantity": float(h["quantity"]),
                        "price": float(h["avg_price"]),
                        "currency": h.get("currency", "USD"),
                        "note": "Imported from legacy data",
                    })
            store["portfolios"][pname] = {"transactions": txs}
        store["active"] = active if active in store["portfolios"] else DEFAULT_PORTFOLIO_NAME
        return store

    return store


def _normalize_store(raw) -> dict:
    """Ensure the store is valid v3. Migrates older schemas automatically."""
    if not isinstance(raw, dict):
        return _store_default()
    if raw.get("version", 1) < 3:
        return _migrate_to_v3(raw)
    # v3 sanity checks
    if "portfolios" not in raw or not raw["portfolios"]:
        raw["portfolios"] = {DEFAULT_PORTFOLIO_NAME: {"transactions": []}}
    if raw.get("active") not in raw["portfolios"]:
        raw["active"] = next(iter(raw["portfolios"]))
    return raw


# ---- JSON backend ----------------------------------------------------------
def _load_json() -> dict:
    if not PORTFOLIO_FILE.exists():
        return _store_default()
    try:
        raw = json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Warning: could not read {PORTFOLIO_FILE}: {e}")
        return _store_default()
    store = _normalize_store(raw)
    if raw.get("version", 1) < 3:
        _save_json(store)
        print("  Migrated portfolio data to v3 (transaction ledger).")
    return store


def _save_json(store: dict) -> None:
    PORTFOLIO_FILE.write_text(
        json.dumps(store, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ---- Postgres backend ------------------------------------------------------
def _pg_conn():
    import psycopg
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    return psycopg.connect(url, autocommit=True)


def _pg_init():
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
        store = _normalize_store(row[0])
        if row[0].get("version", 1) < 3:
            _save_pg(store)
            print("  Migrated Postgres data to v3.")
        return store


def _save_pg(store: dict) -> None:
    _pg_init()
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO portfolio_store (id, data) VALUES (1, %s::jsonb)
            ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data
        """, (json.dumps(store),))


# ---- Public storage API ----------------------------------------------------
def load_store() -> dict:
    return _load_pg() if DATABASE_URL else _load_json()


def save_store(store: dict) -> None:
    if DATABASE_URL:
        _save_pg(store)
    else:
        _save_json(store)


def get_transactions(portfolio_name: str) -> list[dict]:
    store = load_store()
    return store["portfolios"].get(portfolio_name, {}).get("transactions", [])


# ============================================================================
# AVCO ENGINE  — derives portfolio state from transaction list
# ============================================================================

def compute_positions(transactions: list[dict]) -> dict[str, dict]:
    """
    Given a flat list of BUY/SELL transactions, compute current positions
    using Average Cost (AVCO) method.

    Returns dict keyed by ticker:
    {
      "NVDA": {
        "ticker": "NVDA",
        "currency": "USD",
        "quantity": 53.0,           # shares currently held
        "avg_cost": 13.43,          # AVCO per share of remaining position
        "total_cost": 712.79,       # avg_cost × quantity (cost basis of open position)
        "realized_pl": 1240.50,     # cumulative realized P&L from all sells
        "transactions": [...]       # all txs for this ticker
      }
    }
    """
    positions: dict[str, dict] = {}

    for tx in transactions:
        ticker = tx["ticker"].upper()
        if ticker not in positions:
            positions[ticker] = {
                "ticker": ticker,
                "currency": tx.get("currency", "USD"),
                "quantity": 0.0,
                "avg_cost": 0.0,
                "total_cost": 0.0,
                "realized_pl": 0.0,
                "transactions": [],
            }

        p = positions[ticker]
        p["transactions"].append(tx)
        qty = float(tx["quantity"])
        price = float(tx["price"])

        if tx["action"] == "BUY":
            # AVCO: new avg = (old_total_cost + new_cost) / new_total_qty
            new_total_cost = p["total_cost"] + qty * price
            new_qty = p["quantity"] + qty
            p["quantity"] = new_qty
            p["total_cost"] = new_total_cost
            p["avg_cost"] = new_total_cost / new_qty if new_qty else 0.0

        elif tx["action"] == "SELL":
            if qty > p["quantity"] + 1e-9:
                # Shouldn't happen if UI validates — skip gracefully
                continue
            # Realized P&L = (sell price - avg cost) × qty sold
            p["realized_pl"] += (price - p["avg_cost"]) * qty
            p["quantity"] -= qty
            p["total_cost"] = p["avg_cost"] * p["quantity"]
            if p["quantity"] < 1e-9:
                p["quantity"] = 0.0
                p["total_cost"] = 0.0
                # Keep avg_cost for history display

    return positions


def load_portfolio(name: str | None = None) -> list[dict]:
    """Legacy helper — returns flat holdings list derived from transactions.
    Used by the report generator and refresh endpoint."""
    store = load_store()
    name = name or store["active"]
    txs = store["portfolios"].get(name, {}).get("transactions", [])
    positions = compute_positions(txs)
    result = []
    for p in positions.values():
        if p["quantity"] > 0:
            result.append({
                "ticker": p["ticker"],
                "quantity": p["quantity"],
                "avg_price": p["avg_cost"],
                "currency": p["currency"],
            })
    return result




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
  body {{ font-family: Georgia, 'Iowan Old Style', serif; color: #1a2540; background: #f5f8ff; padding: 24px; max-width: 900px; margin: 0 auto; }}
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
    pwa_paths = {"/manifest.json", "/favicon.ico", "/icon-32.png", "/icon-180.png", "/icon-192.png", "/icon-512.png"}
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

_ICON_32  = "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"
_ICON_180 = "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"
_ICON_192 = "iVBORw0KGgoAAAANSUhEUgAAAMAAAADACAIAAADdvvtQAAABCGlDQ1BJQ0MgUHJvZmlsZQAAeJxjYGA8wQAELAYMDLl5JUVB7k4KEZFRCuwPGBiBEAwSk4sLGHADoKpv1yBqL+viUYcLcKakFicD6Q9ArFIEtBxopAiQLZIOYWuA2EkQtg2IXV5SUAJkB4DYRSFBzkB2CpCtkY7ETkJiJxcUgdT3ANk2uTmlyQh3M/Ck5oUGA2kOIJZhKGYIYnBncAL5H6IkfxEDg8VXBgbmCQixpJkMDNtbGRgkbiHEVBYwMPC3MDBsO48QQ4RJQWJRIliIBYiZ0tIYGD4tZ2DgjWRgEL7AwMAVDQsIHG5TALvNnSEfCNMZchhSgSKeDHkMyQx6QJYRgwGDIYMZAKbWPz9HbOBQAAB4HUlEQVR42tX9d4AkZbU+jp9z3requyftzM7MBjaQo4JIUAlKNGdUooIX40UxXr1mxYwJQb1mRUGCIhJVQEURIzmHZXdZWDbvTuxQVe97zvePt6q6uru6p2d28fP7zZ0rsz091VVvnTrvOc95znPQWouIMMsvEWn6KxFhZgDUWqUvbt66ddXKx1euWvX4E09t2bJ1bGwyjCIABAQQce9BIgAAif/fHVbcESX+LwAiIiAAQPqpiAgCEr/X/aEwC4A7t/gERQDQHZEBBAGRlCQfR0QAICwsHB+kfnbSdG7ubDOXjYjuNIVZEAAJEVHcOQunZ4sAAiQIKJIeqn4IBBBwV2tZQASRiBABQQBQWNidlVuCeImYAZMLd/9BRADtewP9fQtGhpctW7rHHrvuvusuo6Mj6cfZyAAAKprxprvFb31b0+vY2YBaDaX1i5lFRGsdG82mzX//121/+9u/7rjr3kdWrt6wYbOthsAWQAAoYwDSeBjMvIJ5v204LwAEt3ANR+t8qunNIwABRHB3wL0YHw0zny55n56+OXuy2fdT5rOyb8LGY+ZeYOvbsPE40OZ1rC9LenxSqlRYODq89567HXLws55/xPMOe96hIyOxMRljiGgOvqP5PJi5g910NiBhYRbtKQCYmpq88aabr7jy2ptu/vvWdU8BMIAHygffI0WI8UVK/IDXLxwBnRfB5EVpvF8NN8t5HRGKHZVI9iZmjhbf6PgY4u65+53EbiZ1UJDxfAKAIpLeCEwdEGbvMzYaFAiI8zyAKJi8XxjFXTpyfMfdvyF9c/1PEg+cHpmS6xAUAMTk/e6AsROVpmfOPRjxhbBlCEKwAUAEoBYs2em4ow9//QmvevGLju/t6wUAYyJEcm547gaUux91tk0RYcva0wCwavXjP/npxZdc9uvVK1YDEPhFKvgq3heEW5+e7bB6RMy6UBFp/87UL0j9IZXEQHPPwdlf+2M27DuNaxX/lWScWdZBYoOxZp8WdyhObnznTaV+nPTN2RdbzpJE3K6LSCxiwxDCKQDcfZ+9Tz/tDW8+/ZTly5dtpzeqe6Cs0XQ2IGut27BWrV79jW985+eXXjG1dSuoPtVTQADrLDLxDA1PVdOCNt6P1juX+3r9brVb3/Q9jR+HkvimTBjTeskNBtGdAeV8Yu5VZ4+QMSBpcrqdL7PpktsYaHwIbl46JBIRrtXAlodGF/3X6Se9/33vWrp0JwAxhpWi2QYzM2xhrT8zs9Z6qlz+2tcuuOBbPxjfugUK8zzfY2s5jjzb+Jv06ezCSppubdZEmlZ2RlfUvS9pFyq2W7tZfGiLn8DYF2Y3w+6uJbuMrT6p6bdpGNB4zkRIpEwQQDQ1umjJ/3zgrPe+552FQqF7V5SuSfMW1sEPMbM7+vW/vfFDH/nUQ/fdD8VB7ftsTPaa4+c711wyL+bYR+PDF+/0LQ9Wh9226YSlKb5pMbsZF6iDJ24y67ZG39bzZx627veOFh/T9sXG99fjy8ZF0EpHYQDh2EEHHfKVr33puGOOFLYs0BoV5S6FiDRkYa03IPknWhtq7QdB+NFPfPq8r38PwPP6+6yJhLF+dtC8QTTs1t2tNbbs7m5rSDJq6XK5c+xy+8Kv/Nir0R92ciGNT1SKFMzFgDpujh3MK8k7pOEdBIrITE+jwg996N2f++zHfc8zxiilunLh3eBA1ljt6VVrnjjj9LffestfVe8ogOTufZ0MqGkFZzSg7J1LDWiW97ub3WoOAFiDAXV0dW3vfe4D9p82oLr9K6WEhatbjjr6qJ//7IfLly/t0oZmNiBjrOfpW/9+20mnvHndE0958+abMGp/vOZctN1iNbm9bCiauw21/rbdNtE5kui8MXV7H2e8edtpQLm5VZrWdTagbFTU9HBCDqKU3ayVp83k1uW7LPvVLy96zqEHRZFJYeG2QbS1tv3TiZGJfM+77rc3nnzymeVKpHt6rIk643Vtk+Q2AXK7mKbpsZ7RzTRl+J0D4TnYUDe2u2O+cgPh2e50eUaWa0CNf8fK8+305MBA8bLLL3rpi4+NolBrr0MwRE2/yy6NMcb3vF9dec0JJ7yxHLDqKVljulq6bIDZdA0x4N52r+kSYm9nPdmraPqsdkF0l9aAje6zXijZ7gSw+ZIRc2yl3bJkT8D9nK5567lJpyV1JSMbhaq3f7JqX/vak6+9/gbP842xHW5QPpAIANYarb1rf3vDCSe80YKPnmZrCWm2D19DVt/GerpMozq8OONvW7fIbjxct5HQ0+eKWs+tZRdDAYyR7hagMlsXadnR2q+ekNYcBkXFV/3mshe/+FgTRSopVeWk8a1nyGK08v76j9te8qLXVCMgrd3bcAYX2N6AsF5R6vz0d+OfJFlE7LoE2M0WOePfdrD7WVjATD6pGdRuhxw6VEzEwTxMDfcGZW4WW/8zUsRhbaDHu/nm3x/07P3bxdSUuxFq5a1+/MkTT3xzpWaw4DHbugvE2Z1HY7WvvsU0bZezcgbYDrzv4gjZjaN1P326Apq5bnIzHzyt+0nbtAbmdIlsWRVKk1PB6088Y936DUqp3Lybcu99EIRvPP0dG9Y+pUs9Epm0kCgI0uaqsjejIVwQVzjcMRl11uzabWFzPnj2EnKdSpPd5z4DzaHJ7EP1ttaTHtCVVAUwk5O7f6bfqfsXREEUzKmWtFuBdBFsZFRP3+rHHj31TW8zxoo0L4uINBsQMyulPvqJL/z9r3/VA/OtMXUDlk6PVLvVlDrVQWaMWzPRHHQT6HSOjrfnq10I1Y4fM0cX0v2hmiLl9teL7SxvNsljfBBENpHXP/qXP97w2c9/XWtlG50QIjbEQK5KeuONf3rxS0/QpUErtsFzJFFYasvY+FvBfPiheU1bq54zOaduKlNpTImIANzNfZpVGD7zbc4tSM0hTO4AG7bfu5uXebvj+yw+RABoqzf/6bojj3heUzDUUI0XkWq1evDzXvTow4+pYqkt1txoQDFHp8lJdiY2zAkfyk3aG4HBdMPnbhKoOeRx3cbOmZSz4W2dbaupONq6aNlVBcgpE7UUv7bHetKTV0rZ6vQBz97/X7f+3vO8LDGTmjavr37j/x594B7d08vW5oVjbR7KNJzrYBpEncoR21/xrm+U3TIx5vIRcwOg///8y1qre+fde8dt53/r+0opEW70QAIMTEiPr3nywIOPmq5EQA14TwO43AI2oAvWoBPvp/NT21rK6JIb1AGGnvPTNsMnJpSSuZr4nCCfdr9tH+7keiBJ2LY4u0cmif9MNG+w9967b12yeJGjZiQeCEEYEPHc8749uW2z8vzuA8OE+N6+/iXSmr9kA9L0f7vJgGZMy6WBip8TlbeLu3NfycbOaVzZ+ubs1yxi2xm9VApJ5/Mn28SaHUpJ0sAN6jLhTVdDFQpjm9d9/RvfbgghmNlaVopWrlrzrEOOrlUNKJRWenIeooWSgzXPjJK1J3XkcEO7KD404cu5fNPu4cG5pWkNAOB28nvapBezcmMk2+UaW915vIY26uv177/7H8uW7eScUOzUEPF7P7ywMr5V+Z7b4DDXl+RWWGZlPTOeblNxJ7OazXZDMyOB2EhemxFYwhaubdZBNi1oq0+dQ+pe/xNsWGrJlrQ6AYnSDE9n/3w7kLbcJ0T5hamxzT/88YX1mMFaS0Tbto3t9+yjNm7YQp52LU5NXlGyUBA2pIvd0OrSBqrWHb0ppapffN57EAAIRZBIOBD0KO0hy03lZtw+OizWjGl8ftTfZeDclHCxBdHkgVgB4HifaaXQt3NgLdHn9mRhHSi8RChBsGT5ogfv/ltff5+IkCtT/Pb3f9j4xOOqUOCOxYpGWBwBusI3O+/o0koFJ8p5T4qDCigFXLYLFxOJ3X7AcMb+k9lBcLMN5BHBcm+PVyxaLlulsKVjrD3245Z0R1djOjZyMRWLa1ev/O3v/4CIlq3rr4JfXnF1bBwirRtYkmFh46lDN661lQWRsyk0BkbQsdqlEO3ExBvfsvzQ5yy21SixyfpX05Y0t7ws68ybEsOsh2v6moV1Zi4ciSQM3/XuvXbdq2CmIqWT9stZcUW2g1jSzY4mme4oBLr8V1cBACGSUt76DRv+8td/id+fYj8Sl1Ey8KC4rrjsP/P5262f3zklQcQYJWp1S46zK0ACKECAHpGZCt/1ib1OPGPBDdetxp4Ss3THKHKtfiSZvCD5J7XWBJuSu9bDNtkmZmBDd8ndGK6rWxFhZYrvvHvLRdftv2w3MWWjFDb47G5Cq9mX/bv30NkVZmZRvX/+6983b9mqlCYA+Nvfb5vcsln53hwhtQ6ONG+3mkMRXkQARWmIpipn/M+Scz936HlffTiqWCJu2gfb7ziYG87n0zRbkIV2YRY02g1kTCcnGm1yEklXrYihgcLN16198snxq/743EVLlK0GCqn7CnQ9fu9uR5v1vty4DqrojW1c//d//jvGgf58y98QLBJmaxTZom79zJq+O0AUyRtmRERy6tu5O5emaDJ62WmLv/TVPW69b92tfyhjqciWsztXneXUHEfXe85Fmgl0iVvFxtb92Ty16Tl0LPS2ubsiQgQWgX54/pZddhn93pXP6u8DMUwoIIIthfecdKHlF9kNRLbbD9VdETj6GgHwzX+5FQCIRW6/8x4Bj4X/fwQ4lxZPR4rMdPSM5/R++Xv7McB1VzweTYfkq65RsaxoQQfizFyRIdzeC2YLUijc/o+pu9aMPe+Qgc9//yAOKyiIIsCAItjlbjCb09seUg2Af8cd94oIrVu/YeXqJ8EvCLeENQhzDvJzUZPWH7IAoOTSvwUQBawU+/CL39/P7wsmrPzj5mkgBdyANWNu8XLGtGs77n2HqDkLYcuMQDOICCifJjdO3XXL5im2rzpx5E1n724rAWnlnvnZ3f6m6KLNVW4PVgS6uHLVmi1bx2j1qjXbto6B50kGn+jg+vLpJi0AY27hArtjOCALsKAIsqAAarTV6ts/ssszDuwrC65bW33koQp4BMLpOTZH5XFc3GTEkBMvi6AwCgO474bNtFuGpHQy04Zza15AtyfUE4bbb53W5G+Oxt97zq7L9yxwLUibRNuBVZjLwG988hln1y3Tuf4jLODpTZu2rHhsFT226vHsWTZ6IGy1EmnieLj0u3341pFlNuNzIEhoK2bnZ/acevaSjVGtiPqxhyent4bkZ/Hqro6WZw04oy/pcNhZlVRT4ayGyBozcQwDoH74/ulJjkKJ+gbNf396T4kY6tWdnBY56QZ0Tm9NS4w4Y4mwXYyBiLZWW7NmDT25dh0II+S1ICVkwjiO61zD20E1pqabiyRg7Bkf2rXYZ4MwEOAnVlZBkGI1jLq7SK6LANIQErOEzgwq0UQbQkQSdJeITXCpuCcXqXOI3aHE0aG3JM5VEGNRIi1PrZsam6gWfG8sCl944sgzDu2zFUMqyWgyxel8U24lLko9Ep9D/tUuBlBEILz2qfW0ZcvWelzZjh4gubH+jsE6sT2YRAps1S7e0z/qNfMmOBSFBtT4mAawTaoWjWAPtgXPG19MeC2J0TTYUH25nA01PYLZLX5GL9upDpgNxIiqZVWZQgAIDXhe9Jq3LQQ2iChzXn3cwfcu+7Vp0zaanJzKuX8NV9sIOrd2PbZrBc9LyLF7ZMxJFBp71AkLigNYiSQijICDIAKwLYg55naYtPfB0qg2Jx1SsSYEMVbXA2wX8cydl01gTFQNogisVTIh4WGvGh5cVDSBxcQZQt6+2ZaKLzD3towZXCwAwPR0mSrVWiIVmJcQup3b8Z2xERxqworaF/na6SXkIyWZO2WtgIZDXjxcAWMAQ+YADHkC4ElcTUmjc4qR5fbtB0l/SN16nDJCx9ze/YpTU3Lf8X86gka5/UOtPzdctyitFXpgwDJyxZjhhd6BRw5AaIgI2y84tulkTRKR2VE0u34nGmGKo4cZ4UFsaKpoTih2dCEmToAjHlxEi/YzZQkNWgMcgh1c7CfhyFwcO8LT3Ag2U9iXw0tJynlgTV8/F4c4ADZgrQED4TMOLyQWLNmiX9PTPtvqxBwi1FZ6HQJoasckbPYlTamvaxND6aAU0YUzb2KQoWQrCQChXbpHf8+oX4sih4dUIFy4ByGxZUYBREryGsZUxw5ihD1tg0SBPBQRs4SCfInPRP+yyaslf4ItAq5dFcKy3LdUTQsRwfCSXQdKA2raWAS0iDWAXZ45D9Q6KwJx4UZAUly4BXNvCrywRbWzfdY2IyO0NXgTyyQsLcV36QwPJ/qmIFnV45b6aGuCnc1gm4uUTc+ECCIB2NGdgUiFFoyIQVu2vNP+xfnLixIKOqg/PqarUfCOBIjnBKF3A2amIFNDqoQCwvsd1kcIxooFYJAayLyl6PeLmPpS5yK03Yd+HcDPGY+WMbKkCjOHPaBT3aG7ALkNgJHVzo3T8v5h34IYEAPCINWA+4f9g46dBxETZVMm7GKtOqdmTX/bZewp3bSCtMeLGYBBBNCKBfLp0JePVsAwihW2KIFYbx4Ue3xg3gG9Xjv6CxEpla1qxb+hpWFWCAVRKEZWUICk/a6UPiLMTeXSnO5gyeTFMegkAABaGWAr1oq1IoJcE3v8f81HL2KrBAGBk5aQLC2f6/CVtDEgrF8FZjtQxEWdsXFmo/KWMFbaGVAHJ5F94lGERBCYSHE5OODowd2f409HhhEMWAvMIqhJ63g3alrDHO5laxW8zXPTWVICZyqexMpsiDRDD3Y739OuDtxFKarLFMAdJYoiC9YyG5AIWAimg9oeR/pHn7LIlitKUW4JGDvnul3UH3d4QtDp9wqAAQt0yscXWaxZy1aEBRiEkYOAo9C067VorhFhJjds6YPIGnJn+L5L1RREIMxOIGhJphqy4haqpRA2KCJCC/e+UZa1tbLYQUvAuYGt68IQmEEiESvsJlqUo+i/zt1pyV59ZsoovwOH3AXX8U6RLQ2JWARurIJBgi5KTvKfWQ3ZvtDKrQql8BIKobLl4NRPLnrGCwpTNRYiC2gRLTOgjG0Oy5MWFaIwpR147apUCEwgjnjBksKUJEBtANVcGudM2WWimCHt4PkuxSVm8xTP2NjV9DsGBsSNK02lwpaUFWsFjYhFqYWgF8kHLlk2MBKaSaCCIkGwDC38yzmAH021z1mEg21qGo2kVwGwzl4RQGlCBDNZfdl/j570sdGxWgAohtkwG7YhiwBsWFXjCpJq7JDuLpzHNtWV5PwZkFsP1kU9Me2Sy6C32QgW28WQkvqYHVPHyLsdacmXwKcNK4NNT7L4bBissGGJWJhwcjocPVjOuWHfPZ7dY8bKbI3S4PZjlLlUXkS4TUe91GPE+oV3jJo7mS8DAiKTBkAwk6HU5JRPLX3rdxZvi0IGNAgG2AJbECMcgXnon+V4/lD7y2rS0+k+X0ioyl3WtnO+NGYWBFsT9XjrTZxhw3ScBARq8lWzJK80yiw3XAp5EoyFq/5WHt27NzShaEUOfxYhwvK0DB/of/zmJb8/V//+R9MTmxjQQNFDTaQY3SQoTnclnInG2nBzmqAjjGGb5ulPba/HDVXB9P8crUlAFBuWwBoroOGAY0qv++TiZx7TN1ENEu0lFIgJD+xBUIX7bx4HUlwvhXZEmKRZUxxnRFxnOmbjw8SN2vOis1FVgzgGtHRld67D5215XeoltKptxDU3QEC67fLNz3ljr0EBYRYkVAyiEBXh1FSoCnzCF3Y65m10+y+33nH9+IoHq9NbbBLVESACCRCBFuWpJCOUWfXSN2GDIpKXeEhcGEFUCqwRiQQYxDIwA2AaaXk9asm+3v7PH3ju6wb2PKYYEW8r10gpRrfFcRygsZR61YqbyuvujbBUgMbEqj3aNNvcABvZ4jKT9HQ2dxNE0GlMlInkU48jzQtGsTYWpKUESemWnYoEzZ3L+bXT2AmmSg3CgCX/kT9XHvlbZeejfDPBSgkzKCRBYAZFyEZviUxhGb7kw/Ne/P6BbU9Emx/V61dEW9dEW9ZVt24Mpsd5aiwqT+HU1hBIYS8SYTwMTiBXn1pEELnDquU+D4hCqExk7HREHg4vLvb0SG+/GhhWA0P+vMV6ZLEe3dkf3oNG9ij09lMNzFSVhQWJLLMg2CRcYQHDUmT4/Xe2itGqJGyzs8y6wipn7HfDtFKPKfaWCSZk5k0DhbU0/m3DLiuZTv2mJvkMrCz1zTmfMheXTonaPTo5CjrppxFyhDd8acs7Dl8WSSSsyDlljs1aQAgxrMomFq2xZ/fCXrt7+7604AEhWGaSAGCaKxP2H9eN/+nS6QfunLSGoaegfAUsrpGpXedXLj7a9DAQIRIyC5dD5mBwUeHIU0Ze9Kb5y/YlLCq/xwfPCjAAMlAEEIKpBbxp2gCAEDAIsEgczTr9f4ki7hnSd18zfc91k9RbZCvJSMZ4maWp1tS+c2HGwgU0JcczZQaNO5roJh5ZLJeQi0RJPIKxibraUFBOOp1zJC+6jrEhczuFhXq9R2+avOXbE8//4EB5U6gLKpZVFxQBJFaCCKAUWaByGaoQOCUTRagJUVuaT/4ovOR9oy89a/GKW8p/unzsr9dPbltfA6WwpIiQ44x3BhXaTGGQAIQUIomtAdQi0Lzvwb3HnTx45Ov7R5Z7AXCNDYutSdUElhmFSYQEgZ0PJ7AgCQ6dVNFEBJCNUJHGNprLP7QeseBG6GFTLdJp6mQpSYBITEgcoYQh+ioOXmEGdwJSd0EAs1PiFRGNOWghdqyISdscSuol41Yzn3M1W1iwpK//1IbFB+qdjyvUNhvPUyjCgIQAAgxAbpXFEoKmeK4KAxoBMkoQgwhr1ihtdzm+eNbxy056onr7b8o3/XLsvn9PWoNQ1FRAdHxdmTk9VhqNZZm2wDK0iI582eAxp87f+8jeQsFOQbSlFgIQoJIEXxdyiJObisqM4EbxsbiIDARIhEWILQgBkrrs7Wu3PBpRv8eWW7b6OFxItYqQUFBsFWwUQBF23qtv7ZMBWxdRd8NDb/k5ndA4k0hILADd2sefmaCG0MU8vXYylLnyhl1ol2QaYEWQ0IZw0WlPnf6r5cuP0JUtxteKUQgIGQQFAayAQkQRBrd7AaEQokJEFBJAhSI4UTWTEhaWwXHvnX/sWSOrb6396dItt1y3bev6KpCGXq3I6QVQ6/kTEaLYgM10gEV65uGlF75h/qGvHRxZhiHIdBRMVoCUEnKtOMIArm/Wuh+AGYCBLQsLclwJjuc6ipA1VhVBo77kzLX3XTOt+grWtNd+ECCFQGIDgMACRIt36zv0xQPHv3nBltX2c6c+Qj2FhL8+B8GQ/D2rMflARaQpGaOEmWQvrUuhJCkoty01x5VRzB/vlVtwmdl6YsUqcmLowoA+lrfAha9dccpPdtv7VaXyNguRUto6iyVAAmIUhSJMSEJuiREUICAqQERQwIRIhGGAAQda087H0DuOWXzi2sV3XhPcdNlT9/xz0kYARUDPB2l08wxSiQB4ZIl6/isWveCNg3se3uORnQSzqcZOMcQqiUAAwAo7AxIAkaQ0gSyMAmDjabPC8dhptMzMpjDkTT5lr/zvxx+6vqx6i9ammwo1xoVAhMZamAYAM7CgcOAxA0e8bvgZx/X2z6ciqGt/vkqYkOpMuOa4GLOhhePsSvOvpDE4whypHQHSmdIJNI/+ShO7bka+YT5VYA787diURYDSgVoowlii6qR/4etXH/+/C49477Dql+oUExApBGSFQAAMqACAHc1AEFEhkgAjkACjKEAEIRBSZBkmKiAQ6SX0grN6XvCOPR65tfqnX6z/1/XT28YFKIkwgUnAI7Pvsb3HnTJ68Mt7hxb7NTATobHWEiKgssAgYBOSi3UmF7sWsSAiMejLIlaA3ZtZxAKgVX3kkb7vyspvP7xu22qm3gJbi6gaFf4BFTIDV5jZeiXc77jS4a8dOugVA6M7FxlsmaOxADxLj/69AqiYpZODEejM/ukwPjYDcYMLohER3Odhcz2qhaAkGWixsU5ZZ1m1ZmEtuWVuj3r8/nRUs2T1iEiMRQ+F1Y2fX//gdVNHf2zhHi/tRT8Kp6wYsSQawRIpqveQEACDIIoSJEAFaEEc9ZWQCQEJEJED2cwV1HbJUd5ZRy0/4UH1/mPvmZrQqN1Tpniy+tZv7Pya941UwFaBN1YDBCJkIYgSWhSLCILbtly442bAMwhnqMQsYEWstRGI8tDrBwi9J/4a3HrBtgeunQCF1KfZWECKp9wnrT+2xmAskOx6QM9zXjVw2KvnLT2oCGBrYsdrFRACVH4Rn7p3+qlHylj0O3Uac4eYGrPOpGNBC5FQOzcnLfFULpm/gaMtLZ0KjfPUt4uX1SJEhCKCygVE1FdYe3d48Umr9zx+4LlvG9z52FJhPkfTthagQlEeqpT+KaiQEIEAFAEJKHBCH0AAykGNIkiCCkVUuWoryD17wuAif2prAJ4vYIEJfb3rYb1boDY9xdpTQMjAVjCJhV2RApjjekfsihhjl5MAJcwSGRZi1YulgipvsA9eU7nn4m2P3lyTUGGvBwBswZ0XESKiDQGCCMAu2KVw0EvnHXbCyJ6H9xR6VADhdC3ihJ0IiJZZg1rxr5qpgBoANrIjOHXSKYoS0KnvwEbAW1rtpz78XaSNW5MWYHrGXrXO01UaNrbErTAT9Vlgb8VN5RU3je90QGH/Nwzt/er+kT0LBiWsRBQKEYAiAmCwSEiIlgkBFIJyMZOIxZRczQlRnoDBFHh0l8KT90SE1gqL4b4h8hfpMlurgYGRY8qsFWCEpM7ggmWXGMZeB0RcVUsMIIgqkjdA4TQ8+c/gwSunH/ztxNhKAwBY8qiP2HKa/IAFrlgA7ltQeOaRQ899Q//+x/cOjGAIWA2DcpkIkRQCAbvR6ghWJAJ1z81T7fN2aeHg5v6z28HnAqIl1nXHuGLXUT875SxnPycl3sY9D+1VO1rVMzoESZJ4s8SppXUoRmZhhRSpAS2s1t1bW3fvxlsvmNrzuL5nnFBccoRXXEBBSFIGZEAlKuE2E5JFIBTlQlEAFRuQKPcog1grPqqFu5cAJgEUEkmNBxeVvBGshAaBHEPElVgtsOMVukKEs5vYIYkIgxErLOQpfwiFeevD5tHfT99/1dSTt1c5FFCEvRoB2DpxO4qJzsClQb183+JzX9v/rFcOj+4qAKpm7VjFopsHT2IBbezwEj64D+Pj4WN3VEFpYc7rc2qUD8AMcNemGNuxh1VA2OFAGLNyISfH7kDiiX+FWYp0V3WlDoWwho/ICAOk14ys/R4b1QKuMIAB0lgqKB8rZbj7sq13XxaO7l3Y92UD+7x6dPRAXwomqtowAKUItaCwEiREAiGH1SAAgIrbglzSADXA/l0cy4sQSIzMW0q2BEEFFCIAJEomwuIa9NGKIIugi3tABIwR1KJ7NGisPGUfuGrqwasnVt86Wd3MABpLSvUxMwELSyqF43qGobdPvfGLSw4/09fgTRrZNm6VAq1ZyOUGXJdSSSBqy1ws0FP317Y9HmFRtZkKnHePML53XU6GbK3GY1ZRIMdNtczVblLmxqZy7kw6tx0GU+ZbraQYlQAhGC72qpeev2Bsc7DxH9NbH5WJJyHcZgACAEGvsHkFbT5v4m/fH192SN+eLy3u/uL+4X2KxrAJAAmsOFIukiVHqFMI6OxJhNC5FNu/lJxTJgQGM7ycmCiyZJV7ULBOaWZhiR8ft5mwMCB5Q2TGacWNUw9eP73qxonxx6sAGkAVBrUqUWVMbMzFJahj9wiCQlKuyE8+vPaWX+Kxb1nwrJcv7B8MKoE1Ft1umTCLUgBPQMBaIcAH/zotIauCskacM+viqZY4OMBONKA2XC7Q0rHS0JV6nEATrt4sQdpcCMB2dtPhOXBdLMCCBTXxZG3F78rH/HBw2esUVam2EbbcV918e7D5wWB8FYTbgIFsBR6/JXj8lom/fWNilyP9g941Mvqckq0KURxBE7AjZFKMrYACJBYGMAZ6dyqqHsVGSAOADO1aCsBGDEgYR40CKZcxhgQlhl4tAdf41vO2PnzV9KYHpoERwFeq2LvY7vTKnvl79d739TEn7Z4ynVsbSoMyPHBD+MANT+56yKaj3jL4nJN3oh5Ti4yKwR0STFs6EEAMYNlGD/+lDEDJSjOAaleTaWkS7C7oadmLdNvSRBdepB0a3v1QGWGO7Sn5IaeU0XCBKFaot/DAJROD++Aeb+upTQR6sVq43Fv8yiIbnH7CPvHv6fHH7PRKqj1aDTZAZcw++JtJUT0vesFArVzTbltCVACAnPagqUSWQUAw4MIirzRE05usKAJS/bv50xwZoRQ/BpefYwztJDqywCyqhzbfHd7yuc2AHpL0LMSB59G8I9XCIwZwm7rtvZvL6whK1Nil01INJcE+JSKrbzerb3/q6s9vftfley9+DgRVImKp80YFEFgYPdr6JD9xTwV8EG6/H2Fu1Xvu7ECdsJckiy61ih/MeRJFjlU1cYwaTS1n5EAd86r/l0r6n+dOD+3XM/gcb3pDqDxixQKsF9Dyl/eXtoRSseFGqm2xT/4kqjzas21tdXosEle8ZxdBi+sbcHuVifcyBEAJWA/o3gU0vc6IB6rP61muaoGICLIbMgkiwijM7PYVjjW+xVr2gTaurqEiVaSlbx5acIxgD+ACijaqO9+ypbaRsC/WJ86qdLbCfG5OpJ6nzQTs/4reoQNsEIoQWlAZLBhExFgo9cFT91QrGxl7PGFXfKUcvlmWd9PUUtmZuZEnjoOkKK6PSvNAuFZNsW5a5pptop32Uas/nXG4RFPBT2mO8KZ3rt9wq1ELdUAgAMJoQtACuipTm21QAv+5WNqf2FYn1pTH19csSWQlEjSMkWDEGDGFjKFQIFizGDAGgoHFsGiLCwgYxUpp0HgjKghUBBCI1JhrwgFIJGhEhUyBYMgYAoWgAqEIacuKUGxExWDecRINSCjAd+h73rattlljn2qxnnxvjohKKzNRPeZ9Q6d9bxdWlhkYwAhbEMMSMRuxkXAkzMiP/mUaBIhgB87x7Kz7JkkEh9AyLqQdS3yGDQ7r7DICRBZkoS6kDNt9tOT0VrtExIIvwQTe8vYtG6+ypUHPaBtEYkWiiHp6C+gzMpit5I8yIAVbcWqNWOUFRiKW0H2LBGJD4Yg5YglBQpFIIGK0WvqWKQAGw/07+TioQmMDkEA4QoiEQ5AAsn8CEYsRsUA1ayZWRwDKHxY0wFrbMX3vp7eW1wMV0c21QVSQBmPNriImrpOHdio6+r8XvO7rCyamKhB5VsAysHBkjeoV8SS0rl0OKmVe/Y8KYLK9zUKrZBZm1EIEsQSNhLIOsx27CpIaGo8kQ6+WzmfX2V5z4z2xQL6KInXr+7fe9YGx8HH0hpTtgYAt9lLPcCEyHNXEW65UX0FCHHvYhBoiAwFDyBgxRgKOpR+KGEEjGAFEzBFzKDCwSxHAguH+PYumCDUrgUDIGDJGQiFD9jtijBhCixFAdRLHH5sCoOLOPo5Q7aHwkQ9P1Tb52KNYEIFmWlsBANJgJ6Kj3z7/hG8PT5ZDy2CILaAIhwYKg/reX08/dQdTj1cLLWvY9li08WELHgHLbARDEbroj27zBgRhXS9U5dUt5mKtTUehro80c9CNTR5eBEQhanrs17U1N9UWH19ccHix95na31n6+3vGKzWuiLezLiwylSkZW1Hb2Q4ZscSxSAslfSmIhJhwaxyn1FDfLj1AAhwN7FUMFUYW6wwtFCffGFdM42GfICzo4dSmaGptBIDFA3qC1bj6Y5VwUlOPIx4qaDu2AFP0Sykyk7XDzxw54f8WTk8FKCyEzICAUQT+fHno6tovTn3ywJOHX33EaC2yvYP68duCYNxgnw88m1slsVTDXD2T0glwnMRAksatkqDTmHaqt5ZwcxAClrS7Ah21GGFG/WVpM+0gY9+xL6OmGyAgQtgHUShPXFl74spacSfs29Xr2VvBCGG/9OxHxV1VZUVtcnU1qIlFUYxAomKV+Pq+iyAYj1xFjqy31KeSx+WouLMfWGFXKa2DZmA5DmM5UZlgy1Sg8gYOx4GKiqdg5aenwmnEEjK7FtQZERMhpcxk5blvGjnxu8OT5cjBmcIAKIG1/SOlB64Yv+j01egXH7x+67PuLg3u5lmOVv9j2l0IS1cloxnD53a4XfZoDKjrVS/JIjHJ7oMdSiedyACtgHIHXCH3bZ3FN5vZSAxAQH0IrGqbobYuhL9ZAKWKqEcFLQFRZY1MjxtVZMWEIBaIyGnoEgogxPkEISMgRAKjttBP1cj3d8ZaaAiIJW4WShrbUdKGKBEBtFYQZGp1BAHhPNn8i+ko0OCTcLeZMyllJ4ND3zh8yo8XVmtWmEGBMCFgGEWlkdJD11R/8V9r2XhU4mCSHroofP5XCmObgqfuCIB0N9PK6gYh3QZA7RBFQtSJqcTCvkgOEsSGxgzMZxV1YNrWgWZMiWlx/aipyNotYShZGteZR5D2oEgiTybsNFA0oIeAngu1g/UEKOBRsNlE05HMAztltaddXwcBJVKpSSXHlZVD8gaLhWEwhgo7FcLAfUBiPkzpE1XXZwExFqkHp9YEACgRRhbBQ9ca0M0lKgVmqnrQGwZP/cHCSi0SI4AslhAhMrZ3uPjglVO/eOMaYxV6aI1AQd132cQBZ3vT2+zYYxEUCnEjHHCHIHp79MWbG1Qgnjkn2U5LgZkbT2dk8UvTyLRsr/SMF9A6prRld4sZDHWtQowrWi5kZyWWhJWgAg9BAxKa6dqKc57ksuIeDCOxgBaUEYwEXBplGCxjxGQYIyPU6xcX+YVR9oZ6TYhWiAUtk7EYR9wCEUskErEYgTAEGPEm/hKt+/kW9JNusq5vD2k0U+agEwdP+dmCigklRCEQUSASRLY4XHjw2uolb3rCGIUaXPUEC1TZWF15bVRZ49kykQIR2IGCmq1FtBYkmnUaT2PsdNKmi27mCKTvyfdGIo3SDNlKRWs/ETZINWJDWV7yeCPYjFQjIigRQcqw4RLpMyz4G39bizatO+AHu6pFEE4wFSSVbyGMOQmAgkhiOVLQu0cfe2B9MNNxDdDNLBKsX5GAIKO11hspbruxfP87VoVTjAVPhBvtBzs8b0qjmQwOfO3AiT9dHEbGGoVKgAmBjYGeEf3wldO/OGOVjTz0RWziKy2ALj18UTC4zAC5mgrtWFmtGcnHujm/EZiFjmADBUiahEtzP76p8Rawg+PJfErLk8xpc2PDB7m6BIEjl0JmqjSgMOBA77bba3e+buW+P1iu9/fsViGfgRAACV3DmRPHQxAKIynu5avBARaIbCr368IgTKUSRMCy0Hy96fKtj37gKbFERY+tAJF0VW0S0spMVg947byTfrYgCmvGaiJH8pHISO+Ievg31UtOf9waDzwQG7d3g6Awoi/rHqqsu19BwRfuMNfgafmSZBB0a/v0zOoWLf1ADU2y7QawNexuOJcdrQtRNydYo+KOjPgjBFCkYrGEk6un73r9Q5N/qNj5VDNiGIxIxPEu5hAdIxTUrL+76t+/GNYsM1khK+h2KwfoWUFjVVWAh+Cp744/ctYTAEoIOGqazddJ4IC0spO1Z7564JSf7xSyRIYQxbJYkSiU0nz98K+rl7xptYQKfQWskDSgckQlABFmBEKFM04bghbBkFmFz+1uPWWosGm4KzO6tbwzEEAGFCQQYMDmk8jxSQIOqs6dlNAwMLo9KSAmcQlCjuQl1HFeQoykb0/0ewS1Dmv+g29Zte2XEzjfqxk0VjGLYbHi/hcsgKlK7279/v79tSobFANoBS2jtRhYCQWMtQEZmldY/8Wtaz65hnoV18KeESjOx5R03EHiXkRIg50Mn/HyvtN+ujC0xkYiJI5JHURcGPYevnL6ktOfsOyBBq4ygICJ0ERoDEQRWNcrgNmRCZ1D0qYJ5R0sqf4nHQXXdNx5kTeKYW5hfByrSwpBNzDq2xZW8kReE3wub2N2Q2GSGAzrkw/aFAgRxCAOeYvfM/+p/9lgTMSkVrxvza5jMP8dw2bMGBFSiIksKwEBA/QIoIhBmwo7CQgII4th1mQKsPmDT2y6aLOaV7ITtmdvtdNZA2s+M+0QyawoXeuiKY/sZLDfy0snX7wkgIgDEBWbT2S4Z7j4yNXly9/8hGFFngZbm//CQb18HhiDRMAslrXvb7l2U7gtBG+GnLwDSabLP2k3qC8fJ+4ute6o14U5Cxd3YeYAWpJVYswZ9dhSf03nEsTi3yCZyRjNJxZjHr6U75nmReHCzw1oDRIR9BVXfebJdZ94IipIiGwMMJNzM662FVkwBiMGK8krLJGAMcglZVE9+Y41my7aSAO+nZDeXWnX745UJ200FoB2GDy2GW7ifI/Z+9jSiT/bKYLIBMAErpksNFIY0Y9eXbn89MeN1egjih14xaLCkaPeYu3v3Ovt3KOX9+hdetWyUmZU3tMZ6zTbTdL0JnXyUb71zTh3fXaVV8R8Yaa5D2OJu0Tq7SLt4g0B0MgVrvwzgOepxV9d4A96Ug2pj9b9eMNT73uSpGALylowLIbBsJO0AiNgBa0VKxIxh8LWsi1RMMlrz1wz+dsyDRZ5knv2sEu+PCD9Mv23IFZditEpzIt7yE6aPY7z3nDJIgvW1MSSGGYWGxrwF6jHrqld/ubV1mpVIKmh3skr7jePJ6ytsKlYW7ambE3NROUIxH3Q02g3bbxJrFZDcWuzdIKSmbljDp9X+u8wAqF930UD8NPZC9Yn9MxcSosvhwBER6sBqkr2Mzt9qccfFC4LDfSNXTu19p1reVxqwqHFSNA4NCj+BiNirEQChiFQEK2Lnjzt0fFbNqshn8ehb39/ydeHeVTsBglWCngpbpoj8Uka7aTZ7Sj/pF8sFV+iUCzF8o+1UPQQPHZl7ZenP24ihR6wdbxZkgjAilgQK9a4nnp0jngOGsPtQp9W9dz8YbRSb/h0nc2COLdxCk7816FIBIL1eDbZkJpFbllaPypnvl3n7Awx33W50RAsTWEQOtKXFUSq3V+FCkRbIVjGi746Ulzu85TFfj325w0T/54yPRhZMVYiK4Yd5wYihlAoARhB+v2tN0yV76vqeT6PlfuerRZ+uc+WrLE4vSEMngpBg3A6vg4bfY/YSbPz871TLloinjU1EEJhZpbAsD+iVl1T+9WbV0eRRs+J/MYYKTMyowgKA7OIIDsWOM9lmko7tkXrztN2KELi82lGtkWXpYYOGpqz3gFnldi34NTt43oAn6LHKVgD4iuZwGA5DH5tsLirgGtJJAqYEmC67n5CC4bRCFohw2hCFEYgshXqeY439KlSADacYINcvsdKRUAB1reV+j5OGu2k3flI75RLF5u+yFYNo7IMzBRGUhjyVl9bvfLMtSbS6EG9rxRB2EogYEksskGJUAxKBNZy3L369GA/bXM0rPNwKb3LDaExYDv3k2mWgE5BdCavnlkzNZ2nlh2+kfktSaOEdmaGXGYGT+sJZI7hJtAr4IrUHg6NptBDs03sfC4eXZQIJG5lx9jrSPxtwP2ABsgwWIbIMkdOCZN7XzfPDpKUwQJFlqoPRgAqZtfXfb0AsNJoJ6Odn1s8+aKlWASuiCXFErJIaLgwX6+6pvLrNz8ZBYR+2ufiWmoJBNgIW2FGscAGbMhO7HV7FU/bO4vunnzRzTuBtANfOpFKOkF62AmYTg26GfuBOikWJW/Xam2hTGZI5gGYJHG3MQJAeHeELy0BCIHiKeClmjSIVVYpsQA2fq4w05zrPp0FmIGEwCMApAFfhpDLIIosiGwD+zBgvSBVR/dJKzMZLj+08IZLF0FfUKuCImWFRSC0YXG4sOr62tVvWW9DjT6IxTrBJG0aFmHDKUkcBVlYhGYlBjWHZChfUCUZkyIiOv4p2xPfsSDXmpFuRyTfFD7nUenqRQps2eek1QTbWXDaPwce2kdDOybQq9kKgPDuPg1puxkiQDRCXJ++kamPSGJAiCxS8ABIDbMMkakAg0BBmXtC3higr+tqdCIgonxlJ82Sg4uvu3RY90VhBUCRERYAG0FxuLDm6trVb11rjecaKgARQMVijPHdifvNsirDLEIcl+W6dEIz2lB3FtbwWbpF5VW6iYoSRX3JOS3BdjpA7fxQVoG/XcOGq8tKdogH1Neuro/WOGsxRhodpucEc3wwW4x6JMJDPTACFvSwr3Yu2s01FiucDGlDSvksKUHKKbySIHgEAGqJMp6SciSItk/sfaFYFIVg6ydBnrKTdqcD1Um/WATzbK1sSSlhYbCRleKwv/ra4Jq3rbUm8T1xUMFprAEIImgjBna8I4e8MyCAUDsBqFzjmDGWaLWwvClPEhNqXBI2YwQ/49iEXP2s3PhrhkPlj4NoItNndrAuCjrYXFFBAAQm+bvhECyABTBkcblCiNgxo4EiwciKYTAWTFwgk9CyicVZgMECRLiHzwiWwQhIWewDBoiQGTIZO0+Fiw5Qr7t0Ac+vRWVmBRFbZglDKA7plVdPX/vWtRxp9FGsAAJQS3eDIDCCm1kUn7GIEWa2bJr8gWzfcMK2PPc6M4czWxgAok6RuFz/lmuSM7wNGyeuS5MITDM5LzNzLnn6Mhy0OGRuBALqeX67rm9sH9UJkqfsPWW9oWjnI7BAWXAxAViwaAAp4RcmvgeFk7FYLq021jOiAGSJioyAECukRyOzJgKfxI1oFyGfeIoXPMN7/UWL1HyRKQRPLKOAGBOVhr2V19Wuf9tGawk953tSkQJEJNe8HKseOzTExGJ+cVHLArQyGp6OeMgtHHOsoMHZvgkmmP2mKHPcO5tdQo4WZ9bYOz5V7UZrd8In073IA9nC+IClIkCEXBNYpsBDIeWEvpmBJf62TpGOgRkYkC1YQWbBgshOngRgmQTF/r0qAYrCpFKBPGVG91UnXDKCCyCYYqPIdeSEEXvz9Yprqr992wZmBRolcoA3gIl/cFh4fbqvABsWIxIJm1gERKwzMJwVlN80J6rDAjascL4AHQiz3oFNaB1YI/Ugt+OsmwahWREB2NGnhwAWABiJ/12Do+aJjdCCLEBvcZGtsCBx2nSeVFab5HQF0Qgt82UecJnFIzVF9s4AVKwRiZq4bIf3Vq+5ZNRbgEE5Uk5KHMSGUBhVq39T+/1Z64U98ISQaEiLY2Q7BRvLAICopMxpk7sYEMuuMz8dCIFxANtRA6odkLaDwDktLLM8DuZ6wlb5uvpukmF7SUsDGuRNWOqs4Ld9XyTC4GP0UNVb0Qs7o1TF9mq9dwkEwYBIovvatBe6U7EiIuApXF5kIQ4Zi4D3BrwBoITCQhp52s7fG1998bC3AGpTBhVGbBG80JjisFp5dXjjuzYKa/AAAvB27fFfsIBDiywCAIaBRTSipepNGyAURBEWNoIinMiCSCx86mxJEtJctzBPbudY7gS7GR1VXVtke0ACmKuryPWo22M0mWA5e7QWlTQFUAG4pUJaCyoR4J0FRcBaMK6BuCEbkEQu05X57IDC5R4HgIIYEt8SxGLPGnma5+9Dr754oV5CtalIEAwzgwRhUBiilVeGN/z3emCN2kXNCADWGDFWLIthFhSlREAixswwBnYPusST6IDdVht3hnSzhUkbXl7TAMm2m0gbcQSXB/KM8+ukO1pg24lg8RgmmZEt0KVRYjqaD8Wx2OLSGzBgq8VgYxpKIACe4nurNI6gACLAJUVEA5YRET0CQbIZaEAQhIARkNiK9CLs4rMBQKD1wg+XwUeliKfN/D30yy9cVFgk0SQDkRVhoTCkwjCt/FXwx3dvBFGihQVQkSCAYYiADbN1LgjYWGssC2eY30IWxAIwAiNbsS4fYobtmH/Xmth30PTIZ5kidN83CrkGK11Eu7OpcUmXyHr7d0pHxDxtDiHwmTcx/GsaiaAitp/MsMIIaJ7yQkVaWcctZXZsWOI4LcSq4BDYfoSAuUB4V5WnCItkp8P5e3ov/8VwYQnXJg0jWhYWCCPw5/Njv4pufv8mRAGNGf1UFEa2Ikwgyv3MVlDiyrQk80pc4MwG2EiczDOK3a4mjHaTXNvvAM3cHgSkBqx+uwPU7nDMuV2yNCt0YG7fSDtBPmlmDAkJgr21rKqIllizne/ZPm3+MiGn368ufdKbDtEn8AjYgpVkxjyjZUMgAshChsxdVfSIJ6PB3b2X/XyksFiFU5aVZpZQOAzBn8+rLw///N4tgChKS10+PDn7eD6qS/kQhYRBoszUBUaxKE6Sw01uscImVmYETjpjZVZrK91/d7iziVJ91zl5p6oWIjRqweQcbSYZ0PzJjPWOxvrLTiew4YGIs5h2l5PeMxuH8wXNa4x6uArP8MH3zbjZJdi8t5m6Yc00fa/sBRV15Aj1FeywJxrYsButAwQuEEEPcWWNn4psBIO7wQt/OqIWUWUyQKWEDQuKAX8EV/3C3vKRLag9JgGuo1sS17jigfecQF8u3IlVNtLmaSvZUoZrLnLIeKwRFI/RpC6CCulYqmoMKlqymaY9RyedeDwrNKiTEN32zbHq3rU1A+KEXDHAQL2a2SkhYOsouwROdEr4KAbgjkjv3xtOhjvRxPH7yryjihHP/9P/TtorQlXeZhcJLu5Vew/Z/iKTFSOAgkXNSGQU3jlhazy0uzr+p6OlpWImjfjaCiODsVIcgsd/Hv314xvI06JaMdTYptkiGIa6DxFBSkr6sRkxs1h2lxSXy8ApemJu4XO2MwI6vbl9p2mCeLppGG3HY8usTqVzQodN08eaF1NyvrtwxqiAp8N939BzyFl9XA5RQZIc2PrAiHj4iY3TT3cDfCUP1cLHg4X9/JIjte5XY+v4gLfNP+ZLO5ltNXvVZjWueDqK/rKB/vKUXjmpQFOvBmCMRAUS3hX0L4FjfrKouBSDKbZK2FgxUjPiDeGKi+xfP7YZfV+UimWBgNychXoXlCBYcI1Ckgh2oou9EpcpAGAALCYtKMKWxXCc1mOzRE7b9U/wx3xkqF0FFDvdU925UtEkyDrbjL2tv5E2r9DsAyQB1MhTtV1eXjzkS8VCj6pMmwd/XqY+n9lAc3tx45w2ECqoaFu0ZKp2/Mv6oFyr1TxFanKj2ectNDnWe8e5Y3B1rfCGvmAemS013BSotVXcpx88LQUI754a8OC4CxeUlthwklkrYMsCbNEflsd+Ft7+ya1Y1IJxH2RD30gGL0XDbBsrwggquxEzigVhQYo5Jm6XyxZcu3InHdoO2zQvtD1S8s5Ma7PznY0M7VTUt0lMo4P+dFYHeAapx7ROPpewOu6yQU/ZSbP0paXnf2PABCao2EPO6QvL0WO/DlWfdvJdGcIuZtpVXcEhWHJkz/Fv75UgMuIjSGRZBMobzb5n90pUuPMbk3TJRv3K/mh5USKy4xH8ZaOaV4x6aLDKL/juYGl3FY5b9JQwWwEJQQ/jYz+K7vzcOJY0IDodcGlMZNKQTkSA0anmp9IfgCAWswAsxvPGmoRtsj4DGxMlySC3XUkGxS23mWb1uBqYt4WllqDrA8acBsl/ri+2mUACLeMWZkLjhbSyk2bpS/wjvjmvZkIwKCDTk8HzvjoPeXrFb2rUT2xajR4BrNJgp3npMaWjz58XFRmqSMq6bF2Emak2xs/8SE8YmPu/s9G7DgtvLEbDCOOCJT+ciPpqctQHhvoX2OoYiAawRgDYoBrCx35Uu/uL27DooQUxjIQxeynFi5HQI8jqH7sxLMnsQnB7Vt1lZkZy1D0JA1NLftAucJRun0ps+QuEdpN7BEDXwbI25ZFceZhWXLKVvt80ybZlx8W8jaV7ySNRGu2kWXqs99xv9htj2CTxklClag46ty8smzU3htSv2WQPjgBMCu20XXJc4ej/mx+BlWlAxdbEEi7uURJR0+uDfd9XqE0MPnZxBL+qeK8o2REKp03vAD7/xbowEJWnCJVYRpdJ6RH9+Perd395jEqesGARaEFfTFJEZmZAjVpBYHhjLSY3Octgx1iLx4PHQ56zKLAbFwUJwSlRJc8OiGwTZszQ3NKE0WUbWGe8HYSoY9w25XC20f/unGplQ6WuojmcAwDG2ZNRmuyUXXQkPvu8eTa0xggpFY+sUiI1NFF48Nfm2fdPrv1jSL1KbELRFiYlPA1Lj/UPu2B+lQ2EAAoZEEJQvUKItsKinH4uRuPmoHOGCcqPXrwNrjPwipHehebw46g0oipVRmUBQJg4In+IVn6vcs+541DUggSWZV4J9poPxroZQ25IghQUVgysr4pbcSJXfpdkwpW4bc8m99U5ZSvxBUjaCSPZQUsitnGyVprQQEZnpwGISYXkcp/dbqikWI94pB5pz1BAaCcH3oYnsGMyycZ8EBXaqXDhYfrgbw8yRkFgrWAUWPbRAJsIhCQKKTDRwef3LTzM43INVCLbR8DTvPPL/EMvmG9MZGtgUKwVEwAM0SM/qN537gQNeSFbR6BgUJWp4JkfKyx7ZSnaWC7evuXIE3p6himcZkYxFiIrobEwRCu+V77n3G1UpDildWsZsFQjDi3XDIQWjWAlgojjca8YB9EQAUaAFpERDYBJ9fDFIabAIpbBKTO6AUGOoe3CKOb6AmZa6hpUmiCnjN39vWhH7NEt3gxzErFsFbfz7Pft0CNv7/akHjOCKEV22ix4bvGgCwYYrYQgitga1ePVNoAe8kQsRwLANgSD9tlf7bv9XXbLPVb1KQHkabvzSwsHfmXI2IgDRM0QklXcM9979FsT9503BcDePL33e/unNgZuPpQNVRiZZ3160PP1klf4hWItqAAqN6sHwYKaT49/t/rANyew6IFQHXpgZrYgQA4xtAKKRBz5MYER2TEaM1r9AkjJDKk0KrVZPnSSB3BiYdTQBZe1IWwkQcy5XJ3/JyJUPzY3JJkdM+//cKQtyf4lpNCWzdCzYf9v9Bk/MlWIDATGqvn+1n+Et568duUF096QF0QcGWEEU4FIy8Hfmjd8oGenmad5+at69v/iYDgdmJpYlCiCwDKU1IMXlO87b5KKmkqF+84bf+wHVT1ExiJbFLAYUhSafT/Z17M/1ibAChgLRsBYi3206lu1B745gUUtgmklH536nXVNgA6QQok4ronW1zGZ0iIQlykEgFmMZNILAgZkENecyiJWxIowo+TFOdkWFtmBkmW5fCCpGxjW6aMdFOykUUnM1ROow/CUdhHSLHN2QQ08bQcP9A741gAUIjvlAYoUUVg/+vXJx79fZdArfjaJBVh2Vm84HSlAAuSqyIA88+ulO84cH3iGv+/nitVqSBGIL2jQgqg+/dAXph+/aEIV/RiqKaq7vrjZ6sElp5bCzYY0OSwnmIqIkUg4it/oD3qrvhWu+MEElZRwQg8QQCIgRFfA55gZ52b5kBt7maYRHNfCYgpSPHzHsTSwPpPNMsdDSZOuI7SuJBNHOa1yTUk7pXQ/QG72nklnUzenHwlZ4IAwIzcnKSOjAZtDqHcRdL19dpNnQaZvDjXItAzsTft9tQ8KbGpaJFR9hWAMHv3s1i0316CgERE879EfjKFPS99VrK03mkAITJmpB559wbCeT0GFxVhUiAGZIvtaPfSJybVXT2CxaDnpKBWFBbnvcxN+wR95jQ42WfDiqzduyIoAMKpBveKb1dU/nsai5rQSJZhqipKb0Gzr5QYCAWGwHPPB3Fq5bD+T1WMaJieW4draYnwnHvQEYCVuK8dOdSCErvox5uiBGqvzzbbcIN8s+cC081oIBJkYvJuWjO4LZKhQyrZ/T73vt3tVH4eTgh75g4Xxf0WPfm6i/IShks9uChsyFouPfHsb+iOLTy8FWwMiFFK2IjAYhVaJRSKCUKQAxPzARysbbqipUtGypPGfiBCR+HjXp8ee5c8fehEGmw2oZDoLCwBhPz1xXuXxi6axRMKAzoOk1IY0imUG656sVOIVIeHDi3MeRlI57TjUIwKbBDAutGCpd/pm8jOQHORsbsOy57yFtRJ6pJ4EpqpKHTq84mkpbVyitDSe5Q0tbPE9kHaQoAapSGkZ7n1un+qX2rQon6jHW/eT8spvT1qjsKTYOgg1rpSSX3j4G1vJHxl+fSHYFiChCGOohYQQjGVVUFCDRz5W3fKPGpaUta6NOLk9iMyIJIxwz8e3HOANDRypa1sjLCBEggKqX544r/LEJdNUcuQekrj1rJG5xggWgVnQNZM40XbX4ZW21Tkih2QVKmPla+HYvbim6zi2iFsJ3bRgycPx67rbT78VUVddFxlptG7YstLSVCPducpMtsdx1q1AqlBYJHt9tV8tlGALYI8KK7jiY2OPfnNSCKGIYt14LUrEpkhQyFcPnrttwxVVHFCRidjJiomYiMUnU6aHPjC+5R816tEJ/aNBZg9ddKfAMt330fGJfwL2q6iKkTD4evV55ScuqVCPx6wEEEhaEeE4+Ynl8gAsgo1LmZLiNy6XdwNT66GxxOXV2EFJPFMzhhwhfk+cxu8AfGS7DAhmL37WHqXhVEsgJrm0TQOaKEtZtLAuWo0EEoI/KHt8oUfvRNE4qhFVuYcffue2TTfWqKQcyJtQZlPVziRh8+ixr0xM/M54w14UirVsaiA+2U3w0Pu2jN9lqEeL5XopOytKFLeEAmk0Ed7/kS1T/xY7T9BXj59ffurygHoJrAAhUqyVhqlKcDp/mMVJeKJNWhJjWio0iC/EHDEAi8CINg6rG2FUSahkgPGbOZPb7wjG3ty2MK7nXMmEg5mq8c2l1oysfUdlRcrkZdxIW1ZJficSowlABGJQl8JdPjdP7eZF09Yb0Bsvq6z9XsXWBHs0ZzcCyBPoJESFj3xxek/q6zumaMYDGsRwLa765Fh5JWCPZiuOhNZ0BKehm0QugB7Yinr0nMl9vjpvw3W1DVdVVYnYCA71qnk9KDGtCwGACCPL6yuJESXDmzkZW+6KDzZTnBeQWNNIkoEggogZ4R909NaEV5coSmGiwt3YAgNNNdEkO55R9HIOjoqZdaPQc/N8wvSDm/LwPPm6rkBCzJ021xT9OODDCmm7yycG/L19ExqK1JrPT265oQq+hz2uspjZ/qVhhpK4UqQQKAtiH/v82B44f/AVhal7w9UfH6s9xdirxUJWSjZWuoiVoN0U9mRDZaSC2El+6N1bbUhUVGwFewjmFxkBQAE6nRURQGKM5aQFiQGs1Acsx4x+SeRbxBEz0Apwwvhwd4EQLDZ0oxmbRJqchnpJltqYw+dNYZ6R5Tc394OEusmGm0IZRErGKralmyE2j/x1KwktOu2NLc+pWJoIcRKrowiQErFAbJd9tN8/xGPLtUd43TfGa6sFS17SoE2QPn+IDdL2AABx/VJAAYFYXH3u1E5bejZeMVVbJ9jjgbV1/gMmSg3ZZYAEUBYGRGZBn4wo9B2aA6Q8FhBj46aTNME2ArHwCgoIGMm0AUI8KTedWOiWw7I4odA4JkKwcXyDaYsJO7iH62wNEXDBV8L4zYXyu/Quc6PUEJHuBmKarYViGx5hs5RCmvjGI4zdSTEwQmQWv7+3dJwnZZi4rrbhx9NcU1jCOH/BeOZKffPMna3p9DwYwSMbwZoLplATFYEtA1IrWNqy1nUcL+0GlcxuH+8XXC86uLFj9USJxVWvUufq4L2Yop8FYxkzJAVOq/FJ4pWGmNCQ/Dozwh3gTiRvT+jmz/RMFf8WLctuKCU5xfZWtgo1SWQmY3yQa7z4nb19rynwWtz8w/K2P1XAU1jEZBgxpp+RUBuwPckfBAUFWQn1KJaUmNypHRgzpbd6tprAEYIADCwCJm73S8ZxxClSSpMDiQFAZxYp2J8YR3JwW8cs6rQuTuNhBACwXEfkEdJ5u/gfjZhzjEMj0UzWM4tim3PnAs3ShXlts5n+jKSMiwRSsaOnFwfe3BP8w2w8f7LyeIhFLz26pBEydXJyjR/hbi3G42TqZL1cgQFEESxo6C/EeiuxsoAIAhqWqRq6YIdFnNxL2gvhGNxWKJ7tDChJK3QT84+z7RWIBiQeVFmfOZGk72kpI1W3iXdXAUKWnGdwO+Ob7twP5iLRc6ucJ3pTjZp4zbObEyW0DlwRUlIJh08ozHtTcduF02M/r5gqUo9Opd3acec6tCPG4LhQ2pubFpcAOZ8j7EyaXPOOxBCzg5FJCSpXI0UGsdwwXympoqfkvLg0xLHPjGFA9x6XakkdzsnoamGTTjKKg7+Tg9h43/9/634SJJp5DtX13HuWRtOSxaZTxTPhOrugtVdMi5TDea/q7X9jYdPnpyb/EqBHUAAxibNJpT+pKTbHVrpZS/lX0rgsgyNgM4KZUokFMTIcExMx9gQoYDiZ+YSCCDbVUYxjW3FTWBDrm6AFZE6O67wjo4152pjUUyUdVxkH2IlIcHxyFlg1s/BmG1vs6M0rW8rAHXvQJk+V0vIzc3oajUADlKH/2ELpcL3ufePRE0wlJYw4s23XVTybtX/zfHJDj1jH+eZgk8Qso/Yalz8xLho6hleagcUfZFmSjmQEACuxPxFyJHXgpDSU7unswpn4EjDPjzq7lbrEaPyvbqVQd2iqhA1dGTtIgKf5DDL0tLY1i7QxoyalQ9Df2dv4mTGpKehRYNxIXJR6Z202/8cm5bvEw7ckFdiwb2LsH5PtI30ta2QYazRIPPizTmert5A4tM/Ws6S4xQsRGQTrgThw0k8q9cwrTaPE6UE4JbKExZhM4uPMhgstDRkxBRHhP7qPtTD1RcOOc4QtNtQ+v5M6EiVGvPmIIW79WRm0hpIAixBktFKb8tSmKmHH8Qkdkq3MOOhm+oPzLs03rd5rHAfI3JADxF4nndgnmCDRcXpV55xLfTKkJFz05KMxFhhpEIOEuOxVN8008ZOna/eYA51jO00n2z7WFQzqaoUEdhqiBwz6jhHRjVBStuSDrTaVayzSoHOVIE/teu3YpuSuePC5g3ezwD3bhlAXMnrJKdacjjVMtqa4oFHX5nXaQ/GnuhV0VMNGoWxAcRyxzPOyI0XG5xpE7xCX09TK02A0mBhKK/ApQkoRIQjooorXkcBY6Yh+JnMI2/a3Qr6GA7WUzAQs25xnURrcZ/wKAgoqQiAFoIDACc4BIIvlBE6s4xfOAmyy8zEnGsWikFDp2M0gAhGzZa77vGZYHxAECd2E5HSIGqcejtn+xwhAdboEMMj2eaCu5YmwWQs70cHlyjRDBUBl6sgaCvPaAecAwNUyQJAxEcwr8qt6bT8fvUz+yh8A1aIaCxg3PEB9OI3rDzSVbQAMUEwWkQAIevpQK2F2vNWGsDJ2EmkBi5DITmwDqAH44MStgIH6qFSs8+qbleFFkbbVirXlxs4/jE/DGwCiHbKNzPZPtMzp0LkVlnxioQDmzyMkMbXnH3XkPvvsHkWWkFis1mrbtm1XXXsjg2ryFkqRrUwD0l777vGMZ+4zMjxsk2oRCwNw0myHmT0u5ve4pJ0QMJMKK0XGhJf+6tpqJUIiyaqAiiBn6zGIIGAZtXfiG99ULPhCmgjZGkTUWl/925s2b9rqFMQxRgATkfK6OFEiFhWFr3jNyxaOzmcG0p4wE8qddz9w1133oVeM6xWY1cpn7flmeuveBzzryMMPsSYEUURIhFEUAVFvqXTFlddt2rQVlZ6dBaC0Jjbdhc919kpXHzkrfmr3dThrpt565qmnn/7G7OtPPvHktdffyNzQ9qaUtpWxI15w5Dmf/ugRhz23WCrtEEd88S8u/9lPLlV+j808+nn1IVC6EE0+deIpp15y0Y9aj3PwD3/6zre/U/UvtHEPbN4umlRquDb96U98+JCDD84e4evf/Nad//6nKvYaY6ERcdVeyUxvff4LDvvVL3+xcOGC1k//+Cc+NbFtCymPZfbCFJ1yrJlz+TQGak7E5kyqneVf4XS5Yow1JtJaMzMpNTE50ahqKKS0rYyddPJJF1/0A601gFhrOGWqN4pvCGbmE0hamcs0OiCysCJ93/33v+1t72TyITt/KA/dJhAbBQPDO33xs5+y1rqp7rG8kAiL/Nfpp/z4xxffdtvdqreHrW0slDY0bKEAAE1OTBljrTVEZK3R2q9Wa2kbYfZSlNZmet3r3nDqz3/2vZ5SKQgDRQoBosgUS8WxsYkz3nzmtddciYXRrnRwtjtTa+2YptxMd8dGZPmiY8gAorXWWimliRQRaaUytiuATAolKO+5914//tG3lFJRFIkAkVJaSSopAuJ0dRKeOta1hpJYVih+kTkWDnrPe/63Vq2Sn/hgluZJiXGdQYgUVzd/8INn777Hbszsez4ppTytlNJaEaJfKH75y59FQmALXB/r1yRvFBPECZRW7qq11kp5Wqt0QnEaxSGgUp4pb3zrW956+aU/6SkVrTW+5xMRixRLxTVPrHnxi1927TXX6d7FccCH3CUiE2chQg3fjZtTjlxD1nqc2FL6Z91X0Z4mfCrlxsSJRmIPSCS2+sEPnt3b2xsZo7VOd0Av8+X7vtf4pbX2PJ39d/rOglf44hfP/estf1S9I9aaHKeemhECEdna9O577vOBs9/OzEqpdNKRO3WttTX22KOPeO1rX2LL20ipDthDY6xR54sTNSSVSISAprLpE5/85A9/9F1EsNYSKQCw1nqed/vttx9zzPG33Xa77h02UQSzhPJmJxqWqy6V7rAoMwRRswO5u38zZtG5RlzQla4QAdCGkV/se/6Rh4mIVip1pJVK+dLLLl+3fpNWqmm8JybYf10FLB25Y1kpNVWe/trXLkC/nzlsV0ytlzER2Aaf/+xn+vr6jTWKUvYtpQU1Z1Bf+Mwnfn/DzbXQkJstXJ+7jgIZ7o5Iax8XuxGZyABMSotlDqa//o2vfeD9Z0dRRERESoSZ2fO8q66+9ozTz5icDFXPfBOFGT196hJHls6/zWrGtxhGqtXr/qHjgT24Y8KaHUAewBQdRgSQyIwuXbB44SJEdNQtZtZav+e9//PjH30PwAcwHaV9Ma8pW4PXC0Qg0Ea1PQ3elS2PveDYY0486QRjIiJK8FKqVCrFYtE5TqXIWrvPfvu85+x3fvkLX9C9o8ZELWi4JPlgY8BQJza5wYbahjVf4U8v+cmpp5wYRYaIXPVXRLT2vvv9H777Xe9lLKhSnzURpA0FDZ2s+c827GCsSJhjKvH/Cziz0wTnbCcae77neTpda/dw/Ou2uwF8r29YFYdVaUSVRlVxNP7f4ogqjbpvKo1SaQEVR6k0Sj2jum+hP2+nwrxR0qqxmN/mHK0oz/vKFz9NSaHeudgwjF7zmtc/8MADADEUiYjM/KH3n7XTsuU2qGa1b1vQ4pzakat9KO3Zanlen3/N1ZedesqJURRprYgIBBBJKf3Rj378rHe+XXQPat9mWLmNA/K2Bx7srI3R3IhPiATxBPn/qA0hQ4NCf7OKoaRlfCCamJiYnJpqxJ/gfz909oKFwx6xr9lT1iPrK/YV+1o8JZqs+/aV9bUteOIr1hBJVA4nNgYTW9gikZfOfM3b40FpxbUtZ7z5Tc997nONMUp5iGgtE9FPfnrhTTf97uvnfcfZjYvJmHn+8PA5n/m4mApie7VHyan7MlsiFYxPLF+26I83/fbFL3phFEUu4LPWkqIwDN90+plf/vIXVWkhADk9F8QU6GouAXUzj6fl6ZEGDmTzrxy3khvH2nLbNB6ezoa0ZoNt6XwFiRtu0PPHt21btXLVokWLrbVaO2FreeNpp77oRS8c3zZGhJxSXbMPvaNQxIpyxMwiNgiiFStW3viHv1z2y6smt41RTx9bA/ljVkWi6rzhBed86qOcISMT4ZYtWz/3+S9pf+CSS375jrededhhzzHGKKWUUtbym08/7Qc/+PFt/76TSvPY2pbyTd0nxb4fAQA8TzOHzz7okF//+pJdd9k5tR4A0Fpv2rT51FPf+Mc/3uj1LDTWinTE/dvHEjILMfim40ibcAOdOgfF4yC7CGW2r+9asF5kap5okY2BU6PSigwHP7vosiOff2QUWScn4rKSBaOjC0ZHZ3sGBxxwwOte99r3v/edbzrj7bf/+04qDXBDf6fT/xIiz1a3feiDH1m6dGlkIq00JOHXF7789XVr1/h9C8LpbR/80Ef++peb0loyM3ue/uLnP/Wil7wWxab8+lxigIgAxPI642Njhxz6nN9ef83o6HBqPcxMRP/617/f+tb/vv/++3TPaGRCJ1CM6Ir2qrtRGdJUasY2P2c4athIT8o3TUlkWTI09aet7t90BEx6VZpQ26aU0VhLhcGfXXjhDTfcVCgUjTHWGmsZAEzyFUVRFEXZfyY/xK+EYRhGobXWWjbGBGG4zz77XHvNr3bZbZmEtVatflKKa1O77/WM973nXdZaRcoZh1LqgQce+N53v0/+YBSFqtj/j7/9+ZJLL1dKMTMiaq2MNccff/zrXn+CrW5T7VL6xjgdAE54zatv/uMfR0eHwzB01pMqvl35m6vvv//OQs98ayWWt54DbpzJ3jvk8Fmkpy6Q0jH5J/h/+YW5EUIjUxwElRF98ilvvPLXVyeYjophOK11AvBk/5n8EL/i+77v+Q5eUUp5WgdBsGjhwi987lNipzPKEJhQxUhs+JlP/W9vb6+zjJSz+/FPfiasTXmFIpEirT1/8JxzPj81OelsKD57kc995qM9/fMksoiN889aolRENMYc+pxDH3ro4csuu9z3/SgyznSUUsaYc7/8hU995nNBZRMCEqnMylEXEfGsyV6zKq4jooasE3jaZEGSe1D/kJi2kAeDN2m4CjOqwvhU8LrXn/byV77kpBNfe+CzDujv76vTWBvcp2RYPnW1Fd/3F++0JLUDz9Mi8vKXv2TxzruvX7uBCj67qaXIipStjD3/mBeeespJxpo0ktVaX3rp5Vf/5goAL5jaDEAWGEBWPjb2yXO++M2vf9kY42B1Y+0+++xz9rvPOvdLX1K9w9ZyyshGbKbZOcd25113veJVr960cX0Y2dPfdKrzQw7+sdac8+lPjC5ccPZZ70Xdq7RvOYqpsTMVSrF9z0YHSZ2mduTGacfNT7uGmYbU7RDwsEVEBtO5y03JvIjkyVozKQ9U8fprr77+2iu9wrxSwXPJGkO97zOG/lIKQAbYJeDTzzj9G18/F9HNRSZhnjdvaI/dd1+/5gnEAmA8RgBYI8LnP/NhImLLKfAtIqtWrTr55NM8v2TZCZUZYEECNlG5PN3T05smt8z8oQ++96JLLl//1Eby/ZgsJCKSAyEqpa656tpNG9d5fYvOOP3MifGxs89+lzEGYkEBiqLg3e98+6KRBae/+cxqZVqX+kwLjWlutymnRb3ln507XDWmKG62dWvHFlAbeh6SwKtdVNfmYAyCYAt9o4BkTThds0kcl1CSBYDiqcYojS4IwFq54Pxvv+Ptb9lvv/2stU6VUkR6SkUAcaowAKBIm8q2004//QUveIHLrbIL+vGPf7TdJTJzWkEnImvt8PD8T3/yf9/x1reRNwpO7RyyFa+GL7/Ug0gorPye97zn3du2bvv0Zz7JbEUYEZXyoih6/etfMzI6fNJJp23aOKZ7+60xyZOCXYQGDWbRRPrLV2DN0APbWQ9SLPP7NINAiO1FZPLqYm0IIGJNML0+mNpkqls5HONwnMNxCcYlHOdwnKNxDsakNi7BuPuVDdMfxsBuXbp0p5GR0RhKTsZrc/Iox5Mlo6h/cOhz53yyaYmdX2Rma63JfLl/WmuTrVnS8MVae8abTnv2IYfbWkUp1VmlVJw8vrUMoIvDnznnU+8++71ECsRBRKS1DsPw6KOef/PNf9htj6WmvE1pT6QNgCddBTrNA+pbnEI3HfW6SWllh5W6uqkGN8kxtV9fRARm31dnnPkuzy8IMBGlV5lxb/XlywZXhrmnoM44480LFoxa6wS5QSkKgmDt2qccKQoBidAGY+99zzm77rI8MsbTuiFn4YyFYw5mFk+QIooL+SwF3//qlz/7whe/uothFbFWKYq1zLo48p1vX7Bt29jPLvyx53nOF7of9tt3r5v/dNMbXn/av//9D90z4na6Bpiggeg9Fznf/NSm47QeSYvh+RXP7Uu02sdrKC2PTO47SSkbjH3805//5Mf/d3sM16VUaeTx8COPrHjkISwUmQEVcVhesnS3D77/bGam5Bl175xtsgwgWitr7XHHHf2qV7/k6iuvUKVRa207t5uwozEmP1rjlUYuveSisfGxX152SX9/v7VWKUVEURQtX7b0xhuuPeXU0373u9/p3kXWRPFWHjflu24QhPbxSLt5FTl/EEt+5kfRbr50vt3IDgWBWvs06n0FmQg4FzVVStnK+HMOe/5H//eDYRjGIW2jk8GWT2lK7xwo50yBmR0keN753zJhRZV6mQXJYxt95lMfHRyc5554Znbx9/ve/4FVq1b7haKbn1qvWmZ9EaKw7SmWzr/gvKGhweyVfuGcT910wx9qkW0zVDsz+yJTFzXGeMXR3//2+pe89GW/vuKKRYsWutTM8zxr7bzB/quu+vXb3vHun1/4Y10aYWZp48Q7TDXJfUPzJtNZ/LRDZyp23FFmIboAdbWO1i44h+w5Hiczi4gxBlwRIhkkwiZSGi/45pfr+qktIoz18cbNNZKGzdd9hEu2f/jDH//sJxdScb61kVKeLY8feOhhb3zTqUEQM/ajyJRKxV/+8srzv3lewtJvcspN/HYCCJct3/lLX/psUKshKQCJQvOMZz7jre94+wXf+LrqGRYbCVhj4sjJXS+iE+dABuBYl0hAxJjIL47+/W9/P/6FL/rNlVfsueeeQRC4Z6BWC5RSP/3x9/r7e7/zrfPBG0aVUZ3qiOukuHlrED3DbW3ZyOqjDqT9tJ62WdWcSmBYb10BAOnv73NYX/qewcHBzFwjICIOx795wQXPfc5zUtx2O782bth4/vnf+vJXvqEKgywECGKjQgG//52vF4vF+u6udRAEH/3Yx0gPeIUebm0AarxXhCCWzz//W295yxl77LF79p3f+OoXf/+7Gx595FHl9wLY+fOH0qv2PA8AiqUCgG0UGkMACG2kSyMP3P/gsce96Jqrr372sw9Iz8398O0LznvmM5/5of/5aCVkARTppDTVBKbk9GB1Np08+9Cz3ZXymtvnEjAJM1DxV1dc9eTaJ4MgJCIRVqQ2bd7EQrGgEyJHUd/A6Katmz//+S+6zqpsU3nc65B2/qVQcsonSttIRZBoYmLykUdW/PO2uzevX4P+oJu0g4hszdCiRb/77XW/vf5aVBpEjLHFYuHuu+9btXKVKg4EoWmFOKQ5IABUVK1V3/bOs1947FFBUFMuDAco+P7CkcFHH4kABFX//33vR7ss3ymKIiQlAp6n//inPwMVk6pcquuOAGBMpHsG1z654aUve+1Z//0WUuCIHIgoLEDU19uz62673Hf/I6iLLQFM26HsTfY0t/QIAfHEk0//5WVXULG/PutjpoEpM/DZZqrqNUiAhdMA1ebqij+/+QPDiYROsP1fBNSnCgVrs9INCGIgGmuJAH0q9IuwQLeeD4mkVgGYbvmNj36fACB6EmwDiBoDzgL4fXmxhEsZGZEkjIAn81opBGgAvALMZkvqyoDqGvPN25NSylbH3/qOd+pMXt3JmzXIa8601bU351iCNCWLUalPwUBdo10YAAw3gYyoSsP1x1LSekjGF0qiXoCY04IGnAjHuUjIWmOgYbinIClVGkVQ2VYuFrHWzqpiKGxVsYTUm3kYBQCtxKwQkUgXh4hSzXAQQGbgfHDZHYFEhLSn9IJ6M33aeQJoreX2zmYWFe52EXT7O57OjedGvhk2kU6wsdsSMau5H9/MuohG46TtdkCWCLAVhqjhDuVFasbaDF+OkFxHeYOHrmNBIq6JsMk7MmO6uQGqFl/s+ilMojvOmfJqK6wQtwpxHrvQDd4l4jo3n7K1TzLMwG7BKZaaISBH7mZp4yGIRdjY7QdWdizVQqfiEWmhOHE23QqQzzg+rANG3h04XS+VI4HU6s0vWCRhat5bESS02Ym18etei1U5sIeAI2ETCzNT0d15ytmjEZFABLiaTKDyFREwSysuyCGCQlCCJFJzDcjKddBmeRBIKBGIdXL2BAVV52lijtbznE2nNYjeIRanMbmS1lHLLQCgZF5Pqw6S4+EFkNIqQFb/QrokdbTivLF+YM0O7aJ22gcs44aHZPzJEPxG9SAEsDK4BPtHNVsiB/cbi0DrHwMbpf0esZo5EnDV+H2w2yGlUj+ufbC2eWUIWqOKId1koyTn4rgmALLs2TSyrDSxMVp1m2EWLCqJ5Vdi2V6/lxfvU9q8xlS3Cddqw7v4Qzt5a+4zUQWy0RQiSM2UhrzdDvW1bzetlvUPGPSoXXvF9lQCng6eBQLoQrHY5VhoyOL5nSncmL4Z24rzZPptWsOqDDE6DdtAqsFhZ8w76VtDkVQ0ecWSvvLD4zedN0lFn624tSUCDqIXvWf0ZR8aGB8rsy0qH1VpCrf1fuygjRPrhbKxJolUzf4v6Tn5gsHh5SKhBd3/71/IpR/YEtZiremsJUvEI7vQyd/p3/c4hRGXekcfu61y4Rlj61ZY1MmjopADu3Av/ZF/jv7k9PF/X1Tb67iBs37Tc9sl9vH3biOtOaONBJHZ5RDvzEtGFuzmVyeqffOL/764euHbthomaDfEpLts5T/zRZ7SfX29bfDidFZc3VxEmmKuXNR3hqtz1XhKlf9j2VyoeywGAAs648BRJILSfP2yL3kP/aly8Vs39sz3X/D2gXUr3QQ3rt9jFtT6bxdXHv1biDo45XsL77+29q8Lp1BNl8cNeioVB0OFEphFB+Kbfjlvy0PBz94+MbkxPPS18074wvxq2HfZuyaU54mNSx+CAFa0L2f8bGjZwfris8Yf//vk8oPHjnnXMPWQWEtpg50wgAYxgZQn108OLaN3X7vgkb9ULn33VgFPMpMBEIGtHPuRASrZT+y5NpwODn7DgO7T1jjbzdyLWH2xUZoNM/IPiHXtoNkzyObqnGSgv0+PjszP83XpqIPmsn78T2aov9gQQGeqXNjotJwovBK0QALAgggohExEse4egSJRPqBQddy1p6YTkEDEikjP/HDBXv7YE/rX/zMOAFhQwoJZCSJST90XPnXvlO5Tp5F+6sGJB/5QBiiAR9n1J7CW4bC3905H0999XXVqrQGEa7844Y3KYf9duvHratvjjMVYC5UUc83u+xJ/j+fjT948dtvPKgBq/UPVf/1iPaCPPtUVkpyOsKeDGi7Y3Tv+E4VVd5V/etoUi0adTkKt47FkdKFXFu4H6+5VN/9fFUBhIXO0ZFcs9il/nmEDgGStFTezR4gZxbITpwa2wsiuZf/p3NqSoIQXLRjRS5ctc1liNxZaBy6TaCmpanU7y1Iw6lmq+hYor49LI6p3SPX2Y6GfSn3Q04+9A+SVjD+gpKx+fub41CZEHfcZkwe1cXXt/8qrzqV3/XHA454n75i+6qOTj/2DqaCFbcbKAUsAgd8zT4VSAc+S8lWPFwU2I8oJwghIS57tr7urNrW2onqLRD7XKituqB7/vuHRvcvbVlUQVKzHCwIAO+2vpyaCR26eVp4HPgIgMLGgtIxd0qpgqv6xn+wZWoIXnTpZ2VZTfSUbSJMyLGp9w+fH+hd4b//NiAlUbXN047mVW344jZ4vyUwoRWRq4b6v7H3FR0a2bQ5MBNUyV6YxqlGtjOUpMzkeTW0xwTY0VX9qdVjd+B8gKgszA+glS5bqPXbflVRBOKvG1Ty0sB0w1VhMkUY6QJrcpemOy56o/CRX1kaCAGTj/YGYlBAhKQISBAGS2pjORiEigB7ddvHkHZeb5QfT0meWD3+/d8J3B89//rawTG50Z10I1qKwWCNRGIkAWwTTkFom0y1MeZNduJcmjbbM0MM2gqG9dGDL5a02BiwEIZ6GKmNPseoLR3YpTj5RI6256gNUAD3wCRuSSLZRYLzKxvvN+jvwlV/ve+J23rQyrFMT6wYET94/fd6xsOSZdnSv6NBTel77rd7H7gifusNiIe5Zs8Lkeff+rvLgzTWxwuzKZBlhY0e2E0RQQvkj4GZV4erGB4kVr9i7+2670B577DZ/dL5EUW5+Piul+vpcmpxIrwGxYSFhkgg5QA7J1iiaVsEEVbdBdTNWNqvKRs3cmHNb1AV56ad7lxxSevyfdOuPJh69AXqWkC6RZDDlxrIzg/UwViCRPNKR3PmzYHQf9YovDhUHgKvlPZ8/8NLPDj36l2DdXWX0FadaswxAuOLmmpkYet1X+0f2EKiakb35xC+NDi0HMNJ4xWgM+57623fNT0+ZBqvf+PN5ipCNUAa9RAII+Ygzh454y/yn7q/efWXl7xdGyrM9IxiLPrpYgkFQTEi1CQimIKpgVCUxJEwiJKJANIACJInHg3RduGxflcp5Z3aUOCKYaGRkeJdddtaLFy3cdedlWzasR/RnhKFn3EfTTaQLLlvaBo8A1BhESVMsiIAApm++evbJPUd/BLc+UCA1f97uwd9+YKqbCUsEObKKgkS9w75XrKUZQAMaahkL/r1Xlf/4Of+Fn/QPPnmosplH95EtK+1vzq6wJSwIMtWfUh/HnpBfvnXqdT/CD925YGyF9O0clQp01+96xp6sxsPa40iWtafnFQt+qRpW+JJTK+++pe/kn8y77C3TDXPkBBBx5+cVn/tWeMH7xJS9JQd6j/45ePKfVdQ+Z4atIAASNQx03MGsrVn6HwKQ2u677zo8PF+dc845d95x9+13/F35fczcrUnOENjHci2Ovt7Ufo8tNWeoK9Zk/9VYfCFdHefbL57e9KANI9nyJN9yXvD375ZZqTzCSVx3q2zFVbeEY0/ZuM+1aZIGCpJa8YfyozeFJqSJzfLX71ev+fDExFrCQjxOSjAdfIzk4br7J++5wkxu4HJV7r0quuI9k+vuYShiOkgFQQC0MIxv5hV/CqrbYMvjwRN3gO+pdQ8F4TQ2YOAK77t6YsXNYcQ0tYn//tPK7z8TVCc1Jjtyhro5Y7SL+btSh7vZnmrc1lEhgoBSSqLJE086+UUvPBZF5NLLrjj1lJNVacQasyOsB5uLlznch3zf1sHViQih4iiCbMm6QLFlNDdGu/EWAhEDIPgEQJjtA8k+TspwNeMWNJHGhJOYDCVLxVMJOLCxCrjj8ReVNKQagkJi2I0kowIAaq5FAACeTkgGmJIJBASCCEDH1SSlUOvMXLCcXGmHDWXuPvrJTgIFUKRtbfM111z9yle+HEVk7dp1+z3jkKlykJ76jnR3DQE4YkMJM92rcIYLczuDGEBMMGEQIE4LnYKNks9xnYw0iojYtBRB6bTIrNI0KqBY2xuZnYo0peMmG86XLSlMbcsdWTJTQdzQPEEkFc/XRRBUCAhOQLHZLBAICdHGkmpOPReliTMublR9e2Lh0zvqu06cRyQlkRkanvfwff9asHABGWuWLt3piCOfh7ZM9DSKlLXn3mIrI6IF8nTzlJQIskW2YC069Lmdo47nB7kht5AFGrCxSAwAIlasAWvBWtdWkQsEu1IZiSAzWgtisXULibuMBNjUB5ezja0nF5MXYWvRWmDrujoEcYYiTyJT1LC2Hbos6oHwrPxNXshFRMiTxxx9xIKFC4yJyCXwb3j9CdJJqWlHmRBu9xEIUSUt4pj5IadWUp+ljE2PchpnUaxxA60TgNoVYZLh0PFfUeObEZBahQYbKc+t35Sx6ex15X905uNoRw3K6WBNjZWuGNt9wwmvjrcK1+YyNjax3zMP2bRxE3jFerfo3O534x82rksOl7gdbtnuPQjQZc0kP1dpd12xl8ZGntAs8dl8eCye5SnNY2Kyf8vxygHVm2w79pIl8WKsptf9iN1Z3lzOOiQikijYacnSB+7718BAv4iQa+6fP3/wlJNeL1yuk44RZ4r123hI3C7B127evz2eDJ9+LdEmUkODB6aZqAfY7bOBOU9p1wuOXcBFTZq1zoBQC0+fdtob5s0bsMai63tyLS+Prlj57IMOq4W5TTaJYxBBRJmlRefdM+rsNJ/uG5wr19X9p+dK99cdT8fj4FyvFNsyI7p7CJuohs2wWE7iUk+fk4o6Mvf0+Pfe/a9ddtnZuT2CpJd77732OOXkkyTaFtPtcs8ptZ7m4Sn4NBR7n7bgfUdJHGUi1i6PuaO831xmByToXLebV7a2L050i9iMn3HGabvuukvSJAmxxJ9zQo89turAgw6rhVaQ4lperlXOYhfADMUxU4XN072qzzXq6JC2X0sSsxTMNod9mlg3TRtUSvBpclqYJ0eXIULn0Gk6lCxngRO2T+AREdn29Rbvveffy5cvcz2+dVTGOaE999z97LPP4nCMSM/UXiGzsSHqqIDeKW54utzSf3zfzPvAjg3d7QaZN6cmOwJL7GLZlVJsJj7wwfftvPNya6wTH657oMRsZWpq+sCDn79m9Wr03dgHnDliz3tDpmhADSFe++uW9unGjlXR76Z0lPMeEchM1Ntu22XIiLNQI98Zc7K5duvjBjfMPo7uMooVBhBFyobTe++7z+3/vrVULDjsEzK5ZXyWzDJv3rzzz/+KcEBxjDaXkL7xeWKIR2En+kpt9u96a3gbQdoOHgy7/k6FAqFVF7azo4rrpQKNZ4tz9DyOeIHZS25zwHi6GwIjsMNGGyPohlBM5oYWNscqDGJBbDwIFgRRvvOtb/b19ko8Oi3NyzJ/qZQyJnrVy1/yzv9+t6lt0V6hq4hnpoDO8WI7hJwNA53z1LI6S0POMcdODWKmPCA/W96ubX1W4X9D3pZZw9lvvw3sKmz7hka5BKW0Cba+//0fOO7Yo5zsRENhrlHkFkRYBMIwesHRL7z933eonkGbqvZvx1aSKSm7n7lF8Db7ZupgjjsqTpLG7rY2vicZbCE4Sy8ztwClZZuSukZnG+kRmeO21fZupi5WBEBpbatjRxxx9B//cJ1ysvmNt6Cl+wkJAEql4iUXXzi6YNgG007KaYeHjUkrY+730xCyylzU+GWOjkXmpuzkxjN32ERbUCTeHv/WxnHW74IiZWuVnZYuv+SSnxaKhdxCWw6gR0TGmD333P3SSy/2tYCJCFVDSt8Op5IZFzw7LDcLpFKHKmDHCmHDDZiB2V1/zwzN//VIWWQ2e0RTb7h0h9PUd+dMz4vkZWrQrq4jne9I000R6bjBYdJvqdiEPSXvl5dfsnz5UmNMq/tpiwi70W7HHXvUhT+9kG0AwITKcRfaur40G2yjuZfcOXY9yY33G1MB9mSDa/lGQZQ8GLxNMVVyZzq7iL7hyLmBdvJbaRoal80M0m+p81I4c4bS8Cnt9ouGQzmjdZfpFqq+XBmfXe9u6PTYZPPzPBJfHQHK2pk4LqcnbBTZSy695IjDn2tM6EKf1t5W6pD3R1F0yimv+8lPf8jRtHBIyoPulBVmB7F38Z7MSmHL69a1+0CeCmkTD6vtWOk20FQ2jN1++LrNi3UP28rHyM08uhX16RwAtWctktJsI5LwF5dc9OpXvTSKQidw3npuIoJOtKvdjTfGeJ53yWW/OvO/3hKEqApFayLALlgE3U2nSlUWEnxV8pCkpj9RLX6+OWFJR8E5VeHcLaZ+Y9zNS3j3jSBvyoNr2C9adHBy4tm89Wyt6kinDTRbCMrc7x1THWoCoCUetKmUZ2uVUsm7+KKfnXDCK6LIjV7A3PwDZuwg0lpHUXTqyW+4/vrrRkcGbHVce8X8cmuzj+dZPp1dBrnYFOs1TehNXswGEDPG5jMG8mnHErcRteXWbCgvbJ9duvAfLyOK1p6tbl20aOi3v702th6lO+TL0EhlamtDYRQdd+wL/vKXPx76nENMZT0RkdoxvWsddCfqIyMznjPDgOkAZEJTl88MQIA06nV2ARM0hQJto9L8bbTTntUNWrtjrKoeIQmAkFZEZCobn3fYYX+95eajjzrc+R7J3wQyaXyqjZ0bN7gvT2tjzL777PXnm/9w1rvfx7UxrpW19vIrOLnFuY4SMN0gPclCZ9yAtA0nE8Yn5kVRLQaaYMEzwpXZ29+xx7JtDNe0yLkPSfa3s7CwOQCb8UeT1h5XyxxMvee97//Tn27cY4/djDFae+1meTeoBDtGYrudO7vbMVulFAD+5qprP/zhjzy24mHQQ0prZtsoFtaY8Of+PFMAPivG+GxdfcswkS4k2dtzQrr59Bnfk+Us5FTBtpMj2sZ64oFAYQR2bJ999v/KV778yle+1GnZYqZ1vTUsazjzbDE1d2WbnmOnsDw2PvGVc7/27f/74fTkVvAGPa2stdwl3NlmIdptN9Kxt9IVJbb/Fs7B/uDpH+04W1g55zKztRqJq3CIqEiZMAQzPjC44Ox3v/PDH/rAwMCAA3s659F1xaamajzMtNulX272EQCsWPHY1752/sWXXFaZ3gbQp4olQEjTunxbaf/K03ozduBk4Zl0+3ZEJXxHExBSEyIkQLLVKsBk38DoGW86+f3vf+/uu+/mMm6VDFaHLkZq5htQ0yp0aPNzrggAHn74kR//+KeX/+rKJ9esBkCgXlUoOLKsxEqAIIA7bLGe7vf/p7xFs3vokGPPFXaKS9jJ4HAbRMBTALJs591PPfUNbznzzXvusQcAmMiQUjiTuH1uC2hzMbWdobVJ1WPhdwAYGxv73e9uuOLXV//5r/8Y27wOwAL4gAXwNCmFkkxdiB1pDKxIMzWwA1UQm2qNHSsrbciGdXSqUXDedRJmUEURmY3bSoeCN03XajqfNvNTEVom2M9KMb6hOpS1A7YMUQgQAIQAanjB4mOOOvKE1776JS95sZvHYIxBxNahn+0CmJxOgTSI7j7fbnp/1owAYMP6DX//57/+/Je/3nHHXStXPrF561YOq5nyO+UtonQkCbZyO7H9P7t/QFsPIh3PZzs/cban1OGVpvOUFpquc/xaF3oWLRzZY89dDzn42Ucefvhzn3foooULY4g4MkhtTWcW22Iy6wqhCxWOdjlCPInCMhJmpxFs2LBx1crHV61atWbtU+s3bBwfnwiCmogbHJ9UtwhdWu5YQ8njL9mKJhI59DLWZxVI34DpaOTWEKSuopBOhXYZOMd+R2Kd+9jXx2BxmtazAAIROhcVt0RL/UOR4j47dKwrIiIWZrbuQpIzJEBgtumgoMxlJvsLW0iIWpmci9wHgTCLgAARIWGChMcicWnDDACg0qVSz+DgvMULR5cvW7b77rvuscduw8PD2dKCO45ThZtb01XWSP4/eTJ0p0Fl3ZYAAAAASUVORK5CYII="
_ICON_512 = "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"
@app.route("/favicon.ico")
def favicon():
    return Response(_b64.b64decode(_ICON_32), mimetype="image/png",
                    headers={"Cache-Control": "public, max-age=86400"})

@app.route("/icon-32.png")
def icon_32():
    return Response(_b64.b64decode(_ICON_32), mimetype="image/png",
                    headers={"Cache-Control": "public, max-age=86400"})

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
        "background_color": "#0a0f1e",
        "theme_color": "#0a0f1e",
        "orientation": "portrait",
        "icons": [
            {"src": "/icon-32.png", "sizes": "32x32", "type": "image/png"},
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
    store["portfolios"][name] = {"transactions": []}
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
    """Read or write the currently-active portfolio's holdings (legacy compat)."""
    store = load_store()
    active = store["active"]
    if request.method == "POST":
        # Accept either a list of holdings (legacy migrate.py) or
        # a dict with 'transactions' key
        body = request.get_json()
        if isinstance(body, list):
            # Legacy: convert holdings list to BUY transactions
            txs = []
            for h in body:
                if not all(k in h for k in ("ticker", "quantity", "avg_price", "currency")):
                    return jsonify({"error": "Missing required fields"}), 400
                txs.append({
                    "id": _new_id(),
                    "action": "BUY",
                    "ticker": h["ticker"].upper(),
                    "quantity": float(h["quantity"]),
                    "price": float(h["avg_price"]),
                    "currency": h["currency"],
                    "note": "Imported",
                })
            store["portfolios"][active] = {"transactions": txs}
            save_store(store)
            return jsonify({"ok": True})
        return jsonify({"error": "Use /api/transactions for v3 operations"}), 400

    # GET: return derived holdings list (for refresh endpoint compatibility)
    txs = store["portfolios"].get(active, {}).get("transactions", [])
    positions = compute_positions(txs)
    result = [
        {"ticker": p["ticker"], "quantity": p["quantity"],
         "avg_price": p["avg_cost"], "currency": p["currency"]}
        for p in positions.values() if p["quantity"] > 0
    ]
    return jsonify(result)


@app.route("/api/transactions", methods=["GET"])
def api_get_transactions():
    """Return all transactions + derived positions for the active portfolio."""
    store = load_store()
    active = store["active"]
    txs = store["portfolios"].get(active, {}).get("transactions", [])
    positions = compute_positions(txs)
    return jsonify({
        "transactions": txs,
        "positions": list(positions.values()),
        "portfolio": active,
    })


@app.route("/api/transactions/add", methods=["POST"])
def api_add_transaction():
    """Add a BUY or SELL transaction."""
    store = load_store()
    active = store["active"]
    data = request.get_json() or {}

    action = (data.get("action") or "").upper()
    ticker = (data.get("ticker") or "").upper().strip()
    try:
        quantity = float(data.get("quantity", 0))
        price    = float(data.get("price", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "quantity and price must be numbers"}), 400
    currency = (data.get("currency") or "USD").upper()
    note     = (data.get("note") or "").strip()[:200]

    if action not in ("BUY", "SELL"):
        return jsonify({"error": "action must be BUY or SELL"}), 400
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    if quantity <= 0:
        return jsonify({"error": "quantity must be positive"}), 400
    if price <= 0:
        return jsonify({"error": "price must be positive"}), 400

    # Validate SELL doesn't exceed held quantity
    if action == "SELL":
        txs = store["portfolios"].get(active, {}).get("transactions", [])
        positions = compute_positions(txs)
        held = positions.get(ticker, {}).get("quantity", 0)
        if quantity > held + 1e-9:
            return jsonify({
                "error": f"Cannot sell {quantity} — only {held:.4g} shares held"
            }), 400

    tx = {
        "id": _new_id(),
        "action": action,
        "ticker": ticker,
        "quantity": quantity,
        "price": price,
        "currency": currency,
        "note": note,
    }
    if active not in store["portfolios"]:
        store["portfolios"][active] = {"transactions": []}
    store["portfolios"][active]["transactions"].append(tx)
    save_store(store)
    return jsonify({"ok": True, "transaction": tx})


@app.route("/api/transactions/delete", methods=["POST"])
def api_delete_transaction():
    """Delete a transaction by id. Validates resulting state is consistent."""
    store = load_store()
    active = store["active"]
    tx_id = (request.get_json() or {}).get("id")
    if not tx_id:
        return jsonify({"error": "id required"}), 400

    txs = store["portfolios"].get(active, {}).get("transactions", [])
    new_txs = [t for t in txs if t["id"] != tx_id]
    if len(new_txs) == len(txs):
        return jsonify({"error": "Transaction not found"}), 404

    # Validate: removing this tx shouldn't result in negative quantities
    positions = compute_positions(new_txs)
    for p in positions.values():
        if p["quantity"] < -1e-9:
            return jsonify({
                "error": f"Cannot delete: would result in negative position for {p['ticker']}"
            }), 400

    store["portfolios"][active]["transactions"] = new_txs
    save_store(store)
    return jsonify({"ok": True})


@app.route("/api/refresh")
def api_refresh():
    """Fetch live data for the active portfolio's holdings + FX."""
    display_ccy = request.args.get("display_ccy", "ILS").upper()
    force = request.args.get("force", "0") == "1"

    store = load_store()
    active = store["active"]
    txs = store["portfolios"].get(active, {}).get("transactions", [])
    positions = compute_positions(txs)

    # Only fetch tickers with open positions
    open_positions = {t: p for t, p in positions.items() if p["quantity"] > 0}
    if not open_positions and not positions:
        return jsonify({"holdings": [], "fx": {}, "display_ccy": display_ccy,
                        "realized_pl_total": 0})

    all_tickers = list({p["ticker"] for p in positions.values()})
    ticker_data = {t: fetch_ticker_data(t, force=force) for t in all_tickers}

    # FX rates
    currencies = {p["currency"] for p in positions.values()}
    fx = {ccy: fetch_fx(ccy, display_ccy) for ccy in currencies}

    # Build enriched holdings (open positions only)
    enriched = []
    for ticker, p in open_positions.items():
        td = ticker_data.get(ticker, {})
        enriched.append({
            "ticker": p["ticker"],
            "quantity": p["quantity"],
            "avg_price": p["avg_cost"],
            "currency": p["currency"],
            "realized_pl": p["realized_pl"],
            "ticker_data": td,
        })

    # Realized P&L across ALL positions (including fully closed ones)
    realized_total = sum(p["realized_pl"] for p in positions.values())

    return jsonify({
        "holdings": enriched,
        "fx": fx,
        "display_ccy": display_ccy,
        "realized_pl_total": realized_total,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
    })


@app.route("/api/health")
def api_health():
    return jsonify({"ok": True})


@app.route("/api/analyze", methods=["POST"])
def api_analyze():
    """Run analyst research on the active portfolio using Claude + web search."""
    if not ANTHROPIC_API_KEY:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured. Add it to your environment variables."}), 503

    data = request.get_json() or {}
    horizon  = data.get("horizon", "Long-term (3–5 years)")
    risk     = data.get("risk", "Moderate")
    force    = data.get("force", False)

    # Build portfolio snapshot from current positions
    store   = load_store()
    active  = store["active"]
    txs     = store["portfolios"].get(active, {}).get("transactions", [])
    positions = compute_positions(txs)
    open_pos  = [p for p in positions.values() if p["quantity"] > 0]

    if not open_pos:
        return jsonify({"error": "No open positions to analyze."}), 400

    portfolio_json = json.dumps([
        {
            "ticker":   p["ticker"],
            "quantity": round(p["quantity"], 4),
            "avg_cost": round(p["avg_cost"], 4),
            "currency": p["currency"],
        }
        for p in open_pos
    ], indent=2)

    # Cache key based on tickers + quantities (not prices)
    cache_key = f"{active}|{portfolio_json}|{horizon}|{risk}"
    if not force and cache_key in ANALYSIS_CACHE:
        ts, cached = ANALYSIS_CACHE[cache_key]
        if time.time() - ts < ANALYSIS_CACHE_TTL:
            return jsonify({"ok": True, "result": cached, "cached": True})

    prompt = f"""You are a professional equity research assistant.
Analyze this portfolio using verified analyst consensus and fundamental data.

Rules:
- Do NOT fabricate ratings or sources. If missing, say "N/A".
- Prefer data from the last 90 days.
- Be concise. No fluff. Bottom-line focus.
- Do NOT fabricate analyst ratings or sources.

Portfolio:
{portfolio_json}

Horizon: {horizon} | Risk: {risk}

---

OUTPUT FORMAT (strict markdown, in this exact order):

## PORTFOLIO SUMMARY

| Metric | Value |
|--------|-------|
| Overall Sentiment | Bull X% / Neutral X% / Bear X% |
| Avg Analyst Score | X.X / 2.0 |
| Portfolio Upside to Avg Target | X% |
| Best Positioned | TICKER — one line why |
| Most At Risk | TICKER — one line why |

**Recommended Actions (max 3):**
1. ...
2. ...
3. ...

---

## INDIVIDUAL STOCKS

For each ticker, one compact block:

**[TICKER]** · Sentiment: Bull/Neutral/Bear · Score: X.X · Upside: X% · **[RECOMMENDATION]**
> One sentence reasoning. Key risk if any.

Use these recommendation labels only: Strong Buy / Buy / Hold / Reduce / Sell
Color the recommendation by wrapping it: use exactly the word with no extra formatting.

Cover every ticker in the portfolio. Keep each block to 2–3 lines maximum.
"""

    try:
        import urllib.request as _req
        import urllib.error as _uerr

        payload = {
            "model": "claude-sonnet-4-5-20251001",
            "max_tokens": 8000,
            "tools": [{"type": "web_search_20250305", "name": "web_search"}],
            "messages": [{"role": "user", "content": prompt}],
        }
        body = json.dumps(payload).encode()
        print(f"[analyze] Calling Anthropic API, model={payload['model']}, portfolio={active}, tickers={[p['ticker'] for p in open_pos]}")

        req = _req.Request(
            "https://api.anthropic.com/v1/messages",
            data=body,
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            method="POST",
        )
        resp = _req.urlopen(req, timeout=120)
        raw = resp.read()
        print(f"[analyze] API response status: {resp.status}, length: {len(raw)} bytes")
        result = json.loads(raw)
        print(f"[analyze] Content blocks: {[b.get('type') for b in result.get('content', [])]}")

        # Extract text blocks only
        text_parts = [
            block["text"]
            for block in result.get("content", [])
            if block.get("type") == "text"
        ]
        analysis_text = "\n".join(text_parts).strip()

        if not analysis_text:
            return jsonify({"error": "No analysis returned from API."}), 500

        ANALYSIS_CACHE[cache_key] = (time.time(), analysis_text)
        return jsonify({"ok": True, "result": analysis_text, "cached": False})

    except Exception as e:
        import traceback, urllib.error as _uerr
        traceback.print_exc()
        if isinstance(e, _uerr.HTTPError):
            try:
                err_body = json.loads(e.read().decode())
                err_msg = err_body.get("error", {}).get("message", str(e))
                print(f"[analyze] Anthropic API error: {err_body}")
            except Exception:
                err_msg = str(e)
        else:
            err_msg = str(e)
        return jsonify({"error": f"Analysis failed: {err_msg}"}), 500


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
<meta name="theme-color" content="#0a0f1e">
<link rel="manifest" href="/manifest.json">
<link rel="apple-touch-icon" href="/icon-180.png">
<link rel="icon" type="image/png" sizes="32x32" href="/favicon.ico">
<link rel="shortcut icon" href="/favicon.ico">
<title>ISAK Stocks · Sign in</title>
<style>
  :root {
    --bg: #0a0f1e; --paper: #0f1929; --paper-2: #162035;
    --ink: #e8f0fe; --muted: #6b7fa3; --line: #1e2d4a;
    --accent: #39d353; --neg: #e05252;
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
    <div style="text-align:center; margin-bottom:20px;">
      <img src="/icon-180.png" alt="ISAK Stocks" style="width:72px;height:72px;border-radius:16px;">
    </div>
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
<meta name="theme-color" content="#0a0f1e">
<link rel="manifest" href="/manifest.json">
<link rel="apple-touch-icon" href="/icon-180.png">
<link rel="icon" type="image/png" sizes="32x32" href="/favicon.ico">
<link rel="shortcut icon" href="/favicon.ico">
<title>ISAK Stocks</title>
<style>
  :root {
    --bg:      #0a0f1e;
    --paper:   #0f1929;
    --paper-2: #162035;
    --ink:     #e8f0fe;
    --muted:   #6b7fa3;
    --line:    #1e2d4a;
    --pos:     #39d353;
    --neg:     #e05252;
    --accent:  #39d353;
    --accent2: #2ab040;
    --navy:    #0a0f1e;
    --gold:    #c9a96e;
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

  /* Brand header */
  .header-brand { display: flex; align-items: center; gap: 16px; }
  .header-logo  { width: 56px; height: 56px; border-radius: 12px; }
  .header-tagline {
    font-family: 'Helvetica Neue', sans-serif;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 3px;
    color: var(--accent);
    margin-top: 2px;
  }

  /* Green glow on refresh button */
  #refresh-btn {
    background: linear-gradient(135deg, #2ab040, #39d353);
    box-shadow: 0 0 12px rgba(57,211,83,0.25);
  }
  #refresh-btn:hover { box-shadow: 0 0 20px rgba(57,211,83,0.4); opacity: 1; }

  /* Summary cards — subtle green top border for good values */
  .card { border-top: 2px solid var(--line); transition: border-color 0.3s; }

  /* Toolbar style update */
  .toolbar { border-left: 3px solid var(--accent); }

  /* Chart area glow */
  #chart-wrap { border-color: var(--line); }

  /* Page tab active — green underline */
  .page-tab.active { color: var(--accent); border-bottom-color: var(--accent); }

  /* Analysis output h2 color */
  .analysis-output h2 { color: var(--accent); }

  /* Positive values extra brightness */
  .pos { color: var(--pos); text-shadow: 0 0 8px rgba(57,211,83,0.3); }

  /* Page-level tabs (Dashboard / Transactions / Analysis) */
  .page-tabs {
    display: flex;
    gap: 0;
    margin-bottom: 32px;
    border-bottom: 1px solid var(--line);
  }
  .page-tab {
    padding: 10px 24px;
    font-family: 'Helvetica Neue', sans-serif;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 2px;
    color: var(--muted);
    cursor: pointer;
    border-bottom: 2px solid transparent;
    margin-bottom: -1px;
    transition: color 0.15s;
  }
  .page-tab.active {
    color: var(--accent);
    border-bottom-color: var(--accent);
  }
  .page-tab:hover { color: var(--ink); }

  /* Analysis controls */
  .analysis-controls {
    display: flex;
    align-items: flex-end;
    gap: 12px;
    flex-wrap: wrap;
    margin-bottom: 16px;
  }

  /* Analysis markdown output */
  .analysis-output {
    background: var(--paper);
    border: 1px solid var(--line);
    border-radius: 4px;
    padding: 28px 32px;
    line-height: 1.7;
  }
  .analysis-output h2 {
    font-family: 'Bodoni 72', 'Didot', 'Playfair Display', 'Georgia', serif;
    font-size: 22px;
    font-weight: 400;
    margin: 28px 0 12px;
    padding-bottom: 6px;
    border-bottom: 1px solid var(--line);
    color: var(--accent);
  }
  .analysis-output h2:first-child { margin-top: 0; }
  .analysis-output h3 {
    font-size: 14px;
    font-family: 'Helvetica Neue', sans-serif;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: var(--muted);
    margin: 16px 0 6px;
    font-weight: 600;
  }
  .analysis-output p { margin: 6px 0 12px; }
  .analysis-output ul { margin: 4px 0 12px 20px; }
  .analysis-output li { margin: 3px 0; }
  .analysis-output strong { color: var(--ink); }
  .analysis-output hr {
    border: none;
    border-top: 1px solid var(--line);
    margin: 24px 0;
  }
  .analysis-output code {
    background: var(--paper-2);
    padding: 1px 5px;
    border-radius: 2px;
    font-size: 12px;
  }
  /* Recommendation highlight */
  .rec-buy       { color: var(--pos); font-weight: 700; }
  .rec-sell      { color: var(--neg); font-weight: 700; }
  .rec-hold      { color: var(--accent); font-weight: 700; }

  /* Transaction form */
  .tx-form {
    display: grid;
    grid-template-columns: 0.7fr 0.8fr 0.8fr 0.8fr 0.6fr 1.2fr auto;
    gap: 8px;
    align-items: end;
    margin-bottom: 12px;
  }
  @media (max-width: 900px) {
    .tx-form { grid-template-columns: 1fr 1fr 1fr; }
  }

  /* Transaction history table */
  .tx-action-buy  { color: var(--pos); font-weight: 700; }
  .tx-action-sell { color: var(--neg); font-weight: 700; }

  /* 4-card summary grid */
  .summary { grid-template-columns: repeat(4, 1fr); }
  @media (max-width: 900px) { .summary { grid-template-columns: 1fr 1fr; } }
  @media (max-width: 500px) { .summary { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<div class="wrap">

<header>
  <div class="header-brand">
    <img src="/icon-180.png" alt="ISAK Stocks" class="header-logo">
    <div>
      <h1>ISAK <em>Stocks</em></h1>
      <div class="header-tagline">Portfolio Intelligence</div>
    </div>
  </div>
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

<!-- TAB NAV -->
<div class="page-tabs">
  <div class="page-tab active" data-tab="dashboard">Dashboard</div>
  <div class="page-tab" data-tab="transactions">Transactions</div>
  <div class="page-tab" data-tab="analysis">📊 Analysis</div>
</div>

<!-- ===================== DASHBOARD TAB ===================== -->
<div id="tab-dashboard">

<section>
  <div class="label">Snapshot · <span id="active-portfolio-name" style="color: var(--accent);">—</span></div>
  <div class="summary">
    <div class="card">
      <div class="label" style="margin:0 0 6px;">Total Paid</div>
      <div class="big" id="total-paid">—</div>
      <div class="sub">Cost basis (open positions)</div>
    </div>
    <div class="card">
      <div class="label" style="margin:0 0 6px;">Worth Now</div>
      <div class="big" id="total-worth">—</div>
      <div class="sub" id="daily-line">—</div>
    </div>
    <div class="card">
      <div class="label" style="margin:0 0 6px;">Unrealized P / L</div>
      <div class="big" id="total-pl">—</div>
      <div class="sub" id="total-pl-pct">—</div>
    </div>
    <div class="card">
      <div class="label" style="margin:0 0 6px;">Realized P / L</div>
      <div class="big" id="realized-pl">—</div>
      <div class="sub">From completed sells</div>
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
  <div id="holdings-area"></div>
</section>

</div><!-- /tab-dashboard -->

<!-- ===================== TRANSACTIONS TAB ===================== -->
<div id="tab-transactions" style="display:none;">

<section>
  <h2>Add Transaction</h2>
  <div class="tx-form">
    <div class="field">
      <label>Action</label>
      <select id="tx-action">
        <option value="BUY">BUY</option>
        <option value="SELL">SELL</option>
      </select>
    </div>
    <div class="field">
      <label>Ticker</label>
      <input type="text" id="tx-ticker" placeholder="NVDA">
    </div>
    <div class="field">
      <label>Quantity</label>
      <input type="number" id="tx-qty" step="any" placeholder="10">
    </div>
    <div class="field">
      <label>Price per share</label>
      <input type="number" id="tx-price" step="any" placeholder="130.00">
    </div>
    <div class="field">
      <label>Currency</label>
      <select id="tx-ccy">
        <option value="USD">USD</option>
        <option value="ILS">ILS</option>
      </select>
    </div>
    <div class="field">
      <label>Note (optional)</label>
      <input type="text" id="tx-note" placeholder="e.g. partial sale">
    </div>
    <button id="tx-submit">Add</button>
  </div>
  <div id="tx-error" class="notice" style="display:none;"></div>
</section>

<section>
  <h2>Transaction History</h2>
  <div id="tx-area"></div>
</section>

</div><!-- /tab-transactions -->

<!-- ===================== ANALYSIS TAB ===================== -->
<div id="tab-analysis" style="display:none;">

<section>
  <h2>Analyst Research</h2>
  <div class="analysis-controls">
    <div class="field">
      <label>Investment Horizon</label>
      <select id="an-horizon">
        <option value="Short-term (under 1 year)">Short-term (&lt;1 year)</option>
        <option value="Medium-term (1–3 years)">Medium-term (1–3 years)</option>
        <option value="Long-term (3–5 years)" selected>Long-term (3–5 years)</option>
      </select>
    </div>
    <div class="field">
      <label>Risk Tolerance</label>
      <select id="an-risk">
        <option value="Conservative">Conservative</option>
        <option value="Moderate" selected>Moderate</option>
        <option value="Aggressive">Aggressive</option>
      </select>
    </div>
    <button id="an-run">Run Analysis</button>
    <button id="an-force" class="ghost" title="Bypass 6-hour cache and re-fetch">Force refresh</button>
  </div>
  <div class="notice" id="an-no-key" style="display:none;">
    <b>ANTHROPIC_API_KEY not set.</b> Add it to your environment variables to enable analyst research.
    <br><span style="font-size:12px;">Set it in Render's Environment tab, or locally: <code>set ANTHROPIC_API_KEY=sk-ant-...</code></span>
  </div>
  <div id="an-status" style="color:var(--muted); font-size:13px; font-style:italic; margin:16px 0;"></div>
</section>

<section id="an-result-section" style="display:none;">
  <div id="an-result" class="analysis-output"></div>
  <div style="color:var(--muted); font-size:11px; margin-top:12px; font-style:italic;" id="an-cache-note"></div>
</section>

</div><!-- /tab-analysis -->

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
  portfolios: [],
  activePortfolio: '',
  holdings: [],
  transactions: [],
  positions: {},         // computed positions from backend
  realizedPlTotal: 0,
  fx: { USD: 1, ILS: 1 },
  displayCcy: 'ILS',
  chartPeriod: '1w',
  activeTab: 'dashboard',
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
async function apiGetTransactions() {
  const r = await fetch('/api/transactions');
  return r.json();
}
async function apiAddTransaction(tx) {
  const r = await fetch('/api/transactions/add', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(tx),
  });
  return { ok: r.ok, ...(await r.json()) };
}
async function apiDeleteTransaction(id) {
  const r = await fetch('/api/transactions/delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id }),
  });
  return { ok: r.ok, ...(await r.json()) };
}

// ============================================================================
// LOAD & REFRESH
// ============================================================================
async function loadInitial() {
  await reloadPortfolioList();
  await loadTransactions();
  render();
  if (state.holdings.length > 0) {
    await refresh(false);
  }
}

async function loadTransactions() {
  const data = await apiGetTransactions();
  state.transactions = data.transactions || [];
  state.positions    = {};
  (data.positions || []).forEach(p => { state.positions[p.ticker] = p; });
  // Derive holdings list from positions
  state.holdings = (data.positions || [])
    .filter(p => p.quantity > 0)
    .map(p => ({
      ticker: p.ticker,
      quantity: p.quantity,
      avg_price: p.avg_cost,
      currency: p.currency,
      realized_pl: p.realized_pl,
      ticker_data: null,
    }));
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
  state.holdings = [];
  state.transactions = [];
  state.positions = {};
  render();
  await loadTransactions();
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
    state.realizedPlTotal = data.realized_pl_total || 0;

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
  renderTransactions();
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
  plp.textContent = `${fmtPct(t.totalPlPct)} unrealized`;
  plp.className = 'sub ' + colorClass(t.totalPlPct);

  // Realized P&L card
  const rpl = document.getElementById('realized-pl');
  if (rpl) {
    const realizedDisp = state.realizedPlTotal * (state.fx[state.holdings[0]?.currency] || 1);
    rpl.textContent = fmtMoney(state.realizedPlTotal, ccy);
    rpl.className = 'big ' + colorClass(state.realizedPlTotal);
  }
}

function renderHoldings() {
  const area = document.getElementById('holdings-area');
  if (state.holdings.length === 0) {
    area.innerHTML = '<div class="empty">No open positions. Add a BUY transaction in the Transactions tab.</div>';
    return;
  }

  const rowsHtml = state.holdings.map((h) => {
    const td = h.ticker_data;
    const pos = state.positions[h.ticker] || {};
    const realizedPl = pos.realized_pl || 0;

    if (!td) {
      return `<tr class="row-error">
        <td><span class="ticker-name">${h.ticker}</span></td>
        <td colspan="6">Click Refresh to load price</td>
      </tr>`;
    }
    if (td.error) {
      return `<tr class="row-error">
        <td><span class="ticker-name">${h.ticker}</span></td>
        <td colspan="6">${td.error}</td>
      </tr>`;
    }
    const dailyPct = ((td.current - td.prev) / td.prev) * 100;
    const value    = h.quantity * td.current;
    const cost     = h.quantity * h.avg_price;
    const unrealPl = value - cost;
    const unrealPct = cost ? (unrealPl / cost) * 100 : 0;
    return `<tr>
      <td><span class="ticker-name">${h.ticker}</span></td>
      <td>${h.quantity % 1 === 0 ? h.quantity : h.quantity.toFixed(4)}</td>
      <td>${fmtMoney(h.avg_price, h.currency)}</td>
      <td>${fmtMoney(td.current, h.currency)}</td>
      <td class="${colorClass(dailyPct)}">${fmtPct(dailyPct)}</td>
      <td>${fmtMoney(value, h.currency)}</td>
      <td class="${colorClass(unrealPl)}">${fmtMoney(unrealPl, h.currency)} <span style="font-size:11px;">(${fmtPct(unrealPct)})</span></td>
      <td class="${colorClass(realizedPl)}">${realizedPl !== 0 ? fmtMoney(realizedPl, h.currency) : '—'}</td>
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
          <th>Unrealized P/L</th>
          <th>Realized P/L</th>
        </tr>
      </thead>
      <tbody>${rowsHtml}</tbody>
    </table>
    <div style="color:var(--muted); font-size:12px; margin-top:8px;">
      Holdings in native currency · Totals converted to ${state.displayCcy} · AVCO cost basis
    </div>
  `;
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

function renderTransactions() {
  const area = document.getElementById('tx-area');
  if (!area) return;

  if (state.transactions.length === 0) {
    area.innerHTML = '<div class="empty">No transactions yet. Add a BUY above to get started.</div>';
    return;
  }

  // Show in reverse order (newest first)
  const rows = [...state.transactions].reverse().map(tx => {
    const actionClass = tx.action === 'BUY' ? 'tx-action-buy' : 'tx-action-sell';
    const total = tx.quantity * tx.price;
    return `<tr>
      <td><span class="${actionClass}">${tx.action}</span></td>
      <td><b>${tx.ticker}</b></td>
      <td style="text-align:right;">${tx.quantity % 1 === 0 ? tx.quantity : parseFloat(tx.quantity).toFixed(4)}</td>
      <td style="text-align:right;">${fmtMoney(tx.price, tx.currency)}</td>
      <td style="text-align:right;">${fmtMoney(total, tx.currency)}</td>
      <td style="color:var(--muted); font-size:12px;">${tx.note || ''}</td>
      <td><button class="danger" data-tx-delete="${tx.id}">✕</button></td>
    </tr>`;
  }).join('');

  area.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Action</th>
          <th>Ticker</th>
          <th style="text-align:right;">Qty</th>
          <th style="text-align:right;">Price</th>
          <th style="text-align:right;">Total</th>
          <th>Note</th>
          <th></th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
    <div style="color:var(--muted); font-size:12px; margin-top:8px;">
      ${state.transactions.length} transaction${state.transactions.length !== 1 ? 's' : ''} ·
      Deleting a transaction recalculates all positions via AVCO.
    </div>
  `;

  area.querySelectorAll('[data-tx-delete]').forEach(btn => {
    btn.addEventListener('click', async (e) => {
      const id = e.target.getAttribute('data-tx-delete');
      const tx = state.transactions.find(t => t.id === id);
      if (!tx) return;
      if (!confirm(`Delete this ${tx.action} of ${tx.quantity} ${tx.ticker}?`)) return;
      const result = await apiDeleteTransaction(id);
      if (!result.ok) {
        alert(result.error || 'Could not delete transaction');
        return;
      }
      await loadTransactions();
      render();
    });
  });
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

// Transaction form submit
document.getElementById('tx-submit').addEventListener('click', async () => {
  const action   = document.getElementById('tx-action').value;
  const ticker   = document.getElementById('tx-ticker').value.trim().toUpperCase();
  const qty      = parseFloat(document.getElementById('tx-qty').value);
  const price    = parseFloat(document.getElementById('tx-price').value);
  const currency = document.getElementById('tx-ccy').value;
  const note     = document.getElementById('tx-note').value.trim();

  const errEl = document.getElementById('tx-error');
  errEl.style.display = 'none';

  if (!ticker || !qty || !price) {
    errEl.innerHTML = 'Please fill in ticker, quantity and price.';
    errEl.style.display = 'block';
    return;
  }

  const btn = document.getElementById('tx-submit');
  btn.disabled = true;
  btn.textContent = '...';

  const result = await apiAddTransaction({ action, ticker, quantity: qty, price, currency, note });

  btn.disabled = false;
  btn.textContent = 'Add';

  if (!result.ok) {
    errEl.innerHTML = `<b>Error:</b> ${result.error || 'Unknown error'}`;
    errEl.style.display = 'block';
    return;
  }

  // Clear form
  ['tx-ticker', 'tx-qty', 'tx-price', 'tx-note'].forEach(id => {
    document.getElementById(id).value = '';
  });

  await loadTransactions();
  render();
  if (state.holdings.length > 0) refresh(false);
});

// Page tab switching
document.querySelectorAll('.page-tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.page-tab').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    state.activeTab = tab.dataset.tab;
    document.getElementById('tab-dashboard').style.display    = state.activeTab === 'dashboard'    ? '' : 'none';
    document.getElementById('tab-transactions').style.display = state.activeTab === 'transactions' ? '' : 'none';
    document.getElementById('tab-analysis').style.display     = state.activeTab === 'analysis'     ? '' : 'none';
  });
});

// ============================================================================
// ANALYSIS TAB
// ============================================================================

// Simple markdown→HTML renderer (no external lib needed)
function markdownToHtml(md) {
  const lines = md.split('\n');
  let html = '';
  let inTable = false;
  let tableHtml = '';
  let headerDone = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Tables
    if (line.trim().startsWith('|')) {
      if (!inTable) {
        inTable = true;
        headerDone = false;
        tableHtml = '<table style="width:100%;border-collapse:collapse;margin:12px 0;">';
      }
      // Skip separator rows (---|---)
      if (/^\|[\s\-:]+\|/.test(line)) {
        headerDone = true;
        continue;
      }
      const cells = line.split('|').filter((_, i, a) => i > 0 && i < a.length - 1);
      const tag = !headerDone ? 'th' : 'td';
      const style = !headerDone
        ? 'style="padding:6px 10px;text-align:left;background:var(--paper-2);font-family:Helvetica Neue,sans-serif;font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:600;"'
        : 'style="padding:6px 10px;border-bottom:1px solid var(--line);font-size:13px;"';
      tableHtml += '<tr>' + cells.map(c => `<${tag} ${style}>${renderInline(c.trim())}</${tag}>`).join('') + '</tr>';
      continue;
    } else if (inTable) {
      html += tableHtml + '</table>';
      inTable = false;
      tableHtml = '';
    }

    // HR
    if (/^---+$/.test(line.trim())) { html += '<hr>'; continue; }
    // H2
    if (line.startsWith('## ')) { html += `<h2>${renderInline(line.slice(3))}</h2>`; continue; }
    // H3
    if (line.startsWith('### ')) { html += `<h3>${renderInline(line.slice(4))}</h3>`; continue; }
    // Blockquote (used for stock reasoning)
    if (line.startsWith('> ')) {
      html += `<div style="border-left:3px solid var(--line);padding:4px 12px;color:var(--muted);font-size:13px;margin:4px 0 8px;">${renderInline(line.slice(2))}</div>`;
      continue;
    }
    // Bullet
    if (/^[\*\-] /.test(line)) { html += `<li>${renderInline(line.slice(2))}</li>`; continue; }
    // Numbered list
    if (/^\d+\. /.test(line)) { html += `<li>${renderInline(line.replace(/^\d+\. /,''))}</li>`; continue; }
    // Empty line
    if (line.trim() === '') { html += '<br>'; continue; }
    // Default paragraph
    html += `<p style="margin:4px 0;">${renderInline(line)}</p>`;
  }

  if (inTable) html += tableHtml + '</table>';

  // Wrap consecutive <li> in <ul>
  html = html.replace(/(<li>.*?<\/li>(\s*<br>\s*)*)+/gs,
    m => '<ul style="margin:4px 0 10px 18px;">' + m.replace(/<br>/g,'') + '</ul>');

  return html;
}

function renderInline(text) {
  return text
    // Bold
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    // Inline code
    .replace(/`(.+?)`/g, '<code>$1</code>')
    // Color recommendation keywords
    .replace(/\b(Strong Buy|Buy)\b(?![^<]*<\/span>)/g, '<span class="rec-buy">$1</span>')
    .replace(/\b(Strong Sell|Sell|Reduce)\b(?![^<]*<\/span>)/g, '<span class="rec-sell">$1</span>')
    .replace(/\b(Hold)\b(?![^<]*<\/span>)/g, '<span class="rec-hold">$1</span>');
}

async function runAnalysis(force = false) {
  const runBtn   = document.getElementById('an-run');
  const forceBtn = document.getElementById('an-force');
  const status   = document.getElementById('an-status');
  const resultSection = document.getElementById('an-result-section');
  const resultEl = document.getElementById('an-result');
  const cacheNote = document.getElementById('an-cache-note');
  const noKeyEl  = document.getElementById('an-no-key');

  runBtn.disabled   = true;
  forceBtn.disabled = true;
  noKeyEl.style.display = 'none';
  resultSection.style.display = 'none';

  const horizon = document.getElementById('an-horizon').value;
  const risk    = document.getElementById('an-risk').value;

  const steps = [
    'Gathering analyst ratings from web sources...',
    'Collecting fundamentals data...',
    'Normalizing ratings and computing scores...',
    'Assessing risk flags...',
    'Building recommendations...',
  ];
  let stepIdx = 0;
  status.textContent = steps[0];
  const stepTimer = setInterval(() => {
    stepIdx = (stepIdx + 1) % steps.length;
    status.textContent = steps[stepIdx];
  }, 4000);

  try {
    const r = await fetch('/api/analyze', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ horizon, risk, force }),
    });
    const data = await r.json();
    clearInterval(stepTimer);

    if (!data.ok) {
      status.textContent = '';
      if (data.error && data.error.includes('ANTHROPIC_API_KEY')) {
        noKeyEl.style.display = 'block';
      } else {
        status.textContent = `Error: ${data.error || 'Unknown error'}`;
        status.style.color = 'var(--neg)';
      }
      return;
    }

    status.textContent = '';
    resultEl.innerHTML = markdownToHtml(data.result);
    cacheNote.textContent = data.cached
      ? 'Showing cached results (up to 6 hours old). Click "Force refresh" for fresh data.'
      : `Analysis generated ${new Date().toLocaleString()} · Results cached for 6 hours`;
    resultSection.style.display = '';
  } catch(e) {
    clearInterval(stepTimer);
    status.textContent = `Request failed: ${e.message}`;
    status.style.color = 'var(--neg)';
  } finally {
    runBtn.disabled   = false;
    forceBtn.disabled = false;
  }
}

document.getElementById('an-run').addEventListener('click', () => runAnalysis(false));
document.getElementById('an-force').addEventListener('click', () => runAnalysis(true));

// Chart period tabs
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
    if ANTHROPIC_API_KEY:
        print(f"  Analyst research:  ON (Anthropic API key set)")
    else:
        print(f"  Analyst research:  off (set ANTHROPIC_API_KEY to enable)")
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
