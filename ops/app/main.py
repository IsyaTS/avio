import os, datetime as dt, logging
import humanize, psycopg, redis
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError
from fastapi import FastAPI, Depends, Request, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pathlib import Path

BASE = Path(__file__).resolve().parent

app = FastAPI(title="Avio Ops")
templates = Jinja2Templates(directory=str(BASE / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")
security = HTTPBasic()

def _load_database_url() -> str:
    return (
        os.getenv("DATABASE_URL")
        or os.getenv("POSTGRES_URL")
        or "postgresql://postgres:postgres@postgres:5432/postgres"
    )


def _normalize_psycopg_dsn(raw: str) -> str:
    if not raw:
        return raw
    try:
        url = make_url(raw)
    except (ArgumentError, ValueError):
        if raw.startswith("postgresql+") and "://" in raw:
            scheme, remainder = raw.split("://", 1)
            base_scheme = scheme.split("+", 1)[0]
            return f"{base_scheme}://{remainder}"
        return raw
    driver = url.drivername or ""
    if "+" in driver:
        url = url.set(drivername=driver.split("+", 1)[0])
    return url.render_as_string(hide_password=False)


DB = _load_database_url()
DB_SYNC_DSN = _normalize_psycopg_dsn(DB)
REDIS_URL = os.getenv("REDIS_URL","redis://redis:6379/0")
OPS_USER = os.getenv("OPS_USER","admin")
OPS_PASS = os.getenv("OPS_PASS","admin")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
_alembic_logger = logging.getLogger("ops.alembic")


@app.on_event("startup")
def _log_alembic_revision() -> None:
    try:
        with psycopg.connect(DB_SYNC_DSN) as conn:
            cur = conn.cursor()
            cur.execute("SELECT version_num FROM alembic_version LIMIT 1")
            row = cur.fetchone()
    except psycopg.errors.UndefinedTable:
        _alembic_logger.info(
            "alembic_revision=unavailable (alembic_version table missing)"
        )
        return
    except Exception:
        _alembic_logger.exception("failed to query Alembic revision")
        return
    if row and row[0]:
        _alembic_logger.info("alembic_revision=%s", row[0])
    else:
        _alembic_logger.warning("alembic_revision=unavailable")


def auth(creds: HTTPBasicCredentials = Depends(security)):
    if not (creds.username == OPS_USER and creds.password == OPS_PASS):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized", headers={"WWW-Authenticate": "Basic"})
    return True

def today_bounds():
    d0 = dt.datetime.utcnow().date()
    d1 = d0 + dt.timedelta(days=1)
    return d0, d1

def get_cols(cur, table):
    cur.execute("""
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name=%s
    """, (table,))
    return {name: dtype for (name, dtype) in cur.fetchall()}

def discover(cur):
    cols = get_cols(cur, 'messages')

    # выбрать имена колонок
    lead = next((c for c in ("lead_id","lead","conversation_id","chat_id") if c in cols), None)
    ts   = next((c for c in ("ts","created_at","datetime","time","date","at","inserted_at") if c in cols), None)
    inc  = next((c for c in ("incoming","is_incoming") if c in cols), None)
    dire = "direction" if "direction" in cols else None
    text = next((c for c in ("text","message","body","content","payload") if c in cols), None)
    idc  = next((c for c in ("id","msg_id") if c in cols), "id")

    if not (lead and ts and (inc or dire)):
        raise RuntimeError("messages schema not recognized")

    # построить выражения направлений с учётом типа колонки
    def inc_out_from(colname):
        dtype = cols[colname]
        if dtype == "boolean":
            return f'"{colname}" = true', f'"{colname}" = false'
        if dtype in ("smallint","integer","bigint","numeric"):
            # по умолчанию считаем 1 = входящее, 0 = исходящее
            return f'"{colname}" = 1', f'"{colname}" = 0'
        # текстовые
        return (
            f"lower(coalesce(\"{colname}\",'')) in ('in','incoming','client','from_client')",
            f"lower(coalesce(\"{colname}\",'')) in ('out','outgoing','agent','to_client')",
        )

    if inc:
        inc_expr, out_expr = inc_out_from(inc)
    else:
        inc_expr, out_expr = inc_out_from(dire)

    # текстовая колонка (приводим к text, если json/jsonb)
    text_expr = "''"
    if text:
        tdt = cols[text]
        if tdt in ("json","jsonb"):
            text_expr = f'("{text}")::text'
        else:
            text_expr = f'coalesce("{text}", \'\')'

    meta = {
        "lead": lead, "ts": ts, "inc_expr": inc_expr, "out_expr": out_expr,
        "text_expr": text_expr, "id": idc
    }
    return meta

def kpis(cur, meta, d0, d1):
    lead, ts, inc_expr, out_expr = meta["lead"], meta["ts"], meta["inc_expr"], meta["out_expr"]

    cur.execute(f'SELECT count(DISTINCT "{lead}") FROM messages WHERE "{ts}" >= %s AND "{ts}" < %s', (d0,d1))
    leads = cur.fetchone()[0] or 0

    cur.execute(f"""
        WITH x AS (
          SELECT "{lead}" AS lead_id,
                 MAX(CASE WHEN {inc_expr} THEN 1 ELSE 0 END) has_in,
                 MAX(CASE WHEN {out_expr} THEN 1 ELSE 0 END) has_out
          FROM messages
          WHERE "{ts}" >= %s AND "{ts}" < %s
          GROUP BY "{lead}"
        )
        SELECT count(*) FROM x WHERE has_in=1 AND has_out=1
    """,(d0,d1))
    replied = cur.fetchone()[0] or 0

    # A/B (если таблиц нет — нули)
    try:
        cur.execute('SELECT count(*) FROM ab_decisions WHERE decided_at >= %s AND decided_at < %s',(d0,d1))
        ab_trials = cur.fetchone()[0] or 0
    except Exception:
        ab_trials = 0
    try:
        cur.execute('SELECT count(*) FROM ab_outcomes WHERE ts >= %s AND ts < %s',(d0,d1))
        ab_succ = cur.fetchone()[0] or 0
    except Exception:
        ab_succ = 0

    rr = (replied/leads*100.0) if leads else 0.0
    return {
        "leads": leads, "replied": replied, "reply_rate": f"{rr:.1f}%",
        "ab_trials": ab_trials, "ab_succ": ab_succ,
    }

@app.get("/", response_class=HTMLResponse)
def index(request: Request, ok: bool = Depends(auth)):
    d0,d1 = today_bounds()
    with psycopg.connect(DB_SYNC_DSN) as conn:
        cur = conn.cursor()
        meta = discover(cur)
        stats = kpis(cur, meta, d0, d1)

        queue_depth = int(r.llen("outbox:send") or 0)
        dlq_depth   = int(r.llen("outbox:dlq") or 0)

        lead, ts, inc_expr, out_expr = meta["lead"], meta["ts"], meta["inc_expr"], meta["out_expr"]
        cur.execute(f"""
          WITH agg AS (
            SELECT "{lead}" AS lead_id,
                   MAX("{ts}") AS last_ts,
                   MAX(CASE WHEN {inc_expr} THEN "{ts}" END) AS last_in_ts,
                   MAX(CASE WHEN {out_expr} THEN "{ts}" END) AS last_out_ts
            FROM messages
            GROUP BY "{lead}"
          )
          SELECT a.lead_id, a.last_ts, a.last_in_ts, a.last_out_ts,
                 COALESCE((SELECT state FROM lead_state s WHERE s.lead_id=a.lead_id),'new') AS state
          FROM agg a
          ORDER BY a.last_ts DESC
          LIMIT 100
        """)
        rows = cur.fetchall()

    def fmt(t):
        if not t: return "-"
        return humanize.naturaltime(dt.datetime.utcnow() - t)

    leads_table = [{"lead_id": L, "state": S, "last": fmt(LT), "last_in": fmt(LI), "last_out": fmt(LO)}
                   for (L,LT,LI,LO,S) in rows]

    return templates.TemplateResponse("index.html", {
        "request": request,
        "title": "Avio Ops",
        "kpis": {**stats, "queue_depth": queue_depth, "dlq_depth": dlq_depth},
        "leads": leads_table
    })

@app.get("/lead/{lead_id}", response_class=HTMLResponse)
def lead_view(lead_id: int, request: Request, ok: bool = Depends(auth)):
    with psycopg.connect(DB_SYNC_DSN) as conn:
        cur = conn.cursor()
        meta = discover(cur)
        lead, ts, inc_expr = meta["lead"], meta["ts"], meta["inc_expr"]
        text_expr, idc = meta["text_expr"], meta["id"]

        cur.execute(f"""
            SELECT {idc}, "{ts}",
                   CASE WHEN {inc_expr} THEN 1 ELSE 0 END AS is_in,
                   {text_expr}
            FROM messages
            WHERE "{lead}"=%s
            ORDER BY "{ts}" DESC
            LIMIT 200
        """,(lead_id,))
        msgs = [{"id": mid, "ts": t.strftime("%Y-%m-%d %H:%M:%S"), "dir": "in" if is_in else "out", "text": txt}
                for (mid,t,is_in,txt) in cur.fetchall()]

        cur.execute("SELECT state, updated_at FROM lead_state WHERE lead_id=%s",(lead_id,))
        row = cur.fetchone()
        state = row[0] if row else "new"
        updated = row[1].strftime("%Y-%m-%d %H:%M:%S") if row else "-"

        cur.execute("SELECT tag, value, ts FROM lead_tags WHERE lead_id=%s ORDER BY id DESC LIMIT 100",(lead_id,))
        tags = [{"tag": t, "value": v or "", "ts": ts.strftime("%Y-%m-%d %H:%M:%S")} for (t,v,ts) in cur.fetchall()]

    return templates.TemplateResponse("lead.html", {"request": request, "lead_id": lead_id, "state": state, "updated": updated, "tags": tags, "msgs": msgs})

@app.get("/api/summary")
def api_summary(ok: bool = Depends(auth)):
    d0,d1 = today_bounds()
    with psycopg.connect(DB_SYNC_DSN) as conn:
        cur = conn.cursor()
        meta = discover(cur)
        stats = kpis(cur, meta, d0, d1)

    return JSONResponse({
        "leads": stats["leads"], "replied": stats["replied"], "reply_rate": float(stats["reply_rate"].rstrip('%')),
        "ab_trials": stats["ab_trials"], "ab_success": stats["ab_succ"],
        "queue_depth": int(r.llen("message_queue") or 0),
        "dlq_depth": int(r.llen("message_dlq") or 0)
    })
