#!/usr/bin/env python3
import csv
import html
import json
import os
import re
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, Any
import shutil

import requests
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, FileResponse

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
REPORTS_DIR = DATA_DIR / "reports"
SECRETS_ENV = Path(os.environ.get("MANGO_SECRETS_ENV", "/app/cibs.env"))

for p in (UPLOAD_DIR, REPORTS_DIR):
    p.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Mango Migrator UI")


def _keepalive_watchdog():
    while True:
        time.sleep(3)
        with STATE.lock:
            running = STATE.running
            cancel_requested = bool(STATE.current.get("cancel_requested"))
            last_keepalive = STATE.current.get("last_keepalive")
        if not running or cancel_requested:
            continue
        if not last_keepalive:
            continue
        if (time.time() - float(last_keepalive)) > KEEPALIVE_TIMEOUT_SEC:
            with STATE.lock:
                STATE.current["cancel_requested"] = True
            _append_log(f"Keepalive timeout (> {KEEPALIVE_TIMEOUT_SEC}s). Auto-stop requested.")


threading.Thread(target=_keepalive_watchdog, daemon=True).start()


class JobState:
    def __init__(self):
        self.lock = threading.Lock()
        self.running = False
        self.current: Dict[str, Any] = {}

    def reset(self):
        self.current = {
            "started_at": None,
            "finished_at": None,
            "stage": "idle",
            "percent": 0,
            "log": [],
            "summary": None,
            "error": None,
            "report_path": None,
            "cancel_requested": False,
            "last_keepalive": None,
        }


STATE = JobState()
STATE.reset()
KEEPALIVE_TIMEOUT_SEC = int(os.environ.get("MANGO_KEEPALIVE_TIMEOUT_SEC", "90"))


def _append_log(msg: str):
    ts = datetime.now(UTC).strftime("%H:%M:%S")
    with STATE.lock:
        STATE.current["log"].append(f"[{ts}] {msg}")
        STATE.current["log"] = STATE.current["log"][-1200:]


def _set_progress(stage: str, percent: int):
    with STATE.lock:
        STATE.current["stage"] = stage
        STATE.current["percent"] = max(0, min(100, percent))


def _cancel_requested() -> bool:
    with STATE.lock:
        return bool(STATE.current.get("cancel_requested"))


def _touch_keepalive():
    with STATE.lock:
        STATE.current["last_keepalive"] = time.time()


def _raise_if_cancelled():
    if _cancel_requested():
        raise RuntimeError("RUN_CANCELLED_BY_USER")


def _cleanup_uploads():
    # Smaže nahrané CSV po běhu (kvůli hygieně dat ve volume).
    for p in UPLOAD_DIR.glob("*"):
        try:
            if p.is_file() or p.is_symlink():
                p.unlink(missing_ok=True)
            elif p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
        except Exception:
            pass


def _load_env(path: Path) -> dict:
    if not path.exists():
        raise RuntimeError(
            f"Secrets file not found: {path}. Ověř mount secrets a že existuje cibs.env."
        )
    if path.is_dir():
        raise RuntimeError(
            f"Secrets path je adresář, ne soubor: {path}. "
            f"V Docker compose mountni celý ../../secrets do /run/secrets a použij /run/secrets/cibs.env."
        )
    env = {}
    for l in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        l = l.strip()
        if not l or l.startswith("#") or "=" not in l:
            continue
        k, v = l.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _soap_call(url: str, verify: bool, method: str, inner: str, timeout: int = 60):
    env = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="API_NAMESPACE">'
        f"<soapenv:Body><api:{method}>{inner}</api:{method}></soapenv:Body></soapenv:Envelope>"
    )
    r = requests.post(
        url,
        data=env.encode("utf-8"),
        headers={"Content-Type": "text/xml; charset=utf-8", "SOAPAction": ""},
        timeout=timeout,
        verify=verify,
    )
    return r.status_code, r.text


def _fix_mojibake(value: str) -> str:
    s = value or ""
    # typické rozbité UTF-8 dekódované jako latin-1/cp1250 (Ăˇ, Ĺ™, ...)
    if any(ch in s for ch in ("Ă", "Ĺ", "Ä", "Ě", "Â")):
        for enc in ("latin-1", "cp1250"):
            try:
                repaired = s.encode(enc).decode("utf-8")
                if repaired.count("�") <= s.count("�"):
                    s = repaired
                    break
            except Exception:
                pass
    return s


def _get(tag: str, text: str) -> str:
    m = re.search(fr"<{tag}[^>]*>(.*?)</{tag}>", text, re.S)
    return _fix_mojibake(m.group(1).strip()) if m else ""


def _esc(s: str) -> str:
    return html.escape((s or "").strip())


def _norm_phone(v: str) -> str:
    d = "".join(ch for ch in (v or "") if ch.isdigit())
    if len(d) == 9:
        d = "420" + d
    return d


def _split_street_house(street: str, byt: str):
    s = (street or "").strip()
    b = (byt or "").strip()
    if b:
        return s, b
    m = re.match(r"^(.*?)[ ,]+(\d+[A-Za-z0-9/\-]*)$", s)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return s, ""


def _sanitize_email(v: str) -> str:
    p = (v or "").strip().lower()
    if not p or " " in p or "@" not in p:
        return ""
    local, _, domain = p.partition("@")
    if "+" in local and domain in ("gmail.com", "googlemail.com"):
        local = local.split("+", 1)[0]
        p = f"{local}@{domain}"
    if re.match(r"^[a-z0-9._%\-]+@[a-z0-9.\-]+\.[a-z]{2,}$", p):
        return p
    return ""


def _first_email(row: dict) -> str:
    for k in ["Email", "Fakturační email", "Technický email", "Obchodní email"]:
        raw = (row.get(k) or "").strip().lower()
        if not raw:
            continue
        parts = [p.strip() for p in re.split(r"[;,]+", raw) if p.strip()]
        for p in parts:
            s = _sanitize_email(p)
            if s:
                return s
    return ""


def _phones_csv(row: dict) -> str:
    arr = []
    for k in ["Mobil", "Telefon", "Fax"]:
        p = _norm_phone(row.get(k, ""))
        if p and p not in arr:
            arr.append(p)
    return ",".join(arr)


def _run_job(deact_csv: Path, import_csv: Path):
    with STATE.lock:
        if STATE.running:
            return
        STATE.running = True
        STATE.reset()
        STATE.current["started_at"] = datetime.now(UTC).isoformat()
        STATE.current["stage"] = "starting"
        STATE.current["percent"] = 1
        STATE.current["last_keepalive"] = time.time()

    report = {
        "started_at": datetime.now(UTC).isoformat(),
        "deactivated": [],
        "deactivate_skipped": [],
        "deactivate_errors": [],
        "created": [],
        "import_skipped": [],
        "import_errors": [],
        "stopped_on_error": False,
    }

    session = None
    url = None
    verify = True

    try:
        env = _load_env(SECRETS_ENV)
        base = env["CIBS_BASE_URL_PROD"] if env.get("CIBS_ENV", "test") == "prod" else env["CIBS_BASE_URL_TEST"]
        url = base.rstrip("/") + "/ws_cibs.php"
        verify = env.get("CIBS_VERIFY_TLS", "true").lower() == "true"
        user, pwd = env["CIBS_USERNAME"], env["CIBS_PASSWORD"]
        ct = "252"

        _set_progress("login", 1)
        _append_log("Login to Mango SOAP")
        _raise_if_cancelled()
        _, lr = _soap_call(url, verify, "ws_session_login", f"<login><![CDATA[{user}]]></login><password><![CDATA[{pwd}]]></password>", 120)
        session = _get("session", lr)
        if not session:
            raise RuntimeError("Login failed")

        _soap_call(url, verify, "ws_session_set_ct", f"<session><![CDATA[{session}]]></session><ct>{ct}</ct>", 120)

        _set_progress("export_active", 3)
        _append_log("Loading active users export from Mango (this can take 1-3 minutes)...")
        _raise_if_cancelled()
        code, resp = _soap_call(url, verify, "ws_users_export", f"<session><![CDATA[{session}]]></session><services_state>ACTIVE</services_state>", 300)
        result = _get("result", resp)
        if code >= 400 or not result or (result.startswith("-") and result != ""):
            raise RuntimeError(f"ws_users_export failed http={code} result={result[:100]}")

        exported_xml = html.unescape(result)
        active_users = {}
        for m in re.finditer(r"<customer\s+([^>]+)/?>", exported_xml, re.S):
            attrs = dict(re.findall(r"(\w+)=\"(.*?)\"", m.group(1)))
            if (attrs.get("active_customer", "1").strip() or "1") != "1":
                continue
            login = (attrs.get("main_login") or "").strip()
            if not login:
                continue
            active_users[login] = {
                "id": attrs.get("user_id", "").strip(),
                "type": attrs.get("type", "S").strip() or "S",
                "first_name": attrs.get("first_name", "").strip() or "-",
                "last_name": attrs.get("last_name", "").strip() or "-",
                "company_name": attrs.get("company_name", "").strip(),
            }
        _append_log(f"Active users loaded: {len(active_users)}")

        with deact_csv.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            deact_rows = list(csv.DictReader(f, delimiter=";"))
        with import_csv.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            import_rows = list(csv.DictReader(f, delimiter=";"))

        total_steps = max(1, len(deact_rows) + len(import_rows))
        done = 0

        _set_progress("deactivation", 5)
        for i, row in enumerate(deact_rows, 1):
            _raise_if_cancelled()
            login = ((row.get("Klientské číslo") or row.get("Uživatelské jméno") or "").strip())
            if not login:
                report["deactivate_skipped"].append({"reason": "no_login", "isp_id": row.get("ID", "")})
            else:
                u = active_users.get(login)
                if not u:
                    report["deactivate_skipped"].append({"reason": "not_found_or_already_inactive", "main_login": login, "isp_id": row.get("ID", "")})
                else:
                    type_val = u["type"]
                    firstname = u["first_name"] if type_val == "S" else ""
                    surname = u["last_name"] if type_val == "S" else ""
                    company = u["company_name"] if type_val == "P" else ""
                    edit_xml = (
                        f"<user><id>{_esc(u['id'])}</id><active>0</active><type>{_esc(type_val)}</type>"
                        f"<firstname>{_esc(firstname)}</firstname><surname>{_esc(surname)}</surname><company>{_esc(company)}</company></user>"
                    )
                    code, resp = _soap_call(url, verify, "ws_user_edit", f"<session><![CDATA[{session}]]></session>{edit_xml}", 180)
                    fault = _get("faultstring", resp)
                    result = _get("result", resp) or _get("return", resp)
                    if fault or code >= 400 or (result.startswith("-") and result != ""):
                        report["deactivate_errors"].append({
                            "main_login": login,
                            "isp_id": row.get("ID", ""),
                            "mango_user_id": u["id"],
                            "http": code,
                            "fault": fault,
                            "result": result,
                        })
                    else:
                        report["deactivated"].append({"main_login": login, "isp_id": row.get("ID", ""), "mango_user_id": u["id"]})

            done += 1
            if i == 1 or i % 25 == 0:
                _append_log(f"deact {i}/{len(deact_rows)} | deactivated={len(report['deactivated'])} skipped={len(report['deactivate_skipped'])} errors={len(report['deactivate_errors'])}")
            _set_progress("deactivation", int(5 + (done / total_steps) * 45))

        _set_progress("import", 50)
        for i, row in enumerate(import_rows, 1):
            _raise_if_cancelled()
            login = ((row.get("Klientské číslo") or row.get("Uživatelské jméno") or "").strip())
            if not login:
                report["import_skipped"].append({"reason": "no_login", "isp_id": row.get("ID", "")})
            else:
                _, chk = _soap_call(url, verify, "ws_users_list", f"<session><![CDATA[{session}]]></session><search><login>{_esc(login)}</login></search>")
                exists = bool(re.search(fr"<login[^>]*>{re.escape(login)}</login>", chk))
                if exists:
                    report["import_skipped"].append({"reason": "already_exists", "main_login": login, "isp_id": row.get("ID", "")})
                else:
                    name = (row.get("Klient") or "").strip()
                    ico = (row.get("IČO") or "").strip()
                    dic = (row.get("DIČ") or "").strip()
                    is_company = bool(ico or dic)
                    type_val = "P" if is_company else "S"
                    if is_company:
                        company, firstname, surname = name, "", ""
                    else:
                        parts = name.split()
                        firstname = parts[0] if parts else "-"
                        surname = " ".join(parts[1:]) if len(parts) > 1 else "-"
                        company = ""

                    street, house = _split_street_house(row.get("Ulice", ""), row.get("Byt", ""))
                    city = (row.get("Město") or "").strip()
                    zipc = (row.get("PSČ") or "").strip()
                    mail = _first_email(row)
                    phones = _phones_csv(row)
                    status = (row.get("Stav") or "").strip().lower()
                    active = "1" if ("aktiv" in status or status == "") else "0"
                    varsym = login if login.isdigit() else ""

                    user_xml = (
                        "<user>"
                        f"<active>{active}</active><mail_info_flag>1</mail_info_flag><login>{_esc(login)}</login><type>{type_val}</type>"
                        f"<firstname>{_esc(firstname)}</firstname><surname>{_esc(surname)}</surname><company>{_esc(company)}</company>"
                        f"<address><street>{_esc(street)}</street><house_id>{_esc(house)}</house_id><city>{_esc(city)}</city><zip>{_esc(zipc)}</zip></address>"
                        "<delivery_address_as_default>0</delivery_address_as_default>"
                        f"<delivery_address><street>{_esc(street)}</street><house_id>{_esc(house)}</house_id><city>{_esc(city)}</city><zip>{_esc(zipc)}</zip></delivery_address>"
                        "<billing_address_as_default>0</billing_address_as_default><billing_address_as_delivery>1</billing_address_as_delivery>"
                        f"<billing_address><street>{_esc(street)}</street><house_id>{_esc(house)}</house_id><city>{_esc(city)}</city><zip>{_esc(zipc)}</zip></billing_address>"
                        f"<phones>{_esc(phones)}</phones><mail>{_esc(mail)}</mail><agreement>{_esc(login)}</agreement><varsym>{_esc(varsym)}</varsym><ico>{_esc(ico)}</ico><dic>{_esc(dic)}</dic>"
                        "</user>"
                    )
                    code, resp = _soap_call(url, verify, "ws_user_create", f"<session><![CDATA[{session}]]></session>{user_xml}", 200)
                    fault = _get("faultstring", resp)
                    result = _get("result", resp) or _get("return", resp)
                    if fault or code >= 400 or (result.startswith("-") and result != ""):
                        report["import_errors"].append({
                            "main_login": login,
                            "isp_id": row.get("ID", ""),
                            "http": code,
                            "fault": fault,
                            "result": result,
                            "mail_used": mail,
                        })
                    else:
                        report["created"].append({"main_login": login, "isp_id": row.get("ID", ""), "mango_user_id": result})

            done += 1
            if i == 1 or i % 25 == 0:
                _append_log(f"import {i}/{len(import_rows)} | created={len(report['created'])} skipped={len(report['import_skipped'])} errors={len(report['import_errors'])}")
            _set_progress("import", int(50 + (done / total_steps) * 50))

        if session:
            _soap_call(url, verify, "ws_session_logout", f"<session><![CDATA[{session}]]></session>", 120)
            session = None
        report["finished_at"] = datetime.now(UTC).isoformat()
        report["deactivated_count"] = len(report["deactivated"])
        report["deactivate_skipped_count"] = len(report["deactivate_skipped"])
        report["deactivate_errors_count"] = len(report["deactivate_errors"])
        report["created_count"] = len(report["created"])
        report["import_skipped_count"] = len(report["import_skipped"])
        report["import_errors_count"] = len(report["import_errors"])

        report_path = REPORTS_DIR / f"run-{int(time.time())}.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        with STATE.lock:
            STATE.current["summary"] = {
                "deactivated_count": report["deactivated_count"],
                "deactivate_skipped_count": report["deactivate_skipped_count"],
                "deactivate_errors_count": report["deactivate_errors_count"],
                "created_count": report["created_count"],
                "import_skipped_count": report["import_skipped_count"],
                "import_errors_count": report["import_errors_count"],
            }
            STATE.current["report_path"] = str(report_path)
            STATE.current["finished_at"] = datetime.now(UTC).isoformat()
            STATE.current["percent"] = 100
            STATE.current["stage"] = "done"
        _append_log(f"Done. Report: {report_path}")

    except Exception as e:
        cancelled = str(e) == "RUN_CANCELLED_BY_USER"
        with STATE.lock:
            STATE.current["error"] = None if cancelled else str(e)
            STATE.current["stage"] = "cancelled" if cancelled else "error"
            STATE.current["finished_at"] = datetime.now(UTC).isoformat()
        if cancelled:
            _append_log("Run cancelled by user.")
        else:
            _append_log(f"ERROR: {e}")
    finally:
        if session and url:
            try:
                _soap_call(url, verify, "ws_session_logout", f"<session><![CDATA[{session}]]></session>", 120)
            except Exception:
                pass
        _cleanup_uploads()
        _append_log("Uploads cleaned (data/uploads)")
        with STATE.lock:
            STATE.running = False
            STATE.current["cancel_requested"] = False


@app.get("/", response_class=HTMLResponse)
def index():
    return (BASE_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/sync-logo.svg")
def sync_logo():
    return FileResponse(BASE_DIR / "sync-logo.svg", media_type="image/svg+xml")


def _read_csv_headers(path: Path):
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        r = csv.DictReader(f, delimiter=";")
        return r.fieldnames or []


def _validate_uploaded_csv(path: Path, required_cols: list[str], label: str):
    headers = _read_csv_headers(path)
    missing = [c for c in required_cols if c not in headers]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"{label}: neplatný CSV formát (chybí sloupce: {', '.join(missing)}).",
        )


@app.post("/api/upload")
async def upload_files(deactivate_csv: UploadFile = File(...), import_csv: UploadFile = File(...)):
    if STATE.running:
        raise HTTPException(status_code=409, detail="Run is in progress")

    d_path = UPLOAD_DIR / "deaktivovani-klienti.csv"
    i_path = UPLOAD_DIR / "kontakty.csv"

    d_path.write_bytes(await deactivate_csv.read())
    i_path.write_bytes(await import_csv.read())

    _validate_uploaded_csv(d_path, ["ID", "Klientské číslo", "Uživatelské jméno"], "deaktivovani-klienti.csv")
    _validate_uploaded_csv(i_path, ["ID", "Klientské číslo", "Uživatelské jméno", "Klient", "Stav"], "kontakty.csv")

    _append_log("CSV files uploaded and validated")
    return {"ok": True, "deactivate_csv": str(d_path), "import_csv": str(i_path)}


@app.post("/api/start")
def start_job():
    if STATE.running:
        raise HTTPException(status_code=409, detail="Run already in progress")

    d_path = UPLOAD_DIR / "deaktivovani-klienti.csv"
    i_path = UPLOAD_DIR / "kontakty.csv"
    if not d_path.exists() or not i_path.exists():
        raise HTTPException(status_code=400, detail="Upload both CSV files first")

    # Ověření i při startu (pro případ ruční manipulace se soubory mezi upload/start)
    _validate_uploaded_csv(d_path, ["ID", "Klientské číslo", "Uživatelské jméno"], "deaktivovani-klienti.csv")
    _validate_uploaded_csv(i_path, ["ID", "Klientské číslo", "Uživatelské jméno", "Klient", "Stav"], "kontakty.csv")

    _append_log("Job starting...")
    t = threading.Thread(target=_run_job, args=(d_path, i_path), daemon=True)
    t.start()
    return {"ok": True}


@app.post("/api/stop")
def stop_job(confirm: str = ""):
    if str(confirm).strip().upper() != "STOP":
        raise HTTPException(status_code=400, detail="Confirmation required: confirm=STOP")
    with STATE.lock:
        if not STATE.running:
            raise HTTPException(status_code=409, detail="No running job")
        STATE.current["cancel_requested"] = True
    _append_log("Stop requested by user. Waiting for safe stop point...")
    return {"ok": True}


@app.post("/api/keepalive")
def keepalive():
    with STATE.lock:
        running = STATE.running
    if running:
        _touch_keepalive()
    return {"ok": True, "running": running}


@app.post("/api/disconnect")
def disconnect(stop: bool = False):
    with STATE.lock:
        running = STATE.running
        if running:
            _touch_keepalive()
            if stop:
                STATE.current["cancel_requested"] = True
    if running and stop:
        _append_log("Client disconnected/unloaded. Auto-stop requested.")
    return {"ok": True, "running": running}


@app.get("/api/reports")
def reports_list(limit: int = 50):
    files = sorted(REPORTS_DIR.glob("run-*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    out = []
    for p in files[: max(1, min(limit, 200))]:
        st = p.stat()
        out.append({
            "name": p.name,
            "path": str(p),
            "size": st.st_size,
            "mtime": st.st_mtime,
        })
    return {"reports": out}


@app.get("/api/reports/{name}")
def report_get(name: str):
    if "/" in name or ".." in name or not name.endswith(".json"):
        raise HTTPException(status_code=400, detail="Invalid report name")
    p = REPORTS_DIR / name
    if not p.exists():
        raise HTTPException(status_code=404, detail="Report not found")
    try:
        data = json.loads(p.read_text(encoding="utf-8", errors="replace"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cannot parse report: {e}")
    return {"name": name, "data": data}


@app.get("/api/reports/{name}/download")
def report_download(name: str):
    if "/" in name or ".." in name or not name.endswith(".json"):
        raise HTTPException(status_code=400, detail="Invalid report name")
    p = REPORTS_DIR / name
    if not p.exists():
        raise HTTPException(status_code=404, detail="Report not found")
    return FileResponse(p, media_type="application/json", filename=name)


@app.get("/api/status")
def status():
    with STATE.lock:
        return JSONResponse({"running": STATE.running, **STATE.current})


@app.get("/api/events")
def events():
    def gen():
        last_sig = None
        while True:
            with STATE.lock:
                payload = {"running": STATE.running, **STATE.current}
            s = json.dumps(payload, ensure_ascii=False)
            sig = (payload.get("running"), payload.get("stage"), payload.get("percent"), len(payload.get("log", [])), payload.get("error"), payload.get("report_path"), payload.get("cancel_requested"))
            if sig != last_sig:
                last_sig = sig
                yield f"data: {s}\n\n"
            else:
                # keepalive (ať UI ví, že stream žije)
                yield ": ping\n\n"
            time.sleep(1)

    return StreamingResponse(gen(), media_type="text/event-stream")
